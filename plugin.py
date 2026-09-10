"""
Poster Enricher — a Dispatcharr plugin.

Walks every EPG programme and injects poster artwork into each programme's
custom_properties so Dispatcharr's XMLTV output carries an <icon> per programme,
giving Plex (and any other client) a picture on every guide card.

Lookup chain (first hit wins):
    existing poster -> TMDB -> TVmaze -> TVDB -> Fanart.tv -> OMDB -> channel logo

Nothing is required. With zero API keys you still get TVmaze (no key) plus the
channel-logo fallback, so no card is ever blank.

Design notes
------------
* Heavy work runs in a background daemon thread so the run() action returns fast
  and the UI stays responsive. Progress is pushed over the websocket channel.
* Iteration uses PK-range pagination (not .iterator()) because we mutate and
  bulk_update rows mid-walk; a server-side cursor plus concurrent writes is the
  classic Postgres footgun.
* A cooperative cancel flag lets stop() actually halt the worker between batches.
* Artwork is written to ProgramData.custom_properties ("icon" + "poster"), the
  same place SD art lives and where the XMLTV writer reads programme icons.
* SD/provider artwork is preserved unless "Overwrite existing" is on.
"""

import hashlib
import os
import threading
import time
import traceback
import urllib.parse
import urllib.request

from . import providers  # local module

_STATE_DIR = os.path.join("/data", "poster_enricher")
# Composite poster images: small logos placed on a poster-sized dark canvas.
_POSTER_DIR = os.path.join("/opt", "dispatcharr", "media", "poster_enricher")
_POSTER_WIDTH = 680
_POSTER_HEIGHT = 1000
_POSTER_BG = (30, 30, 30)  # near-black background
_POSTER_LOGO_SCALE = 0.60  # logo occupies up to 60% of poster width
_POSTER_LOGO_MAX_H = 0.35  # logo occupies up to 35% of poster height
_CACHE_PATH = os.path.join(_STATE_DIR, "cache.sqlite3")
_STATS_PATH = os.path.join(_STATE_DIR, "last_stats.json")
_UNMATCHED_PATH = os.path.join(_STATE_DIR, "unmatched.txt")
_LOG_PATH = os.path.join(_STATE_DIR, "run.log")
_RUN_STATE_PATH = os.path.join(_STATE_DIR, "run_state.json")

# Run state. `cancel` is the cooperative stop flag the worker checks each batch.
_run_lock = threading.Lock()
_running = {"active": False, "started": 0, "total": 0, "done": 0, "cancel": False}

# Auto-run timer (fallback when event hook doesn't fire).
_timer = None
_timer_lock = threading.Lock()


def _write_run_state(active, done=0, total=0, started=0):
    """Write run state to disk so all uWSGI workers can read it."""
    import json
    try:
        os.makedirs(_STATE_DIR, exist_ok=True)
        with open(_RUN_STATE_PATH, "w") as f:
            json.dump({"active": active, "done": done, "total": total,
                       "started": started, "pid": os.getpid()}, f)
    except Exception:
        pass


def _read_run_state():
    """Read run state from disk. Returns dict or None."""
    import json
    try:
        with open(_RUN_STATE_PATH) as f:
            return json.load(f)
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Helpers touching Dispatcharr internals (imported lazily inside functions).
# ---------------------------------------------------------------------------

def _log(context, level, msg):
    logger = context.get("logger") if context else None
    if logger:
        getattr(logger, level, logger.info)(msg)
    # Also write to persistent log file (last 500 lines kept).
    _file_log(level, msg)


def _file_log(level, msg):
    """Append a timestamped line to the persistent run log."""
    try:
        os.makedirs(_STATE_DIR, exist_ok=True)
        ts = time.strftime("%Y-%m-%d %H:%M:%S")
        line = f"[{ts}] {level.upper()}: {msg}\n"
        with open(_LOG_PATH, "a") as f:
            f.write(line)
        # Trim to last 500 lines periodically.
        try:
            with open(_LOG_PATH, "r") as f:
                lines = f.readlines()
            if len(lines) > 600:
                with open(_LOG_PATH, "w") as f:
                    f.writelines(lines[-500:])
        except Exception:
            pass
    except Exception:
        pass


def _ws(message, extra=None):
    try:
        from core.utils import send_websocket_update
        payload = {"type": "plugin", "plugin": "poster_enricher", "message": message}
        if extra:
            payload.update(extra)
        send_websocket_update("updates", "update", payload)
    except Exception:
        pass


_MOVIE_WORDS = ("movie", "film", "cinema", "feature film")
_SKIP_WORDS = ("news", "sport", "weather", "shopping", "infomercial", "teleshopping")


def _categories(program):
    props = getattr(program, "custom_properties", None) or {}
    cats = props.get("categories") or props.get("category") or []
    if isinstance(cats, str):
        cats = [cats]
    return [str(c).lower() for c in cats]


def _guess_is_movie(program):
    """Movie if a category says so, OR if the title carries a (YYYY) year and no
    episode markers and no sub_title — a decent proxy when guides don't tag."""
    cats = _categories(program)
    if any(any(w in c for w in _MOVIE_WORDS) for c in cats):
        return True
    # Heuristic fallback: a year in the title + no episode subtitle looks filmy.
    title = getattr(program, "title", "") or ""
    if providers.extract_year(title):
        sub = getattr(program, "sub_title", "") or ""
        if not sub.strip():
            return True
    return False


def _is_skippable_category(program, cfg):
    if not cfg.get("use_categories", True):
        return False
    return any(any(w in c for w in _SKIP_WORDS) for c in _categories(program))


# Attribute names that can hold a URL on a Logo object or a Channel. Order is
# the default preference: the original provider "url" first, then Dispatcharr's
# own "cache_url". For a *fallback* logo the provider URL is usually safer — it's
# a direct link with no dependency on Dispatcharr's proxy, and it avoids the
# known bug where cache_url is built with the internal :9191 port behind a
# reverse proxy (which would hand clients a broken link). The "logo_url_pref"
# setting can flip this to prefer cache_url.
_URL_ATTR_DEFAULT = ("url", "cache_url", "logo_url", "cached_url", "path", "src")
_URL_ATTR_CACHE_FIRST = ("cache_url", "cached_url", "url", "logo_url", "path", "src")


def _channel_logo_for(program, resolver_cache, candidates=_URL_ATTR_DEFAULT):
    """Best-effort channel logo. Schema varies by Dispatcharr version, so this
    tries several relations defensively and memoizes per EPGData id so we don't
    re-query the same channel thousands of times.

    Returns a URL string or None."""
    try:
        epg = getattr(program, "epg", None)
        if epg is None:
            return None
        epg_id = getattr(epg, "id", None)
        if epg_id in resolver_cache:
            return resolver_cache[epg_id]

        url = None
        # 1) EPGData.icon_url (present in most versions) — cheap, no join.
        icon = getattr(epg, "icon_url", None)
        if icon:
            url = icon

        # 2) A Channel linked to this EPGData. Instead of guessing field names,
        #    we discover the schema once (see _discover_channel_schema) and use
        #    whatever this Dispatcharr version actually calls things.
        if not url:
            schema = _channel_schema()
            if schema and schema.get("Channel") and schema.get("epg_rel"):
                try:
                    Channel = schema["Channel"]
                    ch = Channel.objects.filter(**{schema["epg_rel"]: epg}).first()
                    if ch is not None:
                        url = _extract_logo_url(ch, schema, candidates)
                except Exception:
                    pass

        resolver_cache[epg_id] = url
        return url
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Composite poster generation: logo centered on a poster-sized dark canvas.
# ---------------------------------------------------------------------------

def _make_composite_poster(logo_url, base_url, context=None):
    """Download *logo_url*, composite it onto a poster-sized canvas, save to
    the media directory, and return a URL the media server can fetch.

    Returns a URL string or None on any failure.
    """
    try:
        from PIL import Image
        from io import BytesIO
    except ImportError:
        _log(context, "warning",
             "Poster Enricher: Pillow not installed — pip install Pillow")
        return None

    # Deterministic filename from the logo URL so we don't regenerate.
    url_hash = hashlib.md5(logo_url.encode()).hexdigest()
    filename = f"poster_{url_hash}.png"
    filepath = os.path.join(_POSTER_DIR, filename)

    # Already generated? Just return the URL.
    if os.path.exists(filepath):
        return f"{base_url}/media/poster_enricher/{filename}"

    try:
        # Download the logo. URL-encode spaces and special characters
        # (many picon URLs have spaces: "DOG TV.png", "FREE SPEECH TV.png").
        from urllib.parse import urlsplit, urlunsplit, quote
        parts = urlsplit(logo_url)
        safe_url = urlunsplit((parts.scheme, parts.netloc,
                               quote(parts.path, safe="/"),
                               quote(parts.query, safe="&="),
                               parts.fragment))
        req = urllib.request.Request(safe_url, headers={"User-Agent": "PosterEnricher/1.0"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            logo_data = resp.read()

        if not logo_data:
            _file_log("warning", f"Composite: empty response from {logo_url}")
            return None

        # Detect SVG and convert to PNG before Pillow processes it.
        # SVG files can't be opened by Pillow directly.
        is_svg = (logo_url.lower().endswith(".svg")
                  or logo_data[:200].lstrip().startswith((b"<svg", b"<?xml")))
        if is_svg:
            try:
                import cairosvg
                png_data = cairosvg.svg2png(bytestring=logo_data,
                                            output_width=int(_POSTER_WIDTH * _POSTER_LOGO_SCALE),
                                            output_height=int(_POSTER_HEIGHT * _POSTER_LOGO_MAX_H))
                logo_data = png_data
                _file_log("info", f"Composite: converted SVG to PNG for {logo_url}")
            except ImportError:
                _file_log("warning",
                          f"Composite: SVG logo needs cairosvg — "
                          f"pip install cairosvg (+ apt install libcairo2-dev). "
                          f"Skipping {logo_url}")
                return None
            except Exception as exc:
                _file_log("warning", f"Composite: SVG conversion failed for {logo_url}: {exc}")
                return None

        logo_img = Image.open(BytesIO(logo_data)).convert("RGBA")

        # Scale the logo to fill the target area. Unlike thumbnail() which only
        # shrinks, we explicitly scale UP small logos so they're visible on the
        # poster canvas — most channel logos are tiny (200x80) and need enlarging.
        max_logo_w = int(_POSTER_WIDTH * _POSTER_LOGO_SCALE)
        max_logo_h = int(_POSTER_HEIGHT * _POSTER_LOGO_MAX_H)
        orig_w, orig_h = logo_img.size
        scale = min(max_logo_w / max(orig_w, 1), max_logo_h / max(orig_h, 1))
        new_w = max(int(orig_w * scale), 1)
        new_h = max(int(orig_h * scale), 1)
        logo_img = logo_img.resize((new_w, new_h), Image.LANCZOS)

        # Create the poster canvas.
        canvas = Image.new("RGB", (_POSTER_WIDTH, _POSTER_HEIGHT), _POSTER_BG)

        # Center the logo vertically at about 40% from top (more natural than dead center).
        x = (_POSTER_WIDTH - logo_img.width) // 2
        y = int(_POSTER_HEIGHT * 0.40) - (logo_img.height // 2)
        canvas.paste(logo_img, (x, y), logo_img)  # use alpha as mask

        os.makedirs(_POSTER_DIR, exist_ok=True)
        canvas.save(filepath, "PNG", optimize=True)
        _file_log("info", f"Composite: created {filename} from {logo_url} "
                  f"(logo {logo_img.width}x{logo_img.height})")
        return f"{base_url}/media/poster_enricher/{filename}"

    except Exception as exc:
        _file_log("warning", f"Composite failed for {logo_url}: {exc}")
        return None


def _get_base_url():
    """Best-effort Dispatcharr base URL for serving composites."""
    try:
        from django.conf import settings
        # Dispatcharr stores its own URL in various places; fall back to localhost.
        base = getattr(settings, "BASE_URL", None) or getattr(settings, "SITE_URL", None)
        if base:
            return base.rstrip("/")
    except Exception:
        pass
    # Fallback: read from environment or use localhost.
    return os.environ.get("DISPATCHARR_URL", "http://127.0.0.1:9191").rstrip("/")


def _wrap_plex_proxy(direct_url, plex_url, plex_token):
    """Wrap an image URL through Plex's /photo/:/transcode endpoint.

    This makes the Plex server fetch the image (from Dispatcharr on the local
    network) and serve it to the client through Plex's own relay. That way all
    clients — including mobile apps and remote connections — can load the image
    without needing direct access to Dispatcharr.

    Returns the proxied URL, or the original URL if Plex settings are missing.
    """
    if not plex_url or not plex_token:
        return direct_url
    from urllib.parse import quote
    return (f"{plex_url}/photo/:/transcode"
            f"?url={quote(direct_url, safe='')}"
            f"&width={_POSTER_WIDTH}&height={_POSTER_HEIGHT}"
            f"&X-Plex-Token={plex_token}")


# ---------------------------------------------------------------------------
# ImgBB CDN upload: makes composites accessible from anywhere.
# ---------------------------------------------------------------------------

_IMGBB_CACHE_PATH = os.path.join(_STATE_DIR, "imgbb_urls.json")
_imgbb_url_cache = {}  # in-memory: hash -> CDN URL
_imgbb_cache_loaded = False


def _load_imgbb_cache():
    """Load the imgbb URL cache from disk (once per run)."""
    global _imgbb_url_cache, _imgbb_cache_loaded
    if _imgbb_cache_loaded:
        return
    import json
    try:
        with open(_IMGBB_CACHE_PATH) as f:
            _imgbb_url_cache = json.load(f)
    except Exception:
        _imgbb_url_cache = {}
    _imgbb_cache_loaded = True


def _save_imgbb_cache():
    """Persist the imgbb URL cache to disk."""
    import json
    try:
        os.makedirs(_STATE_DIR, exist_ok=True)
        with open(_IMGBB_CACHE_PATH, "w") as f:
            json.dump(_imgbb_url_cache, f)
    except Exception:
        pass


def _upload_to_imgbb(filepath, url_hash, api_key):
    """Upload a composite image to ImgBB and return the CDN URL.

    Uses a persistent cache so images are only uploaded once. Returns the CDN
    URL on success, or None on failure.
    """
    _load_imgbb_cache()

    # Already uploaded? Return the cached CDN URL.
    if url_hash in _imgbb_url_cache:
        return _imgbb_url_cache[url_hash]

    try:
        import base64
        import json

        with open(filepath, "rb") as f:
            img_data = base64.b64encode(f.read()).decode("utf-8")

        # ImgBB API: POST with base64 image data.
        post_data = urllib.parse.urlencode({
            "key": api_key,
            "image": img_data,
            "name": f"poster_{url_hash}",
        }).encode("utf-8")

        req = urllib.request.Request(
            "https://api.imgbb.com/1/upload",
            data=post_data,
            method="POST",
            headers={"User-Agent": "PosterEnricher/1.0"},
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            result = json.loads(resp.read())

        if result.get("success"):
            cdn_url = result["data"]["url"]
            _imgbb_url_cache[url_hash] = cdn_url
            # Save cache every 50 uploads to avoid losing progress.
            if len(_imgbb_url_cache) % 50 == 0:
                _save_imgbb_cache()
            return cdn_url
        else:
            _file_log("warning", f"ImgBB upload failed for {url_hash}: {result}")
            return None

    except Exception as exc:
        _file_log("warning", f"ImgBB upload error for {url_hash}: {exc}")
        return None


# ---------------------------------------------------------------------------
# Runtime schema discovery for the Channel -> logo relationship.
# Dispatcharr's field names vary by version, so rather than hardcode guesses we
# ask the models themselves (Django's Model._meta) exactly once and cache the
# answer. This means the plugin adapts to whatever schema is actually installed
# without anyone needing to read the source.
# ---------------------------------------------------------------------------

_schema_cache = {"done": False, "data": None}
_schema_lock = threading.Lock()

# URL-ish attribute names we'll read off a logo object or a channel.
# substrings that identify a logo-bearing field on Channel
_LOGO_FIELD_HINTS = ("logo", "icon", "image")
# substrings that identify the EPGData relation on Channel
_EPG_REL_HINTS = ("epg_data", "epgdata", "epg")


def _channel_schema(context=None):
    """Discover, once, how to get from an EPGData to a channel logo URL on this
    install. Returns a dict:
        {"Channel": <model>, "epg_rel": <filter kwarg>,
         "logo_field": <field name or None>, "logo_is_relation": bool}
    or None if no Channel model / relation could be found."""
    if _schema_cache["done"]:
        return _schema_cache["data"]
    with _schema_lock:
        if _schema_cache["done"]:
            return _schema_cache["data"]
        data = _do_discover_channel_schema(context)
        _schema_cache["data"] = data
        _schema_cache["done"] = True
        if context is not None:
            _log(context, "info", f"Poster Enricher: channel-logo schema = {_describe(data)}")
        return data


def _describe(data):
    if not data:
        return "none found"
    return (f"Channel.{data.get('epg_rel')} -> "
            f"logo field '{data.get('logo_field')}' "
            f"(relation={data.get('logo_is_relation')})")


def _do_discover_channel_schema(context):
    try:
        from apps.channels.models import Channel
    except Exception:
        return None

    try:
        fields = list(Channel._meta.get_fields())
    except Exception:
        return None

    field_names = []
    for f in fields:
        name = getattr(f, "name", None)
        if name:
            field_names.append((name, f))

    # 1) Find the relation from Channel to EPGData.
    epg_rel = None
    # Prefer an actual FK whose target model is named EPGData.
    for name, f in field_names:
        rel_model = getattr(getattr(f, "remote_field", None), "model", None)
        if rel_model is not None and getattr(rel_model, "__name__", "") == "EPGData":
            epg_rel = name
            break
    # Otherwise fall back to a name-hint match.
    if epg_rel is None:
        for hint in _EPG_REL_HINTS:
            for name, _ in field_names:
                if name == hint or name.startswith(hint):
                    epg_rel = name
                    break
            if epg_rel:
                break

    # 2) Find the logo-bearing field on Channel.
    logo_field = None
    logo_is_relation = False
    for hint in _LOGO_FIELD_HINTS:
        for name, f in field_names:
            if hint in name.lower():
                logo_field = name
                logo_is_relation = getattr(f, "is_relation", False) or (
                    getattr(f, "remote_field", None) is not None
                )
                break
        if logo_field:
            break

    if not epg_rel and not logo_field:
        return None

    return {
        "Channel": Channel,
        "epg_rel": epg_rel,
        "logo_field": logo_field,
        "logo_is_relation": logo_is_relation,
    }


def _extract_logo_url(channel, schema, candidates=_URL_ATTR_DEFAULT):
    """Pull a URL string off a channel using the discovered logo field."""
    logo_field = schema.get("logo_field")
    if not logo_field:
        # No known logo field — try common URL attrs straight on the channel.
        return _first_url_attr(channel, candidates)

    val = getattr(channel, logo_field, None)
    if val is None:
        return _first_url_attr(channel, candidates)

    # If it's already a string URL, use it.
    if isinstance(val, str):
        return val or None

    # If it's a related object (e.g. a Logo model), read a URL attr off it.
    got = _first_url_attr(val, candidates)
    if got:
        return got

    # Some logo objects need str() (e.g. FieldFile). Try that last.
    try:
        s = str(val)
        if s and ("/" in s or s.startswith("http")):
            return s
    except Exception:
        pass
    return None


def _first_url_attr(obj, candidates=_URL_ATTR_DEFAULT):
    for attr in candidates:
        try:
            v = getattr(obj, attr, None)
        except Exception:
            v = None
        if isinstance(v, str) and v:
            return v
        # FieldFile-like: has a .url property that may raise if no file
        if v is not None and not isinstance(v, str):
            try:
                u = getattr(v, "url", None)
                if isinstance(u, str) and u:
                    return u
            except Exception:
                pass
    return None


# ---------------------------------------------------------------------------
# Worker
# ---------------------------------------------------------------------------

def _enrich_worker(cfg, context):
    import json
    from django.db import transaction
    from apps.epg.models import ProgramData

    run_start = time.time()
    _file_log("info", "=" * 60)
    _file_log("info", "Enrichment run starting")
    _file_log("info", f"Config: chain={cfg.get('chain', [])}, "
              f"overwrite={cfg.get('overwrite')}, "
              f"scope={cfg.get('scope_source_ids') or 'all'}")

    cache = providers.LookupCache(_CACHE_PATH, ttl_days=int(cfg.get("cache_days", 14)))
    stats = {"existing": 0, "channel_logo": 0, "unmatched": 0, "transient": 0,
             "composites_created": 0, "composites_failed": 0}
    unmatched = []
    logo_memo = {}

    # Re-discover the channel-logo schema each run (adapts if Dispatcharr was
    # upgraded since last time), and log what was found so it's visible.
    _schema_cache["done"] = False
    _channel_schema(context)

    scope_ids = cfg.get("scope_source_ids") or []
    base = ProgramData.objects.all().select_related("epg")
    if scope_ids:
        base = base.filter(epg__epg_source_id__in=scope_ids)

    total = base.count()
    _running["total"] = total
    _running["done"] = 0
    _write_run_state(True, done=0, total=total, started=int(run_start))
    _ws(f"Poster enrichment started — {total} programmes", {"total": total, "done": 0})
    _log(context, "info", f"Poster Enricher: {total} programmes in scope")

    overwrite = bool(cfg.get("overwrite", False))
    logo_candidates = (
        _URL_ATTR_CACHE_FIRST if cfg.get("logo_url_pref") == "cache"
        else _URL_ATTR_DEFAULT
    )
    base_url = cfg.get("base_url") or _get_base_url()
    plex_url = cfg.get("plex_url", "")
    plex_token = cfg.get("plex_token", "")
    imgbb_key = cfg.get("imgbb_key", "")
    if imgbb_key:
        _log(context, "info",
             "Poster Enricher: composites will be uploaded to ImgBB CDN")
    elif plex_url and plex_token:
        _log(context, "info",
             f"Poster Enricher: composites will be proxied through Plex at {plex_url}")
    _log(context, "info", f"Poster Enricher: composite base URL = {base_url}")
    BATCH = 500
    processed = 0
    last_pk = 0
    cancelled = False

    while True:
        if _running.get("cancel"):
            cancelled = True
            break

        # PK-range pagination: stable under concurrent writes, no server cursor.
        page = list(
            base.filter(pk__gt=last_pk).order_by("pk")[:BATCH]
        )
        if not page:
            break

        to_update = []
        for program in page:
            last_pk = program.pk
            processed += 1

            props = getattr(program, "custom_properties", None)
            if props is None:
                props = {}
            elif isinstance(props, str):
                try:
                    props = json.loads(props)
                except Exception:
                    props = {}

            # Tier 0: existing artwork
            if not overwrite and (props.get("poster") or props.get("icon")):
                stats["existing"] += 1
                continue

            url, source = (None, None)
            if _is_skippable_category(program, cfg):
                source = "channel_logo"  # go straight to logo below
            else:
                url, source = providers.resolve_poster(
                    program.title, _guess_is_movie(program), cfg, cache, stats
                )

            if not url:
                logo = _channel_logo_for(program, logo_memo, logo_candidates)
                if logo:
                    # Generate a composite poster (logo on dark canvas) so it
                    # doesn't get stretched in the guide grid. Falls back to
                    # the raw logo URL if Pillow isn't available or download fails.
                    composite = _make_composite_poster(logo, base_url, context)
                    if composite:
                        # Priority: ImgBB CDN > Plex proxy > direct URL.
                        url_hash = hashlib.md5(logo.encode()).hexdigest()
                        filepath = os.path.join(_POSTER_DIR, f"poster_{url_hash}.png")
                        if imgbb_key:
                            cdn_url = _upload_to_imgbb(filepath, url_hash, imgbb_key)
                            url = cdn_url or composite
                        else:
                            url = _wrap_plex_proxy(composite, plex_url, plex_token)
                        stats["composites_created"] += 1
                    else:
                        url = logo
                        stats["composites_failed"] += 1
                    source = "channel_logo"
                    stats["channel_logo"] += 1
                else:
                    stats["unmatched"] += 1
                    if len(unmatched) < 2000:
                        unmatched.append(program.title or "(untitled)")

            if url:
                props["poster"] = url
                props["icon"] = url
                props["poster_source"] = source
                program.custom_properties = props
                to_update.append(program)

        if to_update:
            with transaction.atomic():
                ProgramData.objects.bulk_update(to_update, ["custom_properties"])

        _running["done"] = processed
        _write_run_state(True, done=processed, total=total, started=int(run_start))
        _ws(f"Enriching… {processed}/{total}", {"total": total, "done": processed})

    # Persist run artifacts.
    duration_secs = int(time.time() - run_start)
    try:
        os.makedirs(_STATE_DIR, exist_ok=True)
        with open(_STATS_PATH, "w") as f:
            json.dump(
                {"stats": stats, "total": total, "at": int(time.time()),
                 "duration_secs": duration_secs, "cancelled": cancelled},
                f,
            )
        with open(_UNMATCHED_PATH, "w") as f:
            f.write("\n".join(unmatched))
    except Exception:
        _log(context, "warning", "Poster Enricher: failed to write run artifacts")

    verb = "cancelled" if cancelled else "finished"
    _write_run_state(False)
    # Persist imgbb URL cache if we uploaded anything.
    if imgbb_key:
        _save_imgbb_cache()
    _file_log("info", f"Run {verb} in {duration_secs // 60}m{duration_secs % 60}s: "
              f"{processed}/{total} processed — {stats}")
    _ws(
        f"Poster enrichment {verb} — {processed}/{total} processed",
        {"total": total, "done": processed, "stats": stats},
    )
    _log(context, "info", f"Poster Enricher {verb}: {stats}")


def _start_enrichment(cfg, context):
    with _run_lock:
        if _running["active"]:
            return {"status": "busy", "message": "Enrichment already running."}
        _running["active"] = True
        _running["cancel"] = False
        _running["started"] = int(time.time())

    def runner():
        try:
            _enrich_worker(cfg, context)
        except Exception:
            _log(context, "error", "Poster Enricher crashed:\n" + traceback.format_exc())
            _ws("Poster enrichment failed — see logs")
        finally:
            _running["active"] = False
            _running["cancel"] = False

    t = threading.Thread(target=runner, name="poster-enricher", daemon=True)
    t.start()
    return {"status": "queued", "message": "Enrichment started in the background."}


def _arm_timer(interval_hours, cfg, context):
    """(Re)start the auto-run timer. Called after each run completes."""
    global _timer
    if not interval_hours or interval_hours <= 0:
        return
    interval_secs = interval_hours * 3600
    with _timer_lock:
        if _timer is not None:
            _timer.cancel()

        def tick():
            _file_log("info", f"Auto-run timer fired (every {interval_hours}h)")
            _start_enrichment(cfg, context)

        _timer = threading.Timer(interval_secs, tick)
        _timer.daemon = True
        _timer.start()
    _file_log("info", f"Auto-run timer armed: next run in {interval_hours}h")


def _stop_timer():
    global _timer
    with _timer_lock:
        if _timer is not None:
            _timer.cancel()
            _timer = None


# ---------------------------------------------------------------------------
# Settings resolution
# ---------------------------------------------------------------------------

def _resolve_cfg(settings):
    s = settings or {}

    def as_bool(v, default=True):
        if isinstance(v, bool):
            return v
        if v is None:
            return default
        return str(v).strip().lower() in ("1", "true", "yes", "on")

    order_raw = s.get("chain_order", ",".join(providers.DEFAULT_CHAIN)) or ""
    chain = [p.strip() for p in order_raw.split(",")
             if p.strip() in providers.PROVIDER_FUNCS]
    if not chain:
        chain = list(providers.DEFAULT_CHAIN)

    scope_ids = []
    for part in str(s.get("scope_source_ids", "") or "").replace(" ", "").split(","):
        if part.isdigit():
            scope_ids.append(int(part))

    try:
        cache_days = int(s.get("cache_days", 14) or 14)
    except (TypeError, ValueError):
        cache_days = 14

    return {
        "tmdb_key": s.get("tmdb_key", ""),
        "tvdb_key": s.get("tvdb_key", ""),
        "fanart_key": s.get("fanart_key", ""),
        "omdb_key": s.get("omdb_key", ""),
        "tmdb_enabled": as_bool(s.get("tmdb_enabled"), True),
        "tvmaze_enabled": as_bool(s.get("tvmaze_enabled"), True),
        "tvdb_enabled": as_bool(s.get("tvdb_enabled"), True),
        "fanart_enabled": as_bool(s.get("fanart_enabled"), True),
        "omdb_enabled": as_bool(s.get("omdb_enabled"), True),
        "chain": chain,
        "use_categories": as_bool(s.get("use_categories"), True),
        "language": s.get("language", "en") or "en",
        "poster_size": s.get("poster_size", "w500") or "w500",
        "cache_days": cache_days,
        "scope_source_ids": scope_ids,
        "overwrite": as_bool(s.get("overwrite"), False),
        "logo_url_pref": s.get("logo_url_pref", "provider") or "provider",
        "base_url": (s.get("base_url") or "").strip().rstrip("/"),
        "plex_url": (s.get("plex_url") or "").strip().rstrip("/"),
        "plex_token": (s.get("plex_token") or "").strip(),
        "imgbb_key": (s.get("imgbb_key") or "").strip(),
        "auto_interval": max(0, float(s.get("auto_interval", 0) or 0)),
    }


# ---------------------------------------------------------------------------
# Plugin
# ---------------------------------------------------------------------------

class Plugin:
    name = "Poster Enricher"
    version = "1.0.0"
    description = (
        "Automatically adds poster artwork to every EPG programme so your "
        "media server's guide never shows blank cards. Uses a 6-tier lookup "
        "chain (TMDB → TVmaze → TVDB → Fanart.tv → OMDB → channel logo "
        "composite). Zero API keys still gives TVmaze + channel logos."
    )
    author = "ckegels"
    help_url = "https://github.com/ckegels/Dispatcharr-Plex-Poster-Enricher"

    fields = [
        {"id": "sec_keys", "label": "API Keys (all optional)", "type": "info",
         "description": "None are required. Add whichever you have — more keys, "
                        "better coverage. TVmaze needs no key."},
        {"id": "tmdb_key", "label": "TMDB API Key", "type": "string",
         "input_type": "password", "default": "",
         "help_text": "themoviedb.org → Settings → API (v3 key). Best coverage."},
        {"id": "tvdb_key", "label": "TVDB API Key", "type": "string",
         "input_type": "password", "default": ""},
        {"id": "fanart_key", "label": "Fanart.tv API Key", "type": "string",
         "input_type": "password", "default": "",
         "help_text": "Only used when a TMDB key is also set (needs an ID to look up)."},
        {"id": "omdb_key", "label": "OMDB API Key", "type": "string",
         "input_type": "password", "default": ""},

        {"id": "sec_chain", "label": "Lookup Chain", "type": "info",
         "description": "Toggle sources on/off and set their priority order."},
        {"id": "tmdb_enabled", "label": "Use TMDB", "type": "boolean", "default": True},
        {"id": "tvmaze_enabled", "label": "Use TVmaze (no key)", "type": "boolean", "default": True},
        {"id": "tvdb_enabled", "label": "Use TVDB", "type": "boolean", "default": True},
        {"id": "fanart_enabled", "label": "Use Fanart.tv", "type": "boolean", "default": True},
        {"id": "omdb_enabled", "label": "Use OMDB", "type": "boolean", "default": True},
        {"id": "chain_order", "label": "Priority order", "type": "string",
         "default": ",".join(providers.DEFAULT_CHAIN),
         "help_text": "Comma-separated, first hit wins. Ids: tmdb, tvmaze, tvdb, fanart, omdb."},

        {"id": "sec_match", "label": "Smart Matching", "type": "info"},
        {"id": "use_categories", "label": "Skip news/sport via category tags",
         "type": "boolean", "default": True,
         "help_text": "News, sports, weather etc. go straight to the channel logo."},
        {"id": "language", "label": "Poster language", "type": "select", "default": "en",
         "options": [
             {"value": "en", "label": "English"},
             {"value": "de", "label": "German"},
             {"value": "fr", "label": "French"},
             {"value": "es", "label": "Spanish"},
             {"value": "nl", "label": "Dutch"},
             {"value": "it", "label": "Italian"},
         ]},
        {"id": "poster_size", "label": "TMDB poster size", "type": "select", "default": "w500",
         "options": [
             {"value": "w342", "label": "Small (w342)"},
             {"value": "w500", "label": "Medium (w500)"},
             {"value": "w780", "label": "Large (w780)"},
             {"value": "original", "label": "Original"},
         ]},
        {"id": "cache_days", "label": "Cache duration (days)", "type": "number", "default": 14},

        {"id": "sec_scope", "label": "Scope", "type": "info"},
        {"id": "scope_source_ids", "label": "EPG source IDs to enrich", "type": "string",
         "default": "",
         "help_text": "Comma-separated EPGSource IDs. Blank = all. Tip: leave out "
                      "your Schedules Direct source — it already has art."},
        {"id": "overwrite", "label": "Overwrite existing posters", "type": "boolean",
         "default": False,
         "help_text": "Off preserves Schedules Direct / provider artwork."},
        {"id": "logo_url_pref", "label": "Channel-logo URL source", "type": "select",
         "default": "provider",
         "options": [
             {"value": "provider", "label": "Original provider URL (recommended)"},
             {"value": "cache", "label": "Dispatcharr cache URL"},
         ],
         "help_text": "For the logo fallback. Provider URLs are direct CDN links. "
                      "Dispatcharr cache URLs go through Dispatcharr's proxy, which "
                      "can have issues behind a reverse proxy."},
        {"id": "base_url", "label": "Dispatcharr base URL (for poster composites)",
         "type": "text", "default": "",
         "help_text": "Auto-detected if blank. Only set this if composites aren't loading "
                      "(e.g. http://your-server-ip:9191). This is the internal URL that "
                      "Dispatcharr serves composite images from."},
        {"id": "plex_url", "label": "Plex server URL (optional)",
         "type": "text", "default": "",
         "help_text": "Your Plex server's external address — either a domain "
                      "(e.g. https://plex.example.com) or your port-forwarded IP "
                      "(e.g. http://203.0.113.50:32400). Composites are routed "
                      "through Plex so all clients can load them. "
                      "Not needed if you use ImgBB below."},
        {"id": "plex_token", "label": "Plex token",
         "type": "string", "input_type": "password", "default": "",
         "help_text": "Required when Plex URL is set. Find it in Plex's "
                      "Preferences.xml (PlexOnlineToken value) or at the end of "
                      "any Plex URL after X-Plex-Token=."},
        {"id": "imgbb_key", "label": "ImgBB API key (for remote poster access)",
         "type": "string", "input_type": "password", "default": "",
         "help_text": "Free key from api.imgbb.com (sign up, click Get API Key). "
                      "When set, composite channel-logo posters are uploaded to "
                      "ImgBB's free CDN so they work on ALL clients — mobile, remote, "
                      "everywhere — no domain or port forwarding needed. "
                      "Takes priority over the Plex proxy option above. "
                      "Leave blank if you use the Plex URL option or only need "
                      "local access."},
        {"id": "auto_interval", "label": "Auto-run interval (hours)",
         "type": "number", "default": 0,
         "help_text": "Run enrichment every N hours via a background timer. "
                      "0 = disabled (rely on EPG refresh event only). "
                      "Recommended: 6. Starts on first Enrich Now click; "
                      "timer resets on each run. Survives as long as "
                      "Dispatcharr is running."},
    ]

    actions = [
        {"id": "enrich_now", "label": "Enrich EPG Now",
         "description": "Queues a background job that injects posters into every programme.",
         "button_label": "Enrich Now", "button_variant": "filled", "button_color": "blue"},
        {"id": "auto_enrich", "label": "Auto-enrich after EPG refresh",
         "description": "Triggered automatically when an EPG source finishes refreshing.",
         "button_label": "Auto Enrich", "button_variant": "outline", "button_color": "blue",
         "events": ["epg_refresh", "epg_data_refresh", "data_refresh",
                     "epg_update", "epg_refreshed", "refresh_epg"]},
        {"id": "cancel_run", "label": "Cancel Run",
         "description": "Stops an in-progress enrichment between batches.",
         "button_label": "Cancel", "button_variant": "outline", "button_color": "orange"},
        {"id": "view_stats", "label": "View Stats",
         "description": "Per-source hit rates from the last run.",
         "button_label": "View Stats", "button_variant": "outline", "button_color": "blue"},
        {"id": "view_unmatched", "label": "View Unmatched",
         "description": "Titles that fell through to the channel-logo fallback.",
         "button_label": "View Unmatched", "button_variant": "outline", "button_color": "orange"},
        {"id": "view_logs", "label": "View Logs",
         "description": "Show the last 30 log entries from the persistent run log.",
         "button_label": "View Logs", "button_variant": "outline", "button_color": "blue"},
        {"id": "clear_cache", "label": "Clear Cache",
         "description": "Wipe the lookup cache; next run re-queries everything.",
         "button_label": "Clear Cache", "button_variant": "outline", "button_color": "red",
         "confirm": {"required": True, "title": "Clear the lookup cache?",
                     "message": "The next run re-queries every source and will be slow. Continue?"}},
        {"id": "clear_composites", "label": "Clear Composites",
         "description": "Delete generated poster composites. They'll be regenerated on the next run.",
         "button_label": "Clear Composites", "button_variant": "outline", "button_color": "red",
         "confirm": {"required": True, "title": "Clear composite poster images?",
                     "message": "All generated poster images will be deleted. They'll be regenerated on the next enrichment run. Continue?"}},
        {"id": "clear_imgbb", "label": "Clear ImgBB Cache",
         "description": "Clear the cached ImgBB URLs. Next run will re-upload composites.",
         "button_label": "Clear ImgBB Cache", "button_variant": "outline", "button_color": "red",
         "confirm": {"required": True, "title": "Clear ImgBB URL cache?",
                     "message": "Cached ImgBB URLs will be cleared. Next run will re-upload all composites to ImgBB. Continue?"}},
    ]

    # ------------------------------------------------------------------
    def run(self, action, params, context):
        settings = (context or {}).get("settings", {}) or {}
        cfg = _resolve_cfg(settings)
        _file_log("info", f"run() called: action={action}")

        if action == "enrich_now":
            result = _start_enrichment(cfg, context)
            interval = cfg.get("auto_interval", 0)
            if interval > 0:
                _arm_timer(interval, cfg, context)
            return result

        if action == "auto_enrich":
            # Fired by Dispatcharr's event system after an EPG refresh.
            _file_log("info", "auto_enrich action triggered (event-driven)")
            # Skip if a run is already in progress (avoid piling up).
            if _running["active"]:
                _file_log("info", "auto_enrich skipped — run already active")
                _log(context, "info",
                     "Poster Enricher: auto-enrich skipped — run already active")
                return {"status": "ok", "message": "Skipped — already running."}
            _log(context, "info",
                 "Poster Enricher: auto-enrich triggered by EPG refresh")
            result = _start_enrichment(cfg, context)
            interval = cfg.get("auto_interval", 0)
            if interval > 0:
                _arm_timer(interval, cfg, context)
            return result

        if action == "cancel_run":
            # Check both in-memory and disk state.
            run_state = _read_run_state()
            if not _running["active"] and not (run_state and run_state.get("active")):
                return {"status": "ok", "message": "No run in progress."}
            _running["cancel"] = True
            # Write cancel flag to disk so the running worker sees it.
            _write_run_state(True, done=_running.get("done", 0),
                            total=_running.get("total", 0),
                            started=_running.get("started", 0))
            return {"status": "ok", "message": "Cancelling after the current batch…"}

        if action == "view_stats":
            return self._view_stats()

        if action == "view_unmatched":
            return self._view_unmatched()

        if action == "view_logs":
            return self._view_logs()

        if action == "clear_cache":
            providers.LookupCache(_CACHE_PATH, ttl_days=int(cfg["cache_days"])).clear()
            return {"status": "ok", "message": "Lookup cache cleared."}

        if action == "clear_composites":
            import glob
            files = glob.glob(os.path.join(_POSTER_DIR, "poster_*.png"))
            for f in files:
                try:
                    os.remove(f)
                except Exception:
                    pass
            return {"status": "ok",
                    "message": f"Cleared {len(files)} composite poster images."}

        if action == "clear_imgbb":
            global _imgbb_url_cache, _imgbb_cache_loaded
            _imgbb_url_cache = {}
            _imgbb_cache_loaded = False
            try:
                os.remove(_IMGBB_CACHE_PATH)
            except Exception:
                pass
            return {"status": "ok",
                    "message": "ImgBB URL cache cleared. Next run will re-upload."}

        return {"status": "error", "message": f"Unknown action: {action}"}

    # ------------------------------------------------------------------
    def _view_stats(self):
        import json
        # Check disk-based run state (visible to all workers, not just the one running).
        run_state = _read_run_state()
        is_running = _running["active"]  # in-memory (this worker)
        if not is_running and run_state and run_state.get("active"):
            is_running = True  # another worker is running

        if is_running:
            if _running["active"]:
                done, total = _running["done"], _running["total"] or 0
                started = _running.get("started", 0)
            elif run_state:
                done = run_state.get("done", 0)
                total = run_state.get("total", 0)
                started = run_state.get("started", 0)
            else:
                done, total, started = 0, 0, 0
            pct = (done * 100 // total) if total else 0
            elapsed = int(time.time()) - started if started else 0
            mins, secs = divmod(elapsed, 60)
            return {"status": "ok",
                    "message": f"Running: {done}/{total} ({pct}%) — {mins}m{secs}s elapsed"}
        try:
            with open(_STATS_PATH) as f:
                data = json.load(f)
        except Exception:
            return {"status": "ok", "message": "No runs yet."}
        stats = data.get("stats", {})
        total = data.get("total", 0) or 1
        at = data.get("at", 0)
        ts = time.strftime("%Y-%m-%d %H:%M", time.localtime(at)) if at else "unknown"
        duration = data.get("duration_secs", 0)
        dur_str = f"{duration // 60}m{duration % 60}s" if duration else ""

        order = ("tmdb", "tvmaze", "tvdb", "fanart", "omdb",
                 "existing", "channel_logo", "unmatched", "transient")
        parts = [f"{k}: {stats.get(k, 0)} ({stats.get(k, 0) * 100 // total}%)"
                 for k in order if stats.get(k, 0) > 0]
        prefix = "Last run"
        if data.get("cancelled"):
            prefix = "Last run (cancelled)"
        header = f"{prefix} at {ts}"
        if dur_str:
            header += f" ({dur_str})"
        composites_ok = stats.get("composites_created", 0)
        composites_fail = stats.get("composites_failed", 0)
        if composites_ok or composites_fail:
            parts.append(f"composites: {composites_ok} ok / {composites_fail} failed")
        return {"status": "ok", "message": header + " — " + " | ".join(parts)}

    def _view_unmatched(self):
        try:
            with open(_UNMATCHED_PATH) as f:
                titles = [t for t in f.read().splitlines() if t]
        except Exception:
            return {"status": "ok", "message": "No unmatched titles recorded."}
        if not titles:
            return {"status": "ok", "message": "Nothing unmatched — full coverage."}
        preview = ", ".join(titles[:40])
        more = f" … (+{len(titles) - 40} more)" if len(titles) > 40 else ""
        return {"status": "ok", "message": f"{len(titles)} unmatched: {preview}{more}"}

    def _view_logs(self):
        try:
            with open(_LOG_PATH) as f:
                lines = f.readlines()
        except Exception:
            return {"status": "ok", "message": "No log entries yet."}
        last = lines[-30:] if len(lines) > 30 else lines
        text = "".join(last).strip()
        if not text:
            return {"status": "ok", "message": "Log is empty."}
        return {"status": "ok", "message": text}

    # ------------------------------------------------------------------
    def stop(self, context):
        # Cooperative cancel: the worker checks this between batches and exits.
        _running["cancel"] = True
        _stop_timer()
