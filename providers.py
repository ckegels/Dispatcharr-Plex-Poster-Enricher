"""
Artwork provider lookup chain for the Poster Enricher plugin.

Each provider is a small callable that takes a normalized title (and a hint about
whether it's a movie or a series) and returns a poster URL string, or a sentinel.

The chain is ordered by the plugin settings. A SQLite cache sits in front of the
network so repeat lookups of the same title are instant, and so a title that was
already looked up and *definitively* missed is not re-queried until the cache
entry expires.

Key robustness properties (learned the hard way):
* Every provider is rate-limited by a token bucket so a full-guide run does not
  trip TVmaze's ~20-req/10s limit or TMDB's ~40-req/s limit.
* A 404 (real "no match") is cached as a miss. A 429/5xx/network error is NOT
  cached — it returns a TRANSIENT sentinel so the title is retried on the next
  run instead of being poisoned as a permanent miss.
* The cache key includes language + poster size, so changing either setting
  invalidates only the affected rows instead of silently serving stale art.
"""

import json
import os
import re
import sqlite3
import time
import threading
import urllib.parse
import urllib.request
import urllib.error

# ---------------------------------------------------------------------------
# Result sentinels
# ---------------------------------------------------------------------------
# A provider call returns one of:
#   a URL string   -> hit
#   MISS  (None)   -> definitive no-match, safe to cache
#   TRANSIENT      -> rate-limited / network error, do NOT cache, retry later
MISS = None


class _Transient:
    __slots__ = ()

    def __repr__(self):
        return "TRANSIENT"


TRANSIENT = _Transient()


# ---------------------------------------------------------------------------
# Rate limiting (token bucket, one per provider)
# ---------------------------------------------------------------------------

class _TokenBucket:
    """Thread-safe token bucket. `rate` tokens accrue per `per` seconds, capped
    at `burst`. acquire() blocks until a token is available."""

    def __init__(self, rate, per, burst=None):
        self.rate = float(rate)
        self.per = float(per)
        self.capacity = float(burst if burst is not None else rate)
        self.tokens = self.capacity
        self.updated = time.monotonic()
        self._lock = threading.Lock()

    def acquire(self):
        with self._lock:
            while True:
                now = time.monotonic()
                elapsed = now - self.updated
                self.tokens = min(
                    self.capacity, self.tokens + elapsed * (self.rate / self.per)
                )
                self.updated = now
                if self.tokens >= 1.0:
                    self.tokens -= 1.0
                    return
                deficit = 1.0 - self.tokens
                sleep_for = deficit * (self.per / self.rate)
                self._lock.release()
                try:
                    time.sleep(min(sleep_for, 1.0))
                finally:
                    self._lock.acquire()


# Conservative buckets. TVmaze is the tight one (~20/10s); we stay well under.
# TMDB is ~40-50/s; we cap at 30/s to leave headroom for image-CDN connections.
_BUCKETS = {
    "tmdb": _TokenBucket(rate=30, per=1.0, burst=30),
    "tvmaze": _TokenBucket(rate=10, per=10.0, burst=5),
    "tvdb": _TokenBucket(rate=5, per=1.0, burst=5),
    "fanart": _TokenBucket(rate=5, per=1.0, burst=5),
    "omdb": _TokenBucket(rate=5, per=1.0, burst=5),
}


def _throttle(provider):
    b = _BUCKETS.get(provider)
    if b:
        b.acquire()


# ---------------------------------------------------------------------------
# HTTP helper (status-aware)
# ---------------------------------------------------------------------------

_USER_AGENT = "Dispatcharr-PosterEnricher/0.2 (+https://github.com/Dispatcharr)"
_HTTP_TIMEOUT = 12


def _get_json(url, headers=None):
    """GET a URL and parse JSON.

    Returns (data, status):
      (dict/list, 200)  on success
      (None, 404)       on a genuine not-found
      (None, 429)       on rate limit
      (None, 5xx)       on server error
      (None, 0)         on network/parse failure (treat as transient)
    """
    req = urllib.request.Request(url)
    req.add_header("User-Agent", _USER_AGENT)
    req.add_header("Accept", "application/json")
    if headers:
        for k, v in headers.items():
            req.add_header(k, v)
    try:
        with urllib.request.urlopen(req, timeout=_HTTP_TIMEOUT) as resp:
            raw = resp.read()
        return json.loads(raw.decode("utf-8", "replace")), 200
    except urllib.error.HTTPError as e:
        return None, e.code
    except Exception:
        return None, 0


def _classify(status):
    """Map a no-payload HTTP outcome to MISS (real not-found) or TRANSIENT."""
    if status == 404:
        return MISS
    if status in (429, 0) or status >= 500:
        return TRANSIENT
    # Other non-200 with no data (e.g. 401 bad key): cache as miss so we don't
    # hammer a misconfigured provider all run.
    return MISS


# ---------------------------------------------------------------------------
# Title normalization
# ---------------------------------------------------------------------------

_YEAR_RE = re.compile(r"\((?:19|20)\d{2}\)")
_BRACKET_RE = re.compile(r"[\[(].*?[\])]")
_TAG_RE = re.compile(r"\b(HD|SD|FHD|UHD|4K|LIVE|NEW)\b", re.IGNORECASE)
_WS_RE = re.compile(r"\s+")
_SxxExx_RE = re.compile(r"\bS\d{1,2}\s?E\d{1,3}\b", re.IGNORECASE)


def normalize_title(title):
    if not title:
        return ""
    t = title.strip()
    t = _SxxExx_RE.sub(" ", t)
    t = _BRACKET_RE.sub(" ", t)
    t = _YEAR_RE.sub(" ", t)
    t = _TAG_RE.sub(" ", t)
    t = t.replace(":", " ").replace("-", " ")
    t = _WS_RE.sub(" ", t).strip()
    return t


def extract_year(title):
    if not title:
        return None
    m = _YEAR_RE.search(title)
    if m:
        return m.group(0).strip("()")
    return None


# ---------------------------------------------------------------------------
# SQLite cache
# ---------------------------------------------------------------------------

class LookupCache:
    """
    Tiny SQLite key/value cache. Key includes provider, normalized title, and the
    settings that change the *result* (language, size). Value = URL, or '' for a
    known-miss. Only definitive misses are stored; a transient failure is never
    written, so it retries next run.
    """

    def __init__(self, path, ttl_days=14):
        self.path = path
        self.ttl = max(1, int(ttl_days)) * 86400
        self._lock = threading.Lock()
        os.makedirs(os.path.dirname(path), exist_ok=True)
        self._init_db()

    def _connect(self):
        conn = sqlite3.connect(self.path, timeout=30)
        conn.execute("PRAGMA journal_mode=WAL")
        return conn

    def _init_db(self):
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS lookups (
                    k TEXT PRIMARY KEY,
                    url TEXT,
                    fetched_at INTEGER
                )
                """
            )

    @staticmethod
    def _key(provider, norm_title, variant):
        return f"{provider}|{variant}|{norm_title}"

    def get(self, provider, norm_title, variant):
        """Return (hit, url). hit=False means not cached / expired.
        url may be '' meaning a cached definitive miss."""
        k = self._key(provider, norm_title, variant)
        with self._lock, self._connect() as conn:
            row = conn.execute(
                "SELECT url, fetched_at FROM lookups WHERE k = ?", (k,)
            ).fetchone()
        if not row:
            return False, None
        url, fetched_at = row
        if time.time() - fetched_at > self.ttl:
            return False, None
        return True, url

    def put(self, provider, norm_title, variant, url):
        k = self._key(provider, norm_title, variant)
        with self._lock, self._connect() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO lookups (k, url, fetched_at) VALUES (?, ?, ?)",
                (k, url or "", int(time.time())),
            )

    def clear(self):
        with self._lock, self._connect() as conn:
            conn.execute("DELETE FROM lookups")


# ---------------------------------------------------------------------------
# Providers
# ---------------------------------------------------------------------------
# Signature: fn(title, norm_title, year, is_movie, cfg) -> url | MISS | TRANSIENT

_TMDB_IMG = "https://image.tmdb.org/t/p/"


def _tmdb_poster_path(cfg, path):
    size = cfg.get("poster_size", "w500") or "w500"
    return f"{_TMDB_IMG}{size}{path}"


def _tmdb_search(kind, norm_title, year, cfg):
    """One TMDB search call. Returns (results_list_or_None, status)."""
    key = cfg.get("tmdb_key", "").strip()
    lang = cfg.get("language", "en") or "en"
    q = urllib.parse.quote(norm_title)
    url = (
        f"https://api.themoviedb.org/3/search/{kind}"
        f"?api_key={key}&query={q}&language={lang}"
    )
    if year and kind == "movie":
        url += f"&year={year}"
    _throttle("tmdb")
    data, status = _get_json(url)
    if data is None:
        return None, status
    return data.get("results") or [], status


def tmdb_lookup(title, norm_title, year, is_movie, cfg):
    if not cfg.get("tmdb_key", "").strip():
        return MISS
    primary = "movie" if is_movie else "tv"
    secondary = "tv" if is_movie else "movie"

    results, status = _tmdb_search(primary, norm_title, year, cfg)
    if results is None:
        if _classify(status) is TRANSIENT:
            return TRANSIENT
        results = []
    for r in results:
        if r.get("poster_path"):
            return _tmdb_poster_path(cfg, r["poster_path"])

    results2, status2 = _tmdb_search(secondary, norm_title, year, cfg)
    if results2 is None:
        if _classify(status2) is TRANSIENT:
            return TRANSIENT
        results2 = []
    for r in results2:
        if r.get("poster_path"):
            return _tmdb_poster_path(cfg, r["poster_path"])
    return MISS


def tvmaze_lookup(title, norm_title, year, is_movie, cfg):
    # TVmaze is TV-only and needs no key.
    q = urllib.parse.quote(norm_title)
    url = f"https://api.tvmaze.com/singlesearch/shows?q={q}"
    _throttle("tvmaze")
    data, status = _get_json(url)
    if data is None:
        return _classify(status)
    image = data.get("image") or {}
    got = image.get("original") or image.get("medium")
    return got if got else MISS


_tvdb_token_cache = {"token": None, "at": 0}
_tvdb_lock = threading.Lock()


def _tvdb_token(key):
    with _tvdb_lock:
        if _tvdb_token_cache["token"] and time.time() - _tvdb_token_cache["at"] < 3600:
            return _tvdb_token_cache["token"]
        try:
            body = json.dumps({"apikey": key}).encode("utf-8")
            req = urllib.request.Request("https://api4.thetvdb.com/v4/login", data=body)
            req.add_header("Content-Type", "application/json")
            req.add_header("User-Agent", _USER_AGENT)
            with urllib.request.urlopen(req, timeout=_HTTP_TIMEOUT) as resp:
                data = json.loads(resp.read().decode("utf-8", "replace"))
            token = (data.get("data") or {}).get("token")
            if token:
                _tvdb_token_cache["token"] = token
                _tvdb_token_cache["at"] = time.time()
            return token
        except Exception:
            return None


def tvdb_lookup(title, norm_title, year, is_movie, cfg):
    key = cfg.get("tvdb_key", "").strip()
    if not key:
        return MISS
    token = _tvdb_token(key)
    if not token:
        return TRANSIENT  # login failed — may be transient, don't cache a miss
    q = urllib.parse.quote(norm_title)
    url = f"https://api4.thetvdb.com/v4/search?query={q}&limit=5"
    _throttle("tvdb")
    data, status = _get_json(url, headers={"Authorization": f"Bearer {token}"})
    if data is None:
        return _classify(status)
    for r in data.get("data", []) or []:
        img = r.get("image_url") or r.get("thumbnail") or r.get("image")
        if img:
            return img
    return MISS


# In-process memo so the Fanart tier doesn't repeat the TMDB id search.
_tmdb_id_memo = {}
_tmdb_id_lock = threading.Lock()


def _tmdb_id_for(norm_title, year, is_movie, cfg):
    memo_key = (norm_title, year, is_movie, cfg.get("language", "en"))
    with _tmdb_id_lock:
        if memo_key in _tmdb_id_memo:
            return _tmdb_id_memo[memo_key]
    kind = "movie" if is_movie else "tv"
    results, status = _tmdb_search(kind, norm_title, year, cfg)
    result = (None, kind, status)
    if results:
        result = (results[0].get("id"), kind, 200)
    with _tmdb_id_lock:
        _tmdb_id_memo[memo_key] = result
    return result


_tmdb_ext_memo = {}


def _tmdb_external_tvdb_id(tmdb_tv_id, cfg):
    if tmdb_tv_id in _tmdb_ext_memo:
        return _tmdb_ext_memo[tmdb_tv_id]
    key = cfg.get("tmdb_key", "").strip()
    url = f"https://api.themoviedb.org/3/tv/{tmdb_tv_id}/external_ids?api_key={key}"
    _throttle("tmdb")
    data, status = _get_json(url)
    tvdb_id = (data or {}).get("tvdb_id") if data else None
    _tmdb_ext_memo[tmdb_tv_id] = tvdb_id
    return tvdb_id


def fanart_lookup(title, norm_title, year, is_movie, cfg):
    # Fanart.tv is keyed by TMDB/TVDB IDs. Needs a TMDB key to map title -> id.
    key = cfg.get("fanart_key", "").strip()
    tmdb_key = cfg.get("tmdb_key", "").strip()
    if not key or not tmdb_key:
        return MISS
    tmdb_id, kind, status = _tmdb_id_for(norm_title, year, is_movie, cfg)
    if tmdb_id is None:
        return TRANSIENT if _classify(status) is TRANSIENT else MISS
    if kind == "movie":
        _throttle("fanart")
        data, fstatus = _get_json(
            f"https://webservice.fanart.tv/v3/movies/{tmdb_id}?api_key={key}"
        )
        if data is None:
            return _classify(fstatus)
        posters = data.get("movieposter") or []
    else:
        tvdb_id = _tmdb_external_tvdb_id(tmdb_id, cfg)
        if not tvdb_id:
            return MISS
        _throttle("fanart")
        data, fstatus = _get_json(
            f"https://webservice.fanart.tv/v3/tv/{tvdb_id}?api_key={key}"
        )
        if data is None:
            return _classify(fstatus)
        posters = data.get("tvposter") or []
    for p in posters:
        if p.get("url"):
            return p["url"]
    return MISS


def omdb_lookup(title, norm_title, year, is_movie, cfg):
    key = cfg.get("omdb_key", "").strip()
    if not key:
        return MISS
    q = urllib.parse.quote(norm_title)
    url = f"https://www.omdbapi.com/?apikey={key}&t={q}"
    if year:
        url += f"&y={year}"
    _throttle("omdb")
    data, status = _get_json(url)
    if data is None:
        return _classify(status)
    if data.get("Response") == "False":
        # "Movie not found!" is a real miss; "Request limit reached!"/"Invalid
        # API key!" are transient/config — don't cache those as a miss.
        err = str(data.get("Error", "")).lower()
        if "limit" in err or "invalid" in err:
            return TRANSIENT
        return MISS
    poster = data.get("Poster")
    if poster and poster != "N/A":
        return poster
    return MISS


PROVIDER_FUNCS = {
    "tmdb": tmdb_lookup,
    "tvmaze": tvmaze_lookup,
    "tvdb": tvdb_lookup,
    "fanart": fanart_lookup,
    "omdb": omdb_lookup,
}

DEFAULT_CHAIN = ["tmdb", "tvmaze", "tvdb", "fanart", "omdb"]


def _variant(cfg):
    """The part of config that changes a provider's result, folded into the
    cache key: language affects all title searches; size affects TMDB URLs."""
    return f"{cfg.get('language', 'en')}:{cfg.get('poster_size', 'w500')}"


def resolve_poster(title, is_movie, cfg, cache, stats=None):
    """
    Walk the configured provider chain for a single programme title.
    Returns (url, source_id) or (None, None) if every provider missed.
    Transient failures are not cached and count toward stats['transient'].
    """
    norm = normalize_title(title)
    if not norm:
        return None, None
    year = extract_year(title)
    variant = _variant(cfg)

    chain = cfg.get("chain") or DEFAULT_CHAIN
    for pid in chain:
        fn = PROVIDER_FUNCS.get(pid)
        if not fn:
            continue
        if not cfg.get(f"{pid}_enabled", True):
            continue

        hit, cached_url = cache.get(pid, norm, variant)
        if hit:
            if cached_url:
                if stats is not None:
                    stats[pid] = stats.get(pid, 0) + 1
                return cached_url, pid
            continue  # cached definitive miss — try next provider

        result = fn(title, norm, year, is_movie, cfg)

        if result is TRANSIENT:
            if stats is not None:
                stats["transient"] = stats.get("transient", 0) + 1
            continue  # do not cache; retried next run

        cache.put(pid, norm, variant, result if result else "")
        if result:
            if stats is not None:
                stats[pid] = stats.get(pid, 0) + 1
            return result, pid

    return None, None
