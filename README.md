# Poster Enricher — Dispatcharr Plugin

Automatically adds poster artwork to every programme in your EPG, so your media server's guide never shows blank cards.

## How It Works

When Dispatcharr refreshes your EPG, programmes arrive with titles and times but often no artwork. This plugin runs through every programme and finds a poster image using a 6-tier lookup chain:

1. **Existing poster** — if the programme already has artwork (e.g. from Schedules Direct), skip it
2. **TMDB** — The Movie Database (best coverage for movies and TV shows)
3. **TVmaze** — good for reality, talk, and daytime shows (no API key needed)
4. **TVDB** — TheTVDB (additional TV coverage)
5. **Fanart.tv** — high-quality fan artwork
6. **OMDB** — Open Movie Database (last API resort)
7. **Channel logo composite** — if no API finds a match, the channel's logo is placed on a dark poster-sized canvas so it looks clean in the guide instead of stretched

The plugin writes the poster URL into each programme's data. Dispatcharr's XMLTV output then includes it as an `<icon>` tag, and your media server (Plex, Jellyfin, Emby) displays it in the guide.

## Requirements

**Required:**
- Dispatcharr (any recent version)
- Pillow (Python image library) — for generating composite channel-logo posters

**Optional (for extra features):**
- CairoSVG — for channels that use SVG logos (some providers serve `.svg` logos that Pillow can't read)
- One or more API keys for better coverage (see API Keys section below)

### Installing Dependencies

On the Dispatcharr server:
```bash
# Required — composite poster generation
cd /opt/dispatcharr && .venv/bin/pip install Pillow

# Optional — SVG logo support
apt install -y libcairo2-dev libffi-dev libgdk-pixbuf2.0-dev libpango1.0-dev
cd /opt/dispatcharr && .venv/bin/pip install cairosvg
```

## Installation

1. Download `poster_enricher.zip`
2. In Dispatcharr, go to **Plugins** → **Import** → select the ZIP file
3. Enable the plugin (a trust warning modal appears the first time)
4. Configure settings (see below)
5. Click **Enrich Now** to run the first enrichment
6. Restart Dispatcharr so all workers load the plugin: `systemctl restart dispatcharr`

Or manually: drop the three files (`plugin.json`, `plugin.py`, `providers.py`) into `data/plugins/poster_enricher/` and click Refresh on the Plugins page.

## Settings

### API Keys

All optional. With zero keys you still get TVmaze (no key needed) plus the channel-logo fallback — no card is ever blank. Adding keys improves coverage.

| Setting | Where to Get It | What It Adds |
|---------|----------------|--------------|
| **TMDB API key** | [themoviedb.org](https://www.themoviedb.org/) → Settings → API → Request a v3 key | Best movie/TV coverage. Free. |
| **TVDB API key** | [thetvdb.com/api-information](https://thetvdb.com/api-information) | Additional TV show coverage. Free. |
| **Fanart.tv API key** | [fanart.tv/get-an-api-key](https://fanart.tv/get-an-api-key/) | High-quality fan artwork. Requires TMDB key too. Free. |
| **OMDB API key** | [omdbapi.com/apikey.aspx](https://www.omdbapi.com/apikey.aspx) | Last-resort API lookup. Free (1000/day). |

### Lookup Chain

| Setting | Default | Description |
|---------|---------|-------------|
| **Chain order** | `tmdb,tvmaze,tvdb,fanart,omdb` | Comma-separated list controlling which sources are tried and in what order. First hit wins. |
| **Use categories** | On | Skip news, sports, weather, and shopping programmes (go straight to channel logo — they rarely have poster artwork). |

### Smart Matching

| Setting | Default | Description |
|---------|---------|-------------|
| **Language** | `en` | Language code for poster lookups (en, de, fr, es, nl, it). Affects which localised poster is returned. |
| **Poster size** | `w500` | TMDB poster width. Options: w342, w500, w780, original. Larger = higher quality but slower to load. |
| **Cache days** | `14` | How many days to cache API lookup results. After this, results are re-queried. Set lower if you want fresher data; higher to reduce API calls. |

### Scope

| Setting | Default | Description |
|---------|---------|-------------|
| **Scope source IDs** | *(blank = all)* | Comma-separated EPG source IDs to process. Leave blank to enrich all sources. Tip: exclude Schedules Direct if it already provides good artwork — find the source ID on Dispatcharr's EPG Sources page. |
| **Overwrite existing posters** | Off | When off, programmes that already have a poster are skipped (preserves Schedules Direct and provider artwork). Turn on temporarily for a full re-run, then turn off again. |

### Channel Logo Options

| Setting | Default | Description |
|---------|---------|-------------|
| **Channel-logo URL source** | Provider URL | Which URL to use for the raw channel logo. "Provider URL" is a direct CDN link (recommended). "Dispatcharr cache URL" goes through Dispatcharr's proxy (can have issues behind a reverse proxy). |
| **Dispatcharr base URL** | *(auto-detected)* | The internal URL where Dispatcharr serves composite images. Only set this if composites aren't loading (e.g. `http://your-server-ip:9191`). |

### Remote Access (Making Composites Work Everywhere)

Channel-logo composites are generated locally on your Dispatcharr server. By default, only clients on your local network can load them. TMDB/TVmaze/etc. posters are public CDN URLs and work everywhere automatically.

To make composites work on **mobile apps and remote clients**, use ONE of these options:

#### Option A: ImgBB (Easiest — No Domain Needed)

| Setting | Description |
|---------|-------------|
| **ImgBB API key** | Free key from [api.imgbb.com](https://api.imgbb.com) — sign up (email only), click "Get API Key", paste it here. Composites are uploaded to ImgBB's free CDN and get a public URL like `https://i.ibb.co/abc123/poster.png`. Works on ALL clients everywhere. |

#### Option B: Plex Proxy (Requires External Plex Access)

| Setting | Description |
|---------|-------------|
| **Plex server URL** | Your Plex server's external address — either a domain (e.g. `https://plex.example.com`) or your port-forwarded IP (e.g. `http://203.0.113.50:32400`). Composites are routed through Plex's image proxy. |
| **Plex token** | Required with Plex URL. Find it in Plex's `Preferences.xml` (the `PlexOnlineToken` value) or at the end of any Plex URL after `X-Plex-Token=`. |

#### Option C: Local Only (Default)

Leave all remote access fields blank. Composites work on your local network. TMDB/TVmaze posters still work everywhere.

**Priority:** ImgBB (if key set) → Plex proxy (if URL set) → Direct local URL.

### Auto-Run

| Setting | Default | Description |
|---------|---------|-------------|
| **Auto-run interval (hours)** | `0` (disabled) | Set to e.g. `6` to automatically re-run enrichment every 6 hours. The timer starts when you click Enrich Now and survives as long as Dispatcharr is running. Set to 0 for manual-only. |

The plugin also subscribes to EPG refresh events — if Dispatcharr fires an event after an EPG source refreshes, the plugin runs automatically regardless of this timer.

## Actions (Buttons)

| Button | What It Does |
|--------|-------------|
| **Enrich Now** | Start an enrichment run immediately. Runs in the background — you can keep using Dispatcharr. |
| **Auto Enrich** | Triggered automatically by Dispatcharr after an EPG refresh. You don't need to click this. |
| **Cancel Run** | Stop the current run after the current batch finishes. |
| **View Stats** | Show the last run's results: per-source hit counts, duration, timestamp, and composite success/failure counts. Shows live progress if a run is active. |
| **View Unmatched** | List programme titles that fell through all sources to the channel-logo fallback. |
| **View Logs** | Show the last 30 entries from the persistent log file. Useful for diagnosing issues — shows every run start/end, composite failures, auto-enrich triggers, and config used. |
| **Clear Cache** | Wipe the API lookup cache. The next run re-queries every source (slower but fresh). |
| **Clear Composites** | Delete all generated composite poster images. They'll be regenerated on the next run. Use this after changing the logo size or to force new images. |
| **Clear ImgBB Cache** | Clear cached ImgBB CDN URLs. Next run will re-upload all composites to ImgBB. |

## Typical Setup

### Minimal (zero API keys, local only)
1. Install the plugin
2. Install Pillow: `cd /opt/dispatcharr && .venv/bin/pip install Pillow`
3. Click Enrich Now
4. Every programme gets either a TVmaze poster or a channel-logo composite

### Recommended (best coverage, works everywhere)
1. Install the plugin
2. Install Pillow + CairoSVG (see Requirements above)
3. Get a TMDB API key (free, 30 seconds) and paste it in settings
4. Get an ImgBB API key (free, 30 seconds) and paste it in settings
5. Set auto-run interval to `6`
6. Click Enrich Now
7. Done — TMDB covers ~50% of programmes, TVmaze adds ~4%, and the rest get clean composites on a public CDN

### With Plex Domain
1. Same as above but instead of ImgBB, enter your Plex external URL and token
2. Leave ImgBB blank

## Files & State

All state is stored in `/data/poster_enricher/`:

| File | Purpose |
|------|---------|
| `cache.sqlite3` | API lookup results cache (SQLite, WAL mode) |
| `last_stats.json` | Stats from the most recent run |
| `run_state.json` | Active run progress (shared across workers) |
| `unmatched.txt` | Titles that fell through to channel-logo |
| `run.log` | Persistent log (last 500 lines) |
| `imgbb_urls.json` | Cached ImgBB CDN URLs (so images aren't re-uploaded) |

Composite poster images are stored in `/opt/dispatcharr/media/poster_enricher/`.

## Troubleshooting

**"Unknown action" when clicking buttons** — Dispatcharr runs multiple worker processes. After importing a new plugin version, some workers still have old code. Fix: `systemctl restart dispatcharr`.

**Composites not generating** — Check View Logs. Common causes:
- Pillow not installed in Dispatcharr's venv (install with `.venv/bin/pip install Pillow`)
- SVG logos failing (install cairosvg — see Requirements)
- Logo URLs with spaces (fixed in v0.3+)

**Stats stuck on old run** — Click View Stats again (may hit a different worker). After a restart, all workers share the same state file.

**Plugin ran once but didn't run again** — Expected: with Overwrite off, programmes that already have posters are skipped. New programmes from EPG refreshes will be enriched by the auto-run timer or event hook. If you need to re-process everything (e.g. after changing settings), turn Overwrite on temporarily.

**Composites don't show on mobile/remote** — TMDB/TVmaze posters work everywhere (public CDN). Composites are local by default. Set up ImgBB or Plex proxy (see Remote Access above).

**SVG logos failing** — Install cairosvg: `apt install -y libcairo2-dev && .venv/bin/pip install cairosvg`. Without it, SVG logos fall back to the raw URL.
