#!/usr/bin/env python3
"""Build the release zip and the Dispatcharr plugin-repo manifests.

Usage:  python3 scripts/build_release.py

Reads the version from plugin.json, then writes:
  dist/poster_enricher-v<version>.zip      -> upload as the GitHub release asset
  manifest.json                            -> repo manifest (add its raw URL in Dispatcharr)
  metadata/poster_enricher/manifest.json   -> per-plugin detail/version history

The zip is deterministic (fixed timestamps/permissions), so rebuilding the same
source gives the same SHA256 that the manifest advertises.
"""

import datetime
import hashlib
import json
import os
import re
import sys
import zipfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
REPO = "ckegels/Dispatcharr-Plex-Poster-Enricher"
# Must equal the zip's top folder AND the installed plugin key, or Dispatcharr
# installs a second copy with empty settings instead of updating in place.
SLUG = "poster_enricher"
FILES = ("plugin.py", "providers.py", "plugin.json", "README.md", "LICENSE")
RAW_BASE = f"https://raw.githubusercontent.com/{REPO}/main"
ZIP_DATE = (2026, 1, 1, 0, 0, 0)


def read_version():
    with open(os.path.join(ROOT, "plugin.json")) as f:
        version = json.load(f)["version"]
    with open(os.path.join(ROOT, "plugin.py")) as f:
        m = re.search(r'^\s*version = "([^"]+)"', f.read(), re.M)
    if not m or m.group(1) != version:
        sys.exit(f"Version mismatch: plugin.json={version}, "
                 f"plugin.py={m.group(1) if m else 'missing'}")
    return version


def build_zip(version):
    os.makedirs(os.path.join(ROOT, "dist"), exist_ok=True)
    path = os.path.join(ROOT, "dist", f"{SLUG}-v{version}.zip")
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as zf:
        for name in FILES:
            info = zipfile.ZipInfo(f"{SLUG}/{name}", date_time=ZIP_DATE)
            info.external_attr = 0o644 << 16
            info.compress_type = zipfile.ZIP_DEFLATED
            with open(os.path.join(ROOT, name), "rb") as f:
                zf.writestr(info, f.read())
    return path


def write_json(rel_path, data):
    path = os.path.join(ROOT, rel_path)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
        f.write("\n")


def main():
    version = read_version()
    zip_path = build_zip(version)
    with open(zip_path, "rb") as f:
        blob = f.read()
    sha256 = hashlib.sha256(blob).hexdigest()
    md5 = hashlib.md5(blob).hexdigest()
    size_kb = max(1, round(len(blob) / 1024))
    now = datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0).isoformat()
    download_url = (f"https://github.com/{REPO}/releases/download/"
                    f"v{version}/{os.path.basename(zip_path)}")

    with open(os.path.join(ROOT, "plugin.json")) as f:
        meta = json.load(f)
    name = meta["name"]
    author = meta.get("author", "")
    # First paragraph only — the card has limited room.
    description = meta.get("description", "").split("\n\n")[0]

    release = {
        "version": version,
        "last_updated": now,
        "checksum_md5": md5,
        "checksum_sha256": sha256,
        "url": download_url,
        "latest_url": download_url,
        "size": size_kb,
    }

    # Per-plugin metadata: keep history of older versions.
    detail_rel = f"metadata/{SLUG}/manifest.json"
    versions = []
    try:
        with open(os.path.join(ROOT, detail_rel)) as f:
            versions = json.load(f)["manifest"].get("versions", [])
    except (OSError, KeyError, ValueError):
        pass
    versions = [release] + [v for v in versions if v.get("version") != version]

    write_json(detail_rel, {
        "generated_at": now,
        "manifest": {
            "slug": SLUG,
            "name": name,
            "description": description,
            "author": author,
            "maintainers": [author] if author else [],
            "license": "Apache-2.0",
            "repo_url": f"https://github.com/{REPO}",
            "registry_name": REPO,
            "last_updated": now,
            "latest": release,
            "versions": versions,
        },
    })

    write_json("manifest.json", {
        "generated_at": now,
        "manifest": {
            "registry_name": REPO,
            "plugins": [{
                "slug": SLUG,
                "name": name,
                "description": description,
                "manifest_url": f"{RAW_BASE}/{detail_rel}",
                "author": author,
                "license": "Apache-2.0",
                "last_updated": now,
                "latest_version": version,
                "latest_md5": md5,
                "latest_sha256": sha256,
                "latest_url": download_url,
                "latest_size": size_kb,
            }],
        },
    })

    print(f"Built {os.path.relpath(zip_path, ROOT)}  sha256={sha256}")
    print(f"Manifest URL: {RAW_BASE}/manifest.json")
    print(f"Release asset must be uploaded to: {download_url}")


if __name__ == "__main__":
    main()
