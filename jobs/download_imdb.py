"""Download the IMDb non-commercial datasets into $DATA_DIR/raw.

Files already present with the same size as the remote copy are skipped,
so the script is idempotent and can be resumed.
"""

import argparse
import sys

import requests

from imdb_project import config

CHUNK = 1 << 20


def download(name: str, force: bool) -> None:
    url = f"{config.IMDB_BASE_URL}/{name}.tsv.gz"
    target = config.RAW_DIR / f"{name}.tsv.gz"
    head = requests.head(url, timeout=30, allow_redirects=True)
    head.raise_for_status()
    remote_size = int(head.headers.get("Content-Length", -1))
    if target.exists() and not force and target.stat().st_size == remote_size:
        print(f"[skip] {name} already downloaded ({remote_size:,} bytes)")
        return
    print(f"[get ] {url}")
    tmp = target.with_suffix(".part")
    with requests.get(url, stream=True, timeout=60) as resp:
        resp.raise_for_status()
        with open(tmp, "wb") as handle:
            for chunk in resp.iter_content(CHUNK):
                handle.write(chunk)
    tmp.replace(target)
    print(f"[done] {name}: {target.stat().st_size:,} bytes")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--force", action="store_true",
                        help="re-download even if the file exists")
    args = parser.parse_args()
    config.RAW_DIR.mkdir(parents=True, exist_ok=True)
    for name in config.IMDB_TABLES:
        download(name, args.force)
    return 0


if __name__ == "__main__":
    sys.exit(main())
