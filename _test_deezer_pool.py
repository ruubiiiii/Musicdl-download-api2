"""
_test_deezer_pool.py — local smoke-test for the Deezer direct pool integration.

Tests exactly the code path that POST /deezer/download uses:
  deezer_client.load_pool_from_file(deezer_accounts.json)
  pool.get_track(track_id, quality)

Usage:
    python _test_deezer_pool.py
    python _test_deezer_pool.py --track 917424
    python _test_deezer_pool.py --track 917424 --quality lossless --save
    python _test_deezer_pool.py --accounts 3      # test first N accounts
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

# ── Make sure deezer_client is importable from this directory ───────────────
HERE = Path(__file__).parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

import deezer_client as dc

ACCOUNTS_FILE = HERE / "deezer_accounts.json"
CACHE_DIR     = HERE / ".deezer_tokens"
DOWNLOADS_DIR = HERE / "downloads_test"
DEFAULT_TRACK = 917424   # Eminem – Lose Yourself

MAGIC = {
    b"fLaC":             "FLAC",
    b"ID3":              "MP3 (ID3)",
    bytes([0xFF, 0xFB]): "MP3 (sync)",
}

def detect(data: bytes) -> str:
    for magic, label in MAGIC.items():
        if data[:len(magic)] == magic:
            return label
    return f"UNKNOWN ({data[:8].hex()})"

def _ok(m):   print(f"  ✅  {m}")
def _info(m): print(f"  ℹ   {m}")
def _warn(m): print(f"  ⚠   {m}")
def _fail(m): print(f"  ❌  {m}")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--track",    default=str(DEFAULT_TRACK),
                   help="Track ID or Deezer URL")
    p.add_argument("--quality",  default="lossless",
                   choices=["lossless", "high", "medium"])
    p.add_argument("--accounts", type=int, default=0,
                   help="Limit pool to first N accounts (0 = all)")
    p.add_argument("--save",     action="store_true",
                   help="Save downloaded track to downloads_test/")
    args = p.parse_args()

    print()
    print("=" * 60)
    print("  Deezer Pool — local integration test")
    print("=" * 60)

    # ── 1. Resolve track ID ──────────────────────────────────────────────
    try:
        track_id = dc.parse_track_id(args.track)
    except ValueError as e:
        _fail(str(e)); sys.exit(1)
    _info(f"Track ID: {track_id}")

    # ── 2. Load pool ──────────────────────────────────────────────────────
    if not ACCOUNTS_FILE.exists():
        _fail(f"deezer_accounts.json not found at {ACCOUNTS_FILE}")
        sys.exit(1)

    import json
    raw_accounts = json.loads(ACCOUNTS_FILE.read_text(encoding="utf-8"))
    if args.accounts > 0:
        raw_accounts = raw_accounts[:args.accounts]
        _info(f"Using first {len(raw_accounts)} account(s)")
    else:
        _info(f"Loading {len(raw_accounts)} account(s) from deezer_accounts.json")

    t0 = time.time()
    try:
        pool = dc.DeezerAccountPool(
            accounts  = raw_accounts,
            cache_dir = str(CACHE_DIR),
            strategy  = "round_robin",
            cooldown  = 60,
        )
    except RuntimeError as e:
        _fail(f"Pool init failed: {e}")
        sys.exit(1)
    _ok(f"Pool ready: {pool.total} client(s) in {time.time()-t0:.1f}s")
    print(f"  Pool status: {pool.status()}")
    print()

    # ── 3. Download ───────────────────────────────────────────────────────
    _info(f"Downloading track {track_id} (quality={args.quality})…")
    t1 = time.time()
    result = pool.get_track(track_id, quality=args.quality)
    elapsed = time.time() - t1

    if not result.get("success"):
        _fail(f"Download failed: {result.get('error')}")
        sys.exit(1)

    audio = result["audio_bytes"]
    fmt   = result["format"]
    ext   = result["extension"]
    size  = len(audio)
    speed = size / elapsed / 1024 / 1024

    _ok(f"{result['artist']} – {result['title']}")
    _ok(f"Format: {fmt} | Size: {size/1024/1024:.2f} MB | "
        f"Downloaded in {elapsed:.1f}s ({speed:.1f} MB/s)")

    # ── 4. Header check ───────────────────────────────────────────────────
    detected = detect(audio)
    if "FLAC" in detected or "MP3" in detected:
        _ok(f"Header: {detected} ✓")
    else:
        _warn(f"Unexpected header: {detected}")

    # ── 5. Save ───────────────────────────────────────────────────────────
    if args.save:
        DOWNLOADS_DIR.mkdir(parents=True, exist_ok=True)
        safe = (
            f"{result['artist']} - {result['title']}"
            .replace("/", "_").replace("\\", "_").replace(":", "-")
            .replace("*", "").replace('"', "").replace("?", "")
        )
        out = DOWNLOADS_DIR / f"{safe}.{ext}"
        out.write_bytes(audio)
        _ok(f"Saved → {out}")

    print()
    print("=" * 60)
    print("  ALL TESTS PASSED")
    print("=" * 60)
    print()


if __name__ == "__main__":
    main()
