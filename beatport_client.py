"""
beatport_client.py — Beatport direct API client for Oracle render_api.

Provides BeatportClient, BeatportAccountPool, parse_track_id, and
load_pool_from_file() — no external dependencies (stdlib urllib only).

Typical usage inside app.py:
    import beatport_client

    pool = beatport_client.load_pool_from_file(
        accounts_path="/path/to/beatport_accounts.json",
        cache_dir="/path/to/render_api/.beatport_tokens",
    )
    result = await asyncio.get_event_loop().run_in_executor(
        None, pool.get_track_with_download, track_id
    )
"""

import json
import re
import time
import urllib.parse
from pathlib import Path
from typing import Optional

import requests
import urllib3

# Suppress SSL warnings — Beatport's CDN cert triggers strict Python 3.13 checks.
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BASE_URL  = "https://api.beatport.com/v4"
CLIENT_ID = "ryZ8LuyQVPqbK2mBX2Hwt4qSMtnWuTYSqBPO92yQ"

QUALITY_MAP = {
    "lossless": "lossless",
    "high":     "high",
    "medium":   "medium",
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _token_cache_path(username: str, cache_dir: Path) -> Path:
    """Deterministic per-account token-cache filename inside *cache_dir*."""
    safe = re.sub(r"[^a-zA-Z0-9]", "_", username)[:20]
    return cache_dir / f".token_{safe}.json"


def parse_track_id(url: str) -> int:
    """
    Extract the numeric track ID from any Beatport track URL.

    Works for:
      /track/slug/12345
      /fr/track/slug/12345
      /catalog/tracks/12345
    """
    url   = url.split("?")[0].rstrip("/")
    parts = url.split("/")
    try:
        return int(parts[-1])
    except ValueError:
        raise ValueError(f"Could not extract track ID from: {url!r}")


# ---------------------------------------------------------------------------
# BeatportClient
# ---------------------------------------------------------------------------

class BeatportClient:
    def __init__(self, username: str, password: str, quality: str = "lossless",
                 cache_file: Optional[Path] = None):
        self.username    = username
        self.password    = password
        self.quality     = QUALITY_MAP.get(quality, "lossless")
        self._cache_file = cache_file
        self._token: Optional[dict] = None
        if cache_file is not None:
            self._load_token_cache()

    # ---- token cache -------------------------------------------------------

    def _load_token_cache(self):
        if self._cache_file and self._cache_file.exists():
            try:
                data = json.loads(self._cache_file.read_text())
                if data.get("issued_at", 0) + data.get("expires_in", 0) > time.time() + 60:
                    self._token = data
            except Exception:
                pass

    def _save_token_cache(self):
        if self._cache_file:
            try:
                self._cache_file.parent.mkdir(parents=True, exist_ok=True)
                self._cache_file.write_text(json.dumps(self._token, indent=2))
            except Exception:
                pass  # non-fatal

    def _token_valid(self) -> bool:
        if not self._token:
            return False
        expiry = self._token.get("issued_at", 0) + self._token.get("expires_in", 0)
        return time.time() < expiry - 60

    # ---- low-level HTTP (redirect-blocking) --------------------------------

    def _request(self, method: str, endpoint: str, payload=None,
                 content_type: str = "",
                 extra_headers: Optional[dict] = None) -> tuple[int, object, str]:
        """
        Returns (status_code, response_headers, body_str).

        Redirects are NOT followed so the auth-code Location header is readable.
        SSL verification is disabled (Beatport CDN cert fails Python 3.13 strict checks).
        """
        url  = BASE_URL + endpoint

        headers = {
            "accept":     "application/json",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/123.0.0.0 Safari/537.36",
        }
        if content_type:
            headers["content-type"] = content_type
        if self._token and "access_token" in self._token:
            headers["authorization"] = f"Bearer {self._token['access_token']}"
        if extra_headers:
            headers.update(extra_headers)

        # Determine body
        kwargs: dict = dict(headers=headers, timeout=15, verify=False,
                            allow_redirects=False)
        if payload is not None:
            if content_type == "application/json":
                kwargs["json"] = payload
            elif content_type == "application/x-www-form-urlencoded":
                kwargs["data"] = payload

        resp = requests.request(method, url, **kwargs)
        return resp.status_code, resp.headers, resp.text

    # ---- auth flows --------------------------------------------------------

    def _password_grant(self) -> bool:
        """Attempt OAuth2 password grant (fastest path, usually rejected)."""
        payload = {
            "client_id":  CLIENT_ID,
            "grant_type": "password",
            "username":   self.username,
            "password":   self.password,
        }
        status, _, body = self._request(
            "POST", "/auth/o/token/", payload,
            "application/x-www-form-urlencoded",
        )
        if status == 200:
            tok = json.loads(body)
            tok["issued_at"] = int(time.time())
            self._token = tok
            self._save_token_cache()
            return True
        return False

    def _auth_code_flow(self):
        """Full OAuth2 auth-code flow: POST login → GET authorize → POST token."""
        session = requests.Session()
        session.verify = False
        session.headers.update({
            "accept":     "application/json",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/123.0.0.0 Safari/537.36",
        })

        # 1. Login → sessionid cookie (stored automatically in session)
        r = session.post(
            BASE_URL + "/auth/login/",
            json={"username": self.username, "password": self.password},
            timeout=15,
        )
        session_id = session.cookies.get("sessionid")
        if not session_id:
            raise RuntimeError(
                f"Beatport login failed ({r.status_code}) for {self.username!r} — "
                f"no sessionid cookie. Body: {r.text[:200]}"
            )

        # 2. Authorize → Location header contains ?code=
        auth_url = (
            f"{BASE_URL}/auth/o/authorize/"
            f"?client_id={CLIENT_ID}&response_type=code"
        )
        r = session.get(auth_url, allow_redirects=False, timeout=15)
        location   = r.headers.get("Location", "")
        code_match = re.search(r"[?&]code=([^&]+)", location)
        if not code_match:
            raise RuntimeError(
                f"No auth code in Location header: {location!r} (status={r.status_code})"
            )
        code = code_match.group(1)

        # 3. Exchange code for tokens
        r = session.post(
            BASE_URL + "/auth/o/token/",
            data={"client_id": CLIENT_ID, "grant_type": "authorization_code", "code": code},
            timeout=15,
        )
        if r.status_code != 200:
            raise RuntimeError(
                f"Beatport token exchange failed ({r.status_code}) for {self.username!r}: "
                f"{r.text[:200]}"
            )
        tok = r.json()
        tok["issued_at"] = int(time.time())
        self._token = tok
        self._save_token_cache()

    def authenticate(self):
        if self._token_valid():
            return
        if not self._password_grant():
            self._auth_code_flow()

    def _refresh(self):
        if not self._token or "refresh_token" not in self._token:
            return self.authenticate()
        old = self._token
        self._token = None
        status, _, body = self._request(
            "POST", "/auth/o/token/",
            {
                "client_id":     CLIENT_ID,
                "grant_type":    "refresh_token",
                "refresh_token": old["refresh_token"],
            },
            "application/x-www-form-urlencoded",
        )
        if status == 200:
            tok = json.loads(body)
            tok["issued_at"] = int(time.time())
            self._token = tok
            self._save_token_cache()
        else:
            self._token = old
            self.authenticate()

    # ---- API helpers -------------------------------------------------------

    def _api_get(self, endpoint: str) -> tuple[int, str]:
        self.authenticate()
        status, _, body = self._request("GET", endpoint)
        if status == 401:
            self._refresh()
            status, _, body = self._request("GET", endpoint)
        return status, body

    @staticmethod
    def _api_error(status: int, body: str) -> str:
        err_map = {
            403: "Not available (not purchased / outside subscription)",
            404: "Track not found",
            401: "Authentication failed",
        }
        try:
            detail = json.loads(body).get("detail") or json.loads(body).get("error") or body[:200]
        except Exception:
            detail = body[:200]
        return err_map.get(status, f"HTTP {status}: {detail}")

    # ---- public API --------------------------------------------------------

    def get_track(self, track_id: int) -> dict:
        """
        Fetch full track metadata.

        Returns a dict with keys:
            success, id, name, mix_name, slug, artists, remixers,
            bpm, key, genre, subgenre, isrc, length, length_ms,
            publish_date, release{id,name,label,image_url},
            artwork_url, beatport_url, error
        """
        status, body = self._api_get(f"/catalog/tracks/{track_id}/")
        if status != 200:
            return {"success": False, "error": self._api_error(status, body)}

        t = json.loads(body)

        def _name(field):
            if isinstance(field, dict):
                return field.get("display", field.get("name", ""))
            return str(field) if field else ""

        def _artists(lst):
            return [
                {"id": a.get("id"), "name": _name(a.get("name", a.get("artist_name", "")))}
                for a in (lst or [])
            ]

        release = t.get("release") or {}
        label   = release.get("label") or {}
        image   = (t.get("release") or {}).get("image") or t.get("image") or {}

        return {
            "success":      True,
            "id":           t.get("id"),
            "name":         _name(t.get("name")),
            "mix_name":     _name(t.get("mix_name")),
            "slug":         t.get("slug", ""),
            "artists":      _artists(t.get("artists")),
            "remixers":     _artists(t.get("remixers")),
            "bpm":          t.get("bpm"),
            "key":          _name(t.get("key")) if isinstance(t.get("key"), dict) else str(t.get("key", "")),
            "genre":        _name(t.get("genre")),
            "subgenre":     _name(t.get("sub_genre")) if t.get("sub_genre") else None,
            "isrc":         t.get("isrc", ""),
            "length":       t.get("length", ""),
            "length_ms":    t.get("length_ms", 0),
            "publish_date": t.get("publish_date", ""),
            "release": {
                "id":        release.get("id"),
                "name":      _name(release.get("name")),
                "label":     _name(label.get("name")) if isinstance(label, dict) else str(label),
                "image_url": image.get("uri") or image.get("url") or image.get("dynamic_uri", ""),
            },
            "beatport_url": t.get("url", f"https://www.beatport.com/track/{t.get('slug', '')}/{track_id}"),
            "error":        None,
        }

    def get_download_url(self, track_id: int) -> dict:
        """
        Return the direct CDN download URL for a track (no actual download).

        Returns:
            {"success": bool, "location": str|None, "stream_quality": str|None, "error": str|None}
        """
        status, body = self._api_get(
            f"/catalog/tracks/{track_id}/download/?quality={urllib.parse.quote(self.quality)}"
        )
        if status == 200:
            data = json.loads(body)
            return {
                "success":        True,
                "location":       data.get("location"),
                "stream_quality": data.get("stream_quality"),
                "error":          None,
            }
        return {
            "success":        False,
            "location":       None,
            "stream_quality": None,
            "error":          self._api_error(status, body),
        }

    def get_track_with_download(self, track_id: int) -> dict:
        """
        Fetch metadata + CDN URL in two parallel threads.

        Returns the metadata dict with extra keys: download_url, stream_quality.
        """
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            f_meta = pool.submit(self.get_track, track_id)
            f_dl   = pool.submit(self.get_download_url, track_id)
            meta   = f_meta.result()
            dl     = f_dl.result()

        if not meta["success"]:
            return {**meta, "download_url": None, "stream_quality": None}
        if not dl["success"]:
            return {**meta, "success": False, "download_url": None,
                    "stream_quality": None, "error": dl["error"]}
        return {**meta, "download_url": dl["location"], "stream_quality": dl["stream_quality"]}


# ---------------------------------------------------------------------------
# BeatportAccountPool
# ---------------------------------------------------------------------------

class BeatportAccountPool:
    """
    Wraps multiple BeatportClient instances and rotates between them.

    strategy = 'round_robin' : advance cursor on every call
    strategy = 'fallback'    : always start from account 0

    Accounts that return 403/401 are cooled down for *cooldown* seconds.
    """

    def __init__(self, clients: list, strategy: str = "round_robin",
                 cooldown: int = 60):
        if not clients:
            raise ValueError("BeatportAccountPool requires at least one account")
        self.clients   = clients
        self.strategy  = strategy
        self.cooldown  = cooldown
        self.total     = len(clients)
        self._index    = 0
        self._cooldowns: dict = {}  # idx → available_at timestamp

    # ---- internal scheduling -----------------------------------------------

    def _is_available(self, idx: int) -> bool:
        return time.time() >= self._cooldowns.get(idx, 0)

    def _mark_cooldown(self, idx: int):
        self._cooldowns[idx] = time.time() + self.cooldown

    def _next_index(self) -> Optional[int]:
        n = len(self.clients)
        if self.strategy == "fallback":
            for i in range(n):
                if self._is_available(i):
                    return i
            return None
        # round_robin
        for offset in range(n):
            idx = (self._index + offset) % n
            if self._is_available(idx):
                self._index = (idx + 1) % n
                return idx
        return None

    def _pick(self) -> tuple:
        idx = self._next_index()
        if idx is None:
            raise RuntimeError(
                "All Beatport accounts are in cooldown — try again later"
            )
        return idx, self.clients[idx]

    # ---- status ------------------------------------------------------------

    def status(self) -> dict:
        """Return a compact JSON-serialisable summary (for the /beatport/accounts endpoint)."""
        available = sum(1 for i in range(len(self.clients)) if self._is_available(i))
        cooling   = len(self.clients) - available
        return {
            "total":            len(self.clients),
            "available":        available,
            "cooling":          cooling,
            "strategy":         self.strategy,
            "cooldown_seconds": self.cooldown,
        }

    # ---- prewarm ----------------------------------------------------------

    def prewarm(self, max_workers: int = 20) -> dict:
        """
        Authenticate all accounts in parallel so the first real request
        never has to wait for an OAuth flow.

        Accounts that already have a valid cached token are skipped (free).
        Returns {"ok": N, "skipped": N, "failed": N}.
        """
        import concurrent.futures

        def _auth_one(client: BeatportClient) -> str:
            if client._token_valid():
                return "skipped"
            try:
                client.authenticate()
                return "ok"
            except Exception:
                return "failed"

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as pool:
            results = list(pool.map(_auth_one, self.clients))

        counts = {"ok": results.count("ok"),
                  "skipped": results.count("skipped"),
                  "failed": results.count("failed")}
        return counts

    # ---- public API -------------------------------------------------------

    def get_track(self, track_id: int) -> dict:
        idx, client = self._pick()
        res = client.get_track(track_id)
        if not res["success"] and "auth" in (res.get("error") or "").lower():
            self._mark_cooldown(idx)
        return res

    def get_download_url(self, track_id: int) -> dict:
        """Try available accounts in sequence until one returns the CDN URL."""
        tried = 0
        while tried < len(self.clients):
            idx, client = self._pick()
            res = client.get_download_url(track_id)
            tried += 1
            if res["success"]:
                return res
            err = res.get("error", "")
            # Rotate on any account-level restriction or auth/quality error.
            if any(x in err for x in ("400", "403", "401", "auth", "subscription",
                                      "purchased", "quality", "not available")):
                self._mark_cooldown(idx)          # standard cooldown, retry after cooldown_seconds
                continue
            # Hard errors (404 track not found, network) — return immediately
            return res
        return {
            "success":        False,
            "location":       None,
            "stream_quality": None,
            "error":          "All accounts failed or are in cooldown",
        }

    def get_track_with_download(self, track_id: int) -> dict:
        """Fetch metadata once; rotate accounts for the download URL."""
        import concurrent.futures
        idx_meta, client_meta = self._pick()
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            f_meta = pool.submit(client_meta.get_track, track_id)
            f_dl   = pool.submit(self.get_download_url, track_id)
            meta   = f_meta.result()
            dl     = f_dl.result()

        if not meta["success"]:
            return {**meta, "download_url": None, "stream_quality": None}
        if not dl["success"]:
            return {**meta, "success": False, "download_url": None,
                    "stream_quality": None, "error": dl["error"]}
        return {
            **meta,
            "download_url":   dl["location"],
            "stream_quality": dl["stream_quality"],
        }


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

def load_pool_from_file(
    accounts_path: str,
    cache_dir: Optional[str] = None,
    strategy: str = "round_robin",
    cooldown: int = 60,
    default_quality: str = "lossless",
) -> BeatportAccountPool:
    """
    Build a BeatportAccountPool from a JSON accounts file.

    accounts_path : path to beatport_accounts.json
                    Format: [{"username": "...", "password": "...", "quality": "lossless"}, ...]
    cache_dir     : directory for per-account token cache files.
                    Defaults to the same directory as accounts_path.
    strategy      : "round_robin" | "fallback"
    cooldown      : seconds to skip an account after a 403/401
    default_quality : quality to use when not specified per-account

    Raises FileNotFoundError / ValueError on bad input.
    """
    path = Path(accounts_path)
    if not path.exists():
        raise FileNotFoundError(f"Beatport accounts file not found: {path}")

    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, list) or not raw:
        raise ValueError("beatport_accounts.json must be a non-empty JSON array")

    _cache_dir = Path(cache_dir) if cache_dir else path.parent
    _cache_dir.mkdir(parents=True, exist_ok=True)

    clients = []
    for entry in raw:
        username = entry.get("username", "").strip()
        password = entry.get("password", "").strip()
        quality  = entry.get("quality", default_quality)
        if not username or not password:
            raise ValueError(
                f"Each entry in beatport_accounts.json needs 'username' and 'password'. "
                f"Bad entry: {entry}"
            )
        cache_file = _token_cache_path(username, _cache_dir)
        clients.append(BeatportClient(username, password, quality, cache_file))

    return BeatportAccountPool(clients, strategy=strategy, cooldown=cooldown)
