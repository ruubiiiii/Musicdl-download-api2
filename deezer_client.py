"""
deezer_client.py — Deezer direct API client for Oracle render_api.

Provides DeezerClient, DeezerAccountPool, parse_track_id, and
load_pool_from_file() — uses pycryptodome for Blowfish decryption.

Auth flow (two modes):
  A. Email + password (auto-login, recommended):
     1. POST gw-light deezer.getUserData (anon) → checkForm
     2. POST gw-light user.login → logged-in checkForm
     3. POST gw-light user.getArl → ARL token (cached ~80 days)
  B. ARL directly (manual, grab from browser cookies):
     1. GET deezer.com → extract sid cookie
     2. POST gw-light deezer.getUserData with arl cookie → api_token + license_token
     3. Tokens cached per-account in .deezer_tokens/ (~1 h lifetime)

Accounts file supports both formats (see deezer_accounts.template.json).

Download flow per track:
  1. POST gw-light.php song.getData with track_id → TRACK_TOKEN
  2. POST media.deezer.com/v1/get_url → encrypted CDN URL
  3. Stream + decrypt on-the-fly (Blowfish CBC, every 3rd 2048-byte chunk)
  4. Return raw FLAC bytes (caller streams to R2 or disk)

Typical usage inside app.py:
    import deezer_client

    pool = deezer_client.load_pool_from_file(
        accounts_path="/path/to/deezer_accounts.json",
        cache_dir="/path/to/render_api/.deezer_tokens",
    )
    result = await asyncio.get_event_loop().run_in_executor(
        None, pool.get_track, track_id
    )
"""

from __future__ import annotations

import hashlib
import io
import json
import math
import random
import re
import threading
import time
from pathlib import Path
from typing import Optional

import requests
import urllib3
from Crypto.Cipher import AES, Blowfish

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ---------------------------------------------------------------------------
# Deezer constants  (same values as credentials.rs)
# ---------------------------------------------------------------------------

GW_LIGHT_URL   = "https://www.deezer.com/ajax/gw-light.php"
MEDIA_URL      = "https://media.deezer.com/v1/get_url"
DEEZER_API_URL = "https://api.deezer.com"
BLOWFISH_SECRET = "g4el58wc0zvf9na1"

# Mobile API (Android) — used for email/password login
MOBILE_API_URL = "https://api.deezer.com/1.0/gateway.php"
MOBILE_API_KEY = "4VCYIJUCDLOUELGD1V8WBVYBNVDYOXEWSLLZDONGBBDFVXTZJRXPR29JRLQFO6ZE"
# AES-128-ECB key for decrypting the mobile_auth challenge token
MOBILE_SECRET  = bytes([0x56,0x42,0x4B,0x31,0x46,0x53,0x55,0x45,
                         0x58,0x48,0x54,0x53,0x44,0x42,0x4A,0x4A])
MOBILE_UA      = "Deezer/8.0.50.5 (Android; 12; Mobile; us) samsung SM-G977N"

# Format preference order — first available wins
# "FLAC" = lossless 16-bit,  "MP3_320" = lossy 320 kbps,  "MP3_128" = lossy 128 kbps
QUALITY_FORMATS: dict[str, list[dict]] = {
    "lossless": [
        {"cipher": "BF_CBC_STRIPE", "format": "FLAC"},
        {"cipher": "BF_CBC_STRIPE", "format": "MP3_320"},
        {"cipher": "BF_CBC_STRIPE", "format": "MP3_128"},
    ],
    "high": [
        {"cipher": "BF_CBC_STRIPE", "format": "MP3_320"},
        {"cipher": "BF_CBC_STRIPE", "format": "MP3_128"},
    ],
    "medium": [
        {"cipher": "BF_CBC_STRIPE", "format": "MP3_128"},
    ],
}

FORMAT_EXTENSIONS = {
    "FLAC":    "flac",
    "MP3_320": "mp3",
    "MP3_128": "mp3",
}

# How long a cached license_token stays valid (Deezer refreshes them ~hourly)
TOKEN_TTL_SECONDS = 3300  # 55 min — slightly under 1 h to be safe

_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _token_cache_path(arl: str, cache_dir: Path) -> Path:
    """Deterministic per-account token-cache filename."""
    key = hashlib.md5(arl.encode()).hexdigest()[:12]
    return cache_dir / f"deezer_{key}.json"


def parse_track_id(url: str) -> int:
    """
    Extract numeric track ID from any Deezer track URL or short link.

    Accepts:
      https://www.deezer.com/us/track/1234567
      https://www.deezer.com/track/1234567
      https://link.deezer.com/s/...   (short link — resolved via redirect)
      1234567   (raw numeric string)
    """
    try:
        return int(url)
    except ValueError:
        pass
    m = re.search(r"/track/(\d+)", url)
    if m:
        return int(m.group(1))
    # Resolve Deezer short links (link.deezer.com/s/...)
    if "link.deezer.com" in url:
        try:
            import urllib.parse
            r = requests.get(url, allow_redirects=False, timeout=10, verify=False)
            location = urllib.parse.unquote(r.headers.get("Location", ""))
            m = re.search(r"/track/(\d+)", location)
            if m:
                return int(m.group(1))
        except Exception as e:
            raise ValueError(f"Could not resolve Deezer short link {url!r}: {e}") from e
    raise ValueError(f"Cannot extract Deezer track ID from: {url!r}")


def _get_blowfish_key(track_id: str) -> bytes:
    """Derive the 16-byte Blowfish key for a given track ID."""
    h = hashlib.md5(track_id.encode()).hexdigest()
    secret = BLOWFISH_SECRET.encode()
    return bytes(
        ord(h[i]) ^ ord(h[i + 16]) ^ secret[i % len(secret)]
        for i in range(16)
    )


def _decrypt_chunk(chunk: bytes, track_id: str) -> bytes:
    """Decrypt a single 2048-byte Deezer chunk using Blowfish CBC."""
    key = _get_blowfish_key(track_id)
    cipher = Blowfish.new(key, Blowfish.MODE_CBC, iv=b"\x00\x01\x02\x03\x04\x05\x06\x07")
    # pycryptodome requires block-aligned input (Blowfish block = 8 bytes)
    # 2048 is a multiple of 8, so no padding needed
    return cipher.decrypt(chunk)


def _decrypt_stream(encrypted_bytes: bytes, track_id: str) -> bytes:
    """
    Decrypt a full Deezer encrypted stream.

    The stream is split into 2048-byte chunks.
    Every 3rd chunk (index 0, 3, 6 …) is Blowfish-CBC encrypted.
    """
    CHUNK = 2048
    out = io.BytesIO()
    total = len(encrypted_bytes)

    for i, offset in enumerate(range(0, total, CHUNK)):
        chunk = encrypted_bytes[offset : offset + CHUNK]
        if i % 3 == 0 and len(chunk) == CHUNK:
            chunk = _decrypt_chunk(chunk, track_id)
        out.write(chunk)

    return out.getvalue()


# ---------------------------------------------------------------------------
# Legacy CDN URL builder — bypasses media endpoint token rights entirely
# ---------------------------------------------------------------------------

# Deezer format IDs for the legacy CDN URL path
# 1 = FLAC, 3 = MP3_320, 5 = MP3_256, 9 = MP3_128
_LEGACY_FORMAT_NUMBERS = {"FLAC": 1, "MP3_320": 3, "MP3_128": 9}
_LEGACY_AES_KEY = b"jo6aey6haid2Teih"


def _get_encrypted_file_url(sng_id: str, md5_origin: str, media_version: str, fmt: str) -> str:
    """
    Build a Deezer legacy CDN URL directly from track metadata.
    Does NOT require a valid TRACK_TOKEN — works as long as the ARL is active.
    Used as fallback when the media endpoint returns error 2002.

    Credit: streamrip / deemix reverse engineering.
    """
    import binascii
    format_number = _LEGACY_FORMAT_NUMBERS.get(fmt, 1)
    url_bytes = b"\xa4".join([
        md5_origin.encode(),
        str(format_number).encode(),
        sng_id.encode(),
        media_version.encode(),
    ])
    url_hash   = hashlib.md5(url_bytes).hexdigest()
    info_bytes = bytearray(url_hash.encode())
    info_bytes.extend(b"\xa4")
    info_bytes.extend(url_bytes)
    info_bytes.extend(b"\xa4")
    # Pad to 16-byte boundary
    padding = 16 - (len(info_bytes) % 16)
    info_bytes.extend(b"." * padding)
    path = binascii.hexlify(
        AES.new(_LEGACY_AES_KEY, AES.MODE_ECB).encrypt(bytes(info_bytes))
    ).decode()
    return f"https://e-cdns-proxy-{md5_origin[0]}.dzcdn.net/mobile/1/{path}"


def _resolve_via_google_dns(hostname: str) -> Optional[str]:
    """
    Resolve *hostname* by sending a minimal DNS A-query directly to Google
    (8.8.8.8) over UDP.  Works when the system DNS refuses or can't find
    Deezer CDN subdomains.  Returns the first A-record IP, or None on failure.
    """
    import socket
    import struct
    try:
        txid   = random.randint(0, 65535)
        labels = hostname.encode().split(b".")
        qname  = b"".join(bytes([len(lbl)]) + lbl for lbl in labels) + b"\x00"
        packet = (
            struct.pack("!HHHHHH", txid, 0x0100, 1, 0, 0, 0)  # header
            + qname + b"\x00\x01\x00\x01"                      # QTYPE A, QCLASS IN
        )
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.settimeout(5)
        sock.sendto(packet, ("8.8.8.8", 53))
        data, _ = sock.recvfrom(512)
        sock.close()
        # Skip header (12 bytes) + question section
        offset = 12
        while offset < len(data):
            if data[offset] == 0:
                offset += 1; break
            if data[offset] & 0xC0 == 0xC0:
                offset += 2; break
            offset += data[offset] + 1
        offset += 4  # QTYPE + QCLASS
        # Walk answer records
        an_count = struct.unpack("!H", data[6:8])[0]
        for _ in range(an_count):
            if data[offset] & 0xC0 == 0xC0:
                offset += 2
            else:
                while data[offset] != 0:
                    offset += data[offset] + 1
                offset += 1
            rtype, _rclass, _ttl, rdlen = struct.unpack("!HHIH", data[offset:offset + 10])
            offset += 10
            if rtype == 1 and rdlen == 4:  # A record
                return ".".join(str(b) for b in data[offset:offset + 4])
            offset += rdlen
        return None
    except Exception:
        return None


def _get_with_dns_fallback(
    session: requests.Session,
    url: str,
    **kwargs,
) -> requests.Response:
    """
    requests.get wrapper that retries with Google DNS when the system
    resolver can't find Deezer CDN hostnames (common with some ISPs).
    """
    from urllib.parse import urlparse
    try:
        return session.get(url, **kwargs)
    except requests.exceptions.ConnectionError as exc:
        if "getaddrinfo" not in str(exc) and "NameResolution" not in str(exc):
            raise
        parsed   = urlparse(url)
        hostname = parsed.hostname
        ip       = _resolve_via_google_dns(hostname)
        if not ip:
            raise
        ip_url   = url.replace(f"://{hostname}", f"://{ip}", 1)
        headers  = dict(kwargs.pop("headers", {}) or {})
        headers["Host"] = hostname
        return session.get(ip_url, headers=headers, verify=False, **kwargs)


# ---------------------------------------------------------------------------
# Email/password → ARL login  (Deezer Android mobile API flow)
# ---------------------------------------------------------------------------

def _mobile_request(
    session: requests.Session,
    method: str,
    body: Optional[str] = None,
    sid: Optional[str] = None,
    extra_qs: str = "",
    include_input: bool = False,
) -> dict:
    """
    Call the Deezer mobile gateway and return the parsed JSON body.
    GET if body is None, POST otherwise.

    extra_qs params are inserted BEFORE &method= (matching the working URL order).
    include_input adds &input=3 (required for some methods, not for mobile_auth).
    """
    parts = [f"api_key={MOBILE_API_KEY}", "output=3"]
    if extra_qs:
        parts.append(extra_qs)
    if include_input:
        parts.append("input=3")
    parts.append(f"method={method}")
    if sid:
        parts.append(f"sid={sid}")

    url = f"{MOBILE_API_URL}?" + "&".join(parts)
    headers = {
        "User-Agent":      MOBILE_UA,
        "Accept-Encoding": "gzip",
        "Connection":      "Keep-Alive",
        "Host":            "api.deezer.com",
        "Content-Type":    "application/json",
    }
    if body is None:
        resp = session.get(url, headers=headers, timeout=15)
    else:
        resp = session.post(url, data=body, headers=headers, timeout=15)
    resp.raise_for_status()
    return resp.json()


def _gw_call_raw(
    session: requests.Session,
    method: str,
    params: dict,
    api_token: str = "",
) -> dict:
    """Standalone GW-Light call (web API) — used to fetch ARL after mobile login."""
    url = (
        f"{GW_LIGHT_URL}"
        f"?method={method}"
        f"&input=3"
        f"&api_version=1.0"
        f"&api_token={api_token}"
        f"&cid={random.randint(0, 10**9)}"
    )
    resp = session.post(url, json=params, timeout=15)
    resp.raise_for_status()
    body = resp.json()
    if "error" in body and body["error"]:
        raise RuntimeError(f"GW-Light error ({method}): {body['error']}")
    return body.get("results", body)


def _arl_cache_path(email: str, cache_dir: Path) -> Path:
    key = hashlib.md5(email.lower().encode()).hexdigest()[:12]
    return cache_dir / f"deezer_login_{key}.json"


def get_arl_from_login(
    email: str,
    password: str,
    cache_dir: Optional[Path] = None,
    force_refresh: bool = False,
) -> str:
    """
    Log into Deezer with email + password using the Android mobile API and
    return the ARL token (used for all subsequent lossless downloads).

    ARL is cached to disk for ~80 days (Deezer rotates them ~every 3 months).
    Set force_refresh=True to bypass the cache after a password change.

    Mobile API login flow:
      1. GET  mobile_auth            → encrypted TOKEN
      2. AES-128-ECB decrypt TOKEN   → tokenString / tokenKey / userInfoKey
      3. AES-128-ECB re-encrypt      → auth_token
      4. GET  api_checkToken         → sid (session ID)
      5. POST user_checkRegisterConstraints → verify email exists
      6. AES-128-ECB encrypt password → encrypted_password
      7. POST mobile_userAuth        → USER_TOKEN (authenticated session)
      8. GET  user.getArl (gw-light) → ARL cookie token
    """
    cache_dir = cache_dir or Path(".deezer_tokens")
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_path = _arl_cache_path(email, cache_dir)

    # --- Try cached ARL first ---
    if not force_refresh:
        try:
            cached    = json.loads(cache_path.read_text())
            arl       = cached.get("arl", "")
            age_days  = (time.time() - cached.get("saved_at", 0)) / 86400
            if arl and age_days < 80:
                return arl
        except Exception:
            pass

    session = requests.Session()
    session.headers.update({"Accept-Encoding": "gzip"})

    # ── Step 1: get challenge TOKEN ────────────────────────────────────────
    auth_resp  = _mobile_request(
        session,
        "mobile_auth",
        extra_qs="version=8.0.50.5&lang=us",
    )
    token_hex  = auth_resp.get("results", {}).get("TOKEN", "")
    if not token_hex:
        raise RuntimeError(f"mobile_auth returned no TOKEN. Response: {auth_resp}")

    # ── Step 2: decrypt TOKEN → tokenString / tokenKey / userInfoKey ──────
    token_bytes  = bytes.fromhex(token_hex)
    decrypted    = AES.new(MOBILE_SECRET, AES.MODE_ECB).decrypt(token_bytes)
    token_string = decrypted[0:64]
    token_key    = decrypted[64:80]
    user_info_key= decrypted[80:96]

    # ── Step 3: re-encrypt tokenString with tokenKey → auth_token ─────────
    auth_token   = AES.new(token_key, AES.MODE_ECB).encrypt(token_string).hex()

    # ── Step 4: exchange auth_token for a session ID (sid) ────────────────
    check_resp   = _mobile_request(
        session,
        "api_checkToken",
        extra_qs=f"auth_token={auth_token}",
    )
    sid = check_resp.get("results", "")
    if not sid or not isinstance(sid, str):
        raise RuntimeError(f"api_checkToken returned unexpected sid: {sid!r}")

    # ── Step 5: verify the email is registered (optional safety check) ─────
    reg_resp = _mobile_request(
        session,
        "user_checkRegisterConstraints",
        body=json.dumps({"EMAIL": email}),
        sid=sid,
        include_input=True,
    )
    reg_src = json.dumps(reg_resp)
    if "email_not_valid" in reg_src or '"suggestions":{"email":"' in reg_src:
        raise RuntimeError(
            f"Deezer email not recognised: {email!r}. "
            "Check the address or register at deezer.com first."
        )
    if "email_already_used" not in reg_src:
        # unexpected but non-fatal — proceed anyway
        print(f"  ⚠  user_checkRegisterConstraints unexpected response: {reg_src[:120]}")

    # ── Step 6: encrypt password ──────────────────────────────────────────
    # Take UTF-8 hex of password, pad/truncate to exactly 32 hex chars (16 bytes)
    pw_hex   = password.encode("utf-8").hex().lower().ljust(32, "0")[:32]
    pw_bytes = bytes.fromhex(pw_hex)
    encrypted_password = AES.new(user_info_key, AES.MODE_ECB).encrypt(pw_bytes).hex().lower()

    # ── Step 7: authenticate ──────────────────────────────────────────────
    auth_body = json.dumps({"mail": email, "password": encrypted_password})
    user_resp = _mobile_request(
        session,
        "mobile_userAuth",
        body=auth_body,
        sid=sid,
        include_input=True,
    )
    results = user_resp.get("results", {})
    if isinstance(results, dict) and results.get("USER_AUTH_ERROR"):
        raise RuntimeError(
            f"Deezer mobile_userAuth failed: {results['USER_AUTH_ERROR']}"
        )
    if not isinstance(results, dict) or not results.get("USER_TOKEN"):
        # Some error responses contain an error key
        err = user_resp.get("error")
        raise RuntimeError(
            f"mobile_userAuth failed — wrong password? "
            f"error={err!r} results={str(results)[:200]}"
        )

    user_token = results["USER_TOKEN"]
    print(f"  ✓ Deezer mobile auth OK for {email} (USER_TOKEN obtained)")

    # ── Step 8: exchange mobile session for web ARL ───────────────────────
    # Seed a web session using the mobile sid, then call user.getArl
    web_session = requests.Session()
    web_session.headers.update({
        "User-Agent": _UA,
        "Origin":     "https://www.deezer.com",
        "Referer":    "https://www.deezer.com/",
    })
    # The mobile sid is usable on the web API
    web_session.cookies.set("sid", sid, domain=".deezer.com")

    # Get a checkForm token
    user_data  = _gw_call_raw(web_session, "deezer.getUserData", {}, api_token="")
    web_uid    = user_data.get("USER", {}).get("USER_ID", 0)
    check_form = user_data.get("checkForm", "")

    # If the web session isn't logged in, fall back to user.getArl via mobile USER_TOKEN
    if not web_uid or not check_form:
        raise RuntimeError(
            "Could not establish web session from mobile sid. "
            f"web_uid={web_uid!r}"
        )

    arl_result = _gw_call_raw(web_session, "user.getArl", {}, api_token=check_form)
    if isinstance(arl_result, str):
        arl = arl_result
    elif isinstance(arl_result, dict):
        arl = arl_result.get("ARL") or arl_result.get("arl", "")
    else:
        raise RuntimeError(f"Unexpected user.getArl response: {arl_result!r}")

    if not arl or len(arl) < 100:
        raise RuntimeError(
            f"Received invalid ARL (len={len(str(arl))}). "
            "Account may lack required permissions."
        )

    # Cache to disk
    try:
        cache_path.write_text(json.dumps({
            "email":    email,
            "arl":      arl,
            "saved_at": time.time(),
        }))
    except Exception:
        pass

    print(f"  ✓ Deezer ARL obtained for {email} ({len(arl)} chars) — cached")
    return arl


# ---------------------------------------------------------------------------
# DeezerClient — single ARL account
# ---------------------------------------------------------------------------

class DeezerClient:
    """
    A single authenticated Deezer session.

    Authenticates via ARL cookie (no email/password needed).
    Caches api_token + license_token to *cache_dir*.
    """

    def __init__(self, arl: str, cache_dir: Optional[Path] = None):
        if not arl or arl.startswith("<"):
            raise ValueError("ARL token must not be empty or a placeholder.")
        self.arl = arl
        self.cache_dir = cache_dir or Path(".deezer_tokens")
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        self._session = requests.Session()
        self._session.verify = False
        self._session.headers.update({
            "User-Agent":   _UA,
            "Content-Type": "text/plain;charset=UTF-8",
            "Origin":        "https://www.deezer.com",
            "Referer":       "https://www.deezer.com/",
        })

        # Populated by _authenticate()
        self._api_token: Optional[str]     = None
        self._license_token: Optional[str] = None
        self._authenticated_at: float      = 0.0
        self._lock = threading.Lock()

        # Try loading cached tokens first
        self._load_cached_tokens()

    # ------------------------------------------------------------------
    # Authentication
    # ------------------------------------------------------------------

    def _load_cached_tokens(self) -> bool:
        path = _token_cache_path(self.arl, self.cache_dir)
        try:
            data = json.loads(path.read_text())
            age  = time.monotonic() - data.get("saved_at_monotonic", 0)
            if age < TOKEN_TTL_SECONDS:
                self._api_token             = data["api_token"]
                self._license_token         = data["license_token"]
                self._authenticated_at      = time.monotonic() - age
                # Restore cookie jar
                sid = data.get("sid")
                if sid:
                    self._session.cookies.set("arl", self.arl, domain=".deezer.com")
                    self._session.cookies.set("sid", sid,      domain=".deezer.com")
                return True
        except Exception:
            pass
        return False

    def _save_cached_tokens(self, sid: str) -> None:
        path = _token_cache_path(self.arl, self.cache_dir)
        try:
            path.write_text(json.dumps({
                "api_token":           self._api_token,
                "license_token":       self._license_token,
                "sid":                 sid,
                "saved_at_monotonic":  time.monotonic(),
            }))
        except Exception:
            pass

    def _authenticate(self) -> None:
        """
        Full ARL-based auth flow:
          1. GET deezer.com → extract sid from Set-Cookie
          2. POST gw-light deezer.getUserData with arl cookie
          3. Extract checkForm (api_token) + license_token
        """
        # Step 1: get sid
        resp = self._session.get("https://www.deezer.com/", timeout=15)
        resp.raise_for_status()
        sid_match = re.search(r"sid=(fr[0-9a-f]+)", resp.headers.get("Set-Cookie", ""))
        sid = sid_match.group(1) if sid_match else ""

        # Step 2: set cookies
        self._session.cookies.set("arl", self.arl, domain=".deezer.com")
        if sid:
            self._session.cookies.set("sid", sid, domain=".deezer.com")

        # Step 3: getUserData
        data = self._gw_call("deezer.getUserData", {}, api_token="")
        user_id = data.get("USER", {}).get("USER_ID", 0)
        if user_id == 0:
            raise RuntimeError("Invalid ARL — Deezer returned USER_ID=0. Token may be expired.")

        opts = data.get("USER", {}).get("OPTIONS", {})
        if not opts.get("web_lossless", False):
            print(f"  ⚠  ARL {self.arl[:8]}… — account does NOT have lossless enabled "
                  "(no 'web_lossless'). FLAC downloads will likely fail.")

        self._api_token        = data.get("checkForm", "")
        self._license_token    = opts.get("license_token", "")
        self._authenticated_at = time.monotonic()

        self._save_cached_tokens(sid)
        print(f"  ✓ Deezer authenticated: USER_ID={user_id}, "
              f"lossless={opts.get('web_lossless')}, hq={opts.get('web_hq')}, "
              f"license_token={'OK (' + str(len(self._license_token)) + ' chars)' if self._license_token else '⚠ EMPTY'}")

    def _ensure_authenticated(self) -> None:
        with self._lock:
            age = time.monotonic() - self._authenticated_at
            if self._api_token is None or age > TOKEN_TTL_SECONDS:
                self._authenticate()

    # ------------------------------------------------------------------
    # Internal GW-Light API call
    # ------------------------------------------------------------------

    def _gw_call(self, method: str, params: dict, api_token: Optional[str] = None) -> dict:
        """POST to gw-light.php and return response['results']."""
        token = self._api_token if api_token is None else api_token
        url = (
            f"{GW_LIGHT_URL}"
            f"?method={method}"
            f"&input=3"
            f"&api_version=1.0"
            f"&api_token={token or ''}"
            f"&cid={random.randint(0, 10**9)}"
        )
        resp = self._session.post(url, json=params, timeout=15)
        resp.raise_for_status()
        body = resp.json()
        if "error" in body and body["error"]:
            raise RuntimeError(f"GW-Light error for {method}: {body['error']}")
        return body.get("results", body)

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def get_track_metadata(self, track_id: int) -> dict:
        """Fetch track info from the public REST API (no auth required)."""
        resp = self._session.get(f"{DEEZER_API_URL}/track/{track_id}", timeout=10)
        resp.raise_for_status()
        return resp.json()

    def get_track_download(
        self,
        track_id: int,
        quality: str = "lossless",
    ) -> dict:
        """
        Fetch the decrypted audio bytes for a track.

        Returns:
          {
            "success":     True,
            "track_id":    123,
            "title":       "...",
            "artist":      "...",
            "isrc":        "...",
            "format":      "FLAC" | "MP3_320" | "MP3_128",
            "extension":   "flac" | "mp3",
            "audio_bytes": b"...",   # raw decrypted file bytes
          }
        """
        self._ensure_authenticated()

        # ── 1. Single GW call — song.getData returns FALLBACK.TRACK_TOKEN inline ──
        raw = self._gw_call("song.getData", {"sng_id": str(track_id)})

        # Build candidate list: [(sng_id_str, track_token, track_data), ...]
        # FALLBACK already contains its own TRACK_TOKEN — no extra round-trip needed.
        candidates: list[tuple[str, str, dict]] = []
        for src in (raw, raw.get("FALLBACK") or {}):
            sid   = str(src.get("SNG_ID", ""))
            token = src.get("TRACK_TOKEN", "")
            if sid and token and (sid, token) not in [(c[0], c[1]) for c in candidates]:
                candidates.append((sid, token, src))

        if not candidates:
            raise RuntimeError(f"No TRACK_TOKEN for track {track_id}.")

        # ── 2. Try each candidate — batch all formats in ONE media POST per token ──
        formats   = QUALITY_FORMATS.get(quality, QUALITY_FORMATS["lossless"])
        cdn_url:       Optional[str] = None
        chosen_format: Optional[str] = None
        winning_sng:   str           = ""

        for sng_id_str, track_token, _ in candidates:
            payload = {
                "license_token": self._license_token,
                "media": [{"type": "FULL", "formats": formats}],
                "track_tokens": [track_token],
            }
            resp_data = self._session.post(MEDIA_URL, json=payload, timeout=15).json()
            media_list = (resp_data.get("data") or [{}])[0].get("media") or []
            if media_list:
                # API returns media entries in preference order — pick first
                cdn_url       = media_list[0]["sources"][0]["url"]
                chosen_format = media_list[0]["format"]
                winning_sng   = sng_id_str
                break
            # Check whether it's a hard error (not just rights/geo) — if so, stop
            errs  = (resp_data.get("errors")
                     or (resp_data.get("data") or [{}])[0].get("errors") or [])
            codes = {e.get("code") for e in errs}
            if errs and not codes.issubset({2002, 2009}):
                raise RuntimeError(
                    f"Deezer media endpoint error for track {track_id}: {errs}"
                )

        if not cdn_url or not chosen_format:
            raise RuntimeError(
                f"No CDN URL for track {track_id} "
                f"(tried {len(candidates)} token(s), formats: {[f['format'] for f in formats]}). "
                "Track may be unavailable for subscription streaming."
            )

        # ── 3. Download ──
        dl_resp = self._session.get(cdn_url, timeout=60, stream=True)
        dl_resp.raise_for_status()
        encrypted = b"".join(dl_resp.iter_content(chunk_size=65536))

        # ── 4. Decrypt (Blowfish key derived from winning SNG_ID) ──
        audio_bytes = _decrypt_stream(encrypted, winning_sng)

        # ── 5. Public metadata ──
        meta = self.get_track_metadata(track_id)

        return {
            "success":     True,
            "track_id":    track_id,
            "title":       meta.get("title", ""),
            "artist":      meta.get("artist", {}).get("name", ""),
            "album":       meta.get("album", {}).get("title", ""),
            "isrc":        meta.get("isrc", ""),
            "duration":    meta.get("duration", 0),
            "format":      chosen_format,
            "extension":   FORMAT_EXTENSIONS[chosen_format],
            "audio_bytes": audio_bytes,
        }

    def get_direct_url(self, track_id: int, quality: str = "lossless") -> dict:
        """
        Return the encrypted CDN URL without downloading.

        Useful for streaming directly to R2 without buffering on Oracle.
        Note: the stream is still Blowfish-encrypted — caller must decrypt.
        Prefer get_track_download() which handles decryption automatically.

        Returns:
          {
            "success":     True,
            "cdn_url":     "https://...",
            "format":      "FLAC",
            "extension":   "flac",
            "track_id":    123,
            "title":       "...",
            "artist":      "...",
            "isrc":        "...",
          }
        """
        self._ensure_authenticated()

        track_data  = self._gw_call("song.getData", {"sng_id": str(track_id)})
        track_token = track_data.get("TRACK_TOKEN")
        sng_id_str  = str(track_data.get("SNG_ID", track_id))
        if not track_token:
            raise RuntimeError(f"No TRACK_TOKEN for track {track_id}.")

        formats      = QUALITY_FORMATS.get(quality, QUALITY_FORMATS["lossless"])
        cdn_url      = None
        chosen_fmt   = None

        for fmt in formats:
            payload = {
                "license_token": self._license_token,
                "media": [{"type": "FULL", "formats": [fmt]}],
                "track_tokens": [track_token],
            }
            r = self._session.post(MEDIA_URL, json=payload, timeout=15)
            r.raise_for_status()
            try:
                cdn_url    = r.json()["data"][0]["media"][0]["sources"][0]["url"]
                chosen_fmt = fmt["format"]
                break
            except (KeyError, IndexError, TypeError):
                continue

        if not cdn_url:
            raise RuntimeError(f"No CDN URL for track {track_id}.")

        meta = self.get_track_metadata(track_id)
        return {
            "success":   True,
            "cdn_url":   cdn_url,
            "format":    chosen_fmt,
            "extension": FORMAT_EXTENSIONS[chosen_fmt],
            "track_id":  track_id,
            "sng_id":    sng_id_str,   # use this (not track_id) for Blowfish key
            "title":     meta.get("title", ""),
            "artist":    meta.get("artist", {}).get("name", ""),
            "album":     meta.get("album", {}).get("title", ""),
            "isrc":      meta.get("isrc", ""),
            "duration":  meta.get("duration", 0),
        }

    def validate(self) -> bool:
        """Quick health check — returns True if the ARL is still valid."""
        try:
            self._authenticate()
            return True
        except Exception:
            return False


# ---------------------------------------------------------------------------
# DeezerAccountPool — health-aware, least-busy rotation
# ---------------------------------------------------------------------------

class DeezerAccountPool:
    """
    Pool of DeezerClient instances with round-robin / fallback rotation.

    Mirrors the BeatportAccountPool API so app.py can treat both identically.
    """

    def __init__(
        self,
        accounts: list[dict],          # [{"arl": "..."}]
        cache_dir: Optional[str] = None,
        strategy: str = "round_robin",  # "round_robin" | "fallback"
        cooldown: int = 60,             # seconds to avoid a failed account
    ):
        self._cache_dir  = Path(cache_dir) if cache_dir else Path(".deezer_tokens")
        self._strategy   = strategy
        self._cooldown   = cooldown
        self._lock       = threading.Lock()
        self._index      = 0            # for round_robin
        self._last_fail: dict[int, float] = {}

        self._clients: list[DeezerClient] = []
        for acc in accounts:
            arl = acc.get("arl", "").strip()

            # Auto-derive ARL from email/password if not directly provided
            if (not arl or arl.startswith("<")) and acc.get("email") and acc.get("password"):
                try:
                    arl = get_arl_from_login(
                        email=acc["email"].strip(),
                        password=acc["password"],
                        cache_dir=self._cache_dir,
                    )
                except Exception as e:
                    print(f"  ⚠  Deezer login failed for {acc.get('email')}: {e}")
                    continue

            if not arl or arl.startswith("<"):
                continue
            try:
                client = DeezerClient(arl, cache_dir=self._cache_dir)
                self._clients.append(client)
            except Exception as e:
                print(f"  ⚠  Skipping bad Deezer ARL ({arl[:8]}…): {e}")

        if not self._clients:
            raise RuntimeError("DeezerAccountPool: no valid ARL tokens loaded.")

    @property
    def total(self) -> int:
        return len(self._clients)

    def status(self) -> dict:
        now = time.monotonic()
        cooling = sum(
            1 for idx, t in self._last_fail.items()
            if now - t < self._cooldown
        )
        return {
            "total":            self.total,
            "available":        self.total - cooling,
            "cooling":          cooling,
            "strategy":         self._strategy,
            "cooldown_seconds": self._cooldown,
        }

    def _next_client(self) -> tuple[int, DeezerClient]:
        """Pick the next available client based on strategy."""
        now = time.monotonic()
        with self._lock:
            candidates = [
                idx for idx in range(self.total)
                if now - self._last_fail.get(idx, 0) >= self._cooldown
            ]
            if not candidates:
                # All cooling down — reset and try anyway
                candidates = list(range(self.total))

            if self._strategy == "fallback":
                idx = candidates[0]
            else:
                # round_robin among available
                self._index = self._index % len(candidates)
                idx         = candidates[self._index]
                self._index = (self._index + 1) % len(candidates)

        return idx, self._clients[idx]

    def get_track(
        self,
        track_id: int,
        quality: str = "lossless",
    ) -> dict:
        """
        Download and decrypt a track, rotating accounts on failure.

        Returns the same dict as DeezerClient.get_track_download().
        On total failure returns {"success": False, "error": "..."}.
        """
        tried: set[int] = set()
        last_error = "Unknown error"

        for _ in range(self.total):
            idx, client = self._next_client()
            if idx in tried:
                break
            tried.add(idx)

            try:
                result = client.get_track_download(track_id, quality)
                # Success — clear cooldown for this account
                self._last_fail.pop(idx, None)
                return result
            except Exception as e:
                last_error = str(e)
                print(f"  ✗ Deezer account {idx} failed for track {track_id}: {e}")
                with self._lock:
                    self._last_fail[idx] = time.monotonic()

        return {"success": False, "error": last_error}

    def prewarm(self) -> None:
        """Authenticate all accounts in parallel so first request is instant."""
        import concurrent.futures

        def _auth(client: DeezerClient) -> str:
            try:
                client._ensure_authenticated()
                return f"✓ {client.arl[:8]}… OK"
            except Exception as e:
                return f"✗ {client.arl[:8]}… FAILED: {e}"

        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as ex:
            futures = {ex.submit(_auth, c): c for c in self._clients}
            for f in concurrent.futures.as_completed(futures):
                print(f"  Deezer prewarm: {f.result()}")


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

def load_pool_from_file(
    accounts_path: str,
    cache_dir: Optional[str] = None,
    strategy: str = "round_robin",
    cooldown: int = 60,
) -> DeezerAccountPool:
    """
    Load a DeezerAccountPool from a JSON file.

    Supported account formats (can mix both in one file):
      Email + password (auto-login, ARL fetched automatically):
        [{"email": "you@example.com", "password": "yourpassword"}, ...]

      Raw ARL token (grab from deezer.com browser cookies):
        [{"arl": "<192-char-hex-token>"}, ...]

    Legacy single-object format also accepted:
      {"email": "...", "password": "..."}
    """
    path = Path(accounts_path)
    raw  = json.loads(path.read_text())

    if isinstance(raw, dict):
        raw = [raw]

    return DeezerAccountPool(
        accounts  = raw,
        cache_dir = cache_dir or str(path.parent / ".deezer_tokens"),
        strategy  = strategy,
        cooldown  = cooldown,
    )
