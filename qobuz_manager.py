"""
Qobuz Downloader Module
=======================
Multi-account pool with health monitoring, async job queue, and
direct CDN → R2 streaming — zero local disk usage.

Account pool
  101 accounts are hardcoded (Oracle server is private).
  Env vars QOBUZ_EMAIL_N / QOBUZ_PASSWORD_N override the list if set.

Health states per account
  unchecked  → freshly added, not yet probed
  healthy    → authenticated and verified streamable
  degraded   → auth OK but repeated slow/empty responses (≥2 errors)
  dead       → failed too many times (≥5 errors), skipped by scheduler;
               a background checker retries every 30 min

Job queue
  submit_download_job()   → returns job_id immediately
  get_job_status()        → poll a job by id
  list_jobs()             → all recent jobs
  download_qobuz()        → blocking convenience wrapper (used by /qobuz/download)
"""

import os
import re
import time
import json
import threading
import logging
import asyncio
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from io import BytesIO
from typing import Dict, List, Optional, Tuple, Any

import requests
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Quality levels (Qobuz format_id values)
# ---------------------------------------------------------------------------
QUALITY_MAP: Dict[str, int] = {
    "mp3":       5,   # MP3 320 kbps
    "cd":        6,   # FLAC 16-bit 44.1 kHz
    "hi-res":    7,   # FLAC 24-bit ≤96 kHz
    "hi-res-max": 27, # FLAC 24-bit >96 kHz (best available)
}
DEFAULT_QUALITY = "hi-res-max"

# ---------------------------------------------------------------------------
# Session persistence
# ---------------------------------------------------------------------------
# Qobuz user_auth_tokens expire after ~10 days of inactivity.
# We save them to disk so restarts don't require re-authenticating all 101
# accounts from scratch.  Tokens are proactively refreshed 2 days before expiry.
_SESSION_FILE         = os.path.join(os.path.dirname(__file__), "qobuz_sessions.json")
_TOKEN_LIFETIME_SECS  = 10 * 24 * 3600   # 10 days — observed expiry window
_TOKEN_REFRESH_BEFORE = 2  * 24 * 3600   # refresh 2 days before expiry (at 8 days)
_PROACTIVE_CHECK_INTERVAL = 3600         # check for near-expiry tokens every hour


# ===========================================================================
# BUNDLE TOKENS (shared across all accounts, cached after first fetch)
# ===========================================================================

_bundle_lock = threading.Lock()
_cached_app_id: Optional[str] = None
_cached_secrets: Optional[List[str]] = None


def _get_bundle_tokens() -> Tuple[str, List[str]]:
    """
    Scrape app_id and signing secrets from the Qobuz web player bundle.
    Result is cached for the process lifetime (tokens change rarely).
    """
    global _cached_app_id, _cached_secrets

    with _bundle_lock:
        if _cached_app_id and _cached_secrets:
            return _cached_app_id, _cached_secrets

        logger.info("🔑 Fetching Qobuz bundle tokens …")
        try:
            from qobuz_dl.bundle import Bundle
            bundle = Bundle()
            app_id = bundle.get_app_id()
            secrets = [s for s in bundle.get_secrets().values() if s]
            _cached_app_id = app_id
            _cached_secrets = secrets
            logger.info(f"   app_id={app_id}, secrets={len(secrets)}")
            return app_id, secrets
        except Exception as exc:
            raise RuntimeError(f"Failed to fetch Qobuz bundle tokens: {exc}") from exc


# ===========================================================================
# ACCOUNT CONFIG
# ===========================================================================

# Hardcoded account pool (Oracle server is private — safe to embed here).
# env vars QOBUZ_EMAIL_N / QOBUZ_PASSWORD_N are checked first;
# if none are set, the list below is used as-is.
_HARDCODED_ACCOUNTS = [
    ("jb-qklxmxmqlxla@qobuz.com",                              "qobuz.Jiju77"),
    ("testbirds+13US@qobuz.com",                               "Hires0430&"),
    ("testqobuz@qobuz.com",                                    "Xpiibe3237!"),
    ("adriel@01mars.com",                                      "23Eden10"),
    ("cy-Audra@qobuz.com",                                     "Azerty78"),
    ("act47750",                                               "arkestra"),
    ("patland",                                                "coucut40"),
    ("testbirds+8SE@qobuz.com",                               "Hires0430&"),
    ("testprofile+at1@qobuz.com",                             "HiRes4Life!!!"),
    ("ericpy@hotmail.com",                                     "Qop@ze78"),
    ("anais.langlet+ggb@qobuz.com",                           "Qobuz2018"),
    ("quentin.leredde+testzbq-qle-fr@qobuz.com",              "qle-fr"),
    ("testbirds+6DK@qobuz.com",                               "Hires0430&"),
    ("gloria.gietl+900@qobuz.com",                            "qobuz123"),
    ("j20190131_doubletags2@qobuz.com",                        "Julien77"),
    ("jb20170912_7@qobuz.com",                                 "jiju77"),
    ("hugo.lancel+0987@qobuz.com",                             "hugoflo"),
    ("stagiaire.marketing1+test78777887@qobuz.com",            "azerty"),
    ("anais.langlet+19022@qobuz.com",                         "QObuz2018"),
    ("testprofile+nl5@qobuz.com",                             "HiRes4L!fe"),
    ("hilary.edes@gmail.com",                                  "H0m31nmyB0dy"),
    ("maria.gomez+testsv@qobuz.com",                          "Password+12345"),
    ("gaxuxa.caldichoury+premium-annual-US@qobuz.com",        "Qobuz2019!"),
    ("cy-Anthony@qobuz.com",                                   "Azerty78"),
    ("swordshield777@yahoo.com",                               "!Zcb1357"),
    ("dela.mensah+testzbq20150814@qobuz.com",                 "reptile"),
    ("tstCfg01@qobuz.com",                                     "tstCfg01"),
    ("victor.kolmann+iam2@qobuz.com",                         "Kolmann94"),
    ("testprofile+fi2@qobuz.com",                             "HiRes4L!fe"),
    ("anais.langlet+06052@qobuz.com",                         "Qobuz2018"),
    ("anthony.iksil+duo2@qobuz.com",                          "poipoiP0!"),
    ("salvo2080@gmail.com",                                    "Marolino2080."),
    ("ygzurersoy@gmail.com",                                   "Uyelik2023"),
    ("anais.langlet+gde4@qobuz.com",                          "Qobuz2018"),
    ("cy-Eldred@qobuz.com",                                    "Azerty78"),
    ("david.craff+showFR1@qobuz.com",                         "Qobuz12345"),
    ("naim+sup01@qobuz.com",                                   "Qbz12345#"),
    ("bowers-wilkins+10@qobuz.com",                            "Qbz12345#"),
    ("cy-Heather@qobuz.com",                                   "Azerty78"),
    ("florian.lamirault+france000@qobuz.com",                 "Yvonnick1234"),
    ("anais.langlet+us@qobuz.com",                            "Qobuz2018"),
    ("luislecm18@gmail.com",                                   "Miaesmiaf1"),
    ("jj+test2007201564671284@qobuz.com",                     "Julien77"),
    ("anais.langlet+cjdj@qobuz.com",                         "Qobuz2018"),
    ("gloria.gietl+41@qobuz.com",                             "qobuz123"),
    ("qobuzqa+prodde05@qobuz.com",                            "Qobuzqa1234"),
    ("julien.boudry+20200708@qobuz.com",                      "Julien77"),
    ("ian_buysse@yahoo.com",                                   "Qobuz$15str390"),
    ("jj+test200720184465325@qobuz.com",                      "Julien77"),
    ("pierre.bollack+br@qobuz.com",                           "Password&1234"),
    ("testbirds+11DE@qobuz.com",                              "Hires0430&"),
    ("thomas.bonnenfant@qobuz.com",                           "Komorowski92!"),
    ("gaxuxa.caldichoury+hifi-monthly-FR@qobuz.com",          "Qobuz2019!"),
    ("testprofile+lude4@qobuz.com",                           "HiRes4L!fe"),
    ("anais.langlet+eli@qobuz.com",                           "Qobuz2018"),
    ("anais.langlet+JP@qobuz.com",                            "Qobuz2018"),
    ("testprofile+es2@qobuz.com",                             "HiRes4L!fe"),
    ("lina.sbai@qobuz.com",                                    "Lisb*0202_tmp"),
    ("jtest_20190131_sanstag@qobuz.com",                      "julien77"),
    ("enzo.carpentier+salutz2845@qobuz.com",                  "Qobuz12345"),
    ("cjbramirez75@gmail.com",                                 "3Timonypumb@"),
    ("anais.langlet+chde2020@qobuz.com",                      "Qobuz2018"),
    ("account-test+fnac-01@qobuz.com",                        "Qbz12345#"),
    ("cy-Caleb@qobuz.com",                                     "Azerty78"),
    ("anais.langlet+test14@qobuz.com",                        "anais.test14"),
    ("gaxuxa.caldichoury+onboardtest-duode@qobuz.com",        "Qobuz2022!"),
    ("testprofile+chfr4@qobuz.com",                           "HiRes4L!fe"),
    ("gaxuxa.caldichoury+hifi-monthly-US@qobuz.com",          "Qobuz2019!"),
    ("quentin.leredde+testzbq-2@qobuz.com",                   "qlzbq2"),
    ("julien.boudry+test221120171@qobuz.com",                 "jiju77"),
    ("testprofile+ir4@qobuz.com",                             "HiRes4L!fe"),
    ("gaxuxa.caldichoury+studio-monthly-fr@qobuz.com",        "Qobuz2019!"),
    ("harman+05@qobuz.com",                                    "Qbz12345#"),
    ("testprofile+uk7@qobuz.com",                             "HiRes4L!fe"),
    ("gaxuxa.caldichoury+test2012@qobuz.com",                 "Qobuz120!"),
    ("matisse.varlin@qobuz.com",                               "VarlinM25"),
    ("naim+sup03@qobuz.com",                                   "Qbz12345#"),
    ("lina.sbai+testppitaly@qobuz.com",                       "Lsenai92"),
    ("cypress.automatisation+1@qobuz.com",                    "Cypress78"),
    ("lina.sbai+testprprd@qobuz.com",                         "Lsenai92"),
    ("anais.langlet+it1@qobuz.com",                           "Qobuz2018"),
    ("byeonghunny@gmail.com",                                  "Dkrkrha1$&@"),
    ("richard.barazzuol@gmail.com",                           "Qobuz1234#"),
    ("a.darriet@amento.fr",                                    "IgOrAz4331"),
    ("emepride2022@gmail.com",                                 "Eme492024"),
    ("eljundifeito@outlook.es",                                "<Gustav0!1972!>"),
    ("louchebem2@gmail.com",                                   "Arthom31"),
    ("tiago17srv@gmail.com",                                   "trc,141719QOBUZ"),
    ("gil.laborie@gmail.com",                                  "P@sden0n"),
    ("onurtoru@gmail.com",                                     "Dqxg8ddx2!"),
    ("jmanner123456@gmail.com",                                "Sc0rpion12"),
    ("kamileecher@gmail.com",                                  "Yldrm1993."),
    ("blakeblake760@gmail.com",                                "Kirbyaltmusic12!"),
    ("joscarlos.0312@gmail.com",                               "Doun12.."),
    ("henrysf1958@gmail.com",                                  "Martina2005"),
    ("didier.hardas@gmx.fr",                                   "204618eL93"),
    ("sport_3x_compliant@hidmail.org",                        "Qobuz123"),
    ("marcosegovia.mb@gmail.com",                              "Ultrasone10"),
    ("mickmanleops",                                           "Odiolerin1"),
    ("torgeir_viddal@hotmail.com",                             "32Cucumber"),
    ("clement.ducroquet.pv@gmail.com",                        "Amazon17052019"),
    ("tubedac@gmail.com",                                      "spliTT67#"),
]


# ===========================================================================
# SESSION FILE  —  load / save / restore helpers
# ===========================================================================

_sessions_file_lock = threading.Lock()


def _load_sessions() -> Dict[str, Dict]:
    """
    Load persisted sessions from disk.
    Returns a dict keyed by account_id::

        {
            "1": {
                "email":            "user@example.com",
                "user_auth_token":  "...",
                "app_id":           "...",
                "secret":           "...",
                "membership_label": "Studio",
                "authenticated_at": 1708598400.0   # unix epoch
            },
            ...
        }
    """
    try:
        if os.path.exists(_SESSION_FILE):
            with open(_SESSION_FILE, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            logger.info(f"📂 Loaded {len(data)} saved Qobuz sessions from {_SESSION_FILE}")
            return data
    except Exception as exc:
        logger.warning(f"⚠️  Could not load sessions file: {exc}")
    return {}


def _save_session(account_id: str, session_data: Dict) -> None:
    """
    Persist (or update) a single account's session on disk.
    Thread-safe: uses a module-level lock.
    """
    with _sessions_file_lock:
        current = {}
        if os.path.exists(_SESSION_FILE):
            try:
                with open(_SESSION_FILE, "r", encoding="utf-8") as fh:
                    current = json.load(fh)
            except Exception:
                pass
        current[account_id] = session_data
        try:
            with open(_SESSION_FILE, "w", encoding="utf-8") as fh:
                json.dump(current, fh, indent=2)
        except Exception as exc:
            logger.warning(f"⚠️  Could not save session for account {account_id}: {exc}")


def _restore_client(email: str, uat: str, app_id: str, secret: str):
    """
    Re-create a qopy.Client skeleton from saved credentials WITHOUT doing
    a fresh login round-trip.  The saved user_auth_token is injected
    directly into the requests.Session headers.

    If the token has already expired (API call fails) the caller should
    fall back to a full login.
    """
    from qobuz_dl.qopy import Client

    # Bypass __init__ (which would force a login) using object.__new__
    obj = object.__new__(Client)
    obj.secrets = []
    obj.id      = str(app_id)
    obj.session = __import__("requests").Session()
    obj.session.headers.update({
        "User-Agent":        (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:83.0) "
            "Gecko/20100101 Firefox/83.0"
        ),
        "X-App-Id":          str(app_id),
        "Content-Type":      "application/json;charset=UTF-8",
        "X-User-Auth-Token": uat,
    })
    obj.base  = "https://www.qobuz.com/api.json/0.2/"
    obj.sec   = secret
    obj.uat   = uat
    obj.label = "restored"
    return obj


def load_qobuz_accounts() -> List[Dict]:
    """
    Return the account list to authenticate.

    Priority:
    1. QOBUZ_EMAIL_N / QOBUZ_PASSWORD_N env vars (if set, they replace the hardcoded list)
    2. Hardcoded list above (default — used when no env vars are set)
    """
    # Check for env-var overrides
    env_accounts: List[Dict] = []
    i = 1
    while True:
        email = os.getenv(f"QOBUZ_EMAIL_{i}")
        password = os.getenv(f"QOBUZ_PASSWORD_{i}")
        if not email or not password:
            break
        env_accounts.append({"id": str(i), "email": email, "password": password})
        i += 1

    if env_accounts:
        return env_accounts

    # Use hardcoded pool
    return [
        {"id": str(i + 1), "email": email, "password": password}
        for i, (email, password) in enumerate(_HARDCODED_ACCOUNTS)
    ]


# ===========================================================================
# ACCOUNT HEALTH
# ===========================================================================

class AccountHealth(str, Enum):
    UNCHECKED = "unchecked"  # hasn't been probed yet
    HEALTHY   = "healthy"    # confirmed streamable
    DEGRADED  = "degraded"   # auth ok, but repeated errors (2–4)
    DEAD      = "dead"       # too many failures (≥5), skipped by scheduler


# Thresholds
_FAIL_DEAD_THRESHOLD  = 5     # consecutive errors before marking DEAD
_FAIL_DEGRADE_THRESHOLD = 2   # errors before marking DEGRADED
_HEALTH_CHECK_INTERVAL  = 1800  # seconds between background health probes (30 min)
_HEALTH_CHECK_TRACK_ID  = 5966783  # known Qobuz track used as canary


# ===========================================================================
# QOBUZ CLIENT POOL  —  health-aware, least-busy rotation
# ===========================================================================

class QobuzClientPool:
    """
    Thread-safe pool of authenticated qopy.Client instances.

    • Initialises all accounts at startup (parallel, up to 20 threads).
    • Tracks health state per account (unchecked → healthy / degraded / dead).
    • Routes requests to the least-busy *healthy* account.
    • Marks accounts degraded/dead on repeated errors; revives on success.
    • Background thread re-probes dead/degraded accounts every 30 min.
    """

    def __init__(self):
        self._clients:      Dict[str, Any]            = {}  # id → qopy.Client
        self._emails:       Dict[str, str]            = {}
        self._passwords:    Dict[str, str]            = {}
        self._active:       Dict[str, int]            = {}  # concurrent downloads
        self._health:       Dict[str, AccountHealth]  = {}
        self._fail_count:   Dict[str, int]            = {}  # consecutive errors
        self._last_check:   Dict[str, float]          = {}  # epoch of last probe
        self._auth_times:   Dict[str, float]          = {}  # epoch of last successful login
        self._lock          = threading.Lock()
        self._checker_thread: Optional[threading.Thread] = None

    # ------------------------------------------------------------------
    # Initialisation
    # ------------------------------------------------------------------

    def initialize(self, accounts: List[Dict]) -> None:
        """
        Authenticate all accounts concurrently (up to 20 threads).

        Fast-path: if a saved session file exists and the token is still fresh
        (< TOKEN_LIFETIME - TOKEN_REFRESH_BEFORE old), the saved token is
        restored directly — no network login round-trip required.

        Slow-path: full login via qopy.Client for accounts with no saved
        session, an expired token, or a failed restore.
        """
        if not accounts:
            logger.warning("⚠️  No Qobuz accounts to initialise.")
            return

        app_id, secrets = _get_bundle_tokens()
        saved_sessions = _load_sessions()
        lock = threading.Lock()
        ok_count = restored_count = fresh_login_count = 0
        now = time.time()

        def _auth_one(acc: Dict):
            nonlocal ok_count, restored_count, fresh_login_count
            acc_id = acc["id"]
            email  = acc["email"]
            pwd    = acc["password"]

            # ---- try fast-path restore ----
            saved = saved_sessions.get(acc_id)
            if saved and saved.get("user_auth_token") and saved.get("secret"):
                age = now - saved.get("authenticated_at", 0)
                if age < (_TOKEN_LIFETIME_SECS - _TOKEN_REFRESH_BEFORE):
                    try:
                        client = _restore_client(
                            email,
                            saved["user_auth_token"],
                            saved.get("app_id", app_id),
                            saved["secret"],
                        )
                        # Trust the token if it's less than 1 day old — no network call needed.
                        # For older tokens do a quick canary check to catch silent expiry.
                        if age > 86400:
                            client.get_track_meta(_HEALTH_CHECK_TRACK_ID)
                        with lock:
                            self._clients[acc_id]    = client
                            self._emails[acc_id]     = email
                            self._passwords[acc_id]  = pwd
                            self._active[acc_id]     = 0
                            self._health[acc_id]     = AccountHealth.HEALTHY
                            self._fail_count[acc_id] = 0
                            self._last_check[acc_id] = now
                            self._auth_times[acc_id] = saved.get("authenticated_at", now)
                            ok_count        += 1
                            restored_count  += 1
                        logger.info(
                            f"⚡ [{acc_id:>3}] {email} — restored "
                            f"(token age {age/3600:.1f}h)"
                        )
                        return
                    except Exception as exc:
                        logger.debug(
                            f"   [{acc_id}] restore failed ({exc}), falling back to login"
                        )

            # ---- slow-path: full fresh login ----
            try:
                client = self._make_client(email, pwd, app_id, secrets)
                auth_time = time.time()
                _save_session(acc_id, {
                    "email":            email,
                    "user_auth_token":  client.uat,
                    "app_id":           app_id,
                    "secret":           client.sec,
                    "membership_label": getattr(client, "label", ""),
                    "authenticated_at": auth_time,
                })
                with lock:
                    self._clients[acc_id]    = client
                    self._emails[acc_id]     = email
                    self._passwords[acc_id]  = pwd
                    self._active[acc_id]     = 0
                    self._health[acc_id]     = AccountHealth.HEALTHY
                    self._fail_count[acc_id] = 0
                    self._last_check[acc_id] = now
                    self._auth_times[acc_id] = auth_time
                    ok_count          += 1
                    fresh_login_count += 1
                logger.info(f"✓ [{acc_id:>3}] {email} — fresh login")
            except Exception as exc:
                logger.warning(f"✗ [{acc_id:>3}] {email}: {exc}")

        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as pool:
            list(pool.map(_auth_one, accounts))

        logger.info(
            f"✓ Qobuz pool ready: {ok_count}/{len(accounts)} accounts "
            f"({restored_count} restored from cache, {fresh_login_count} fresh logins)"
        )

        # Start background health-check + proactive-refresh thread
        self._start_health_checker()

    @staticmethod
    def _make_client(email: str, password: str, app_id: str, secrets: List[str]):
        from qobuz_dl.qopy import Client
        return Client(email, password, app_id, secrets)

    # ------------------------------------------------------------------
    # Rotation — skips dead accounts
    # ------------------------------------------------------------------

    def acquire(self) -> Tuple[str, Any]:
        """
        Return (account_id, client) for the best available account.

        Selection priority:
          1. healthy   accounts, sorted by active downloads (least-busy first)
          2. unchecked accounts (treated as potentially healthy)
          3. degraded  accounts (fallback when nothing else is available)

        dead accounts are always skipped.
        Raises RuntimeError when no account is usable at all.
        """
        with self._lock:
            if not self._clients:
                raise RuntimeError("No Qobuz accounts available.")

            def _score(acc_id: str) -> Tuple[int, int]:
                """Lower score = higher priority."""
                h = self._health.get(acc_id, AccountHealth.UNCHECKED)
                if h == AccountHealth.DEAD:      return (3, 9999)
                if h == AccountHealth.HEALTHY:   priority = 0
                elif h == AccountHealth.UNCHECKED: priority = 1
                else:                             priority = 2  # DEGRADED
                return (priority, self._active.get(acc_id, 0))

            candidates = [
                acc_id for acc_id in self._clients
                if self._health.get(acc_id) != AccountHealth.DEAD
            ]
            if not candidates:
                # All accounts dead — pick absolute least-busy as last resort
                candidates = list(self._clients.keys())

            if not candidates:
                raise RuntimeError("No Qobuz accounts available.")

            account_id = min(candidates, key=_score)
            self._active[account_id] = self._active.get(account_id, 0) + 1
            h = self._health.get(account_id, AccountHealth.UNCHECKED)
            logger.info(
                f"⚖️  Account {account_id} selected "
                f"| health={h} | active={self._active[account_id]}"
            )
            return account_id, self._clients[account_id]

    def release(self, account_id: str) -> None:
        """Decrement active counter; call in a finally block."""
        with self._lock:
            if account_id in self._active:
                self._active[account_id] = max(0, self._active[account_id] - 1)

    # ------------------------------------------------------------------
    # Health tracking — called after every download attempt
    # ------------------------------------------------------------------

    def mark_success(self, account_id: str) -> None:
        """Reset failure counter and promote account to healthy."""
        with self._lock:
            self._fail_count[account_id] = 0
            old = self._health.get(account_id)
            self._health[account_id] = AccountHealth.HEALTHY
            self._last_check[account_id] = time.time()
            if old != AccountHealth.HEALTHY:
                logger.info(f"💚 Account {account_id} → healthy")

    def mark_failure(self, account_id: str, reason: str = "") -> None:
        """Increment failure counter; degrade or kill the account."""
        with self._lock:
            count = self._fail_count.get(account_id, 0) + 1
            self._fail_count[account_id] = count

            if count >= _FAIL_DEAD_THRESHOLD:
                self._health[account_id] = AccountHealth.DEAD
                logger.warning(
                    f"💀 Account {account_id} → dead "
                    f"({count} consecutive failures). {reason}"
                )
            elif count >= _FAIL_DEGRADE_THRESHOLD:
                self._health[account_id] = AccountHealth.DEGRADED
                logger.warning(
                    f"🟡 Account {account_id} → degraded "
                    f"({count} failures). {reason}"
                )

    # ------------------------------------------------------------------
    # Re-authentication
    # ------------------------------------------------------------------

    def reauthenticate(self, account_id: str) -> bool:
        """Re-create a fresh client. Returns True on success. Saves session on success."""
        with self._lock:
            if account_id not in self._emails:
                return False
            email    = self._emails[account_id]
            password = self._passwords[account_id]

        logger.info(f"🔄 Re-authenticating Account {account_id} ({email}) …")
        try:
            app_id, secrets = _get_bundle_tokens()
            client = self._make_client(email, password, app_id, secrets)
            auth_time = time.time()
            _save_session(account_id, {
                "email":            email,
                "user_auth_token":  client.uat,
                "app_id":           app_id,
                "secret":           client.sec,
                "membership_label": getattr(client, "label", ""),
                "authenticated_at": auth_time,
            })
            with self._lock:
                self._clients[account_id]    = client
                self._health[account_id]     = AccountHealth.HEALTHY
                self._fail_count[account_id] = 0
                self._last_check[account_id] = auth_time
                self._auth_times[account_id] = auth_time
            logger.info(f"✓ Account {account_id} re-authenticated and session saved")
            return True
        except Exception as exc:
            logger.error(f"Re-auth failed for Account {account_id}: {exc}")
            with self._lock:
                self._health[account_id] = AccountHealth.DEAD
            return False

    # ------------------------------------------------------------------
    # Background health checker
    # ------------------------------------------------------------------

    def _start_health_checker(self) -> None:
        """Spawn a daemon thread that periodically probes all accounts."""
        if self._checker_thread and self._checker_thread.is_alive():
            return
        t = threading.Thread(target=self._health_check_loop, daemon=True, name="qobuz-health")
        t.start()
        self._checker_thread = t
        logger.info("🩺 Qobuz health-check thread started")

    def _health_check_loop(self) -> None:
        """
        Runs forever; alternates between:
          • proactive token-refresh pass   (every _PROACTIVE_CHECK_INTERVAL = 1 h)
          • full health-check probe pass   (every _HEALTH_CHECK_INTERVAL    = 30 min)

        The proactive refresh runs at a *shorter* interval so near-expiry tokens
        are caught well before the 10-day window closes.
        """
        # Initial delay — let startup settle first
        time.sleep(60)
        last_proactive = 0.0
        last_health    = 0.0
        while True:
            now = time.time()
            if now - last_proactive >= _PROACTIVE_CHECK_INTERVAL:
                self._proactive_refresh_pass()
                last_proactive = time.time()
            if now - last_health >= _HEALTH_CHECK_INTERVAL:
                self._run_health_checks()
                last_health = time.time()
            time.sleep(300)  # poll every 5 min; actual work only when due

    def _proactive_refresh_pass(self) -> None:
        """
        Find accounts whose token is >= (_TOKEN_LIFETIME - _TOKEN_REFRESH_BEFORE)
        old (i.e. 8+ days since last login) and force a fresh re-authentication
        before the 10-day expiry window closes.
        """
        now = time.time()
        with self._lock:
            targets = [
                acc_id for acc_id in self._clients
                if (
                    self._health.get(acc_id) != AccountHealth.DEAD
                    and (now - self._auth_times.get(acc_id, 0))
                        >= (_TOKEN_LIFETIME_SECS - _TOKEN_REFRESH_BEFORE)
                )
            ]

        if not targets:
            return

        logger.info(
            f"🔁 Proactive token refresh: {len(targets)} account(s) "
            f"approaching 10-day expiry …"
        )

        def _refresh_one(acc_id: str):
            age_days = (now - self._auth_times.get(acc_id, 0)) / 86400
            logger.info(
                f"   🔄 [{acc_id}] token is {age_days:.1f} days old — refreshing"
            )
            self.reauthenticate(acc_id)

        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as pool:
            list(pool.map(_refresh_one, targets))

    def _run_health_checks(self) -> None:
        """
        Probe every account that is dead or unchecked (or due a periodic check).
        Uses a lightweight track/get call (metadata only, no signing needed).
        Runs accounts in parallel with a thread pool.
        """
        now = time.time()
        with self._lock:
            targets = [
                acc_id for acc_id in self._clients
                if (
                    self._health.get(acc_id) in (AccountHealth.DEAD, AccountHealth.UNCHECKED)
                    or (now - self._last_check.get(acc_id, 0)) > _HEALTH_CHECK_INTERVAL
                )
            ]

        if not targets:
            return

        logger.info(f"🩺 Health-checking {len(targets)} account(s) …")

        def _probe(acc_id: str):
            with self._lock:
                client = self._clients.get(acc_id)
            if not client:
                return
            try:
                # Lightweight canary: fetch metadata for a known public track
                client.get_track_meta(_HEALTH_CHECK_TRACK_ID)
                self.mark_success(acc_id)
                logger.debug(f"   ✓ [{acc_id}] healthy")
            except Exception as exc:
                err = str(exc).lower()
                if "ineligible" in err or "free account" in err:
                    # Account is authenticated but can't stream — mark dead
                    with self._lock:
                        self._health[acc_id] = AccountHealth.DEAD
                        self._last_check[acc_id] = time.time()
                    logger.warning(f"   💀 [{acc_id}] ineligible (free account?)")
                elif "401" in err or "auth" in err or "invalid" in err:
                    # Session expired — try re-auth
                    logger.warning(f"   🔑 [{acc_id}] session expired, re-authing …")
                    self.reauthenticate(acc_id)
                else:
                    self.mark_failure(acc_id, reason=str(exc)[:80])
                    logger.debug(f"   ✗ [{acc_id}] {exc}")

        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as pool:
            list(pool.map(_probe, targets))

    # ------------------------------------------------------------------
    # Status
    # ------------------------------------------------------------------

    @property
    def total(self) -> int:
        return len(self._clients)

    @property
    def available(self) -> int:
        """Number of non-dead accounts."""
        with self._lock:
            return sum(
                1 for h in self._health.values()
                if h != AccountHealth.DEAD
            )

    def status(self) -> Dict:
        now = time.time()
        with self._lock:
            accounts = {}
            for acc_id in self._clients:
                auth_ts  = self._auth_times.get(acc_id, 0)
                age_secs = now - auth_ts if auth_ts else None
                expires_in = (
                    max(0, _TOKEN_LIFETIME_SECS - age_secs)
                    if age_secs is not None else None
                )
                accounts[acc_id] = {
                    "email":               self._emails.get(acc_id, "?"),
                    "health":              self._health.get(acc_id, AccountHealth.UNCHECKED),
                    "active_downloads":    self._active.get(acc_id, 0),
                    "fail_count":          self._fail_count.get(acc_id, 0),
                    "last_checked":        (
                        datetime.fromtimestamp(self._last_check[acc_id]).isoformat()
                        if self._last_check.get(acc_id)
                        else None
                    ),
                    "token_age_days":      round(age_secs / 86400, 2) if age_secs is not None else None,
                    "token_expires_in_days": round(expires_in / 86400, 2) if expires_in is not None else None,
                    "authenticated_at":    (
                        datetime.fromtimestamp(auth_ts).isoformat() if auth_ts else None
                    ),
                }
            counts = {h.value: 0 for h in AccountHealth}
            for v in accounts.values():
                counts[v["health"]] += 1
            return {
                "summary": {
                    "total":      len(accounts),
                    "healthy":    counts[AccountHealth.HEALTHY],
                    "unchecked":  counts[AccountHealth.UNCHECKED],
                    "degraded":   counts[AccountHealth.DEGRADED],
                    "dead":       counts[AccountHealth.DEAD],
                    "available":  counts[AccountHealth.HEALTHY] + counts[AccountHealth.UNCHECKED] + counts[AccountHealth.DEGRADED],
                    "token_refresh_threshold_days": round((_TOKEN_LIFETIME_SECS - _TOKEN_REFRESH_BEFORE) / 86400, 1),
                    "token_lifetime_days":          round(_TOKEN_LIFETIME_SECS / 86400, 1),
                },
                "accounts": accounts,
            }


# ===========================================================================
# JOB QUEUE
# ===========================================================================

class JobStatus(str, Enum):
    QUEUED     = "queued"
    PROCESSING = "processing"
    COMPLETED  = "completed"
    FAILED     = "failed"


@dataclass
class JobRecord:
    job_id:         str
    url:            str
    quality:        str
    upload_to_r2:   bool            = False
    status:         JobStatus       = JobStatus.QUEUED
    created_at:     datetime        = field(default_factory=datetime.utcnow)
    started_at:     Optional[datetime] = None
    completed_at:   Optional[datetime] = None
    result:         Optional[Dict]  = None
    error:          Optional[str]   = None
    account_used:   Optional[str]   = None
    queue_position: int             = 0

    def to_dict(self) -> Dict:
        return {
            "job_id":         self.job_id,
            "url":            self.url,
            "quality":        self.quality,
            "upload_to_r2":   self.upload_to_r2,
            "status":         self.status,
            "queue_position": self.queue_position,
            "created_at":     self.created_at.isoformat(),
            "started_at":     self.started_at.isoformat() if self.started_at else None,
            "completed_at":   self.completed_at.isoformat() if self.completed_at else None,
            "result":         self.result,
            "error":          self.error,
            "account_used":   self.account_used,
        }


# How many jobs run concurrently (one worker = one download slot)
_QUEUE_WORKERS = 15
# How long to keep completed/failed jobs before purging (seconds)
_JOB_TTL = 3600


class QobuzDownloadQueue:
    """
    Async job queue for Qobuz downloads.

    • Multiple workers consume jobs from an asyncio.Queue concurrently.
    • Each worker picks the least-busy *healthy* account per job.
    • Completed/failed jobs are kept for _JOB_TTL seconds, then purged.
    """

    def __init__(self, num_workers: int = _QUEUE_WORKERS):
        self._queue: Optional[asyncio.Queue] = None
        self._jobs:  Dict[str, JobRecord]    = {}
        self._lock   = threading.Lock()
        self._workers: List[asyncio.Task]    = []
        self._num_workers = num_workers
        self._started = False
        # These are injected by initialize_qobuz() so the queue knows
        # how to call R2 without circular imports
        self._s3_client    = None
        self._r2_bucket    = ""
        self._r2_public_url = ""

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self, s3_client, r2_bucket: str, r2_public_url: str) -> None:
        """Create workers. Must be called from an async context (FastAPI startup)."""
        if self._started:
            return
        self._queue = asyncio.Queue()
        self._s3_client     = s3_client
        self._r2_bucket     = r2_bucket
        self._r2_public_url = r2_public_url

        for _ in range(self._num_workers):
            task = asyncio.create_task(self._worker())
            self._workers.append(task)

        # Periodically purge old jobs
        asyncio.create_task(self._purge_loop())
        self._started = True

    # ------------------------------------------------------------------
    # Job submission
    # ------------------------------------------------------------------

    async def submit(self, url: str, quality: str, upload_to_r2: bool = False) -> str:
        """Enqueue a download job. Returns job_id immediately."""
        if not self._started or self._queue is None:
            raise RuntimeError("Queue not started. Call queue.start() first.")

        job_id = uuid.uuid4().hex[:12]
        position = self._queue.qsize() + 1
        record = JobRecord(
            job_id=job_id,
            url=url,
            quality=quality,
            upload_to_r2=upload_to_r2,
            queue_position=position,
        )
        with self._lock:
            self._jobs[job_id] = record
            # Update position numbers for already-queued jobs
            self._recalculate_positions()

        await self._queue.put(job_id)
        logger.info(f"📥 Job {job_id} queued (position {position}) — {url}")
        return job_id

    # ------------------------------------------------------------------
    # Job inspection
    # ------------------------------------------------------------------

    def get_job(self, job_id: str) -> Optional[Dict]:
        with self._lock:
            rec = self._jobs.get(job_id)
            return rec.to_dict() if rec else None

    def list_jobs(self, limit: int = 50) -> List[Dict]:
        with self._lock:
            jobs = sorted(
                self._jobs.values(),
                key=lambda j: j.created_at,
                reverse=True,
            )[:limit]
            return [j.to_dict() for j in jobs]

    def queue_depth(self) -> int:
        if self._queue is None:
            return 0
        return self._queue.qsize()

    # ------------------------------------------------------------------
    # Internal worker
    # ------------------------------------------------------------------

    async def _worker(self) -> None:
        loop = asyncio.get_event_loop()
        while True:
            job_id = await self._queue.get()
            try:
                with self._lock:
                    rec = self._jobs.get(job_id)
                if rec is None:
                    continue

                # Mark processing
                rec.status     = JobStatus.PROCESSING
                rec.started_at = datetime.utcnow()
                rec.queue_position = 0
                with self._lock:
                    self._recalculate_positions()
                logger.info(f"⚙️  Job {job_id} processing …")

                # Run the actual download in a thread (sync qobuz-dl calls)
                fmt_id = QUALITY_MAP.get(rec.quality, QUALITY_MAP[DEFAULT_QUALITY])
                url_type, item_id = parse_qobuz_url(rec.url)

                account_id, client = qobuz_pool.acquire()
                rec.account_used = account_id
                try:
                    if url_type == "track":
                        result = await loop.run_in_executor(
                            None,
                            lambda: _process_track(
                                item_id, fmt_id, client, account_id,
                                rec.upload_to_r2,
                                self._s3_client, self._r2_bucket, self._r2_public_url,
                            ),
                        )
                    elif url_type == "album":
                        result = await loop.run_in_executor(
                            None,
                            lambda: _process_album(
                                item_id, fmt_id, client, account_id,
                                rec.upload_to_r2,
                                self._s3_client, self._r2_bucket, self._r2_public_url,
                            ),
                        )
                    else:
                        raise ValueError(f"Unsupported URL type: {url_type!r}")

                    rec.status       = JobStatus.COMPLETED
                    rec.result       = result
                    rec.completed_at = datetime.utcnow()
                    qobuz_pool.mark_success(account_id)
                    logger.info(f"✅ Job {job_id} completed")

                except Exception as exc:
                    rec.status       = JobStatus.FAILED
                    rec.error        = str(exc)
                    rec.completed_at = datetime.utcnow()
                    # Decide if it's an account problem or a content problem
                    err = str(exc).lower()
                    if any(k in err for k in ("401", "auth", "token", "ineligible", "invalid app")):
                        qobuz_pool.mark_failure(account_id, reason=str(exc)[:80])
                        threading.Thread(
                            target=qobuz_pool.reauthenticate,
                            args=(account_id,),
                            daemon=True,
                        ).start()
                    else:
                        qobuz_pool.mark_success(account_id)  # content error, not account error
                    logger.error(f"❌ Job {job_id} failed: {exc}")

                finally:
                    qobuz_pool.release(account_id)

            except Exception as exc:
                # Unexpected worker error — don't crash the worker
                logger.exception(f"Worker error on job {job_id}: {exc}")
            finally:
                self._queue.task_done()

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _recalculate_positions(self) -> None:
        """Assign sequential queue_position to all QUEUED jobs (must hold _lock)."""
        queued = [
            j for j in self._jobs.values()
            if j.status == JobStatus.QUEUED
        ]
        queued.sort(key=lambda j: j.created_at)
        for i, j in enumerate(queued, start=1):
            j.queue_position = i

    async def _purge_loop(self) -> None:
        """Remove old completed/failed jobs every 10 minutes."""
        while True:
            await asyncio.sleep(600)
            cutoff = datetime.utcnow().timestamp() - _JOB_TTL
            with self._lock:
                to_del = [
                    jid for jid, rec in self._jobs.items()
                    if rec.completed_at
                    and rec.completed_at.timestamp() < cutoff
                ]
                for jid in to_del:
                    del self._jobs[jid]
            if to_del:
                logger.info(f"🧹 Purged {len(to_del)} old Qobuz jobs")


# ===========================================================================
# GLOBAL SINGLETONS
# ===========================================================================

qobuz_pool  = QobuzClientPool()
qobuz_queue = QobuzDownloadQueue(num_workers=_QUEUE_WORKERS)


def initialize_qobuz() -> None:
    """Authenticate all accounts. Call at API startup (sync part only)."""
    accounts = load_qobuz_accounts()
    if not accounts:
        logger.warning("⚠️  No Qobuz accounts found.")
        return
    qobuz_pool.initialize(accounts)


async def start_queue(s3_client, r2_bucket: str, r2_public_url: str) -> None:
    """Start queue workers. Must be called from an async context (FastAPI startup)."""
    await qobuz_queue.start(s3_client, r2_bucket, r2_public_url)


# ---------------------------------------------------------------------------
# Public job API
# ---------------------------------------------------------------------------

async def submit_download_job(url: str, quality: str = DEFAULT_QUALITY, upload_to_r2: bool = False) -> str:
    """Enqueue a download job and return the job_id immediately."""
    return await qobuz_queue.submit(url, quality, upload_to_r2)


def get_job_status(job_id: str) -> Optional[Dict]:
    """Return current status dict for a job, or None if not found."""
    return qobuz_queue.get_job(job_id)


def list_jobs(limit: int = 50) -> List[Dict]:
    """Return the most recent jobs (newest first)."""
    return qobuz_queue.list_jobs(limit)


def queue_stats() -> Dict:
    """Summary: queue depth + account health."""
    pool_status = qobuz_pool.status()
    jobs = qobuz_queue.list_jobs(limit=1000)
    return {
        "queue_depth":  qobuz_queue.queue_depth(),
        "workers":      _QUEUE_WORKERS,
        "accounts":     pool_status["summary"],
        "jobs": {
            "queued":     sum(1 for j in jobs if j["status"] == JobStatus.QUEUED),
            "processing": sum(1 for j in jobs if j["status"] == JobStatus.PROCESSING),
            "completed":  sum(1 for j in jobs if j["status"] == JobStatus.COMPLETED),
            "failed":     sum(1 for j in jobs if j["status"] == JobStatus.FAILED),
        },
    }


def force_refresh_all_sessions() -> Dict:
    """
    Immediately trigger a proactive re-authentication pass for every
    non-dead account (regardless of token age).  Useful for an admin
    endpoint to rotate all tokens on demand.

    Returns a summary dict: {"triggered": N, "accounts": [id, ...]}
    """
    with qobuz_pool._lock:
        targets = [
            acc_id for acc_id in qobuz_pool._clients
            if qobuz_pool._health.get(acc_id) != AccountHealth.DEAD
        ]

    def _do_refresh(acc_id: str):
        qobuz_pool.reauthenticate(acc_id)

    import concurrent.futures
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as pool:
        list(pool.map(_do_refresh, targets))

    return {"triggered": len(targets), "accounts": targets}


# ===========================================================================
# URL PARSING
# ===========================================================================

_URL_RE = re.compile(
    r"(?:https?://(?:www|open|play)\.qobuz\.com)?"
    r"(?:/[a-z]{2}-[a-z]{2})?"
    r"/(album|artist|track|playlist|label)"
    r"(?:/[-\w]+)?/([\w\d]+)"
)


def parse_qobuz_url(url: str) -> Tuple[str, str]:
    """
    Return (url_type, item_id) for any supported Qobuz URL.

    Supported formats:
        https://play.qobuz.com/track/12345678
        https://open.qobuz.com/album/abcdefgh
        https://www.qobuz.com/us-en/album/some-name/12345678
    """
    match = _URL_RE.search(url)
    if not match:
        raise ValueError(f"Unrecognised Qobuz URL: {url!r}")
    return match.group(1), match.group(2)  # (type, id)


# ===========================================================================
# CDN → R2 STREAMING (NO LOCAL DISK)
# ===========================================================================

def _stream_to_r2(
    cdn_url: str,
    object_key: str,
    s3_client,
    bucket: str,
    r2_public_url: str,
    mime_type: str = "audio/flac",
) -> str:
    """
    Pipe audio bytes from a Qobuz CDN URL directly into a CloudFlare R2
    multipart upload — no temporary files are written to disk.

    Returns the public R2 URL for the uploaded file.
    """
    logger.info(f"🚀 Qobuz CDN → R2  [{object_key}]")
    t0 = time.time()

    dl_headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:83.0) "
            "Gecko/20100101 Firefox/83.0"
        )
    }

    response = requests.get(cdn_url, headers=dl_headers, stream=True, timeout=60)
    response.raise_for_status()

    total_bytes = int(response.headers.get("content-length", 0))
    size_mb = total_bytes / (1024 * 1024) if total_bytes else 0
    logger.info(f"   Size: {size_mb:.1f} MB")

    # ---- start multipart upload ----------------------------------------
    mpu = s3_client.create_multipart_upload(
        Bucket=bucket,
        Key=object_key,
        ContentType=mime_type,
    )
    upload_id = mpu["UploadId"]
    parts: List[Dict] = []
    part_num = 1
    buf = BytesIO()
    PART_SIZE = 8 * 1024 * 1024  # 8 MB (R2/S3 min is 5 MB)
    transferred = 0

    try:
        for raw_chunk in response.iter_content(chunk_size=1024 * 1024):  # 1 MB HTTP chunks
            if not raw_chunk:
                continue
            buf.write(raw_chunk)
            transferred += len(raw_chunk)

            if buf.tell() >= PART_SIZE:
                buf.seek(0)
                part = s3_client.upload_part(
                    Bucket=bucket,
                    Key=object_key,
                    UploadId=upload_id,
                    PartNumber=part_num,
                    Body=buf.read(),
                )
                parts.append({"PartNumber": part_num, "ETag": part["ETag"]})
                pct = (transferred / total_bytes * 100) if total_bytes else 0
                logger.info(f"   📤 Part {part_num} ({pct:.0f}%)")
                part_num += 1
                buf = BytesIO()

        # Upload any remaining bytes (final part may be <PART_SIZE)
        if buf.tell() > 0:
            buf.seek(0)
            part = s3_client.upload_part(
                Bucket=bucket,
                Key=object_key,
                UploadId=upload_id,
                PartNumber=part_num,
                Body=buf.read(),
            )
            parts.append({"PartNumber": part_num, "ETag": part["ETag"]})
            logger.info(f"   📤 Final part {part_num} (100%)")

        # ---- complete upload -------------------------------------------
        s3_client.complete_multipart_upload(
            Bucket=bucket,
            Key=object_key,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts},
        )

        elapsed = time.time() - t0
        speed = size_mb / elapsed if elapsed > 0 else 0
        logger.info(f"✓ Streamed in {elapsed:.1f}s ({speed:.1f} MB/s) — no disk used")
        return f"{r2_public_url}/{object_key}"

    except Exception as exc:
        try:
            s3_client.abort_multipart_upload(
                Bucket=bucket, Key=object_key, UploadId=upload_id
            )
        except Exception:
            pass
        raise RuntimeError(f"Qobuz → R2 stream failed: {exc}") from exc


# ===========================================================================
# PUBLIC DOWNLOAD ENTRY POINT
# ===========================================================================

async def download_qobuz(
    url: str,
    quality: str = DEFAULT_QUALITY,
    upload_to_r2: bool = False,
    s3_client=None,
    r2_bucket: str = "",
    r2_public_url: str = "",
) -> Dict:
    """
    Resolve a Qobuz URL to a download URL.

    Default (upload_to_r2=False):
      Returns the signed Qobuz CDN URL in ~200-500 ms — no file transfer.
      URL is valid for ~10 minutes, long enough for the client to start
      downloading directly.

    With upload_to_r2=True:
      Streams the file to Cloudflare R2 and returns a permanent public URL.
      Slow — takes as long as the file itself takes to download.
    """
    url_type, item_id = parse_qobuz_url(url)
    fmt_id = QUALITY_MAP.get(quality, QUALITY_MAP[DEFAULT_QUALITY])

    account_id, client = qobuz_pool.acquire()
    try:
        loop = asyncio.get_event_loop()

        if url_type == "track":
            result = await loop.run_in_executor(
                None,
                lambda: _process_track(
                    item_id, fmt_id, client, account_id,
                    upload_to_r2, s3_client, r2_bucket, r2_public_url,
                ),
            )
        elif url_type == "album":
            result = await loop.run_in_executor(
                None,
                lambda: _process_album(
                    item_id, fmt_id, client, account_id,
                    upload_to_r2, s3_client, r2_bucket, r2_public_url,
                ),
            )
        else:
            raise ValueError(
                f"Unsupported Qobuz URL type: '{url_type}'. "
                "Only 'track' and 'album' are currently supported."
            )

        qobuz_pool.mark_success(account_id)
        return result

    except Exception as exc:
        err = str(exc).lower()
        if any(k in err for k in ("401", "auth", "token", "ineligible", "invalid app")):
            qobuz_pool.mark_failure(account_id, reason=str(exc)[:80])
            threading.Thread(
                target=qobuz_pool.reauthenticate, args=(account_id,), daemon=True
            ).start()
        else:
            qobuz_pool.mark_success(account_id)
        raise

    finally:
        qobuz_pool.release(account_id)


# ---------------------------------------------------------------------------
# Internal helpers    (synchronous, safe to run in executor)
# ---------------------------------------------------------------------------

def _safe_filename(text: str) -> str:
    """Strip unsafe characters and truncate to 100 chars."""
    cleaned = re.sub(r'[^\w\s\-.]', '', text).strip().replace(' ', '_')
    return cleaned[:100]


def _process_track(
    track_id: str,
    fmt_id: int,
    client,
    account_id: str,
    upload_to_r2: bool = False,
    s3_client=None,
    bucket: str = "",
    r2_public_url: str = "",
) -> Dict:
    """
    Resolve a Qobuz track to a download URL.

    Default (upload_to_r2=False): returns the signed Qobuz CDN URL instantly
    (~100-300 ms — just two API calls). Valid for ~10 minutes.

    With upload_to_r2=True: streams the file to Cloudflare R2 and returns
    a permanent public URL (slow — takes as long as the file download).
    """
    meta     = client.get_track_meta(track_id)
    url_data = client.get_track_url(track_id, fmt_id=fmt_id)

    if "sample" in url_data:
        raise ValueError("Only a sample is available for this track at the requested quality.")

    cdn_url   = url_data["url"]
    mime_type = url_data.get("mime_type", "audio/flac")
    srate     = url_data.get("sampling_rate")
    bdepth    = url_data.get("bit_depth")

    performer  = (
        (meta.get("performer") or {}).get("name")
        or (meta.get("album", {}).get("artist") or {}).get("name", "Unknown")
    )
    title      = meta.get("title", "Unknown")
    album_name = (meta.get("album") or {}).get("title", "Unknown")
    duration   = meta.get("duration", 0)
    ext        = "mp3" if int(fmt_id) == 5 else "flac"

    if upload_to_r2 and s3_client and bucket:
        safe_name  = _safe_filename(f"{performer} - {title}")
        object_key = f"qobuz/{track_id}_{safe_name}.{ext}"
        download_url = _stream_to_r2(cdn_url, object_key, s3_client, bucket, r2_public_url, mime_type)
        url_type = "r2"
    else:
        download_url = cdn_url
        object_key   = None
        url_type     = "cdn"  # signed, expires in ~10 min

    return {
        "success":       True,
        "type":          "track",
        "url_type":      url_type,
        "track_id":      track_id,
        "title":         title,
        "performer":     performer,
        "album":         album_name,
        "duration":      duration,
        "sampling_rate": srate,
        "bit_depth":     bdepth,
        "quality_label": f"{bdepth}bit/{srate}kHz" if bdepth and srate else quality_label(fmt_id),
        "download_url":  download_url,
        "object_key":    object_key,
        "account_used":  account_id,
    }


def _process_album(
    album_id: str,
    fmt_id: int,
    client,
    account_id: str,
    upload_to_r2: bool = False,
    s3_client=None,
    bucket: str = "",
    r2_public_url: str = "",
) -> Dict:
    """
    Resolve all tracks in a Qobuz album to download URLs.

    Default (upload_to_r2=False): returns signed CDN URLs for every track
    instantly (one getFileUrl API call per track, runs in parallel).

    With upload_to_r2=True: streams every track to R2 (slow).
    """
    meta = client.get_album_meta(album_id)

    if not meta.get("streamable"):
        raise ValueError("This album is not streamable on Qobuz.")

    album_title = meta.get("title", "Unknown")
    artist_name = (meta.get("artist") or {}).get("name", "Unknown")
    tracks      = (meta.get("tracks") or {}).get("items", [])

    results: List[Dict] = []

    def _resolve_track(track):
        tid = str(track["id"])
        try:
            url_data = client.get_track_url(tid, fmt_id=fmt_id)
            if "sample" in url_data:
                return {"track_id": tid, "skipped": "sample only"}

            cdn_url   = url_data["url"]
            mime_type = url_data.get("mime_type", "audio/flac")
            t_title   = track.get("title", "Unknown")
            t_num     = track.get("track_number", 0)
            ext       = "mp3" if int(fmt_id) == 5 else "flac"

            if upload_to_r2 and s3_client and bucket:
                safe_name  = _safe_filename(f"{t_num:02d}_{t_title}")
                object_key = f"qobuz/{album_id}/{tid}_{safe_name}.{ext}"
                download_url = _stream_to_r2(cdn_url, object_key, s3_client, bucket, r2_public_url, mime_type)
                url_type = "r2"
            else:
                download_url = cdn_url
                object_key   = None
                url_type     = "cdn"

            return {
                "track_id":     tid,
                "track_number": t_num,
                "title":        t_title,
                "download_url": download_url,
                "object_key":   object_key,
                "url_type":     url_type,
            }
        except Exception as exc:
            logger.error(f"  Track {tid} failed: {exc}")
            return {"track_id": tid, "error": str(exc)}

    # Resolve all track URLs in parallel (getFileUrl is independent per track)
    import concurrent.futures
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as pool:
        results = list(pool.map(_resolve_track, tracks))

    downloaded = sum(1 for r in results if "download_url" in r)

    return {
        "success":      True,
        "type":         "album",
        "album_id":     album_id,
        "title":        album_title,
        "artist":       artist_name,
        "total_tracks": len(tracks),
        "downloaded":   downloaded,
        "tracks":       results,
        "account_used": account_id,
    }


def quality_label(fmt_id: int) -> str:
    labels = {5: "MP3 320", 6: "16bit/44.1kHz", 7: "24bit/≤96kHz", 27: "24bit/>96kHz"}
    return labels.get(fmt_id, str(fmt_id))
