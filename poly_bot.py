import asyncio
import logging
import sqlite3
import time
from datetime import datetime
from logging.handlers import RotatingFileHandler
from typing import Dict, List, Optional, Tuple
import aiohttp

# ============================================================
# CONFIG  
# ============================================================
TELEGRAM_TOKEN = "YOUR_TELEGRAM_TOKEN"
TELEGRAM_CHAT_ID = "YOUR_TELEGRAM_CHAT_ID"

# Strict insider signal
MAX_FIRST_TRADE_AGE_DAYS = 10 # Max allowed age of the wallet's first detected Polymarket trade
MAX_GROUPED_BETS_FOR_SIGNAL = 3 # Max number of grouped bets allowed for a strict insider-style signal. Lower value = stricter filter
DEFAULT_MIN_BET_USD = 5_000 # Min USD size required for a strict INSIDER signal. now only bets of $5k+ can trigger strict insider alerts
CONCENTRATION_THRESHOLD = 0.80 # Min concentration required for a strict insider signal - current_bet_size / total_wallet_volume

# Separate threshold for BIG BET / ACTIVE WALLET
ENABLE_BIG_BET_ALERTS = True
BIG_BET_MIN_USD = 80_000 # BIG BET alerts  start only from $80k
BIG_BET_ALERT_COOLDOWN_SECONDS = 6 * 3600 # Cooldown between repeated BIG BET alerts for the same grouped bet

# Optional per-category overrides for INSIDER threshold
# Example: {"politics": 500}
CATEGORY_MIN_BET_USD: Dict[str, float] = {}

# Optional whitelist. Empty = all categories allowed.
# Example: {"politics"}
CATEGORY_WHITELIST = set()

# Near-real-time polling
POLL_INTERVAL_SECONDS = 8 # how often the bot polls Polymarket for new trades. Lower = faster updates, but more API requests.
GLOBAL_FETCH_LIMIT = 1000 # don't remember what it is, just leave it alone
RAW_POLL_FILTER_CASH_USD = 500
USER_HISTORY_PAGE_LIMIT = 200
MAX_USER_HISTORY_PAGES = 10 # Max number of wallet history pages to scan.
HEARTBEAT_SECONDS = 120 # How often the bot prints heartbeat / health status.
NO_NEW_RAW_TRADES_WARNING_SECONDS = 900

# Caching
EVENT_CACHE_TTL_SECONDS = 3600
WALLET_PROFILE_CACHE_TTL_SECONDS = 60  # How long wallet profile data stays cached in memory.

# Cleanup / retention
SEEN_RAW_RETENTION_DAYS = 7 # Smth related to data cleansing - just leave it alone too
BET_GROUP_RETENTION_DAYS = 45
EVENT_CACHE_RETENTION_DAYS = 7
WALLET_RETENTION_DAYS = 30 # How long cached event metadata stays in the database.
SENT_ALERT_RETENTION_DAYS = 14 # Used for anti-spam / cooldown logic

# Infrastructure
DATA_API = "https://data-api.polymarket.com"
GAMMA_API = "https://gamma-api.polymarket.com"
DB_PATH = "insider_bot_v666.sqlite3"
LOG_PATH = "insider_bot_v666.log"

# Status messaging
SEND_STARTUP_TO_TELEGRAM = True
SEND_WARNINGS_TO_TELEGRAM = True
SEND_HEARTBEAT_TO_TELEGRAM = False

# ============================================================
# LOGGING
# ============================================================
logger = logging.getLogger("insider_bot")
logger.setLevel(logging.INFO)
logger.handlers.clear()

formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

file_handler = RotatingFileHandler(
    LOG_PATH,
    maxBytes=20 * 1024 * 1024,
    backupCount=5,
    encoding="utf-8",
)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

log = logger

# ============================================================
# STATE
# ============================================================
stats = {
    "started_at": time.time(),
    "polls": 0,
    "rows_fetched": 0,
    "new_raw_rows": 0,
    "groups_touched": 0,
    "groups_evaluated": 0,
    "wallet_history_calls": 0,
    "signals_sent": 0,
    "big_bet_alerts_sent": 0,
    "last_poll_duration_sec": 0.0,
    "last_raw_trade_seen_at": 0.0,
    "last_error": "",
}
wallet_profile_cache: Dict[str, Tuple[float, dict]] = {}

# ============================================================
# DB
# ============================================================
def init_db() -> sqlite3.Connection:
    db = sqlite3.connect(DB_PATH, check_same_thread=False)
    db.execute("""
        CREATE TABLE IF NOT EXISTS seen_raw_trades (
            trade_key TEXT PRIMARY KEY,
            seen_at REAL NOT NULL
        )
    """)
    db.execute("""
        CREATE TABLE IF NOT EXISTS bet_groups (
            group_key TEXT PRIMARY KEY,
            wallet TEXT NOT NULL,
            transaction_hash TEXT,
            condition_id TEXT,
            event_slug TEXT,
            market_slug TEXT,
            title TEXT,
            outcome TEXT,
            side TEXT,
            first_ts REAL NOT NULL,
            last_ts REAL NOT NULL,
            total_usd REAL NOT NULL DEFAULT 0,
            total_tokens REAL NOT NULL DEFAULT 0,
            raw_rows INTEGER NOT NULL DEFAULT 0,
            category TEXT,
            alerted INTEGER NOT NULL DEFAULT 0,
            updated_at REAL NOT NULL
        )
    """)
    db.execute("""
        CREATE TABLE IF NOT EXISTS wallets (
            wallet TEXT PRIMARY KEY,
            first_trade_ts REAL,
            grouped_bets INTEGER NOT NULL DEFAULT 0,
            total_volume_usd REAL NOT NULL DEFAULT 0,
            max_group_usd REAL NOT NULL DEFAULT 0,
            flagged INTEGER NOT NULL DEFAULT 0,
            last_reason TEXT,
            last_checked REAL NOT NULL DEFAULT 0
        )
    """)
    db.execute("""
        CREATE TABLE IF NOT EXISTS event_cache (
            event_slug TEXT PRIMARY KEY,
            category TEXT,
            title TEXT,
            cached_at REAL NOT NULL
        )
    """)
    db.execute("""
        CREATE TABLE IF NOT EXISTS sent_alerts (
            alert_key TEXT PRIMARY KEY,
            sent_at REAL NOT NULL
        )
    """)
    db.commit()
    return db

DB = init_db()

# ============================================================
# HELPERS
# ============================================================
def now_ts() -> float:
    return time.time()

def safe_float(value, default=0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default

def parse_ts(value) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        value = float(value)
        if value > 1e12:
            return value / 1000.0
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
        except Exception:
            try:
                fv = float(value)
                return fv / 1000.0 if fv > 1e12 else fv
            except Exception:
                return None
    return None

def usd_fmt(x: float) -> str:
    return f"${x:,.2f}"

def compact_wallet(wallet: str) -> str:
    if not wallet or len(wallet) < 16:
        return wallet or "unknown"
    return f"{wallet[:8]}...{wallet[-6:]}"

def format_age(age_seconds: Optional[float]) -> str:
    if age_seconds is None:
        return "unknown"
    days = age_seconds / 86400
    if days < 1:
        return f"{age_seconds / 3600:.1f}h"
    return f"{days:.1f}d"

def build_event_url(event_slug: Optional[str]) -> str:
    return f"https://polymarket.com/event/{event_slug}" if event_slug else "https://polymarket.com"

def build_wallet_url(wallet: str) -> str:
    return f"https://polymarket.com/profile/{wallet}"

def raw_trade_key(trade: dict) -> str:
    tx = trade.get("transactionHash") or ""
    wallet = trade.get("proxyWallet") or ""
    condition_id = trade.get("conditionId") or ""
    outcome = str(trade.get("outcome") or "")
    side = str(trade.get("side") or "")
    ts = str(trade.get("timestamp") or "")
    size = str(trade.get("size") or "")
    price = str(trade.get("price") or "")
    return "|".join([tx, wallet, condition_id, outcome, side, ts, size, price])

def group_key_from_trade(trade: dict) -> str:
    tx = trade.get("transactionHash") or ""
    wallet = trade.get("proxyWallet") or ""
    condition_id = trade.get("conditionId") or ""
    outcome = str(trade.get("outcome") or "")
    side = str(trade.get("side") or "")
    return "|".join([tx, wallet, condition_id, outcome, side])

def trade_cash_usd(trade: dict) -> float:
    return safe_float(trade.get("size")) * safe_float(trade.get("price"))

def trade_tokens(trade: dict) -> float:
    return safe_float(trade.get("size"))

def effective_min_bet_usd(category: Optional[str]) -> float:
    if category:
        cat_key = category.strip().lower()
        if cat_key in CATEGORY_MIN_BET_USD:
            return CATEGORY_MIN_BET_USD[cat_key]
    return DEFAULT_MIN_BET_USD

def should_track_category(category: Optional[str]) -> bool:
    if not CATEGORY_WHITELIST:
        return True
    if not category:
        return False
    whitelist = {x.strip().lower() for x in CATEGORY_WHITELIST}
    return category.strip().lower() in whitelist

def db_row_exists(query: str, params: tuple) -> bool:
    return DB.execute(query, params).fetchone() is not None

def was_alert_sent_recently(alert_key: str, cooldown_seconds: int) -> bool:
    row = DB.execute(
        "SELECT sent_at FROM sent_alerts WHERE alert_key=?",
        (alert_key,),
    ).fetchone()
    if not row:
        return False
    return (time.time() - row[0]) < cooldown_seconds

def mark_alert_sent(alert_key: str):
    DB.execute(
        "INSERT OR REPLACE INTO sent_alerts(alert_key, sent_at) VALUES(?,?)",
        (alert_key, time.time()),
    )
    DB.commit()

def purge_wallet_profile_cache():
    now = now_ts()
    dead = [
        wallet
        for wallet, (cached_at, _) in wallet_profile_cache.items()
        if now - cached_at > WALLET_PROFILE_CACHE_TTL_SECONDS
    ]
    for wallet in dead:
        del wallet_profile_cache[wallet]
    if dead:
        log.info(f"[CACHE] purged wallet_profile_cache entries={len(dead)}")

# ============================================================
# TELEGRAM
# ============================================================
async def send_telegram(session: aiohttp.ClientSession, text: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": False,
    }
    try:
        async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status != 200:
                body = await r.text()
                log.warning(f"[TG] send failed [{r.status}]: {body}")
    except Exception as e:
        log.error(f"[TG] send exception: {e}")

async def maybe_send_status(session: aiohttp.ClientSession, text: str, send: bool):
    log.info(text.replace("\n", " | "))
    if send:
        await send_telegram(session, text)

# ============================================================
# API CHECKS
# ============================================================
async def check_public_apis(session: aiohttp.ClientSession):
    try:
        async with session.get(
            f"{DATA_API}/trades",
            params={"limit": 1, "takerOnly": "false"},
            timeout=aiohttp.ClientTimeout(total=10),
        ) as r:
            if r.status == 200:
                await maybe_send_status(session, "✅ <b>Data API OK</b>", SEND_STARTUP_TO_TELEGRAM)
            else:
                await maybe_send_status(session, f"❌ <b>Data API failed</b> [{r.status}]", True)
    except Exception as e:
        await maybe_send_status(session, f"❌ <b>Data API unreachable</b>\n<code>{str(e)[:300]}</code>", True)

    try:
        async with session.get(
            f"{GAMMA_API}/events",
            params={"limit": 1, "active": "true", "closed": "false"},
            timeout=aiohttp.ClientTimeout(total=10),
        ) as r:
            if r.status == 200:
                await maybe_send_status(session, "✅ <b>Gamma API OK</b>", SEND_STARTUP_TO_TELEGRAM)
            else:
                await maybe_send_status(session, f"❌ <b>Gamma API failed</b> [{r.status}]", True)
    except Exception as e:
        await maybe_send_status(session, f"❌ <b>Gamma API unreachable</b>\n<code>{str(e)[:300]}</code>", True)

# ============================================================
# EVENT CATEGORY CACHE
# ============================================================
async def get_event_category(
    session: aiohttp.ClientSession,
    event_slug: Optional[str],
    market_slug: Optional[str],
) -> Tuple[Optional[str], Optional[str]]:
    cache_key = f"{event_slug or ''}|{market_slug or ''}"

    row = DB.execute(
        "SELECT category, title, cached_at FROM event_cache WHERE event_slug=?",
        (cache_key,),
    ).fetchone()

    now = now_ts()
    if row and (now - row[2]) < EVENT_CACHE_TTL_SECONDS:
        return row[0], row[1]

    if event_slug:
        try:
            async with session.get(
                f"{GAMMA_API}/events/slug/{event_slug}",
                timeout=aiohttp.ClientTimeout(total=10),
            ) as r:
                if r.status == 200:
                    data = await r.json()
                    category = data.get("category")
                    title = data.get("title")
                    if category or title:
                        DB.execute(
                            "INSERT OR REPLACE INTO event_cache(event_slug, category, title, cached_at) VALUES(?,?,?,?)",
                            (cache_key, category, title, now),
                        )
                        DB.commit()
                        return category, title
                else:
                    log.debug(f"[GAMMA] event slug lookup failed [{r.status}] for {event_slug}")
        except Exception as e:
            log.warning(f"[GAMMA] event slug lookup exception for {event_slug}: {e}")

    if market_slug:
        try:
            async with session.get(
                f"{GAMMA_API}/markets/slug/{market_slug}",
                timeout=aiohttp.ClientTimeout(total=10),
            ) as r:
                if r.status == 200:
                    data = await r.json()
                    category = data.get("category")
                    title = data.get("question") or data.get("title")

                    if not category and isinstance(data.get("event"), dict):
                        category = data["event"].get("category")
                    if not title and isinstance(data.get("event"), dict):
                        title = data["event"].get("title")

                    DB.execute(
                        "INSERT OR REPLACE INTO event_cache(event_slug, category, title, cached_at) VALUES(?,?,?,?)",
                        (cache_key, category, title, now),
                    )
                    DB.commit()
                    return category, title
                else:
                    log.debug(f"[GAMMA] market slug lookup failed [{r.status}] for {market_slug}")
        except Exception as e:
            log.warning(f"[GAMMA] market slug lookup exception for {market_slug}: {e}")

    return None, None

# ============================================================
# GLOBAL TRADES
# ============================================================
async def fetch_recent_global_trades(session: aiohttp.ClientSession) -> List[dict]:
    params = {
        "limit": GLOBAL_FETCH_LIMIT,
        "offset": 0,
        "takerOnly": "false",
        "filterType": "CASH",
        "filterAmount": RAW_POLL_FILTER_CASH_USD,
    }
    async with session.get(
        f"{DATA_API}/trades",
        params=params,
        timeout=aiohttp.ClientTimeout(total=20),
    ) as r:
        if r.status != 200:
            raise RuntimeError(f"global trades fetch failed [{r.status}]")
        data = await r.json()
        return data if isinstance(data, list) else data.get("data", [])

def insert_seen_raw_trade(trade_key: str):
    DB.execute(
        "INSERT OR IGNORE INTO seen_raw_trades(trade_key, seen_at) VALUES(?,?)",
        (trade_key, now_ts()),
    )

def raw_trade_seen(trade_key: str) -> bool:
    return db_row_exists("SELECT 1 FROM seen_raw_trades WHERE trade_key=?", (trade_key,))

def upsert_group_from_trade(trade: dict):
    group_key = group_key_from_trade(trade)
    ts = parse_ts(trade.get("timestamp")) or now_ts()
    cash = trade_cash_usd(trade)
    tokens = trade_tokens(trade)

    row = DB.execute(
        "SELECT first_ts, last_ts, total_usd, total_tokens, raw_rows, alerted, category "
        "FROM bet_groups WHERE group_key=?",
        (group_key,),
    ).fetchone()

    values = {
        "wallet": trade.get("proxyWallet"),
        "transaction_hash": trade.get("transactionHash"),
        "condition_id": trade.get("conditionId"),
        "event_slug": trade.get("eventSlug"),
        "market_slug": trade.get("slug"),
        "title": trade.get("title"),
        "outcome": trade.get("outcome"),
        "side": trade.get("side"),
    }

    if row is None:
        DB.execute("""
            INSERT INTO bet_groups(
                group_key, wallet, transaction_hash, condition_id, event_slug, market_slug, title,
                outcome, side, first_ts, last_ts, total_usd, total_tokens, raw_rows,
                category, alerted, updated_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            group_key,
            values["wallet"],
            values["transaction_hash"],
            values["condition_id"],
            values["event_slug"],
            values["market_slug"],
            values["title"],
            values["outcome"],
            values["side"],
            ts,
            ts,
            cash,
            tokens,
            1,
            None,
            0,
            now_ts(),
        ))
    else:
        first_ts, last_ts, total_usd, total_tokens, raw_rows, alerted, category = row
        DB.execute("""
            UPDATE bet_groups
            SET wallet=?,
                transaction_hash=?,
                condition_id=?,
                event_slug=?,
                market_slug=?,
                title=?,
                outcome=?,
                side=?,
                first_ts=?,
                last_ts=?,
                total_usd=?,
                total_tokens=?,
                raw_rows=?,
                updated_at=?
            WHERE group_key=?
        """, (
            values["wallet"],
            values["transaction_hash"],
            values["condition_id"],
            values["event_slug"],
            values["market_slug"],
            values["title"],
            values["outcome"],
            values["side"],
            min(first_ts, ts),
            max(last_ts, ts),
            total_usd + cash,
            total_tokens + tokens,
            raw_rows + 1,
            now_ts(),
            group_key,
        ))

def get_group(group_key: str):
    row = DB.execute("""
        SELECT group_key, wallet, transaction_hash, condition_id, event_slug, market_slug, title, outcome, side,
               first_ts, last_ts, total_usd, total_tokens, raw_rows, category, alerted, updated_at
        FROM bet_groups
        WHERE group_key=?
    """, (group_key,)).fetchone()
    if not row:
        return None
    keys = [
        "group_key", "wallet", "transaction_hash", "condition_id", "event_slug", "market_slug", "title", "outcome", "side",
        "first_ts", "last_ts", "total_usd", "total_tokens", "raw_rows", "category", "alerted", "updated_at"
    ]
    return dict(zip(keys, row))

# ============================================================
# WALLET HISTORY
# ============================================================
def history_group_key(trade: dict) -> str:
    tx = trade.get("transactionHash") or ""
    condition_id = trade.get("conditionId") or ""
    outcome = str(trade.get("outcome") or "")
    side = str(trade.get("side") or "")
    return "|".join([tx, condition_id, outcome, side])

async def fetch_wallet_profile(session: aiohttp.ClientSession, wallet: str) -> dict:
    cached = wallet_profile_cache.get(wallet)
    now = now_ts()
    if cached and (now - cached[0]) < WALLET_PROFILE_CACHE_TTL_SECONDS:
        return cached[1]

    stats["wallet_history_calls"] += 1

    grouped: Dict[str, dict] = {}
    offset = 0
    pages = 0
    capped = False

    while pages < MAX_USER_HISTORY_PAGES:
        pages += 1
        params = {
            "user": wallet,
            "limit": USER_HISTORY_PAGE_LIMIT,
            "offset": offset,
            "takerOnly": "false",
        }
        async with session.get(
            f"{DATA_API}/trades",
            params=params,
            timeout=aiohttp.ClientTimeout(total=20),
        ) as r:
            if r.status != 200:
                raise RuntimeError(f"wallet history failed [{r.status}] for {wallet}")
            data = await r.json()
            rows = data if isinstance(data, list) else data.get("data", [])

        if not rows:
            break

        for trade in rows:
            gk = history_group_key(trade)
            ts = parse_ts(trade.get("timestamp")) or now
            usd = trade_cash_usd(trade)
            tokens = trade_tokens(trade)
            item = grouped.get(gk)
            if item is None:
                grouped[gk] = {
                    "group_key": gk,
                    "condition_id": trade.get("conditionId"),
                    "event_slug": trade.get("eventSlug"),
                    "market_slug": trade.get("slug"),
                    "title": trade.get("title"),
                    "outcome": trade.get("outcome"),
                    "side": trade.get("side"),
                    "transaction_hash": trade.get("transactionHash"),
                    "first_ts": ts,
                    "last_ts": ts,
                    "total_usd": usd,
                    "total_tokens": tokens,
                }
            else:
                item["first_ts"] = min(item["first_ts"], ts)
                item["last_ts"] = max(item["last_ts"], ts)
                item["total_usd"] += usd
                item["total_tokens"] += tokens

        if len(grouped) > MAX_GROUPED_BETS_FOR_SIGNAL:
            break

        if len(rows) < USER_HISTORY_PAGE_LIMIT:
            break

        offset += USER_HISTORY_PAGE_LIMIT
    else:
        capped = True

    grouped_bets = list(grouped.values())
    grouped_bets.sort(key=lambda x: x["first_ts"])

    first_trade_ts = min((x["first_ts"] for x in grouped_bets), default=None)
    total_volume_usd = sum(x["total_usd"] for x in grouped_bets)
    max_group_usd = max((x["total_usd"] for x in grouped_bets), default=0.0)

    profile = {
        "wallet": wallet,
        "first_trade_ts": first_trade_ts,
        "grouped_bets_count": len(grouped_bets),
        "total_volume_usd": total_volume_usd,
        "max_group_usd": max_group_usd,
        "grouped_bets": grouped_bets,
        "capped": capped,
    }

    wallet_profile_cache[wallet] = (now, profile)
    return profile

def upsert_wallet_state(wallet: str, profile: dict, flagged: bool, reason: str):
    DB.execute("""
        INSERT INTO wallets(wallet, first_trade_ts, grouped_bets, total_volume_usd, max_group_usd, flagged, last_reason, last_checked)
        VALUES(?,?,?,?,?,?,?,?)
        ON CONFLICT(wallet) DO UPDATE SET
            first_trade_ts=excluded.first_trade_ts,
            grouped_bets=excluded.grouped_bets,
            total_volume_usd=excluded.total_volume_usd,
            max_group_usd=excluded.max_group_usd,
            flagged=excluded.flagged,
            last_reason=excluded.last_reason,
            last_checked=excluded.last_checked
    """, (
        wallet,
        profile.get("first_trade_ts"),
        profile.get("grouped_bets_count", 0),
        profile.get("total_volume_usd", 0.0),
        profile.get("max_group_usd", 0.0),
        1 if flagged else 0,
        reason,
        now_ts(),
    ))
    DB.commit()

def wallet_already_flagged(wallet: str) -> bool:
    row = DB.execute("SELECT flagged FROM wallets WHERE wallet=?", (wallet,)).fetchone()
    return bool(row and row[0])

# ============================================================
# SIGNAL EVALUATION
# ============================================================
def build_alert_message(group: dict, profile: dict, category: Optional[str]) -> str:
    avg_price = 0.0
    if group["total_tokens"] > 0:
        avg_price = group["total_usd"] / group["total_tokens"]

    age_seconds = None
    if profile.get("first_trade_ts"):
        age_seconds = now_ts() - profile["first_trade_ts"]

    concentration = group["total_usd"] / max(profile["total_volume_usd"], 1.0)

    return (
        f"🔥 <b>INSIDER SIGNAL DETECTED</b>\n"
        f"{'─'*32}\n"
        f"👛 Wallet: <code>{compact_wallet(group['wallet'])}</code>\n"
        f"⏱ First trade age: <b>{format_age(age_seconds)}</b>\n"
        f"📊 Grouped bets: <b>{profile['grouped_bets_count']}</b>\n"
        f"💰 This bet: <b>{usd_fmt(group['total_usd'])}</b>\n"
        f"🧲 Concentration: <b>{concentration * 100:.1f}%</b>\n"
        f"🏷 Category: <b>{category or 'unknown'}</b>\n\n"
        f"📋 Market:\n{(group.get('title') or 'Unknown market')[:300]}\n"
        f"🎯 Outcome: <b>{group.get('outcome') or '?'}</b>\n"
        f"↔️ Side: <b>{group.get('side') or '?'}</b>\n"
        f"💵 Avg price: <b>{avg_price * 100:.2f}%</b>\n"
        f"🧩 Raw fills grouped: <b>{group.get('raw_rows')}</b>\n\n"
        f"🔗 <a href=\"{build_event_url(group.get('event_slug'))}\">Open market</a>\n"
        f"👛 <a href=\"{build_wallet_url(group['wallet'])}\">Open wallet</a>\n"
        f"🧾 Tx: <code>{(group.get('transaction_hash') or 'n/a')[:56]}</code>"
    )

def format_big_bet_alert(group: dict, profile: dict, failed: list[str]) -> str:
    wallet = group["wallet"]
    first_trade_ts = profile.get("first_trade_ts")
    age_seconds = (now_ts() - first_trade_ts) if first_trade_ts else None
    concentration = group["total_usd"] / max(profile["total_volume_usd"], 1.0)
    avg_price = 0.0
    if group["total_tokens"] > 0:
        avg_price = group["total_usd"] / group["total_tokens"]

    return (
        f"🐋 <b>BIG BET / ACTIVE WALLET</b>\n"
        f"{'─' * 32}\n"
        f"👛 Wallet: <code>{compact_wallet(wallet)}</code>\n"
        f"⏱️ First trade age: <b>{format_age(age_seconds)}</b>\n"
        f"📊 Grouped bets: <b>{profile['grouped_bets_count']}</b>\n"
        f"💰 This bet: <b>{usd_fmt(group['total_usd'])}</b>\n"
        f"🧲 Concentration: <b>{concentration * 100:.1f}%</b>\n"
        f"🏷 Category: <b>{group.get('category') or 'unknown'}</b>\n"
        f"❌ Failed insider checks: <b>{', '.join(failed)}</b>\n\n"
        f"📋 Market:\n{(group.get('title') or 'Unknown market')[:300]}\n"
        f"🎯 Outcome: <b>{group.get('outcome') or '?'}</b>\n"
        f"↔️ Side: <b>{group.get('side') or '?'}</b>\n"
        f"💵 Avg price: <b>{avg_price * 100:.2f}%</b>\n"
        f"🧩 Raw fills grouped: <b>{group.get('raw_rows', 0)}</b>\n\n"
        f"🔗 <a href=\"{build_event_url(group.get('event_slug'))}\">Open market</a>\n"
        f"👛 <a href=\"{build_wallet_url(group['wallet'])}\">Open wallet</a>"
    )

async def evaluate_group_for_signal(session: aiohttp.ClientSession, group_key: str):
    stats["groups_evaluated"] += 1
    group = get_group(group_key)
    if not group:
        return

    if group["alerted"]:
        return

    wallet = group["wallet"]
    if not wallet:
        return

    if wallet_already_flagged(wallet):
        log.info(f"[SKIP] wallet already flagged: {compact_wallet(wallet)}")
        return

    category = group.get("category")
    title = group.get("title")
    if category is None or title is None:
        fetched_category, fetched_title = await get_event_category(
            session,
            group.get("event_slug"),
            group.get("market_slug"),
        )
        category = fetched_category or category
        title = fetched_title or title
        DB.execute(
            "UPDATE bet_groups SET category=?, title=?, updated_at=? WHERE group_key=?",
            (category, title, now_ts(), group_key),
        )
        DB.commit()
        group["category"] = category
        group["title"] = title

    if not should_track_category(category):
        reason = f"category filtered out ({category or 'unknown'})"
        upsert_wallet_state(wallet, {
            "first_trade_ts": None,
            "grouped_bets_count": 0,
            "total_volume_usd": 0.0,
            "max_group_usd": 0.0,
        }, False, reason)
        log.info(f"[REJECT] {compact_wallet(wallet)} | {reason}")
        return

    insider_min_bet_usd = effective_min_bet_usd(category)
    if group["total_usd"] < insider_min_bet_usd:
        reason = f"group_usd {group['total_usd']:.2f} < threshold {insider_min_bet_usd:.2f}"
        upsert_wallet_state(wallet, {
            "first_trade_ts": None,
            "grouped_bets_count": 0,
            "total_volume_usd": 0.0,
            "max_group_usd": 0.0,
        }, False, reason)
        log.info(f"[REJECT] {compact_wallet(wallet)} | {reason}")
        return

    profile = await fetch_wallet_profile(session, wallet)

    if profile["capped"] and profile["grouped_bets_count"] <= MAX_GROUPED_BETS_FOR_SIGNAL:
        reason = "wallet history capped before proving <= max grouped bets"
        upsert_wallet_state(wallet, profile, False, reason)
        log.warning(f"[REJECT] {compact_wallet(wallet)} | {reason}")
        return

    first_trade_ts = profile["first_trade_ts"]
    age_ok = first_trade_ts is not None and (now_ts() - first_trade_ts) <= MAX_FIRST_TRADE_AGE_DAYS * 86400
    grouped_ok = profile["grouped_bets_count"] <= MAX_GROUPED_BETS_FOR_SIGNAL
    concentration = group["total_usd"] / max(profile["total_volume_usd"], 1.0)
    concentration_ok = concentration >= CONCENTRATION_THRESHOLD
    bet_size_ok = group["total_usd"] >= insider_min_bet_usd

    checks = {
        "first_trade_age": age_ok,
        "grouped_bets": grouped_ok,
        "bet_size": bet_size_ok,
        "concentration": concentration_ok,
    }
    failed = [k for k, v in checks.items() if not v]

    log.info(
        "[CHECK] wallet=%s age=%s grouped_bets=%s this_bet=%s total=%s concentration=%.3f category=%s %s",
        compact_wallet(wallet),
        format_age((now_ts() - first_trade_ts) if first_trade_ts else None),
        profile["grouped_bets_count"],
        usd_fmt(group["total_usd"]),
        usd_fmt(profile["total_volume_usd"]),
        concentration,
        category or "unknown",
        "✅ SIGNAL" if not failed else f"❌ {failed}",
    )

    if failed:
        upsert_wallet_state(wallet, profile, False, ", ".join(failed))

        if ENABLE_BIG_BET_ALERTS and group["total_usd"] >= BIG_BET_MIN_USD:
            alert_key = f"bigbet:{group['group_key']}"
            if not was_alert_sent_recently(alert_key, BIG_BET_ALERT_COOLDOWN_SECONDS):
                msg = format_big_bet_alert(group, profile, failed)
                await send_telegram(session, msg)
                mark_alert_sent(alert_key)
                stats["big_bet_alerts_sent"] += 1

        return

    msg = build_alert_message(group, profile, category)
    await send_telegram(session, msg)
    stats["signals_sent"] += 1

    DB.execute("UPDATE bet_groups SET alerted=1, updated_at=? WHERE group_key=?", (now_ts(), group_key))
    DB.commit()
    upsert_wallet_state(wallet, profile, True, "flagged")

# ============================================================
# POLLING
# ============================================================
async def polling_loop(session: aiohttp.ClientSession):
    while True:
        cycle_started = now_ts()
        touched_groups = set()
        try:
            stats["polls"] += 1

            trades = await fetch_recent_global_trades(session)
            fetched_count = len(trades)
            stats["rows_fetched"] += fetched_count

            if fetched_count >= GLOBAL_FETCH_LIMIT:
                log.warning(
                    "[POLL] fetched exactly limit=%s rows; consider increasing GLOBAL_FETCH_LIMIT or reducing POLL_INTERVAL_SECONDS",
                    GLOBAL_FETCH_LIMIT,
                )

            trades.sort(key=lambda t: parse_ts(t.get("timestamp")) or 0)

            new_rows = 0
            for trade in trades:
                tkey = raw_trade_key(trade)
                if raw_trade_seen(tkey):
                    continue

                insert_seen_raw_trade(tkey)
                upsert_group_from_trade(trade)
                touched_groups.add(group_key_from_trade(trade))
                new_rows += 1

            DB.commit()

            if new_rows > 0:
                stats["new_raw_rows"] += new_rows
                stats["last_raw_trade_seen_at"] = now_ts()

            stats["groups_touched"] += len(touched_groups)

            log.info(
                "[POLL] cycle=%s fetched=%s new_raw=%s touched_groups=%s last_raw_seen=%s",
                stats["polls"],
                fetched_count,
                new_rows,
                len(touched_groups),
                int(stats["last_raw_trade_seen_at"]) if stats["last_raw_trade_seen_at"] else "never",
            )

            for group_key in touched_groups:
                await evaluate_group_for_signal(session, group_key)

            if stats["polls"] % 100 == 0:
                cleanup_db()
                purge_wallet_profile_cache()

        except Exception as e:
            stats["last_error"] = str(e)
            log.exception(f"[POLL ERROR] {e}")

        stats["last_poll_duration_sec"] = now_ts() - cycle_started
        await asyncio.sleep(POLL_INTERVAL_SECONDS)

# ============================================================
# CLEANUP / HEALTH
# ============================================================
def cleanup_db():
    now = now_ts()
    raw_cutoff = now - SEEN_RAW_RETENTION_DAYS * 86400
    group_cutoff = now - BET_GROUP_RETENTION_DAYS * 86400
    event_cutoff = now - EVENT_CACHE_RETENTION_DAYS * 86400
    wallet_cutoff = now - WALLET_RETENTION_DAYS * 86400
    sent_alert_cutoff = now - SENT_ALERT_RETENTION_DAYS * 86400

    DB.execute("DELETE FROM seen_raw_trades WHERE seen_at < ?", (raw_cutoff,))
    DB.execute("DELETE FROM bet_groups WHERE updated_at < ? AND alerted = 0", (group_cutoff,))
    DB.execute("DELETE FROM event_cache WHERE cached_at < ?", (event_cutoff,))
    DB.execute("DELETE FROM wallets WHERE flagged = 0 AND last_checked < ?", (wallet_cutoff,))
    DB.execute("DELETE FROM sent_alerts WHERE sent_at < ?", (sent_alert_cutoff,))
    DB.commit()
    log.info("[CLEANUP] database cleanup completed")

async def heartbeat_loop(session: aiohttp.ClientSession):
    while True:
        uptime_min = round((now_ts() - stats["started_at"]) / 60, 1)
        wallets_count = DB.execute("SELECT COUNT(*) FROM wallets").fetchone()[0]
        flagged_count = DB.execute("SELECT COUNT(*) FROM wallets WHERE flagged = 1").fetchone()[0]
        groups_count = DB.execute("SELECT COUNT(*) FROM bet_groups").fetchone()[0]

        last_raw_delta = None
        if stats["last_raw_trade_seen_at"]:
            last_raw_delta = int(now_ts() - stats["last_raw_trade_seen_at"])

        msg = (
            f"💓 INSIDER BOT HEARTBEAT | "
            f"uptime_min={uptime_min} | polls={stats['polls']} | fetched_rows={stats['rows_fetched']} | "
            f"new_raw_rows={stats['new_raw_rows']} | touched_groups={stats['groups_touched']} | "
            f"evaluated_groups={stats['groups_evaluated']} | wallet_history_calls={stats['wallet_history_calls']} | "
            f"wallets={wallets_count} | groups={groups_count} | flagged={flagged_count} | "
            f"insider_signals={stats['signals_sent']} | big_bet_alerts={stats['big_bet_alerts_sent']} | "
            f"last_raw_trade_sec_ago={last_raw_delta if last_raw_delta is not None else 'never'} | "
            f"last_poll_sec={stats['last_poll_duration_sec']:.2f} | last_error={stats['last_error'] or 'none'}"
        )
        log.info(msg)
        if SEND_HEARTBEAT_TO_TELEGRAM:
            await send_telegram(session, msg)
        await asyncio.sleep(HEARTBEAT_SECONDS)

async def no_raw_trade_watchdog(session: aiohttp.ClientSession):
    while True:
        if stats["last_raw_trade_seen_at"]:
            delta = now_ts() - stats["last_raw_trade_seen_at"]
            if delta > NO_NEW_RAW_TRADES_WARNING_SECONDS:
                await maybe_send_status(
                    session,
                    f"⚠️ <b>NO NEW RAW TRADES</b>\n"
                    f"No new public raw trades above <b>{usd_fmt(RAW_POLL_FILTER_CASH_USD)}</b> "
                    f"for <b>{int(delta)} sec</b>.\n"
                    f"Check polling, filters, or API availability.",
                    SEND_WARNINGS_TO_TELEGRAM,
                )
                stats["last_raw_trade_seen_at"] = now_ts()
        await asyncio.sleep(60)

# ============================================================
# MAIN
# ============================================================
async def main():
    if not TELEGRAM_TOKEN or TELEGRAM_TOKEN == "PASTE_NEW_TELEGRAM_BOT_TOKEN_HERE":
        raise RuntimeError("Paste TELEGRAM_TOKEN into the code.")
    if not TELEGRAM_CHAT_ID or TELEGRAM_CHAT_ID == "PASTE_TELEGRAM_CHAT_ID_HERE":
        raise RuntimeError("Paste TELEGRAM_CHAT_ID into the code.")

    startup_msg = (
        f"🚀 <b>Polymarket Insider Bot started</b>\n"
        f"Fresh wallet rule: first trade age ≤ <b>{MAX_FIRST_TRADE_AGE_DAYS}d</b>\n"
        f"Grouped bets ≤ <b>{MAX_GROUPED_BETS_FOR_SIGNAL}</b>\n"
        f"INSIDER threshold ≥ <b>{usd_fmt(DEFAULT_MIN_BET_USD)}</b>\n"
        f"BIG BET threshold ≥ <b>{usd_fmt(BIG_BET_MIN_USD)}</b>\n"
        f"Concentration ≥ <b>{CONCENTRATION_THRESHOLD * 100:.0f}%</b>\n"
        f"Raw polling floor ≥ <b>{usd_fmt(RAW_POLL_FILTER_CASH_USD)}</b>\n"
        f"Poll interval: <b>{POLL_INTERVAL_SECONDS}s</b>\n"
        f"Category overrides: <b>{CATEGORY_MIN_BET_USD or 'none'}</b>\n"
        f"Category whitelist: <b>{sorted(CATEGORY_WHITELIST) if CATEGORY_WHITELIST else 'all'}</b>"
    )

    async with aiohttp.ClientSession() as session:
        await maybe_send_status(session, startup_msg, SEND_STARTUP_TO_TELEGRAM)
        await check_public_apis(session)

        await asyncio.gather(
            polling_loop(session),
            heartbeat_loop(session),
            no_raw_trade_watchdog(session),
        )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Bot stopped.")
