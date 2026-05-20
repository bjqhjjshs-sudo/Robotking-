#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════════╗
║  ALPHABOT PRO — MAIN ORCHESTRATOR  v2.0                                 ║
║                                                                          ║
║  Pipeline complet :                                                      ║
║    Market Data → HTF Bias → AMD Filter → FVG/OB → Session Filter       ║
║    → News Filter → Spread Filter → Module M → Module H                  ║
║    → Effective RR → Correlation Exposure → Telegram Delivery            ║
║                                                                          ║
║  Hardening intégré :                                                    ║
║    ✅ Cooldown par symbole (2h)                                          ║
║    ✅ Anti-duplicate setup hash (6h TTL)                                 ║
║    ✅ News hard filter — NO-TRADE zone dans ±20 min                     ║
║    ✅ Spread live filter (ratio spread/ATR)                              ║
║    ✅ Session quality score                                              ║
║    ✅ Killzone engine (London 07-10 UTC / NY 12-15 UTC)                 ║
║    ✅ Correlation / USD exposure cap                                     ║
║    ✅ OHLC cache avec TTL par timeframe                                  ║
║    ✅ RR calculé sur effective entry (ask/bid + spread)                  ║
║    ✅ Render watchdog — exception isolation, jamais de crash silencieux  ║
║                                                                          ║
║  Variables d'environnement requises :                                   ║
║    TELEGRAM_BOT_TOKEN   — token @BotFather                              ║
║    TELEGRAM_CHAT_ID     — ID du canal/groupe                            ║
║    ACCOUNT_BALANCE      — balance USD (ex. "2000")                      ║
║                                                                          ║
║  Dépendances :                                                           ║
║    pip install yfinance python-dotenv requests                          ║
╚══════════════════════════════════════════════════════════════════════════╝
"""

from __future__ import annotations

import asyncio
import datetime
import hashlib
import logging
import os
import time
from typing import Dict, List, Optional, Tuple

import requests
import yfinance as yf
from dotenv import load_dotenv

# ── Modules AlphaBot ──────────────────────────────────────────────────────
from alphabot_survival_v4 import (
    compute_position_size,
    DailyRiskManager,
    detect_market_structure,
    detect_fvg,
    detect_order_blocks,
)
from alphabot_module_m import compute_effective_sl, pipeline_sl_to_risk, fmt_sl_block

# ══════════════════════════════════════════════════════════════════════════
#  CONFIGURATION
# ══════════════════════════════════════════════════════════════════════════

load_dotenv()

BOT_TOKEN       = os.environ["TELEGRAM_BOT_TOKEN"]
CHAT_ID         = os.environ["TELEGRAM_CHAT_ID"]
ACCOUNT_BALANCE = float(os.getenv("ACCOUNT_BALANCE", "2000"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("AlphaBotPRO")

# ── Tickers yfinance ──────────────────────────────────────────────────────
YF_TICKERS: Dict[str, str] = {
    "XAUUSD": "GC=F",
    "XAGUSD": "SI=F",
    "EURUSD": "EURUSD=X",
    "GBPUSD": "GBPUSD=X",
    "USDJPY": "JPY=X",
    "AUDUSD": "AUDUSD=X",
    "USDCHF": "CHF=X",
    "USDCAD": "CAD=X",
    "GBPJPY": "GBPJPY=X",
    "NAS100": "NQ=F",
}

# Crypto Binance — API publique (aucune clé requise)
BINANCE_SYMBOLS: Dict[str, str] = {
    "BTCUSDT": "BTCUSDT",
    "ETHUSDT":  "ETHUSDT",
}

# ── Tiers de priorité ─────────────────────────────────────────────────────
TIER_1 = ["XAUUSD", "BTCUSDT"]
TIER_2 = ["XAGUSD", "ETHUSDT", "GBPJPY", "NAS100"]
TIER_3 = ["EURUSD", "GBPUSD", "USDJPY", "AUDUSD", "USDCHF", "USDCAD"]
ALL_SYMBOLS = TIER_1 + TIER_2 + TIER_3

# ── Corrélation USD : groupes à exposure limitée ─────────────────────────
# Chaque groupe = paires qui bougent ensemble quand USD se déplace
USD_CORR_GROUPS: Dict[str, List[str]] = {
    "USD_STRENGTH": ["EURUSD", "GBPUSD", "AUDUSD", "USDCHF", "USDCAD", "USDJPY"],
    "RISK_ON":      ["XAUUSD", "XAGUSD", "NAS100", "BTCUSDT", "ETHUSDT"],
    "GBP":          ["GBPUSD", "GBPJPY"],
}
MAX_CORR_EXPOSURE = 2   # max 2 positions simultanées dans le même groupe

# ── Spreads typiques en pips ──────────────────────────────────────────────
SPREAD_PIPS: Dict[str, float] = {
    "XAUUSD":  3.0,  "XAGUSD":  5.0,
    "BTCUSDT": 20.0, "ETHUSDT": 5.0,
    "EURUSD":  1.2,  "GBPUSD":  1.5,
    "USDJPY":  1.0,  "AUDUSD":  1.5,
    "USDCHF":  2.0,  "USDCAD":  2.0,
    "GBPJPY":  2.5,  "NAS100":  4.0,
}

# Pip size par symbole (pour convertir spread pips → prix)
PIP_SIZE: Dict[str, float] = {
    "XAUUSD": 0.10,   "XAGUSD": 0.001,
    "BTCUSDT": 1.0,   "ETHUSDT": 0.01,
    "EURUSD": 0.0001, "GBPUSD": 0.0001,
    "USDJPY": 0.01,   "AUDUSD": 0.0001,
    "USDCHF": 0.0001, "USDCAD": 0.0001,
    "GBPJPY": 0.01,   "NAS100": 1.0,
}

# ── Symboles concernés par le hard news filter ────────────────────────────
NEWS_SENSITIVE = {"XAUUSD", "EURUSD", "GBPUSD", "NAS100", "USDJPY", "XAGUSD"}

# ── Risk parameters ───────────────────────────────────────────────────────
FIXED_RISK_USD = float(os.getenv("FIXED_RISK_USD", "50"))  # Risque fixe 50 USD par trade

# ── Timing ───────────────────────────────────────────────────────────────
MAX_SIGNALS_PER_DAY  = 10
MIN_GAP_GLOBAL_MIN   = 30   # gap global minimum entre signaux
SYMBOL_COOLDOWN_H    = 2    # cooldown par symbole en heures
SETUP_HASH_TTL_H     = 6    # durée de vie du hash anti-duplicate
SCAN_INTERVAL_SEC    = 300  # cycle de scan (5 min)


# ══════════════════════════════════════════════════════════════════════════
#  KILLZONE ENGINE
# ══════════════════════════════════════════════════════════════════════════

# Fenêtres horaires UTC (début inclusif, fin exclusif)
KILLZONES = {
    "LONDON_OPEN":  (7,  10),   # 07:00–10:00 UTC
    "NY_OPEN":      (12, 15),   # 12:00–15:00 UTC
}

# Score de qualité de session — multiplicateur appliqué au score setup
SESSION_QUALITY: Dict[str, float] = {
    "LONDON_OPEN":       1.3,
    "NY_OPEN":           1.2,
    "LONDON":            1.0,
    "LONDON_NY_OVERLAP": 1.1,
    "NEW_YORK":          0.9,
    "ASIAN":             0.5,
    "OFF":               0.4,
}

def get_current_session() -> str:
    """
    Retourne la session + killzone active selon l'heure UTC.

    Ordre de priorité : killzones d'abord, puis sessions générales.
    """
    h = datetime.datetime.utcnow().hour
    for kz, (start, end) in KILLZONES.items():
        if start <= h < end:
            return kz
    if 7  <= h < 12:  return "LONDON"
    if 12 <= h < 16:  return "LONDON_NY_OVERLAP"
    if 16 <= h < 21:  return "NEW_YORK"
    if 0  <= h <  7:  return "ASIAN"
    return "OFF"

def session_to_module_m(session: str) -> str:
    """Mappe les sessions étendues vers les clés de Module M."""
    mapping = {
        "LONDON_OPEN":       "LONDON",
        "NY_OPEN":           "NEW_YORK",
        "LONDON":            "LONDON",
        "LONDON_NY_OVERLAP": "LONDON_NY_OVERLAP",
        "NEW_YORK":          "NEW_YORK",
        "ASIAN":             "ASIAN",
        "OFF":               "OFF",
    }
    return mapping.get(session, "OFF")

def session_allows_tier3(session: str) -> bool:
    """Tier 3 : uniquement sessions actives (pas Asie, pas OFF)."""
    return session in ("LONDON", "LONDON_OPEN", "LONDON_NY_OVERLAP", "NY_OPEN", "NEW_YORK")


# ══════════════════════════════════════════════════════════════════════════
#  NEWS ENGINE
# ══════════════════════════════════════════════════════════════════════════

_news_cache: Dict = {"events": [], "expires": 0}

def _fetch_news_events() -> List[Dict]:
    """Télécharge le calendrier ForexFactory JSON de la semaine. Cache 15 min."""
    global _news_cache
    now = time.time()

    if now < _news_cache["expires"]:
        return _news_cache["events"]

    try:
        resp = requests.get(
            "https://nfs.faireconomy.media/ff_calendar_thisweek.json",
            timeout=6,
        )
        if resp.status_code == 200:
            _news_cache = {"events": resp.json(), "expires": now + 900}
            return _news_cache["events"]
    except Exception as e:
        log.warning("ForexFactory fetch failed : %s", e)

    _news_cache["expires"] = now + 300   # retry dans 5 min
    return _news_cache.get("events", [])


def get_news_risk() -> str:
    """Niveau de risque news global : NONE | MEDIUM | HIGH | NFP."""
    events   = _fetch_news_events()
    utc_now  = datetime.datetime.utcnow()
    risk     = "NONE"

    for ev in events:
        ev_time_str = ev.get("date", "")
        if not ev_time_str:
            continue
        try:
            ev_dt = datetime.datetime.fromisoformat(ev_time_str.replace("Z", "+00:00"))
            ev_dt = ev_dt.replace(tzinfo=None)
        except ValueError:
            continue

        diff_min = (utc_now - ev_dt).total_seconds() / 60
        if not (-30 <= diff_min <= 30):
            continue

        impact = ev.get("impact", "").upper()
        title  = ev.get("title", "").upper()

        if "NON-FARM" in title or "NFP" in title:
            return "NFP"
        elif impact == "HIGH":
            risk = "HIGH"
        elif impact == "MEDIUM" and risk == "NONE":
            risk = "MEDIUM"

    return risk


def news_hard_block(symbol: str, news_risk: str) -> bool:
    """
    Retourne True si ce symbole doit être bloqué (NO-TRADE zone).

    Règle : si news HIGH ou NFP dans ±20 min, bloquer les paires sensibles.
    MEDIUM → avertissement seulement (Module M élargit le SL suffisamment).
    """
    if symbol not in NEWS_SENSITIVE:
        return False

    events  = _fetch_news_events()
    utc_now = datetime.datetime.utcnow()

    for ev in events:
        ev_time_str = ev.get("date", "")
        if not ev_time_str:
            continue
        try:
            ev_dt = datetime.datetime.fromisoformat(ev_time_str.replace("Z", "+00:00"))
            ev_dt = ev_dt.replace(tzinfo=None)
        except ValueError:
            continue

        diff_min = (utc_now - ev_dt).total_seconds() / 60
        if not (-20 <= diff_min <= 20):
            continue

        impact = ev.get("impact", "").upper()
        title  = ev.get("title", "").upper()

        if "NON-FARM" in title or "NFP" in title:
            log.info("  %s : BLOCKED — NFP dans %.0f min", symbol, diff_min)
            return True
        if impact == "HIGH":
            log.info("  %s : BLOCKED — HIGH news dans %.0f min", symbol, diff_min)
            return True

    return False


# ══════════════════════════════════════════════════════════════════════════
#  DATA CACHE + FETCHER
# ══════════════════════════════════════════════════════════════════════════

# TTL par timeframe en secondes
_CACHE_TTL = {"15m": 120, "30m": 180, "1h": 300, "4h": 900, "1d": 3600}
_ohlc_cache: Dict[Tuple[str, str], Dict] = {}


def _cache_get(symbol: str, interval: str) -> Optional[List[Dict]]:
    key  = (symbol, interval)
    entry = _ohlc_cache.get(key)
    if not entry:
        return None
    ttl = _CACHE_TTL.get(interval, 300)
    if time.time() - entry["ts"] > ttl:
        return None
    return entry["data"]


def _cache_set(symbol: str, interval: str, data: List[Dict]):
    _ohlc_cache[(symbol, interval)] = {"ts": time.time(), "data": data}


def fetch_candles_yf(symbol: str, interval: str, bars: int = 150) -> Optional[List[Dict]]:
    ticker = YF_TICKERS.get(symbol)
    if not ticker:
        return None

    period_map = {"15m": "5d", "30m": "7d", "1h": "30d", "4h": "60d", "1d": "1y"}
    period = period_map.get(interval, "30d")

    try:
        df = yf.download(ticker, period=period, interval=interval,
                         progress=False, auto_adjust=True)
        if df.empty or len(df) < 20:
            return None

        df = df.tail(bars)
        candles = []
        for ts, row in df.iterrows():
            candles.append({
                "time":   ts.timestamp(),
                "open":   float(row["Open"]),
                "high":   float(row["High"]),
                "low":    float(row["Low"]),
                "close":  float(row["Close"]),
                "volume": float(row.get("Volume", 0)),
            })
        return candles

    except Exception as e:
        log.error("yfinance %s/%s : %s", symbol, interval, e)
        return None


def fetch_candles_binance(symbol: str, interval: str, bars: int = 150) -> Optional[List[Dict]]:
    binance_sym = BINANCE_SYMBOLS.get(symbol, symbol)
    url    = "https://api.binance.com/api/v3/klines"
    params = {"symbol": binance_sym, "interval": interval, "limit": bars}

    try:
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        candles = []
        for k in resp.json():
            candles.append({
                "time":   k[0] / 1000,
                "open":   float(k[1]),
                "high":   float(k[2]),
                "low":    float(k[3]),
                "close":  float(k[4]),
                "volume": float(k[5]),
            })
        return candles
    except Exception as e:
        log.error("Binance %s/%s : %s", symbol, interval, e)
        return None


def fetch_candles(symbol: str, interval: str, bars: int = 150) -> Optional[List[Dict]]:
    """Fetch avec cache TTL. Route vers Binance ou yfinance."""
    cached = _cache_get(symbol, interval)
    if cached is not None:
        return cached

    if symbol in BINANCE_SYMBOLS:
        data = fetch_candles_binance(symbol, interval, bars)
    else:
        data = fetch_candles_yf(symbol, interval, bars)

    if data:
        _cache_set(symbol, interval, data)
    return data


# ══════════════════════════════════════════════════════════════════════════
#  INDICATEURS TECHNIQUES
# ══════════════════════════════════════════════════════════════════════════

def compute_atr(candles: List[Dict], period: int = 14) -> float:
    if len(candles) < period + 1:
        return 0.0
    trs = []
    for i in range(1, len(candles)):
        h, l, pc = candles[i]["high"], candles[i]["low"], candles[i-1]["close"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    atr = sum(trs[:period]) / period
    for tr in trs[period:]:
        atr = (atr * (period - 1) + tr) / period
    return round(atr, 8)


def compute_ema(closes: List[float], period: int) -> List[float]:
    if len(closes) < period:
        return []
    k = 2 / (period + 1)
    ema = [sum(closes[:period]) / period]
    for c in closes[period:]:
        ema.append(c * k + ema[-1] * (1 - k))
    return ema


def get_htf_bias(symbol: str) -> str:
    """Bias directionnel H4 via EMA50/EMA200 + position prix."""
    candles = fetch_candles(symbol, "4h", bars=220)
    if not candles or len(candles) < 205:
        return "NEUTRAL"

    closes = [c["close"] for c in candles]
    ema50  = compute_ema(closes, 50)
    ema200 = compute_ema(closes, 200)

    if not ema50 or not ema200:
        return "NEUTRAL"

    price = closes[-1]
    if price > ema50[-1] > ema200[-1]:
        return "BULLISH"
    if price < ema50[-1] < ema200[-1]:
        return "BEARISH"
    return "NEUTRAL"


def get_live_spread(symbol: str, candles_m1: Optional[List[Dict]] = None) -> float:
    """
    Spread live estimé en pips.
    Si on a des bougies M1, utilise le range moyen des 3 dernières comme proxy.
    Sinon retourne la valeur statique de SPREAD_PIPS.
    """
    static = SPREAD_PIPS.get(symbol, 3.0)
    if not candles_m1 or len(candles_m1) < 3:
        return static

    pip = PIP_SIZE.get(symbol, 0.0001)
    recent_ranges = [
        (c["high"] - c["low"]) / pip
        for c in candles_m1[-3:]
    ]
    avg_range = sum(recent_ranges) / len(recent_ranges)
    # Proxy: spread ≈ 5% du range M1 pour les paires liquides
    return max(static, avg_range * 0.05)


# ══════════════════════════════════════════════════════════════════════════
#  SPREAD LIVE FILTER
# ══════════════════════════════════════════════════════════════════════════

def spread_is_toxic(symbol: str, spread_pips: float, atr_m15: float) -> bool:
    """
    Bloque si spread > 25% de l'ATR M15.
    En pips : spread_price / atr_price.

    Signale rollover, widening broker, ou liquidité toxique.
    """
    pip = PIP_SIZE.get(symbol, 0.0001)
    spread_price = spread_pips * pip
    if atr_m15 <= 0:
        return False
    ratio = spread_price / atr_m15
    if ratio > 0.25:
        log.info("  %s : SPREAD TOXIQUE — spread=%.2f pips ratio=%.2f ATR", symbol, spread_pips, ratio)
        return True
    return False


# ══════════════════════════════════════════════════════════════════════════
#  VOLUME INSTITUTIONNEL — Axe 1
#  Volume relatif (anomalie) + Delta proxy directionnel
# ══════════════════════════════════════════════════════════════════════════

def compute_volume_profile(candles: List[Dict], lookback: int = 20) -> Dict:
    """
    Retourne un profil volume complet sur les N dernières bougies.

    Champs retournés :
        vol_ratio       — volume de la dernière bougie / moyenne N bougies
        delta_ratio     — ratio volume haussier vs total (proxy delta)
        absorption      — True si narrow-range + volume ×2 (absorption institutionnelle)
        exhaustion      — True si wide-range + volume ×2 + bougie de clôture faible
        vol_signal      — "STRONG_BUY" | "STRONG_SELL" | "ABSORPTION" | "EXHAUSTION" | "NEUTRAL"
        institutional   — True si au moins un signal fort détecté
    """
    if not candles or len(candles) < lookback + 2:
        return {"vol_signal": "NEUTRAL", "institutional": False,
                "vol_ratio": 1.0, "delta_ratio": 0.5,
                "absorption": False, "exhaustion": False}

    recent   = candles[-(lookback + 1):-1]   # N bougies historiques
    last     = candles[-1]                    # bougie courante
    last_vol = last.get("volume", 0)

    # ── Volume relatif ─────────────────────────────────────────────────────
    avg_vol  = sum(c.get("volume", 0) for c in recent) / max(len(recent), 1)
    vol_ratio = last_vol / avg_vol if avg_vol > 0 else 1.0

    # ── Delta proxy : pour chaque bougie, si close > open → volume haussier
    bull_vol = sum(
        c.get("volume", 0) for c in candles[-lookback:]
        if c["close"] >= c["open"]
    )
    total_vol = sum(c.get("volume", 0) for c in candles[-lookback:])
    delta_ratio = bull_vol / total_vol if total_vol > 0 else 0.5

    # ── Absorption : bougie narrow-range + volume anormalement élevé
    last_range   = last["high"] - last["low"]
    atr_approx   = sum(
        abs(candles[i]["high"] - candles[i]["low"])
        for i in range(-min(14, len(candles)), -1)
    ) / 14
    absorption = (
        last_range < 0.5 * atr_approx   # range étroit
        and vol_ratio >= 1.8             # mais volume ×1.8
    )

    # ── Exhaustion : wide-range + volume élevé + clôture au mauvais bout
    body_ratio = abs(last["close"] - last["open"]) / max(last_range, 1e-10)
    exhaustion = (
        last_range > 1.5 * atr_approx   # bougie large
        and vol_ratio >= 1.8             # volume élevé
        and body_ratio < 0.35            # petite mèche fermée (rejet)
    )

    # ── Signal synthétique ─────────────────────────────────────────────────
    if absorption:
        sig = "ABSORPTION"
    elif exhaustion:
        sig = "EXHAUSTION"
    elif vol_ratio >= 1.5 and delta_ratio >= 0.65:
        sig = "STRONG_BUY"
    elif vol_ratio >= 1.5 and delta_ratio <= 0.35:
        sig = "STRONG_SELL"
    else:
        sig = "NEUTRAL"

    institutional = sig in ("STRONG_BUY", "STRONG_SELL", "ABSORPTION", "EXHAUSTION")

    return {
        "vol_signal":    sig,
        "institutional": institutional,
        "vol_ratio":     round(vol_ratio, 2),
        "delta_ratio":   round(delta_ratio, 3),
        "absorption":    absorption,
        "exhaustion":    exhaustion,
    }


def volume_confirms_direction(vol_profile: Dict, direction: str) -> bool:
    """
    Vérifie si le volume institutionnel valide la direction du trade.

    LONG  → STRONG_BUY ou ABSORPTION (acheteurs absorbent les vendeurs)
    SHORT → STRONG_SELL ou EXHAUSTION (vendeurs dominent / rejet haussier)
    """
    sig = vol_profile.get("vol_signal", "NEUTRAL")
    dr  = vol_profile.get("delta_ratio", 0.5)

    if direction == "LONG":
        return sig in ("STRONG_BUY", "ABSORPTION") or dr >= 0.60
    elif direction == "SHORT":
        return sig in ("STRONG_SELL", "EXHAUSTION") or dr <= 0.40
    return False


# ══════════════════════════════════════════════════════════════════════════
#  LIQUIDITÉ RÉELLE — Axe 2
#  Equal Highs / Equal Lows → BSL/SSL → Inducement zones
#  Double confirmation : M15 + H4
# ══════════════════════════════════════════════════════════════════════════

def detect_liquidity_zones(candles: List[Dict], atr: float,
                           tolerance_factor: float = 0.15) -> Dict:
    """
    Détecte les zones de liquidité réelles sur une série de bougies.

    Equal highs = BSL (Buy-Side Liquidity) — stops des shorts
    Equal lows  = SSL (Sell-Side Liquidity) — stops des longs

    Retourne :
        bsl_zones  — liste de niveaux BSL (equal highs)
        ssl_zones  — liste de niveaux SSL (equal lows)
        inducement — True si le prix a récemment sweeped un niveau
        swept_bsl  — True si BSL sweeped sur la dernière bougie
        swept_ssl  — True si SSL sweeped sur la dernière bougie
    """
    if not candles or len(candles) < 10 or atr <= 0:
        return {"bsl_zones": [], "ssl_zones": [],
                "inducement": False, "swept_bsl": False, "swept_ssl": False}

    tol      = atr * tolerance_factor
    highs    = [c["high"] for c in candles]
    lows     = [c["low"]  for c in candles]
    last_c   = candles[-1]

    # ── Equal Highs (BSL) ──────────────────────────────────────────────────
    bsl_zones: List[float] = []
    for i in range(len(highs) - 2):
        h = highs[i]
        # Cherche un autre high dans la tolérance ATR, avant la bougie courante
        matches = [
            j for j in range(i + 2, len(highs) - 1)
            if abs(highs[j] - h) <= tol
        ]
        if matches:
            bsl_level = (h + highs[matches[-1]]) / 2
            # Filtre doublons
            if not any(abs(bsl_level - existing) <= tol for existing in bsl_zones):
                bsl_zones.append(round(bsl_level, 8))

    # ── Equal Lows (SSL) ───────────────────────────────────────────────────
    ssl_zones: List[float] = []
    for i in range(len(lows) - 2):
        l = lows[i]
        matches = [
            j for j in range(i + 2, len(lows) - 1)
            if abs(lows[j] - l) <= tol
        ]
        if matches:
            ssl_level = (l + lows[matches[-1]]) / 2
            if not any(abs(ssl_level - existing) <= tol for existing in ssl_zones):
                ssl_zones.append(round(ssl_level, 8))

    # ── Sweep detection sur la dernière bougie ─────────────────────────────
    swept_bsl = any(last_c["high"] > lvl > last_c["close"] for lvl in bsl_zones)
    swept_ssl = any(last_c["low"]  < lvl < last_c["close"] for lvl in ssl_zones)
    inducement = swept_bsl or swept_ssl

    return {
        "bsl_zones":  sorted(bsl_zones),
        "ssl_zones":  sorted(ssl_zones),
        "inducement": inducement,
        "swept_bsl":  swept_bsl,
        "swept_ssl":  swept_ssl,
    }


def detect_liquidity_double(
    candles_m15: List[Dict], candles_h4: List[Dict],
    atr_m15: float, atr_h4: float,
) -> Dict:
    """
    Double confirmation M15 + H4.

    Un sweep est significatif seulement s'il est confirmé sur les deux TF.
    Retourne le résultat fusionné + un score de qualité liquidité (0–3).
    """
    liq_m15 = detect_liquidity_zones(candles_m15, atr_m15)
    liq_h4  = detect_liquidity_zones(candles_h4,  atr_h4)

    # Score : 1 pt par TF pour inducement, +1 bonus si les deux confirment
    score = 0
    if liq_m15["inducement"]:
        score += 1
    if liq_h4["inducement"]:
        score += 1
    if liq_m15["inducement"] and liq_h4["inducement"]:
        score += 1  # bonus double confirmation

    swept_bsl = liq_m15["swept_bsl"] or liq_h4["swept_bsl"]
    swept_ssl = liq_m15["swept_ssl"] or liq_h4["swept_ssl"]

    return {
        "m15": liq_m15,
        "h4":  liq_h4,
        "liq_score":      score,          # 0–3
        "swept_bsl":      swept_bsl,
        "swept_ssl":      swept_ssl,
        "inducement_any": liq_m15["inducement"] or liq_h4["inducement"],
        "inducement_both": liq_m15["inducement"] and liq_h4["inducement"],
    }


# ══════════════════════════════════════════════════════════════════════════
#  AMD PHASE DETECTOR — Axe 3 (version refondée)
#  Accumulation / Manipulation / Distribution
#  Confirmation : structure BOS/CHoCH + volume + sweep de liquidité
# ══════════════════════════════════════════════════════════════════════════

def detect_amd_phase(
    candles_h4: List[Dict],
    candles_m15: List[Dict],
    vol_profile: Optional[Dict] = None,
    liq_data: Optional[Dict] = None,
) -> str:
    """
    AMD propre avec confirmation structure + volume + liquidité.

    ACCUMULATION  — range compressé, volume faible, pas de sweep
    MANIPULATION  — sweep de liquidité confirmé + volume institutionnel
    DISTRIBUTION  — expansion directionnelle post-sweep avec BOS

    Priorité : MANIPULATION > DISTRIBUTION > ACCUMULATION
    """
    if not candles_h4 or len(candles_h4) < 20:
        return "UNKNOWN"

    atr_h4 = compute_atr(candles_h4, 14)
    if atr_h4 <= 0:
        return "UNKNOWN"

    # ── Structure H4 : range des 10 dernières bougies ─────────────────────
    h4_window = candles_h4[-10:]
    highs_h4  = [c["high"] for c in h4_window]
    lows_h4   = [c["low"]  for c in h4_window]
    h4_range  = max(highs_h4) - min(lows_h4)
    range_ratio = h4_range / (atr_h4 * 10) if atr_h4 > 0 else 1.0

    # ── BOS M15 : la dernière bougie casse un high/low récent ─────────────
    bos_bullish = bos_bearish = False
    if candles_m15 and len(candles_m15) >= 10:
        m15_window = candles_m15[-10:]
        prev_high  = max(c["high"] for c in m15_window[:-1])
        prev_low   = min(c["low"]  for c in m15_window[:-1])
        last_m15   = candles_m15[-1]
        bos_bullish = last_m15["close"] > prev_high
        bos_bearish = last_m15["close"] < prev_low

    bos_confirmed = bos_bullish or bos_bearish

    # ── Volume institutionnel présent ? ───────────────────────────────────
    vol_inst = vol_profile.get("institutional", False) if vol_profile else False

    # ── Sweep de liquidité détecté ? ─────────────────────────────────────
    sweep_detected = False
    if liq_data:
        sweep_detected = liq_data.get("swept_bsl", False) or liq_data.get("swept_ssl", False)

    # ── Classification AMD ────────────────────────────────────────────────
    # MANIPULATION : sweep + volume institutionnel
    if sweep_detected and vol_inst:
        return "MANIPULATION"

    # DISTRIBUTION : BOS confirmé + range en expansion
    if bos_confirmed and range_ratio >= 0.7:
        return "DISTRIBUTION"

    # ACCUMULATION : range compressé, pas de signal fort
    if range_ratio < 0.6 and not sweep_detected:
        return "ACCUMULATION"

    # Par défaut : DISTRIBUTION si le range est suffisant
    if range_ratio >= 0.6:
        return "DISTRIBUTION"

    return "ACCUMULATION"


# ══════════════════════════════════════════════════════════════════════════
#  SIZING FIXE — Axe 4
#  Risque fixe 50 USD par trade
#  Lot = 50 / (distance_SL_pips × valeur_pip_USD)
# ══════════════════════════════════════════════════════════════════════════

# Valeur d'un pip en USD pour 1 lot standard (approximations stables)
PIP_VALUE_USD: Dict[str, float] = {
    "XAUUSD":  10.0,   # 1 lot = 100 oz → 0.1$ pip
    "XAGUSD":   5.0,
    "BTCUSDT":  1.0,   # varie, approx
    "ETHUSDT":  0.10,
    "EURUSD":  10.0,
    "GBPUSD":  10.0,
    "USDJPY":   9.0,   # approx USD/JPY
    "AUDUSD":  10.0,
    "USDCHF":  10.0,
    "USDCAD":  10.0,
    "GBPJPY":   9.0,
    "NAS100":   1.0,   # par point index
}


def compute_fixed_lot(
    symbol:      str,
    entry:       float,
    sl:          float,
    risk_usd:    float = 50.0,
) -> Dict:
    """
    Calcule le lot pour un risque fixe en USD.

    Formule :
        sl_pips   = |entry - sl| / pip_size
        pip_val   = valeur d'un pip en USD pour 1 lot
        lot       = risk_usd / (sl_pips × pip_val)

    Retourne :
        lot         — lot arrondi à 2 décimales (min 0.01)
        sl_pips     — distance SL en pips
        pip_val     — valeur pip USD utilisée
        risk_usd    — risque effectif en USD
    """
    pip   = PIP_SIZE.get(symbol, 0.0001)
    pv    = PIP_VALUE_USD.get(symbol, 10.0)

    sl_distance = abs(entry - sl)
    sl_pips     = sl_distance / pip if pip > 0 else 0

    if sl_pips <= 0 or pv <= 0:
        return {"lot": 0.01, "sl_pips": 0, "pip_val": pv, "risk_usd": risk_usd}

    raw_lot   = risk_usd / (sl_pips * pv)
    lot       = max(0.01, round(raw_lot, 2))

    # Risque effectif recalculé avec le lot arrondi
    effective_risk = lot * sl_pips * pv

    return {
        "lot":          lot,
        "sl_pips":      round(sl_pips, 1),
        "pip_val":      pv,
        "risk_usd":     round(effective_risk, 2),
    }


# ══════════════════════════════════════════════════════════════════════════
#  UPGRADE 1 — TRADE QUALITY SCORE  (A+ / A / B / REJECT)
#  Score institutionnel composite sur 10 points
# ══════════════════════════════════════════════════════════════════════════

SCORE_GRADES = {
    "A+":     (9, 10),
    "A":      (7,  9),
    "B":      (5,  7),
    "REJECT": (0,  5),
}

def compute_trade_quality(
    vol_profile: Dict,
    liq_data:    Dict,
    amd:         str,
    rr:          float,
    session:     str,
    regime:      str,    # TRENDING | RANGING | EXPANSION
    displaced:   bool,   # False si fake displacement détecté
    map_bonus:   float = 0.0,  # bonus liquidité map persistante (0.0–1.0)
) -> Dict:
    """
    Score institutionnel composite sur 10 points.

    Volume          → 0–2 pts
    Liquidité       → 0–3 pts
    AMD phase       → 0–2 pts
    RR              → 0–1 pt
    Session         → 0–1 pt
    Régime          → 0–1 pt
    Liq map bonus   → 0–1 pt  (niveaux matures non-sweepés)
    Displacement    → malus −2 si fake

    Retourne :
        score     — float 0–10
        grade     — "A+" | "A" | "B" | "REJECT"
        breakdown — détail par critère
    """
    breakdown: Dict[str, float] = {}

    # ── Volume (0–2) ───────────────────────────────────────────────────────
    sig = vol_profile.get("vol_signal", "NEUTRAL")
    dr  = vol_profile.get("delta_ratio", 0.5)
    if sig in ("STRONG_BUY", "STRONG_SELL"):
        vol_pts = 2.0
    elif sig in ("ABSORPTION", "EXHAUSTION"):
        vol_pts = 1.5
    elif vol_profile.get("vol_ratio", 1.0) >= 1.3:
        vol_pts = 1.0
    else:
        vol_pts = 0.0
    # Bonus delta fort
    if dr >= 0.70 or dr <= 0.30:
        vol_pts = min(vol_pts + 0.5, 2.0)
    breakdown["volume"] = vol_pts

    # ── Liquidité (0–3) ────────────────────────────────────────────────────
    liq_pts = float(liq_data.get("liq_score", 0))   # déjà 0–3
    breakdown["liquidity"] = liq_pts

    # ── AMD phase (0–2) ────────────────────────────────────────────────────
    amd_pts = {"MANIPULATION": 2.0, "DISTRIBUTION": 1.0, "UNKNOWN": 0.0}.get(amd, 0.0)
    breakdown["amd"] = amd_pts

    # ── RR (0–1) ───────────────────────────────────────────────────────────
    if rr >= 3.0:
        rr_pts = 1.0
    elif rr >= 2.0:
        rr_pts = 0.5
    else:
        rr_pts = 0.0
    breakdown["rr"] = rr_pts

    # ── Session (0–1) ──────────────────────────────────────────────────────
    sess_pts = 1.0 if session in ("LONDON_OPEN", "NY_OPEN") else \
               0.5 if session in ("LONDON", "LONDON_NY_OVERLAP", "NEW_YORK") else 0.0
    breakdown["session"] = sess_pts

    # ── Régime (0–1) ───────────────────────────────────────────────────────
    regime_pts = {"EXPANSION": 1.0, "TRENDING": 0.75, "RANGING": 0.25}.get(regime, 0.5)
    breakdown["regime"] = regime_pts

    # ── Liq map bonus (0–1) ────────────────────────────────────────────────
    # Niveaux BSL/SSL matures (>24h) non-sweepés = cibles institutionnelles confirmées
    breakdown["liq_map_bonus"] = round(min(map_bonus, 1.0), 2)

    # ── Malus fake displacement ────────────────────────────────────────────
    disp_malus = 0.0 if displaced else -2.0
    breakdown["displacement_malus"] = disp_malus

    raw   = sum(breakdown.values())
    score = round(max(0.0, min(10.0, raw)), 2)

    # Grade
    grade = "REJECT"
    for g, (lo, hi) in SCORE_GRADES.items():
        if lo <= score < hi:
            grade = g
            break
    if score >= 9:
        grade = "A+"

    return {"score": score, "grade": grade, "breakdown": breakdown}


# ══════════════════════════════════════════════════════════════════════════
#  UPGRADE 2 — FAKE DISPLACEMENT FILTER
#  Large candle + faible delta → probable piège, pas vrai momentum
#  Critique sur Gold / NAS100 (thin liquidity, spread expansion)
# ══════════════════════════════════════════════════════════════════════════

# Symboles à risque élevé de fake displacement
FAKE_DISP_SENSITIVE = {"XAUUSD", "XAGUSD", "NAS100", "BTCUSDT", "GBPJPY"}

def is_fake_displacement(
    candles:     List[Dict],
    vol_profile: Dict,
    atr:         float,
    symbol:      str,
) -> bool:
    """
    Détecte un faux mouvement institutionnel.

    Un fake displacement combine :
        1. Bougie large (range > 1.5×ATR)
        2. Delta faible (delta_ratio 0.35–0.65 = pas de direction dominante)
        3. Corps faible (body < 40% du range = rejet, pas continuation)

    Optionnel : plus strict sur les symboles sensibles (Gold, NAS).

    Retourne True si c'est probablement un piège.
    """
    if not candles or atr <= 0:
        return False

    last = candles[-1]
    candle_range = last["high"] - last["low"]
    body         = abs(last["close"] - last["open"])
    body_ratio   = body / max(candle_range, 1e-10)
    dr           = vol_profile.get("delta_ratio", 0.5)
    vol_ratio    = vol_profile.get("vol_ratio", 1.0)

    # Condition de base : bougie large
    if candle_range < 1.5 * atr:
        return False   # bougie normale → pas de fake displacement

    # Delta ambigu = pas de camp dominant
    delta_ambiguous = 0.35 <= dr <= 0.65

    # Corps faible = rejet / indécision
    body_weak = body_ratio < 0.40

    # Volume élevé + rejet = absorption (déjà traité) ou piège
    high_vol_rejection = vol_ratio >= 1.5 and body_weak

    # Symboles sensibles : seuil plus bas
    if symbol in FAKE_DISP_SENSITIVE:
        return delta_ambiguous or body_weak
    else:
        return delta_ambiguous and (body_weak or high_vol_rejection)


# ══════════════════════════════════════════════════════════════════════════
#  UPGRADE 3 — LIQUIDITY MAP PERSISTANTE
#  Mémoire des niveaux BSL/SSL par symbole, avec âge et statut sweep
#  Un niveau H4 non-sweeped depuis 3 jours >> niveau récent
# ══════════════════════════════════════════════════════════════════════════

import json
import pathlib

_LIQ_MAP_PATH = pathlib.Path("alphabot_liq_map.json")

# Structure en mémoire :
# { symbol: { "bsl": [{level, tf, ts, swept}], "ssl": [...] } }
_liq_map: Dict[str, Dict] = {}


def _load_liq_map():
    global _liq_map
    if _LIQ_MAP_PATH.exists():
        try:
            _liq_map = json.loads(_LIQ_MAP_PATH.read_text())
        except Exception:
            _liq_map = {}


def _save_liq_map():
    try:
        _LIQ_MAP_PATH.write_text(json.dumps(_liq_map, indent=2))
    except Exception as e:
        log.warning("Liquidity map save failed : %s", e)


def update_liq_map(symbol: str, liq_data: Dict, tf: str = "M15+H4"):
    """
    Met à jour la carte de liquidité persistante pour un symbole.

    Ajoute les nouveaux niveaux BSL/SSL détectés.
    Marque comme swept les niveaux qui ont été touchés.
    Purge les niveaux de plus de 7 jours.
    """
    global _liq_map
    now_ts  = time.time()
    cutoff  = now_ts - 7 * 86400   # 7 jours

    if symbol not in _liq_map:
        _liq_map[symbol] = {"bsl": [], "ssl": []}

    sym_map = _liq_map[symbol]

    # ── Purge anciens niveaux ──────────────────────────────────────────────
    sym_map["bsl"] = [z for z in sym_map["bsl"] if z["ts"] > cutoff]
    sym_map["ssl"] = [z for z in sym_map["ssl"] if z["ts"] > cutoff]

    # ── Ajouter nouveaux BSL ───────────────────────────────────────────────
    existing_bsl = [z["level"] for z in sym_map["bsl"]]
    for lvl in liq_data.get("m15", {}).get("bsl_zones", []) + \
               liq_data.get("h4", {}).get("bsl_zones", []):
        if not any(abs(lvl - e) < lvl * 0.001 for e in existing_bsl):
            sym_map["bsl"].append({
                "level": lvl, "tf": tf,
                "ts": now_ts, "swept": False,
                "age_h": 0,
            })
            existing_bsl.append(lvl)

    # ── Ajouter nouveaux SSL ───────────────────────────────────────────────
    existing_ssl = [z["level"] for z in sym_map["ssl"]]
    for lvl in liq_data.get("m15", {}).get("ssl_zones", []) + \
               liq_data.get("h4", {}).get("ssl_zones", []):
        if not any(abs(lvl - e) < lvl * 0.001 for e in existing_ssl):
            sym_map["ssl"].append({
                "level": lvl, "tf": tf,
                "ts": now_ts, "swept": False,
                "age_h": 0,
            })
            existing_ssl.append(lvl)

    # ── Marquer sweeps ─────────────────────────────────────────────────────
    for zone in sym_map["bsl"]:
        if liq_data.get("swept_bsl") and not zone["swept"]:
            zone["swept"]  = True
            zone["age_h"]  = round((now_ts - zone["ts"]) / 3600, 1)

    for zone in sym_map["ssl"]:
        if liq_data.get("swept_ssl") and not zone["swept"]:
            zone["swept"]  = True
            zone["age_h"]  = round((now_ts - zone["ts"]) / 3600, 1)

    _save_liq_map()


def get_liq_map_quality(symbol: str) -> Dict:
    """
    Retourne un score de qualité basé sur les niveaux persistants.

    Un niveau non-sweeped depuis > 24h sur H4 = haute valeur.

    Retourne :
        old_bsl_count  — BSL anciens non-sweepés (> 24h)
        old_ssl_count  — SSL anciens non-sweepés (> 24h)
        map_bonus      — bonus score 0.0–1.0
    """
    sym_map  = _liq_map.get(symbol, {"bsl": [], "ssl": []})
    now_ts   = time.time()
    old_th   = 24 * 3600   # > 24h = niveau mature

    old_bsl = sum(
        1 for z in sym_map["bsl"]
        if not z["swept"] and (now_ts - z["ts"]) > old_th
    )
    old_ssl = sum(
        1 for z in sym_map["ssl"]
        if not z["swept"] and (now_ts - z["ts"]) > old_th
    )

    # Bonus : plus il y a de niveaux matures non-sweepés, plus c'est intéressant
    bonus = min((old_bsl + old_ssl) * 0.15, 1.0)

    return {
        "old_bsl_count": old_bsl,
        "old_ssl_count": old_ssl,
        "map_bonus":     round(bonus, 2),
    }


# ══════════════════════════════════════════════════════════════════════════
#  UPGRADE 4 — REGIME FILTER
#  TRENDING / RANGING / EXPANSION
#  Le régime conditionne l'efficacité AMD, FVG, et le score global
# ══════════════════════════════════════════════════════════════════════════

def detect_market_regime(candles_h4: List[Dict], atr_h4: float) -> str:
    """
    Détecte le régime de marché sur H4.

    EXPANSION  — forte volatilité directionnelle (momentum réel)
    TRENDING   — tendance claire mais volatilité normale
    RANGING    — range compressé, pas de direction

    Critères :
        EMA20 vs EMA50    → direction
        ATR ratio         → volatilité relative
        HH/HL ou LH/LL   → structure de marché
    """
    if not candles_h4 or len(candles_h4) < 55:
        return "RANGING"

    closes = [c["close"] for c in candles_h4]
    ema20  = compute_ema(closes, 20)
    ema50  = compute_ema(closes, 50)

    if not ema20 or not ema50:
        return "RANGING"

    # ── ATR ratio : ATR courant vs ATR moyen 50 bougies ───────────────────
    atr_now = atr_h4
    # ATR moyen sur les 50 dernières (proxy : range moyen)
    recent_ranges = [
        candles_h4[i]["high"] - candles_h4[i]["low"]
        for i in range(-50, -1)
    ]
    atr_avg = sum(recent_ranges) / max(len(recent_ranges), 1)
    atr_ratio = atr_now / atr_avg if atr_avg > 0 else 1.0

    # ── Structure : HH/HL (bullish) ou LH/LL (bearish) ────────────────────
    highs = [c["high"] for c in candles_h4[-10:]]
    lows  = [c["low"]  for c in candles_h4[-10:]]

    hh = highs[-1] > max(highs[:-1])   # Higher High
    hl = lows[-1]  > min(lows[:-1])    # Higher Low
    lh = highs[-1] < max(highs[:-1])   # Lower High
    ll = lows[-1]  < min(lows[:-1])    # Lower Low

    trending_bullish = hh and hl and ema20[-1] > ema50[-1]
    trending_bearish = lh and ll and ema20[-1] < ema50[-1]
    trending = trending_bullish or trending_bearish

    # ── Classification ─────────────────────────────────────────────────────
    if atr_ratio >= 1.4 and trending:
        return "EXPANSION"    # volatilité élevée + structure directionnelle
    elif trending:
        return "TRENDING"     # structure directionnelle, volatilité normale
    else:
        return "RANGING"      # pas de structure claire


# ══════════════════════════════════════════════════════════════════════════
#  UPGRADE 5 — POST-TRADE ANALYTICS
#  Journalisation JSON de chaque signal envoyé
#  Base pour l'analyse statistique edge (WR par session, AMD, vol…)
# ══════════════════════════════════════════════════════════════════════════

_ANALYTICS_PATH = pathlib.Path("alphabot_analytics.jsonl")


def log_trade_analytics(signal: Dict, grade: str, score: float, regime: str):
    """
    Enregistre chaque signal dans un fichier JSONL (une ligne = un trade).

    Format conçu pour analyse pandas directe :
        df = pd.read_json('alphabot_analytics.jsonl', lines=True)

    Champs utiles pour analyse edge :
        symbol, session, amd, vol_signal, liq_score,
        regime, grade, score, rr, sl_pips, lot, risk_usd,
        swept_bsl, swept_ssl, direction, htf_bias, timestamp
    """
    record = {
        "timestamp":    signal["timestamp"].isoformat(),
        "symbol":       signal["symbol"],
        "tier":         signal["tier"],
        "direction":    signal["direction"],
        "session":      signal["session"],
        "htf_bias":     signal["htf_bias"],
        "amd":          signal["amd_phase"],
        "vol_signal":   signal["vol_signal"],
        "vol_ratio":    signal["vol_ratio"],
        "delta_ratio":  signal["delta_ratio"],
        "liq_score":    signal["liq_score"],
        "swept_bsl":    signal["swept_bsl"],
        "swept_ssl":    signal["swept_ssl"],
        "regime":       regime,
        "grade":        grade,
        "score":        score,
        "rr":           signal["rr"],
        "sl_pips":      signal["sl_pips"],
        "lot":          signal["lot"],
        "risk_usd":     signal["risk_usd"],
        "effective_entry": signal["effective_entry"],
        "effective_sl":    signal["effective_sl"],
        "tp":              signal["tp"],
        # result à remplir manuellement ou via module de suivi futur
        "result":       None,   # "WIN" | "LOSS" | "BE"
        "pnl_usd":      None,
    }
    try:
        with open(_ANALYTICS_PATH, "a") as f:
            f.write(json.dumps(record) + "\n")
        log.debug("Analytics logged : %s %s grade=%s score=%.1f",
                  signal["symbol"], signal["direction"], grade, score)
    except Exception as e:
        log.warning("Analytics write failed : %s", e)


# ══════════════════════════════════════════════════════════════════════════
#  CORRELATION EXPOSURE GUARD
# ══════════════════════════════════════════════════════════════════════════

class CorrelationGuard:
    """
    Maintient l'exposition active par groupe de corrélation.

    FIX v4.2 : les positions sont maintenant horodatées et purgées
    automatiquement après CORR_TTL_H heures, évitant le blocage
    permanent des groupes en cas de non-appel à release().
    """

    CORR_TTL_H = 8   # durée de vie d'une position corrélée (heures)

    def __init__(self):
        # group_name → { symbol: registered_timestamp }
        self._active: Dict[str, Dict[str, float]] = {g: {} for g in USD_CORR_GROUPS}

    def _purge(self):
        """Supprime les positions dont le TTL est dépassé."""
        cutoff = time.time() - self.CORR_TTL_H * 3600
        for group in self._active:
            self._active[group] = {
                sym: ts
                for sym, ts in self._active[group].items()
                if ts > cutoff
            }

    def can_add(self, symbol: str, direction: str) -> Tuple[bool, str]:
        """Vérifie si ajouter ce symbole dépasse le cap de corrélation."""
        self._purge()
        for group, members in USD_CORR_GROUPS.items():
            if symbol not in members:
                continue
            active = self._active[group]
            if len(active) >= MAX_CORR_EXPOSURE and symbol not in active:
                return False, f"Correlation cap groupe {group} ({len(active)}/{MAX_CORR_EXPOSURE})"
        return True, "OK"

    def register(self, symbol: str):
        self._purge()
        for group, members in USD_CORR_GROUPS.items():
            if symbol in members:
                self._active[group][symbol] = time.time()

    def release(self, symbol: str):
        """Libération manuelle anticipée (optionnelle — TTL prend le relais)."""
        for group in self._active.values():
            group.pop(symbol, None)


# ══════════════════════════════════════════════════════════════════════════
#  SIGNAL TRACKER (daily cap + cooldown par symbole + anti-duplicate)
# ══════════════════════════════════════════════════════════════════════════

class SignalTracker:
    """
    Gère :
        - Compteur journalier (reset à minuit UTC)
        - Gap global minimum entre signaux
        - Cooldown par symbole (2h)
        - Hash anti-duplicate (TTL 6h)
    """

    def __init__(self):
        self.count_today        = 0
        self.last_signal_ts:   Optional[datetime.datetime] = None
        self.symbol_last_ts:   Dict[str, datetime.datetime] = {}
        self.sent_hashes:      Dict[str, datetime.datetime] = {}
        self._current_day      = datetime.date.today()

    def _daily_reset(self):
        today = datetime.date.today()
        if today != self._current_day:
            log.info("Nouveau jour — reset compteurs (%d signaux envoyés hier)", self.count_today)
            self.count_today   = 0
            self._current_day  = today

    def _purge_old_hashes(self):
        cutoff = datetime.datetime.utcnow() - datetime.timedelta(hours=SETUP_HASH_TTL_H)
        expired = [h for h, ts in self.sent_hashes.items() if ts < cutoff]
        for h in expired:
            del self.sent_hashes[h]

    def make_setup_hash(self, symbol: str, direction: str,
                        entry: float, sl: float, tp: float) -> str:
        """Hash stable sur les paramètres clés d'un setup."""
        raw = f"{symbol}_{direction}_{entry:.4f}_{sl:.4f}_{tp:.4f}"
        return hashlib.sha256(raw.encode()).hexdigest()[:16]

    def can_send(self, symbol: str, setup_hash: str) -> Tuple[bool, str]:
        self._daily_reset()
        self._purge_old_hashes()
        now = datetime.datetime.utcnow()

        if self.count_today >= MAX_SIGNALS_PER_DAY:
            return False, f"Cap journalier ({MAX_SIGNALS_PER_DAY}/jour)"

        if self.last_signal_ts:
            gap_min = (now - self.last_signal_ts).total_seconds() / 60
            if gap_min < MIN_GAP_GLOBAL_MIN:
                return False, f"Gap global trop court ({gap_min:.0f}min < {MIN_GAP_GLOBAL_MIN}min)"

        if symbol in self.symbol_last_ts:
            sym_gap_h = (now - self.symbol_last_ts[symbol]).total_seconds() / 3600
            if sym_gap_h < SYMBOL_COOLDOWN_H:
                return False, f"{symbol} cooldown ({sym_gap_h:.1f}h < {SYMBOL_COOLDOWN_H}h)"

        if setup_hash in self.sent_hashes:
            return False, f"Setup déjà envoyé (hash {setup_hash})"

        return True, "OK"

    def record(self, symbol: str, setup_hash: str):
        now = datetime.datetime.utcnow()
        self.count_today        += 1
        self.last_signal_ts      = now
        self.symbol_last_ts[symbol] = now
        self.sent_hashes[setup_hash] = now
        log.info("Signal #%d envoyé — %s | hash %s", self.count_today, symbol, setup_hash)


# ══════════════════════════════════════════════════════════════════════════
#  SIGNAL PIPELINE
# ══════════════════════════════════════════════════════════════════════════

def run_signal_pipeline(
    symbol:    str,
    session:   str,
    news_risk: str,
    htf_bias:  str,
    tier:      int,
) -> Optional[Dict]:
    """
    Pipeline complet pour un symbole — v3.0 (4 axes refondus).

    Axes actifs :
        1. Volume institutionnel (relatif + delta proxy)
        2. Liquidité réelle (BSL/SSL equal H/L, sweep, M15+H4)
        3. AMD propre (sweep + BOS + volume)
        4. Sizing fixe 50 USD

    Retourne un dict signal ou None si le setup est invalide / filtré.
    """
    log.info("  Scanning %s (Tier %d) bias=%s session=%s", symbol, tier, htf_bias, session)

    # ── Session filter ─────────────────────────────────────────────────────
    if tier == 3 and not session_allows_tier3(session):
        return None
    if tier == 3 and htf_bias == "NEUTRAL":
        return None

    # ── News hard block ────────────────────────────────────────────────────
    if news_hard_block(symbol, news_risk):
        return None

    # ── Fetch candles ──────────────────────────────────────────────────────
    h4_candles  = fetch_candles(symbol, "4h",  bars=220)
    m15_candles = fetch_candles(symbol, "15m", bars=150)

    if not h4_candles or not m15_candles:
        log.warning("  %s : données insuffisantes", symbol)
        return None

    # ── ATR ────────────────────────────────────────────────────────────────
    atr_m15 = compute_atr(m15_candles, 14)
    atr_h4  = compute_atr(h4_candles,  14)
    if atr_m15 <= 0 or atr_h4 <= 0:
        return None

    # ── Spread live filter ─────────────────────────────────────────────────
    spread_pips = get_live_spread(symbol)
    if spread_is_toxic(symbol, spread_pips, atr_m15):
        return None

    # ══ AXE 1 : Volume institutionnel ═════════════════════════════════════
    vol_profile = compute_volume_profile(m15_candles, lookback=20)
    log.debug(
        "  %s Vol: signal=%s ratio=%.2f delta=%.3f inst=%s",
        symbol, vol_profile["vol_signal"], vol_profile["vol_ratio"],
        vol_profile["delta_ratio"], vol_profile["institutional"],
    )

    # ══ AXE 2 : Liquidité BSL/SSL double TF ═══════════════════════════════
    liq_data = detect_liquidity_double(m15_candles, h4_candles, atr_m15, atr_h4)
    log.debug(
        "  %s Liq: score=%d swept_bsl=%s swept_ssl=%s",
        symbol, liq_data["liq_score"],
        liq_data["swept_bsl"], liq_data["swept_ssl"],
    )

    # ── Upgrade 3 : Liquidity map persistante ─────────────────────────────
    update_liq_map(symbol, liq_data)
    liq_map_quality = get_liq_map_quality(symbol)

    # ── Upgrade 4 : Regime filter ──────────────────────────────────────────
    regime = detect_market_regime(h4_candles, atr_h4)
    log.debug("  %s Regime: %s", symbol, regime)

    # Ranging : on n'entre pas en AMD sans expansion — trop de faux signaux
    if regime == "RANGING" and not liq_data["inducement_both"]:
        log.debug("  %s : RANGING sans double inducement — skip", symbol)
        return None

    # ══ AXE 3 : AMD propre ════════════════════════════════════════════════
    amd = detect_amd_phase(h4_candles, m15_candles, vol_profile, liq_data)
    if amd == "ACCUMULATION":
        log.debug("  %s : ACCUMULATION — attente", symbol)
        return None

    # ── Prix actuel ────────────────────────────────────────────────────────
    entry_price = m15_candles[-1]["close"]

    # ── Structures SMC M15 ─────────────────────────────────────────────────
    try:
        fvg_list = detect_fvg(m15_candles)
    except Exception:
        fvg_list = []

    try:
        ob_list = detect_order_blocks(m15_candles)
    except Exception:
        ob_list = []

    # ── Construction setup ─────────────────────────────────────────────────
    direction:   Optional[str]   = None
    tech_sl:     Optional[float] = None
    tp_price:    Optional[float] = None
    setup_type   = "—"
    sweep_zones: List[Dict]      = []

    if htf_bias == "BULLISH":
        # Préférence OB sous le prix → FVG si pas d'OB
        bull_obs = [ob for ob in ob_list
                    if ob.get("type") == "BULLISH"
                    and ob.get("high", 0) < entry_price
                    and abs(entry_price - ob["high"]) < 2 * atr_m15]
        bull_fvgs = [fvg for fvg in fvg_list
                     if fvg.get("type") == "BULLISH"
                     and fvg.get("top", 0) < entry_price
                     and abs(entry_price - fvg["top"]) < 2 * atr_m15]

        # Bonus : setup plus fort si SSL sweepée (on entre dans la liquidité)
        ssl_swept = liq_data["swept_ssl"]

        if bull_obs:
            ob         = bull_obs[-1]
            direction  = "LONG"
            # SL sous le low de l'OB — si SSL sweepée, SL sous le sweep
            sl_anchor  = min(ob["low"], min(liq_data["m15"]["ssl_zones"] or [ob["low"]]))
            tech_sl    = sl_anchor - 0.1 * atr_m15
            rr_mult    = 3.0 if (ssl_swept and vol_profile["institutional"]) else 2.5
            tp_price   = entry_price + rr_mult * abs(entry_price - tech_sl)
            setup_type = "OB Bullish" + (" + SSL Sweep" if ssl_swept else "")
            sweep_zones.append({"low": ob["low"], "high": ob["high"]})
        elif bull_fvgs:
            fvg        = bull_fvgs[-1]
            direction  = "LONG"
            tech_sl    = fvg["bottom"] - 0.1 * atr_m15
            rr_mult    = 2.5 if (ssl_swept and vol_profile["institutional"]) else 2.0
            tp_price   = entry_price + rr_mult * abs(entry_price - tech_sl)
            setup_type = "FVG Bullish" + (" + SSL Sweep" if ssl_swept else "")
            sweep_zones.append({"low": fvg["bottom"], "high": fvg["top"]})

    elif htf_bias == "BEARISH":
        bear_obs = [ob for ob in ob_list
                    if ob.get("type") == "BEARISH"
                    and ob.get("low", 9e9) > entry_price
                    and abs(ob["low"] - entry_price) < 2 * atr_m15]
        bear_fvgs = [fvg for fvg in fvg_list
                     if fvg.get("type") == "BEARISH"
                     and fvg.get("bottom", 0) > entry_price
                     and abs(fvg["bottom"] - entry_price) < 2 * atr_m15]

        bsl_swept = liq_data["swept_bsl"]

        if bear_obs:
            ob         = bear_obs[-1]
            direction  = "SHORT"
            sl_anchor  = max(ob["high"], max(liq_data["m15"]["bsl_zones"] or [ob["high"]]))
            tech_sl    = sl_anchor + 0.1 * atr_m15
            rr_mult    = 3.0 if (bsl_swept and vol_profile["institutional"]) else 2.5
            tp_price   = entry_price - rr_mult * abs(tech_sl - entry_price)
            setup_type = "OB Bearish" + (" + BSL Sweep" if bsl_swept else "")
            sweep_zones.append({"low": ob["low"], "high": ob["high"]})
        elif bear_fvgs:
            fvg        = bear_fvgs[-1]
            direction  = "SHORT"
            tech_sl    = fvg["top"] + 0.1 * atr_m15
            rr_mult    = 2.5 if (bsl_swept and vol_profile["institutional"]) else 2.0
            tp_price   = entry_price - rr_mult * abs(tech_sl - entry_price)
            setup_type = "FVG Bearish" + (" + BSL Sweep" if bsl_swept else "")
            sweep_zones.append({"low": fvg["bottom"], "high": fvg["top"]})

    if direction is None or tech_sl is None:
        log.debug("  %s : pas de setup valide (bias=%s)", symbol, htf_bias)
        return None

    # ── Validation volume direction ────────────────────────────────────────
    vol_ok = volume_confirms_direction(vol_profile, direction)
    if not vol_ok:
        log.debug("  %s : volume ne confirme pas %s (sig=%s delta=%.2f)",
                  symbol, direction, vol_profile["vol_signal"], vol_profile["delta_ratio"])
        return None

    # ── Upgrade 2 : Fake displacement filter ──────────────────────────────
    fake_disp = is_fake_displacement(m15_candles, vol_profile, atr_m15, symbol)
    if fake_disp:
        log.info("  %s : FAKE DISPLACEMENT — large candle sans delta réel — skip", symbol)
        return None

    # ── RR brut (mid price) ────────────────────────────────────────────────
    rr_brut = abs(tp_price - entry_price) / max(abs(tech_sl - entry_price), 1e-10)
    if rr_brut < 1.5:
        log.debug("  %s : RR brut trop faible (%.1f)", symbol, rr_brut)
        return None

    # ── Module M pipeline (SL expansion + qualité) ────────────────────────
    htf_alignment = "D1_H4_ALIGNED" if htf_bias != "NEUTRAL" else "H4_ONLY"
    m_session     = session_to_module_m(session)

    # Volume quality pour Module M
    vol_quality = "STRONG" if vol_profile["institutional"] else "NORMAL"
    vol_regime  = "EXPANSION" if amd == "DISTRIBUTION" else "CONTRACTION"

    pipeline = pipeline_sl_to_risk(
        tech_sl           = tech_sl,
        entry             = entry_price,
        symbol            = symbol,
        atr               = atr_m15,
        balance           = ACCOUNT_BALANCE,
        quant_verdict     = "INSTITUTIONAL" if vol_profile["institutional"] else "STANDARD",
        risk_amount_usd   = FIXED_RISK_USD,
        htf_alignment     = htf_alignment,
        volume_quality    = vol_quality,
        volatility_regime = vol_regime,
        session           = m_session,
        spread_pips       = spread_pips,
        news_risk         = news_risk,
        sweep_zones       = sweep_zones,
    )

    if pipeline["blocked"]:
        log.debug("  %s : pipeline bloqué — %s", symbol, pipeline.get("reason"))
        return None

    sl_r = pipeline["sl_result"]
    if not sl_r["sl_valid"]:
        return None

    effective_sl = sl_r["effective_sl"]

    # ── Effective entry (ask pour LONG, bid pour SHORT) ────────────────────
    pip         = PIP_SIZE.get(symbol, 0.0001)
    half_spread = (spread_pips / 2) * pip
    if direction == "LONG":
        effective_entry = entry_price + half_spread
    else:
        effective_entry = entry_price - half_spread

    # ══ AXE 4 : Sizing fixe 50 USD ════════════════════════════════════════
    sizing = compute_fixed_lot(
        symbol   = symbol,
        entry    = effective_entry,
        sl       = effective_sl,
        risk_usd = FIXED_RISK_USD,
    )

    # ── RR final sur effective entry / effective SL ────────────────────────
    sl_dist  = abs(effective_entry - effective_sl)
    tp_dist  = abs(tp_price - effective_entry)
    rr_final = tp_dist / max(sl_dist, 1e-10)

    if rr_final < 1.5:
        log.debug("  %s : RR effectif trop faible après Module M (%.1f)", symbol, rr_final)
        return None

    # ── Session quality score ──────────────────────────────────────────────
    sess_score = SESSION_QUALITY.get(session, 0.5)

    # ── Upgrade 1 : Trade quality score A+/A/B/REJECT ─────────────────────
    quality = compute_trade_quality(
        vol_profile = vol_profile,
        liq_data    = liq_data,
        amd         = amd,
        rr          = rr_final,
        session     = session,
        regime      = regime,
        displaced   = not fake_disp,   # False si fake displacement (déjà filtré mais pour le score)
        map_bonus   = liq_map_quality["map_bonus"],  # FIX v4.2 : bonus niveaux matures
    )
    grade = quality["grade"]
    score = quality["score"]

    # Seuil minimum : on n'envoie que B et au-dessus
    if grade == "REJECT":
        log.info("  %s : grade REJECT (score=%.1f) — setup insuffisant", symbol, score)
        return None

    signal = {
        "symbol":           symbol,
        "tier":             tier,
        "direction":        direction,
        "entry":            entry_price,
        "effective_entry":  effective_entry,
        "tech_sl":          tech_sl,
        "effective_sl":     effective_sl,
        "tp":               tp_price,
        "rr":               round(rr_final, 2),
        "lot":              sizing["lot"],
        "risk_usd":         sizing["risk_usd"],
        "sl_pips":          sizing["sl_pips"],
        "atr_m15":          atr_m15,
        "spread_pips":      round(spread_pips, 1),
        "session":          session,
        "session_score":    sess_score,
        "news_risk":        news_risk,
        "htf_bias":         htf_bias,
        "amd_phase":        amd,
        "setup_type":       setup_type,
        "setup_score":      score,
        "grade":            grade,
        "score_breakdown":  quality["breakdown"],
        "regime":           regime,
        "liq_map_bonus":    liq_map_quality["map_bonus"],
        "old_bsl":          liq_map_quality["old_bsl_count"],
        "old_ssl":          liq_map_quality["old_ssl_count"],
        "vol_signal":       vol_profile["vol_signal"],
        "vol_ratio":        vol_profile["vol_ratio"],
        "delta_ratio":      vol_profile["delta_ratio"],
        "liq_score":        liq_data["liq_score"],
        "swept_bsl":        liq_data["swept_bsl"],
        "swept_ssl":        liq_data["swept_ssl"],
        "sl_quality":       sl_r["sl_quality"],
        "sl_expansion":     sl_r["total_expansion_pips"],
        "sl_r":             sl_r,
        "timestamp":        datetime.datetime.utcnow(),
    }

    log.info(
        "  ✅ SIGNAL %s %s [%s] grade=%s score=%.1f regime=%s — "
        "E=%.5f SL=%.5f TP=%.5f RR=%.1f:1 Lot=%.2f Risk=$%.0f SLpips=%.1f Vol=%s Liq=%d",
        symbol, direction, setup_type, grade, score, regime,
        effective_entry, effective_sl, tp_price,
        rr_final, sizing["lot"], sizing["risk_usd"], sizing["sl_pips"],
        vol_profile["vol_signal"], liq_data["liq_score"],
    )

    # ── Upgrade 5 : Journalisation analytics ──────────────────────────────
    log_trade_analytics(signal, grade=grade, score=score, regime=regime)

    return signal


# ══════════════════════════════════════════════════════════════════════════
#  TELEGRAM SENDER
# ══════════════════════════════════════════════════════════════════════════

def send_telegram(text: str) -> bool:
    url  = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    data = {"chat_id": CHAT_ID, "text": text, "parse_mode": "HTML"}
    try:
        resp = requests.post(url, data=data, timeout=15)
        if not resp.ok:
            log.error("Telegram %d : %s", resp.status_code, resp.text[:200])
            return False
        return True
    except Exception as e:
        log.error("Telegram send failed : %s", e)
        return False


# ══════════════════════════════════════════════════════════════════════════
#  SIGNAL FORMATTER
# ══════════════════════════════════════════════════════════════════════════

_TIER_STARS   = {1: "⭐⭐⭐", 2: "⭐⭐", 3: "⭐"}
_DIR_ICO      = {"LONG": "📈", "SHORT": "📉"}
_AMD_ICO      = {"MANIPULATION": "🎭", "DISTRIBUTION": "💰",
                 "ACCUMULATION": "🔄", "UNKNOWN": "❓"}
_SESSION_ICO  = {
    "LONDON_OPEN": "🇬🇧🔔", "NY_OPEN": "🇺🇸🔔",
    "LONDON": "🇬🇧", "LONDON_NY_OVERLAP": "🌍",
    "NEW_YORK": "🇺🇸", "ASIAN": "🌏", "OFF": "🌙",
}
_NEWS_LINE = {
    "NONE":   "",
    "MEDIUM": "\n⚠️ <b>NEWS MEDIUM</b> — spread élargi — SL protégé",
    "HIGH":   "\n🚨 <b>NEWS HIGH</b> — SL expansé au max",
    "NFP":    "\n🔴 <b>NFP</b> — SL ×2 ATR",
}
_SCORE_BAR = {
    range(0, 5):    "▱▱▱▱▱",
    range(5, 6):    "▰▱▱▱▱",
    range(6, 8):    "▰▰▰▱▱",
    range(8, 10):   "▰▰▰▰▱",
    range(10, 15):  "▰▰▰▰▰",
}

def _score_bar(score: float) -> str:
    s = int(score * 10)
    for r, bar in _SCORE_BAR.items():
        if s in r:
            return bar
    return "▰▰▰▰▰"


def format_signal(sig: Dict) -> str:
    dir_ico   = _DIR_ICO.get(sig["direction"], "")
    sess_ico  = _SESSION_ICO.get(sig["session"], "")
    amd_ico   = _AMD_ICO.get(sig["amd_phase"], "")
    stars     = _TIER_STARS.get(sig["tier"], "")
    news_txt  = _NEWS_LINE.get(sig["news_risk"], "")
    sl_block  = fmt_sl_block(sig["sl_r"])

    # ── Grade badge ────────────────────────────────────────────────────────
    grade_ico = {"A+": "🏆", "A": "🥇", "B": "🥈"}.get(sig["grade"], "")
    grade_bar = {
        "A+": "▰▰▰▰▰", "A": "▰▰▰▰▱", "B": "▰▰▰▱▱"
    }.get(sig["grade"], "▰▰▱▱▱")

    # ── Régime ────────────────────────────────────────────────────────────
    regime_ico = {"EXPANSION": "🚀", "TRENDING": "📊", "RANGING": "↔️"}.get(sig["regime"], "")

    # ── Volume line ────────────────────────────────────────────────────────
    vol_ico = {
        "STRONG_BUY":  "🟢", "STRONG_SELL": "🔴",
        "ABSORPTION":  "🔵", "EXHAUSTION":  "🟠", "NEUTRAL": "⚪",
    }.get(sig["vol_signal"], "⚪")
    vol_line = (
        f"{vol_ico} Vol: <b>{sig['vol_signal']}</b>  "
        f"×{sig['vol_ratio']:.1f}  |  Δ {sig['delta_ratio']:.0%}"
    )

    # ── Liquidité line ─────────────────────────────────────────────────────
    liq_parts = []
    if sig["swept_bsl"]:
        liq_parts.append("BSL✓")
    if sig["swept_ssl"]:
        liq_parts.append("SSL✓")
    liq_str  = " + ".join(liq_parts) if liq_parts else "—"
    map_line = ""
    if sig.get("old_bsl", 0) + sig.get("old_ssl", 0) > 0:
        map_line = f"  🗺 Map: {sig['old_bsl']}BSL/{sig['old_ssl']}SSL mature"
    liq_line = f"💧 Liq: <b>{liq_str}</b>  score {sig['liq_score']}/3{map_line}"

    return (
        f"<b>━━━━━━━━━━━━━━━━━━━━━━━━━━━━━</b>\n"
        f"{dir_ico} <b>ALPHABOT PRO</b> — <code>{sig['symbol']}</code>  {stars}\n"
        f"<b>━━━━━━━━━━━━━━━━━━━━━━━━━━━━━</b>\n\n"
        f"{grade_ico} <b>Grade {sig['grade']}</b>  {grade_bar}  "
        f"<i>score {sig['setup_score']:.1f}/10</i>\n"
        f"{dir_ico} <b>{sig['direction']}</b>   {amd_ico} <i>{sig['amd_phase']}</i>   "
        f"{regime_ico} <i>{sig['regime']}</i>\n"
        f"📋 Setup : <b>{sig['setup_type']}</b>\n"
        f"🔭 Bias H4 : <b>{sig['htf_bias']}</b>\n"
        f"{sess_ico} Session : <b>{sig['session']}</b>\n\n"
        f"{vol_line}\n"
        f"{liq_line}\n\n"
        f"🎯 <b>ENTRY</b>   : <code>{sig['effective_entry']:.5f}</code>\n"
        f"🛑 <b>STOP</b>    : <code>{sig['effective_sl']:.5f}</code>  "
        f"({sig['sl_pips']:.0f}p)  <i>({sig['sl_quality']} +{sig['sl_expansion']:.0f}p)</i>\n"
        f"✅ <b>TARGET</b>  : <code>{sig['tp']:.5f}</code>\n"
        f"📐 <b>RR</b>      : <b>{sig['rr']:.1f}:1</b>  "
        f"(spread: {sig['spread_pips']:.1f}p)\n\n"
        f"💼 Lot : <b>{sig['lot']}</b>  |  Risque : <b>${sig['risk_usd']:.0f}</b>\n"
        f"{sl_block}"
        f"{news_txt}\n\n"
        f"<i>AlphaBot PRO v4.0 · {sig['timestamp'].strftime('%H:%M UTC')}</i>"
    )


def send_startup_message():
    msg = (
        "🟢 <b>ALPHABOT PRO v4.0 — DÉMARRAGE</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⏱ {datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}\n"
        f"💰 Balance : <b>${ACCOUNT_BALANCE:,.0f}</b>\n"
        f"🎯 Risque fixe : <b>${FIXED_RISK_USD:.0f} / trade</b>\n"
        f"📊 Symboles : <b>{len(ALL_SYMBOLS)}</b>  "
        f"(T1: {len(TIER_1)} | T2: {len(TIER_2)} | T3: {len(TIER_3)})\n"
        f"🔁 Scan : <b>{SCAN_INTERVAL_SEC // 60} min</b>\n"
        f"📈 Cap signaux/jour : <b>{MAX_SIGNALS_PER_DAY}</b>\n"
        f"⏸ Cooldown symbole : <b>{SYMBOL_COOLDOWN_H}h</b>\n"
        f"🔗 Corr. cap : <b>{MAX_CORR_EXPOSURE} positions/groupe</b>\n"
        "🔬 Volume : relatif + delta proxy + fake disp.\n"
        "💧 Liquidité : BSL/SSL M15+H4 + map persistante\n"
        "🎭 AMD : sweep + BOS + volume\n"
        "🏆 Score : A+/A/B/REJECT sur 10pts\n"
        "📈 Régime : EXPANSION/TRENDING/RANGING\n"
        "📝 Analytics : alphabot_analytics.jsonl\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    )
    send_telegram(msg)


# ══════════════════════════════════════════════════════════════════════════
#  BOUCLE PRINCIPALE — WATCHDOG INTÉGRÉ
# ══════════════════════════════════════════════════════════════════════════

async def scan_once(
    tracker:   SignalTracker,
    corr_guard: CorrelationGuard,
) -> None:
    """Un cycle complet de scan sur tous les symboles."""
    session   = get_current_session()
    news_risk = get_news_risk()

    log.info(
        "=== SCAN — Session: %s | News: %s | Signaux: %d/%d",
        session, news_risk, tracker.count_today, MAX_SIGNALS_PER_DAY,
    )

    for tier, symbols in [(1, TIER_1), (2, TIER_2), (3, TIER_3)]:
        for symbol in symbols:

            # ── Pre-checks globaux ─────────────────────────────────────────
            # Vérifié avant le pipeline pour ne pas fetch si inutile
            can_global, _ = tracker.can_send(symbol, "placeholder_preflight")
            if not can_global:
                continue

            try:
                htf_bias = get_htf_bias(symbol)

                signal = run_signal_pipeline(
                    symbol    = symbol,
                    session   = session,
                    news_risk = news_risk,
                    htf_bias  = htf_bias,
                    tier      = tier,
                )

                if not signal:
                    await asyncio.sleep(1)
                    continue

                # ── Setup hash ────────────────────────────────────────────
                sh = tracker.make_setup_hash(
                    symbol    = symbol,
                    direction = signal["direction"],
                    entry     = signal["effective_entry"],
                    sl        = signal["effective_sl"],
                    tp        = signal["tp"],
                )

                # ── Vérification tracker (avec vrai hash) ─────────────────
                can, reason = tracker.can_send(symbol, sh)
                if not can:
                    log.info("  %s : signal non envoyé — %s", symbol, reason)
                    await asyncio.sleep(1)
                    continue

                # ── Correlation guard ─────────────────────────────────────
                corr_ok, corr_reason = corr_guard.can_add(symbol, signal["direction"])
                if not corr_ok:
                    log.info("  %s : bloqué corrélation — %s", symbol, corr_reason)
                    await asyncio.sleep(1)
                    continue

                # ── Envoi Telegram ────────────────────────────────────────
                msg = format_signal(signal)
                ok  = send_telegram(msg)

                if ok:
                    tracker.record(symbol, sh)
                    corr_guard.register(symbol)
                else:
                    log.error("  %s : échec envoi Telegram", symbol)

            except Exception as e:
                log.error("  Pipeline %s : %s", symbol, e, exc_info=True)

            await asyncio.sleep(2)   # politesse API


async def scan_loop():
    """Boucle infinie avec watchdog — ne crashe jamais silencieusement."""
    tracker    = SignalTracker()
    corr_guard = CorrelationGuard()

    send_startup_message()
    _load_liq_map()
    log.info("AlphaBot PRO v4.0 démarré — cycle %ds", SCAN_INTERVAL_SEC)

    while True:
        cycle_start = time.time()

        try:
            await scan_once(tracker, corr_guard)
        except Exception as e:
            # Watchdog : log l'erreur, attend 30s, reprend
            log.critical("WATCHDOG — erreur cycle : %s", e, exc_info=True)
            send_telegram(f"⚠️ <b>AlphaBot PRO — Erreur cycle</b>\n<code>{str(e)[:200]}</code>")
            await asyncio.sleep(30)
            continue

        elapsed = time.time() - cycle_start
        wait    = max(0, SCAN_INTERVAL_SEC - elapsed)
        log.info("Cycle terminé en %.1fs — prochain scan dans %.0fs", elapsed, wait)
        await asyncio.sleep(wait)


# ══════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    asyncio.run(scan_loop())

