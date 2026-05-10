
"""
ALPHABOT FUTURES v5.3 -- EXIT ENGINE + BE ADAPTATIF
Full Auto | Binance USDT-M Futures | Capital micro (<$50)
21 marches scannes | Top N paires selectionnees par score
SL structurel (SwingHL) | Agent IA Anthropic validateur
Risk adaptatif | TP fractionne 50/30/20%
Pure stdlib | Pydroid3 Android compatible

HERITE v5.0 :
  [1] BOS displacement filter : body >= 0.6xATR + close %
      au-dela de la structure -> elimine les fake breakouts 1m
  [2] HTF M15 bias : direction alignee avec structure M15
  [3] Session filter dur : London + NY uniquement
  [4] BTC correlation gate : altcoins bloques si BTC contra
  [5] Volatility spike filter : bougie >= 3.5xATR -> skip
  [6] Score rebalance : CRT +1, displacement +1, max 7
  [7] Break-even automatique v5.0

NOUVEAU v5.3 -- STRATEGIC EXIT ENGINE :

  [E1] Break-even ADAPTATIF -- 3 modes :
       * vol HIGH   -> trigger BE_FAST_R (0.3R) -- ultra-reactif
       * vol NORMAL -> trigger BE_TRIGGER_R (0.5R) -- standard
       * score=7 + HTF aligne -> buffer fees / BE_TIGHT_BUF_MULT
         (BE plus serre, verrouille plus de profit sur setups elite)

  [E2] SL Failover manuel (_close_at_sl)
       Si Binance lag/echoue a declencher le SL : ordre MARKET
       Python-side pour eviter les pertes ouvertes non protegees.

  [E3] Partial close R2 (_close_partial)
       Si mark atteint 2R et TP2 Binance non encore touche :
       cloture manuelle 30% de la position en MARKET.
       Active via PARTIAL_CLOSE_R2 = True.

  [E4] Trailing stop ATR (_close_trail)
       Active apres TRAIL_R_START x R (defaut : 2R).
       SL = mark +/- ATR x TRAIL_ATR_MULT (mark-based, robuste
       vs micro-spikes en futures, evite les faux stop-outs).
       Ne recule jamais : le SL ne peut qu avancer.

  [E5] Time exit (_close_time_exit)
       Sortie forcee si trade ouvert > MAX_TRADE_HOURS (defaut: 6h).
       Libere le capital immobilise par des positions zombies.

  [E6] Anti race-condition systematique
       trade["closing"] = True dans TOUS les moteurs de fermeture.
       Bloque toute double execution (Binance TP + trailing + SL
       en parallele) -> zero ordre double, zero position fantome.

  [E7] close_reason structure (journal + banners console)
       SL_HARD | SL_MANUAL | TP_PARTIAL | TRAIL_STOP |
       TIME_EXIT | BE_EXIT | BINANCE_TP | BINANCE_CLOSED

  [E8] CLOSE_MODE configurable
       BINANCE_ONLY : 100% exchange orders (comportement v5.0)
       STRATEGIC    : moteur Python complet (defaut v5.3)
       HYBRID       : Binance prioritaire + fallback Python
"""


import os, json, csv, time, hmac, hashlib, math, copy
from datetime import datetime
from urllib import request as urlreq, parse as urlparse, error as urlerr
from typing import Optional, Tuple, List, Dict

# ???????????????????????????????????????????????????????????????
#  ?  CONFIGURATION
# ???????????????????????????????????????????????????????????????
API_KEY    = os.environ.get("BINANCE_KEY",    "UhM8iOqQvoWF6vVO16LqK88cebdS063DufgqsLs1hjq8Puj9kiF0WffgnM73B9pd")
API_SECRET = os.environ.get("BINANCE_SECRET", "0ecLSeiUZLNLKFou8GCVY9VGJunpu2QBNlBOmV0Sr7MUA4Ye3tqpIDMoThjmiLP7")
TG_TOKEN   = os.environ.get("TG_TOKEN",       "7403481925:AAEDticdpHEhdCrVbwmopMG7QIi31bWxrwA-Nw")
TG_CHAT_ID = os.environ.get("TG_CHAT_ID",     "7403481925")

# ?? Anthropic AI Agent ????????????????????????????????????????
ANTHROPIC_API_KEY  = os.environ.get("ANTHROPIC_API_KEY", "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")
ANTHROPIC_MODEL    = "claude-sonnet-4-20250514"
AI_MIN_CONFIDENCE  = 68   # score minimum IA pour autoriser le trade (0-100)
AI_ENABLED         = True  # False = desactive l'agent (mode legacy)

# ═══════════════════════════════════════════════════════════════
#  ⚙️  PARAMÈTRES DE TRADING
# ═══════════════════════════════════════════════════════════════
LEVERAGE          = 20
MAX_MARGIN_PCT    = 0.15
MIN_BALANCE_USD   = 1.0

RISK_TIERS = [
    (1.0,  1.5,  0.050),
    (1.5,  2.0,  0.050),
    (2.0,  3.0,  0.045),
    (3.0,  5.0,  0.040),
    (5.0,  999,  0.035),
]

SIGNAL_RISK_SCALE = {
    7: 0.080,
    6: 0.065,
    5: 0.050,
}
RISK_MAX_CAP = 0.08
RISK_MIN_PCT = 0.05

FEE_RATE          = 0.0004
MAX_POSITIONS     = 3
COOLDOWN_MIN      = 15
SCAN_INTERVAL_SEC = 60
KLINES_LIMIT      = 220
TG_SUMMARY_CYCLES = 60
MAX_CONSEC_SL     = 3
PAUSE_AFTER_SL_MIN= 30
PROFIT_LOCK_PCT   = 0.50

TP_SPLIT = [
    {"r": 1.0, "pct": 0.50},
    {"r": 2.0, "pct": 0.30},
    {"r": 3.0, "pct": 0.20},
]

SYMBOLS = [
    "ETHUSDT",   "BNBUSDT",   "DOGEUSDT",  "SOLUSDT",   "XRPUSDT",
    "APTUSDT",   "LINKUSDT",  "OPUSDT",    "AVAXUSDT",  "ADAUSDT",
    "LTCUSDT",   "MATICUSDT", "UNIUSDT",   "AAVEUSDT",  "NEARUSDT",
    "FTMUSDT",   "XLMUSDT",   "TRXUSDT",   "SANDUSDT",  "ALGOUSDT",
    "ETCUSDT",
]
TOP_N_SYMBOLS = 8

OB_LOOKBACK        = 5
FIB_MIN            = 0.50
FIB_MAX            = 0.90
IMBALANCE_MIN_FILL = 0.65
CRT_BODY_RATIO     = 0.55
CRT_WICK_RATIO     = 0.60
CRT_ENGULF_MULT    = 1.30
CRT_TWEEZER_TOL    = 0.0015
ATR_PERIOD         = 14
ATR_LOW_MULT       = 0.003
ATR_HIGH_MULT      = 0.018
SL_ATR_MIN_FACTOR  = 1.5
SCORE_THRESH       = {"LOW": 6, "NORMAL": 5, "HIGH": 5}
SCORE_MAX          = 7

SNIPER_MODE         = True
SNIPER_COOLDOWN_MIN = 60
SNIPER_MIN_SCORE    = 6

# ── v5.0 -- Filtres qualite d'entree ─────────────────────────
# [P1-C] Sessions autorisees (UTC)
SESSION_WHITELIST   = {"LONDON", "NY"}   # Asie + Dead bloques
SESSION_ASIA_TRADE  = False              # True = autorise l'Asie (deconseille)

# [P1-B] HTF M15 bias
HTF_CACHE_SEC       = 900               # 15min de cache
HTF_GATE_ENABLED    = True              # False = desactive le filtre HTF

# [P1-D] BTC correlation
BTC_CORR_ENABLED    = True              # False = desactive le filtre BTC
BTC_CACHE_SEC       = 600              # 10min de cache

# [P1-E] Volatility spike (news/liquidation)
SPIKE_ATR_MULT      = 3.5              # bougie > 3.5xATR -> skip

# [P1-G] Break-even automatique
BE_TRIGGER_R        = 0.5              # declenche BE quand uPnL >= 0.5xrisk_usd
BE_ENABLED          = True

# ══════════════════════════════════════════════════════════════
#  🚪  v5.3 -- STRATEGIC EXIT ENGINE
# ══════════════════════════════════════════════════════════════

# Mode de clôture global
# BINANCE_ONLY : 100% ordres exchange (comportement v5.0)
# STRATEGIC    : moteur Python complet (trailing, partials, time exit)
# HYBRID       : Binance en priorite + fallback Python si lag API
CLOSE_MODE          = "STRATEGIC"

# ── Trailing stop ─────────────────────────────────────────────
# Active après TRAIL_R_STARTxR de profit
# SL deplace à : mark +/- ATR x TRAIL_ATR_MULT
# (mark-based = robuste vs micro-spikes en futures)
TRAIL_R_START       = 2.0              # R minimum pour activer le trailing
TRAIL_ATR_MULT      = 1.0             # distance trailing = ATR x mult

# ── Partial close ─────────────────────────────────────────────
# Clôture partielle manuelle si TP2 Binance non touche
PARTIAL_CLOSE_R2    = True            # active la sortie partielle à 2R

# ── Time exit ─────────────────────────────────────────────────
# Sortie forcee si trade bloque au-delà de MAX_TRADE_HOURS
MAX_TRADE_HOURS     = 6               # heures max avant exit force

# ── Adaptive BE ───────────────────────────────────────────────
# BE_TRIGGER_R dejà defini ci-dessus (0.5) -- conserve pour NORMAL
BE_FAST_R           = 0.3             # trigger ultra-rapide en vol HIGH
BE_TIGHT_BUF_MULT   = 3              # diviseur du fee_buffer si score=7 + HTF aligne

GRN="\033[92m"; RED="\033[91m"; YEL="\033[93m"
CYN="\033[96m"; MAG="\033[95m"; BLD="\033[1m"; RST="\033[0m"
def grn(t): return f"{GRN}{t}{RST}"
def red(t): return f"{RED}{t}{RST}"
def yel(t): return f"{YEL}{t}{RST}"
def cyn(t): return f"{CYN}{t}{RST}"
def mag(t): return f"{MAG}{t}{RST}"
def bld(t): return f"{BLD}{t}{RST}"
def sep(c="─", n=64): return cyn(c * n)

# ═══════════════════════════════════════════════════════════════
#  📈  SESSION STATE
# ═══════════════════════════════════════════════════════════════
class SessionState:
    def __init__(self, start_balance: float):
        self.start_balance   = start_balance
        self.peak_balance    = start_balance
        self.current_balance = start_balance
        self.session_pnl     = 0.0
        self.total_trades    = 0
        self.wins            = 0
        self.losses          = 0
        self.consecutive_sl  = 0
        self.paused          = False
        self.pause_until     = 0.0
        self.start_time      = time.time()
        self.last_summary_cycle = 0
        self.last_trade_time    = 0.0
        self.longs           = 0
        self.shorts          = 0
        self.total_rr        = 0.0
        self.max_drawdown    = 0.0
        # Stats IA
        self.ai_confirmed    = 0
        self.ai_rejected     = 0

    @property
    def win_rate(self) -> float:
        if self.total_trades == 0: return 0.0
        return round(self.wins / self.total_trades * 100, 1)

    @property
    def session_gain_mult(self) -> float:
        if self.start_balance <= 0: return 1.0
        return self.current_balance / self.start_balance

    def adaptive_risk_pct(self, score: int = 5) -> float:
        mult      = self.session_gain_mult
        base_risk = RISK_TIERS[-1][2]
        for t_min, t_max, risk in RISK_TIERS:
            if t_min <= mult < t_max:
                base_risk = risk
                break
        if self.current_balance < 10.0:
            signal_risk = SIGNAL_RISK_SCALE.get(score, RISK_MIN_PCT)
            return min(signal_risk, RISK_MAX_CAP)
        signal_bonus = {7: 1.60, 6: 1.30, 5: 1.00}.get(score, 1.00)
        return min(base_risk * signal_bonus, RISK_MAX_CAP)

    def record_win(self, pnl: float, direction: str = "", rr: float = 0.0):
        self.wins           += 1
        self.total_trades   += 1
        self.session_pnl    += pnl
        self.consecutive_sl  = 0
        self.total_rr       += rr
        self.peak_balance    = max(self.peak_balance, self.current_balance + pnl)
        if direction == "LONG":  self.longs  += 1
        if direction == "SHORT": self.shorts += 1

    def record_loss(self, pnl: float, direction: str = "", rr: float = 0.0):
        self.losses         += 1
        self.total_trades   += 1
        self.session_pnl    += pnl
        self.consecutive_sl += 1
        self.total_rr       += rr
        dd = self.peak_balance - self.current_balance
        if dd > self.max_drawdown: self.max_drawdown = dd
        if direction == "LONG":  self.longs  += 1
        if direction == "SHORT": self.shorts += 1

    @property
    def avg_rr(self) -> float:
        if self.total_trades == 0: return 0.0
        return round(self.total_rr / self.total_trades, 2)

    def check_pause(self) -> Tuple[bool, str]:
        if self.paused:
            if time.time() < self.pause_until:
                remaining = round((self.pause_until - time.time()) / 60, 1)
                return True, f"Pause active -- {remaining}min restantes"
            else:
                self.paused = False
                return False, ""
        if self.consecutive_sl >= MAX_CONSEC_SL:
            self.paused      = True
            self.pause_until = time.time() + PAUSE_AFTER_SL_MIN * 60
            return True, f"{MAX_CONSEC_SL} SL consecutifs"
        if self.session_pnl > 0:
            gain_protected = self.session_pnl * PROFIT_LOCK_PCT
            balance_floor  = self.start_balance + gain_protected
            if self.current_balance < balance_floor:
                return True, (
                    f"Profit lock declenche "
                    f"(balance ${self.current_balance:.2f} "
                    f"< floor ${balance_floor:.2f})"
                )
        return False, ""

    def session_duration(self) -> str:
        elapsed = int(time.time() - self.start_time)
        h, m    = divmod(elapsed // 60, 60)
        return f"{h}h{m:02d}m"

    def sniper_can_trade(self) -> Tuple[bool, str]:
        if not SNIPER_MODE: return True, "sniper OFF"
        elapsed_min = (time.time() - self.last_trade_time) / 60
        if elapsed_min < SNIPER_COOLDOWN_MIN:
            wait = round(SNIPER_COOLDOWN_MIN - elapsed_min, 1)
            return False, f"sniper cooldown {wait}min"
        return True, "OK"

    def sniper_record_trade(self):
        self.last_trade_time = time.time()

# ═══════════════════════════════════════════════════════════════
#  🤖  AGENT IA ANTHROPIC -- VALIDATEUR DE SIGNAUX
# ═══════════════════════════════════════════════════════════════
class AISignalVerifier:
    """
    Agent Claude qui valide chaque signal avant exécution.

    Analyse :
      • Qualité technique du signal (score, CRT, Fib, imbalance)
      • Cohérence avec la structure de marché récente
      • Corrélations fondamentales (macro, volatilité, heure)
      • Risque/récompense ajusté au contexte

    Retourne un verdict avec score de confiance 0-100.
    Score < AI_MIN_CONFIDENCE → trade rejeté.
    """

    def _build_prompt(self, symbol: str, sig: dict,
                      highs: list, lows: list, closes: list,
                      regime: str, ss: "SessionState",
                      btc_trend: str = "NEUTRAL") -> str:

        n  = min(60, len(closes))
        c  = closes
        h  = highs
        l  = lows

        change_5m  = (c[-1] - c[-5])  / c[-5]  * 100 if len(c) >= 5  else 0
        change_15m = (c[-1] - c[-15]) / c[-15] * 100 if len(c) >= 15 else 0
        change_1h  = (c[-1] - c[-60]) / c[-60] * 100 if len(c) >= 60 else 0

        high_20  = max(h[-20:]) if len(h) >= 20 else h[-1]
        low_20   = min(l[-20:]) if len(l) >= 20 else l[-1]
        range_20 = high_20 - low_20
        pos_in_range = (c[-1] - low_20) / range_20 * 100 if range_20 > 0 else 50

        rr = abs(sig["tps"][-1]["price"] - sig["entry"]) / abs(sig["entry"] - sig["sl_raw"]) if abs(sig["entry"] - sig["sl_raw"]) > 0 else 0

        now_hour = datetime.now().hour
        session  = "ASIE" if 0 <= now_hour < 8 else ("LONDON" if 8 <= now_hour < 13 else ("NY" if 13 <= now_hour < 21 else "OVERLAP/DEAD"))

        return f"""Tu es un trader institutionnel senior spécialisé en crypto futures.
Ton rôle : valider ou rejeter ce signal AVANT exécution réelle.
Sois STRICT. Un faux positif coûte du capital réel.

════ SIGNAL SMC/ICT ════
Paire        : {symbol}
Direction    : {sig['direction']}
Entrée       : {sig['entry']:.6f}
Stop-Loss    : {sig['sl_raw']:.6f} ({abs(sig['entry']-sig['sl_raw'])/sig['entry']*100:.3f}% distance)
TP1 (+1R)    : {sig['tps'][0]['price']:.6f}
TP2 (+2R)    : {sig['tps'][1]['price']:.6f}
TP3 (+3R)    : {sig['tps'][2]['price']:.6f}
R:R max      : 1:{rr:.2f}
Score global : {sig['score']}/{SCORE_MAX}
Pattern CRT  : {sig['crt_name']}
Zone Fib     : {sig['fib_zone']}
Déclencheur  : {sig['reason']}
Imbalance    : {sig.get('imb_fill', 0):.1f}% rempli

════ CONTEXTE MARCHÉ ════
Volatilité   : {regime} (LOW=calme, NORMAL=sain, HIGH=dangereux)
Session      : {session} [OK] (filtre session v5 déjà passé)
Variation 5m : {change_5m:+.2f}%
Variation 15m: {change_15m:+.2f}%
Variation 1h : {change_1h:+.2f}%
Haut 20mn    : {high_20:.6f}
Bas  20mn    : {low_20:.6f}
Position dans la range 20mn : {pos_in_range:.1f}% (0%=bas, 100%=haut)

════ [v5.0] ALIGNEMENT HTF ════
HTF M15 bias : {sig.get('htf_bias', 'N/A')} (BULLISH/BEARISH/NEUTRAL)
BTC M15 trend: {btc_trend}
Note : ces deux filtres ont DÉJÀ été vérifiés par le moteur technique.
       Si HTF ou BTC sont contre la direction, le signal a été rejeté.
       Ta mission ici est de vérifier la QUALITÉ du setup, pas juste l'alignement.

════ ÉTAT DU BOT ════
Balance      : ${ss.current_balance:.2f} USDT
SL consécutifs: {ss.consecutive_sl}/{MAX_CONSEC_SL}
Win rate     : {ss.win_rate}%
PnL session  : ${ss.session_pnl:+.4f}
Capital×     : {ss.session_gain_mult:.2f}x

════ CORRÉLATIONS À ANALYSER ════
1. SETUP : Score {sig['score']}/7 + HTF {sig.get('htf_bias','?')} + BTC {btc_trend} — combo VRAIMENT solide ?
2. MOMENTUM : Variation {change_15m:+.2f}% sur 15mn — dans le sens du trade ?
3. RISQUE : SL {abs(sig['entry']-sig['sl_raw'])/sig['entry']*100:.3f}% — cohérent avec vol {regime} ?
4. POSITION : Prix à {pos_in_range:.0f}% de la range — optimal pour {sig['direction']} ?
5. QUALITÉ : Le CRT "{sig['crt_name']}" est-il réellement fort dans ce contexte ?

════ FORMAT DE RÉPONSE ════
Réponds UNIQUEMENT en JSON valide (sans markdown, sans backticks) :
{{
  "verdict": "CONFIRME" ou "REJETTE",
  "confidence": <entier 0-100>,
  "reasoning": "<2-3 phrases max expliquant ta décision>",
  "risk_adjustment": <float 0.5 à 1.2 — facteur multiplicateur du risque>,
  "key_risk": "<principal risque identifié>",
  "key_strength": "<principale force du signal>",
  "session_quality": "<FAVORABLE | NEUTRE | DEFAVORABLE pour ce trade>"
}}"""

    def verify(self, symbol: str, sig: dict,
               highs: list, lows: list, closes: list,
               regime: str, ss: "SessionState",
               btc_trend: str = "NEUTRAL") -> dict:
        """
        Envoie le signal à Claude pour validation.
        Retourne un dict avec verdict, confidence, reasoning, risk_adjustment.
        """
        default_ok = {
            "confirmed": True, "confidence": 65,
            "reasoning": "Agent IA non configure -- signal accepte avec risque reduit",
            "risk_adjustment": 0.8,
            "key_risk": "Validation IA absente",
            "key_strength": "Signal technique positif",
            "session_quality": "NEUTRE",
        }

        if not AI_ENABLED:
            return default_ok

        if ANTHROPIC_API_KEY in ("COLLE_TA_CLE_ANTHROPIC", "", None):
            log("Agent IA: cle Anthropic absente -- fallback risque reduit", "WARN")
            return default_ok

        try:
            prompt = self._build_prompt(symbol, sig, highs, lows, closes, regime, ss,
                                        btc_trend=btc_trend)

            data = json.dumps({
                "model"     : ANTHROPIC_MODEL,
                "max_tokens": 600,
                "messages"  : [{"role": "user", "content": prompt}],
            }).encode("utf-8")

            req = urlreq.Request(
                "https://api.anthropic.com/v1/messages",
                data=data,
                headers={
                    "Content-Type"      : "application/json",
                    "x-api-key"         : ANTHROPIC_API_KEY,
                    "anthropic-version" : "2023-06-01",
                },
                method="POST",
            )

            with urlreq.urlopen(req, timeout=25) as resp:
                raw    = json.loads(resp.read())
                text   = raw["content"][0]["text"].strip()
                # Nettoyage markdown si Claude en met quand même
                text   = text.replace("```json", "").replace("```", "").strip()
                parsed = json.loads(text)

            verdict    = parsed.get("verdict", "REJETTE")
            confidence = max(0, min(100, int(parsed.get("confidence", 0))))
            confirmed  = (verdict == "CONFIRME") and (confidence >= AI_MIN_CONFIDENCE)

            return {
                "confirmed"      : confirmed,
                "confidence"     : confidence,
                "reasoning"      : parsed.get("reasoning", "--"),
                "risk_adjustment": float(parsed.get("risk_adjustment", 1.0)),
                "key_risk"       : parsed.get("key_risk", "--"),
                "key_strength"   : parsed.get("key_strength", "--"),
                "session_quality": parsed.get("session_quality", "NEUTRE"),
            }

        except json.JSONDecodeError as e:
            log(f"Agent IA: JSON invalide -> {e} | Fallback risque reduit", "WARN")
            return {**default_ok, "risk_adjustment": 0.7,
                    "reasoning": f"Reponse IA non parsable: {e}"}
        except Exception as e:
            log(f"Agent IA: erreur reseau -> {e} | Fallback risque reduit", "WARN")
            return {**default_ok, "risk_adjustment": 0.75,
                    "reasoning": f"Erreur IA ({type(e).__name__}) -- risque reduit"}


# Instance globale de l'agent
_ai_verifier = AISignalVerifier()

# ═══════════════════════════════════════════════════════════════
#  📝  LOGGER
# ═══════════════════════════════════════════════════════════════
LOG_FILE = f"alphabot_v4_{datetime.now().strftime('%Y%m%d')}.log"

def log(msg: str, level: str = "INFO"):
    ts  = datetime.now().strftime("%H:%M:%S")
    col = {"INFO": CYN, "TRADE": GRN, "WARN": YEL,
           "ERROR": RED, "PAUSE": MAG, "AI": MAG}.get(level, RST)
    line = f"[{ts}][{level}] {msg}"
    print(f"{col}{line}{RST}")
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass

# ═══════════════════════════════════════════════════════════════
#  📨  TELEGRAM
# ═══════════════════════════════════════════════════════════════
_tg_enabled = False

def _tg_raw(msg: str):
    if not _tg_enabled: return
    try:
        url  = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
        data = json.dumps({
            "chat_id"   : TG_CHAT_ID,
            "text"      : msg,
            "parse_mode": "HTML",
        }).encode("utf-8")
        req = urlreq.Request(
            url, data=data,
            headers={"Content-Type": "application/json"},
        )
        urlreq.urlopen(req, timeout=10)
    except Exception as e:
        log(f"Telegram erreur: {e}", "WARN")

def tg_check() -> bool:
    global _tg_enabled
    if TG_TOKEN in ("COLLE_TON_TOKEN_TG", "", None):
        log("Telegram non configure -- notifications desactivees", "WARN")
        return False
    try:
        url = f"https://api.telegram.org/bot{TG_TOKEN}/getMe"
        with urlreq.urlopen(url, timeout=8) as r:
            data = json.loads(r.read())
            if data.get("ok"):
                _tg_enabled = True
                log(f"Telegram OK -> @{data['result']['username']}", "INFO")
                return True
    except Exception as e:
        log(f"Telegram check echoue: {e}", "WARN")
    return False

def tg_send(msg: str):
    _tg_raw(msg)

def tg_startup(ss: "SessionState"):
    eff_lev    = get_effective_leverage(ss.start_balance)
    eff_margin = get_effective_margin_pct(ss.start_balance)
    micro_str  = " 🔬 MICRO COMPTE" if is_micro_account(ss.start_balance) else ""
    ai_str     = "[OK] Active" if AI_ENABLED and ANTHROPIC_API_KEY not in ("COLLE_TA_CLE_ANTHROPIC","") else "❌ Desactive"
    _tg_raw(
        f"<b>🤖 AlphaBot Futures v4.0 -- DÉMARRÉ{micro_str}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"💰 Balance depart : <code>${ss.start_balance:.2f} USDT</code>\n"
        f"⚙️ Levier effectif: <b>{eff_lev}x</b> ISOLATED\n"
        f"📊 Risque initial : {ss.adaptive_risk_pct(score=5)*100:.1f}%/trade\n"
        f"🤖 Agent IA       : {ai_str} (confiance min {AI_MIN_CONFIDENCE}%)\n"
        f"🌐 Pool marches   : {len(SYMBOLS)} paires\n"
        f"🏆 Top selection  : {TOP_N_SYMBOLS} meilleures paires\n"
        f"⏸️ Pause auto     : après {MAX_CONSEC_SL} SL ({PAUSE_AFTER_SL_MIN}min)\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"<i>Bugs v3.0 corriges | Agent IA validateur actif 💪</i>"
    )

def tg_ai_verdict(symbol: str, sig: dict, ai_result: dict):
    """Notification Telegram du verdict IA pour chaque signal."""
    emoji  = "[OK]" if ai_result["confirmed"] else "❌"
    color  = "🟢" if ai_result["confirmed"] else "🔴"
    risk_str = f"{ai_result['risk_adjustment']:.1f}x"
    _tg_raw(
        f"<b>🤖 Agent IA -- {emoji} {ai_result['confidence']}% confiance</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"💱 Paire     : <b>{symbol}</b> {sig['direction']}\n"
        f"🎯 Verdict   : {color} <b>{'CONFIRME' if ai_result['confirmed'] else 'REJETTE'}</b>\n"
        f"📊 Confiance : <b>{ai_result['confidence']}%</b> (seuil: {AI_MIN_CONFIDENCE}%)\n"
        f"⚠️ Risquex   : <code>{risk_str}</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"💭 Analyse   : {ai_result['reasoning']}\n"
        f"* Force      : {ai_result['key_strength']}\n"
        f"⛔ Risque    : {ai_result['key_risk']}\n"
        f"🕐 Session   : {ai_result.get('session_quality','--')}"
    )

def tg_trade_open(trade: dict, ss: "SessionState"):
    d        = trade["direction"]
    arrow    = "🟢 LONG" if d == "LONG" else "🔴 SHORT"
    tps      = trade["tps"]
    entry    = trade["entry"]
    sl       = trade["sl"]
    score    = trade["score"]
    eff_lev  = get_effective_leverage(ss.current_balance)
    notional = trade["qty"] * entry
    margin   = notional / eff_lev
    fees_est = notional * FEE_RATE * 2
    sl_pct   = round(abs(entry - sl) / entry * 100, 3)
    sl_usd   = abs(entry - sl) * trade["qty"]
    micro    = "🔬 MICRO" if is_micro_account(ss.current_balance) else ""
    tier_label = {7: "🏆 ÉLITE", 6: "💎 PREMIUM", 5: "[OK] SOLIDE"}.get(score, "-")
    risk_used  = ss.adaptive_risk_pct(score=score) * 100
    ai_conf    = trade.get("ai_confidence", "N/A")
    ai_adj     = trade.get("ai_risk_adj", 1.0)

    tp_lines = ""
    for i, t in enumerate(tps):
        tp_dist = abs(t["price"] - entry)
        tp_gain = tp_dist * trade["qty"] * t["pct"] - fees_est * t["pct"]
        tp_lines += (
            f"  TP{i+1} <code>{t['price']:.6f}</code>  "
            f"{int(t['pct']*100)}%pos  @{t['r']}R  "
            f"~+<code>${tp_gain:.4f}</code>\n"
        )

    rr = round(abs(tps[-1]["price"]-entry)/abs(entry-sl), 2) if abs(entry-sl) > 0 else 0
    growth = round((ss.session_gain_mult - 1) * 100, 1)
    growth_str = f"+{growth}%" if growth >= 0 else f"{growth}%"

    _tg_raw(
        f"<b>⚡ TRADE OUVERT -- {arrow}</b> {micro}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"💱 Paire     : <b>{trade['symbol']}</b>\n"
        f"🎯 Entree    : <code>{entry:.6f}</code>\n"
        f"🛑 SL        : <code>{sl:.6f}</code>  ({sl_pct}% / -${sl_usd:.4f})\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{tp_lines}"
        f"📐 R:R max   : 1:{rr}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📦 Quantite  : <code>{trade['qty']}</code>\n"
        f"💵 Notionnel : <code>${notional:.2f}</code>\n"
        f"🔐 Marge     : <code>${margin:.2f}</code>  ({eff_lev}x ISOLÉE)\n"
        f"💸 Frais est.: <code>~${fees_est:.4f}</code>\n"
        f"⚠️ Risque    : <code>${trade['risk_usd']:.4f}</code>  ({risk_used:.1f}%  x{ai_adj:.1f} IA)\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 Signal    : {tier_label}  Score:{score}/{SCORE_MAX}\n"
        f"🤖 IA        : [OK] {ai_conf}% confiance\n"
        f"🕯️ CRT       : {trade['crt_name']}\n"
        f"📐 Fibonacci : {trade['fib_zone']}\n"
        f"🧲 Raison    : {trade['reason']}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"💰 Balance   : <code>${ss.current_balance:.2f}</code>\n"
        f"📈 Session   : {ss.wins}W / {ss.losses}L  |  WR {ss.win_rate}%\n"
        f"💹 PnL sess  : <code>${ss.session_pnl:+.4f}</code>  ({growth_str})\n"
        f"🤖 IA stats  : [OK]{ss.ai_confirmed} confirmes / ❌{ss.ai_rejected} rejetes"
    )

def tg_trade_close(trade: dict, pnl: float, reason: str, ss: "SessionState"):
    win      = pnl >= 0
    emoji    = "🏆 WIN" if win else "💔 LOSS"
    dur      = round((time.time() - trade.get("open_time", time.time())) / 60, 1)
    notional = trade["qty"] * trade["entry"]
    fees_est = notional * FEE_RATE * 2
    pnl_net  = pnl - fees_est
    r_dist   = abs(trade["entry"] - trade["sl"])
    r_real   = round(abs(pnl) / (r_dist * trade["qty"]), 2) if r_dist > 0 else 0
    r_str    = f"+{r_real}R 🎯" if win else f"-{r_real}R"
    growth   = round((ss.session_gain_mult - 1) * 100, 1)
    growth_str = f"+{growth}%" if growth >= 0 else f"{growth}%"
    tp_str   = ", ".join(f"TP{i+1}" for i,t in enumerate(trade.get("tps",[])) if t.get("hit")) or "aucun"

    _tg_raw(
        f"<b>{emoji} -- {trade['symbol']} {trade['direction']}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🎯 Entree    : <code>{trade['entry']:.6f}</code>\n"
        f"[OK] TP touches : {tp_str}\n"
        f"🔍 Clôture   : {reason}  |  ⏱️ {dur}min\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 PnL brut  : <code>${pnl:+.4f}</code>  ({r_str})\n"
        f"💸 Frais     : <code>-${fees_est:.4f}</code>\n"
        f"💡 PnL net   : <code>${pnl_net:+.4f}</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"💰 Balance   : <code>${ss.current_balance:.2f}</code>\n"
        f"📈 Session   : {ss.wins}W / {ss.losses}L  |  WR {ss.win_rate}%\n"
        f"💹 PnL sess  : <code>${ss.session_pnl:+.4f}</code>  ({growth_str})\n"
        f"🤖 IA stats  : [OK]{ss.ai_confirmed} confirmes / ❌{ss.ai_rejected} rejetes"
    )

def tg_pause(reason: str, ss: "SessionState"):
    _tg_raw(
        f"<b>⏸️ PAUSE AUTOMATIQUE</b>\n"
        f"⚠️ Raison    : {reason}\n"
        f"⏳ Duree     : {PAUSE_AFTER_SL_MIN}min\n"
        f"📊 Session   : {ss.wins}W/{ss.losses}L\n"
        f"💰 Balance   : <code>${ss.current_balance:.2f}</code>\n"
        f"<i>Reprise dans {PAUSE_AFTER_SL_MIN}min...</i>"
    )

def tg_resume(ss: "SessionState"):
    _tg_raw(
        f"<b>▶️ REPRISE DU BOT</b>\n"
        f"💰 Balance: <code>${ss.current_balance:.2f}</code>\n"
        f"📊 {ss.wins}W/{ss.losses}L | PnL: ${ss.session_pnl:+.4f}"
    )

def tg_hourly_summary(ss: "SessionState", positions: dict):
    pos_lines = ""
    for sym, t in positions.items():
        pos_lines += f"\n  • {sym} {t['direction']} @ {t['entry']:.6f}  SL:{t['sl']:.6f}"
    if not pos_lines: pos_lines = "\n  Aucune position ouverte"
    growth     = round((ss.session_gain_mult - 1) * 100, 1)
    growth_str = f"+{growth}%" if growth >= 0 else f"{growth}%"
    _tg_raw(
        f"<b>📊 RÉSUMÉ -- AlphaBot v4.0</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"💰 Balance     : <code>${ss.current_balance:.2f}</code>\n"
        f"📈 Capital x   : {ss.session_gain_mult:.2f}x  ({growth_str})\n"
        f"💹 PnL session : <code>${ss.session_pnl:+.4f}</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🏆 Bilan       : {ss.wins}W / {ss.losses}L  |  WR {ss.win_rate}%\n"
        f"📐 R:R moyen   : {ss.avg_rr:+.2f}R\n"
        f"🤖 Agent IA    : [OK]{ss.ai_confirmed} OK / ❌{ss.ai_rejected} rejetes\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⏰ Duree sess  : {ss.session_duration()}\n"
        f"<b>Positions actives :</b>{pos_lines}"
    )

# ═══════════════════════════════════════════════════════════════
#  🔑  BINANCE FUTURES API
# ═══════════════════════════════════════════════════════════════
BASE_URL = "https://fapi.binance.com"
_time_offset_ms: int = 0

def sync_server_time():
    global _time_offset_ms
    try:
        url = f"{BASE_URL}/fapi/v1/time"
        with urlreq.urlopen(url, timeout=10) as r:
            server_time = json.loads(r.read())["serverTime"]
            _time_offset_ms = server_time - int(time.time() * 1000)
            log(f"⏱️  Synchro horloge OK -- offset: {_time_offset_ms}ms", "INFO")
    except Exception as e:
        log(f"Synchro horloge echouee: {e}", "WARN")
        _time_offset_ms = 0

def _get_timestamp() -> int:
    return int(time.time() * 1000) + _time_offset_ms

def _sign(qs: str) -> str:
    return hmac.new(API_SECRET.encode(), qs.encode(), hashlib.sha256).hexdigest()

def _request(method: str, path: str,
             params: dict = None, signed: bool = False) -> Optional[any]:
    params = dict(params or {})
    if signed:
        params["timestamp"]  = _get_timestamp()
        params["recvWindow"] = 10000
        qs  = urlparse.urlencode(params)
        params["signature"] = _sign(qs)

    qs  = urlparse.urlencode(params)
    url = f"{BASE_URL}{path}"

    try:
        if method == "GET":
            req = urlreq.Request(
                f"{url}?{qs}",
                headers={"X-MBX-APIKEY": API_KEY},
            )
        elif method == "POST":
            req = urlreq.Request(
                url, data=qs.encode(), method="POST",
                headers={
                    "X-MBX-APIKEY": API_KEY,
                    "Content-Type": "application/x-www-form-urlencoded",
                },
            )
        elif method == "DELETE":
            req = urlreq.Request(
                f"{url}?{qs}", method="DELETE",
                headers={"X-MBX-APIKEY": API_KEY},
            )
        else:
            return None

        with urlreq.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read().decode())

    except urlerr.HTTPError as e:
        body = e.read().decode()
        # ── 418 : IP bannie par Binance ───────────────────────
        if e.code == 418:
            # Extraire la durée du ban si disponible dans le message
            wait_sec = 90  # attente minimale prudente
            try:
                banned_data = json.loads(body)
                banned_msg  = banned_data.get("msg", "")
                # Format : "banned until <timestamp_ms>"
                if "banned until" in banned_msg:
                    ban_ts_ms  = int(banned_msg.split("banned until")[1].strip().split(".")[0])
                    wait_sec   = max(90, (ban_ts_ms - int(time.time() * 1000)) // 1000 + 5)
                    wait_min   = round(wait_sec / 60, 1)
                    log(
                        f"🚫 IP BANNIE Binance (418) -- ban jusqu'à {ban_ts_ms}ms "
                        f"-- attente {wait_min}min",
                        "ERROR",
                    )
                    tg_send(
                        f"🚫 <b>IP Bannie Binance !</b>\n"
                        f"Durée : <code>{wait_min} min</code>\n"
                        f"Bot en pause automatique -- reprise dans {wait_min}min."
                    )
                    # On attend par tranches de 30s pour rester réactif
                    slept = 0
                    while slept < wait_sec:
                        chunk = min(30, wait_sec - slept)
                        time.sleep(chunk)
                        slept += chunk
                        remaining = round((wait_sec - slept) / 60, 1)
                        if remaining > 0:
                            log(f"  🚫 Ban Binance -- encore {remaining}min...", "WARN")
                else:
                    log(f"🚫 IP Bannie Binance (418) -- attente {wait_sec}s", "ERROR")
                    tg_send(f"🚫 <b>IP Bannie Binance (418)</b> -- attente {wait_sec}s")
                    time.sleep(wait_sec)
            except Exception:
                log(f"🚫 IP Bannie Binance (418) -- attente {wait_sec}s", "ERROR")
                time.sleep(wait_sec)
            return None
        # ── 429 : rate limit (pas encore banni) ──────────────
        elif e.code == 429:
            log(f"⚠️ Rate limit Binance (429) -- attente 10s", "WARN")
            time.sleep(10)
            return None
        log(f"API {method} {path} -> HTTP {e.code}: {body}", "ERROR")
        return None
    except Exception as e:
        log(f"API {method} {path} -> {e}", "ERROR")
        return None

def get_klines(symbol: str, interval: str = "1m", limit: int = 220):
    return _request("GET", "/fapi/v1/klines",
                    {"symbol": symbol, "interval": interval, "limit": limit})

def get_exchange_info():
    return _request("GET", "/fapi/v1/exchangeInfo")

def get_mark_price(symbol: str) -> Optional[dict]:
    return _request("GET", "/fapi/v1/markPrice", {"symbol": symbol})

def get_balance_usdt() -> float:
    for _attempt in range(3):
        resp = _request("GET", "/fapi/v2/balance", {}, signed=True)
        if isinstance(resp, list):
            for a in resp:
                if a.get("asset") == "USDT":
                    return float(a.get("availableBalance", 0))
            return 0.0
        if _attempt < 2:
            log(f"Balance indisponible (essai {_attempt+1}/3) -- retry 5s...", "WARN")
            time.sleep(5)
    return 0.0

def get_open_positions() -> List[dict]:
    resp = _request("GET", "/fapi/v2/positionRisk", {}, signed=True)
    if not isinstance(resp, list): return []
    return [p for p in resp if abs(float(p.get("positionAmt", 0))) > 1e-9]

def set_leverage_api(symbol: str, lev: int) -> bool:
    resp = _request("POST", "/fapi/v1/leverage",
                    {"symbol": symbol, "leverage": lev}, signed=True)
    return isinstance(resp, dict) and "leverage" in resp

def set_margin_isolated(symbol: str):
    _request("POST", "/fapi/v1/marginType",
             {"symbol": symbol, "marginType": "ISOLATED"}, signed=True)

def place_order(symbol: str, side: str, order_type: str,
                quantity: str, stop_price: str = None,
                reduce_only: bool = False) -> Optional[dict]:
    params = {
        "symbol"    : symbol,
        "side"      : side,
        "type"      : order_type,
        "quantity"  : quantity,
        "reduceOnly": "true" if reduce_only else "false",
    }
    if stop_price:
        params["stopPrice"]   = stop_price
        params["workingType"] = "MARK_PRICE"
        params["timeInForce"] = "GTC"
    return _request("POST", "/fapi/v1/order", params, signed=True)

def cancel_all_orders(symbol: str):
    _request("DELETE", "/fapi/v1/allOpenOrders", {"symbol": symbol}, signed=True)

# ═══════════════════════════════════════════════════════════════
#  📐  CACHE SYMBOLES
# ═══════════════════════════════════════════════════════════════
_sym_info: Dict[str, dict] = {}

def load_symbol_info() -> bool:
    global _sym_info
    for _attempt in range(5):
        info = get_exchange_info()
        if info and "symbols" in info:
            break
        log(f"Impossible de charger exchangeInfo (essai {_attempt+1}/5) -- retry 30s...", "ERROR")
        time.sleep(30)
    else:
        log("exchangeInfo toujours indisponible après 5 essais -- abandon", "ERROR")
        return False
    for s in info["symbols"]:
        sym = s["symbol"]
        d   = {"stepSize": 1.0, "tickSize": 0.0001,
               "minQty": 1.0, "minNotional": 5.0}
        for f in s.get("filters", []):
            ft = f["filterType"]
            if ft == "LOT_SIZE":
                d["stepSize"] = float(f["stepSize"])
                d["minQty"]   = float(f["minQty"])
            elif ft == "PRICE_FILTER":
                d["tickSize"] = float(f["tickSize"])
            elif ft == "MIN_NOTIONAL":
                d["minNotional"] = float(f.get("notional", 5.0))
        _sym_info[sym] = d
    log(f"Exchange info: {len(_sym_info)} symboles charges", "INFO")
    return True

def _prec(step: float) -> int:
    if step <= 0 or step >= 1: return 0
    return max(0, -int(math.floor(math.log10(step))))

def round_step(v: float, step: float) -> float:
    if step <= 0: return v
    return round(math.floor(v / step) * step, _prec(step))

def round_tick(v: float, tick: float) -> float:
    if tick <= 0: return v
    return round(round(v / tick) * tick, _prec(tick))

def fmt_qty(qty: float, step: float) -> str: return f"{qty:.{_prec(step)}f}"
def fmt_px(px: float, tick: float)   -> str: return f"{px:.{_prec(tick)}f}"

# ═══════════════════════════════════════════════════════════════
#  💰  RISK MANAGER v4
# ═══════════════════════════════════════════════════════════════
def calc_atr(highs: list, lows: list, closes: list, n: int = ATR_PERIOD) -> float:
    if len(highs) < n + 1: return 0.0
    trs = [max(highs[i] - lows[i],
               abs(highs[i] - closes[i-1]),
               abs(lows[i]  - closes[i-1])) for i in range(-n, 0)]
    return sum(trs) / len(trs)

def structural_sl(highs: list, lows: list, direction: str,
                  entry: float, atr: float) -> Tuple[float, str]:
    """
    SL basé sur la structure du marché (swing high/low).
    LONG  → SL sous le dernier swing low
    SHORT → SL au-dessus du dernier swing high
    """
    margin = atr * 0.5

    if direction == "LONG":
        recent_lows = lows[-21:-1]
        if recent_lows:
            sl     = min(recent_lows) - margin
            source = "SwingLow"
        else:
            sl     = entry - atr * 2
            source = "ATRx2"
        if sl >= entry:
            sl = entry - atr * 2
            source = "ATRx2_fix"
    else:
        recent_highs = highs[-21:-1]
        if recent_highs:
            sl     = max(recent_highs) + margin
            source = "SwingHigh"
        else:
            sl     = entry + atr * 2
            source = "ATRx2"
        if sl <= entry:
            sl = entry + atr * 2
            source = "ATRx2_fix"

    # Buffer frais minimum
    fee_buffer = entry * FEE_RATE * 4
    if abs(entry - sl) < fee_buffer:
        sl     = entry - fee_buffer if direction == "LONG" else entry + fee_buffer
        source = "FEE_MIN"

    return sl, source

def dynamic_sl(entry: float, sl_signal: float, direction: str,
               atr: float) -> Tuple[float, str]:
    sl_struct = (entry - atr * SL_ATR_MIN_FACTOR if direction == "LONG"
                 else entry + atr * SL_ATR_MIN_FACTOR)
    if direction == "LONG":
        sl = min(sl_signal, sl_struct)
    else:
        sl = max(sl_signal, sl_struct)
    source = "SMC+ATR"
    fee_buffer  = entry * FEE_RATE * 2
    min_sl_dist = fee_buffer * 2
    if abs(entry - sl) < min_sl_dist:
        sl     = entry - min_sl_dist if direction == "LONG" else entry + min_sl_dist
        source = "FEE_BUFFER"
    return sl, source

def adaptive_max_positions(balance: float) -> int:
    if balance < 10:  return 1
    if balance < 30:  return 2
    return 3

def get_effective_leverage(balance: float) -> int:
    if balance < 2:   return 75
    if balance < 5:   return 50
    if balance < 15:  return 30
    if balance < 50:  return 25
    return LEVERAGE

def get_effective_margin_pct(balance: float) -> float:
    if balance < 2:   return 0.40
    if balance < 5:   return 0.45
    if balance < 15:  return 0.35
    if balance < 50:  return 0.25
    return MAX_MARGIN_PCT

def is_micro_account(balance: float) -> bool:
    return balance < 10.0

def calc_position_size(symbol: str, balance: float, risk_pct: float,
                       entry: float, sl: float) -> Tuple[float, float, str]:
    if symbol not in _sym_info:
        return 0.0, 0.0, f"Symbole inconnu: {symbol}"
    info = _sym_info[symbol]

    eff_lev        = get_effective_leverage(balance)
    eff_margin_pct = get_effective_margin_pct(balance)
    sl_dist_pct    = abs(entry - sl) / entry if entry > 0 else 0

    if sl_dist_pct < 0.0015:
        return 0.0, 0.0, f"SL trop serre ({sl_dist_pct*100:.3f}%) -> skip"
    if sl_dist_pct > 0.05:
        return 0.0, 0.0, f"SL trop large ({sl_dist_pct*100:.2f}%) -> skip"

    risk_usd     = balance * risk_pct
    notional_raw = risk_usd / sl_dist_pct
    max_notional = balance * eff_margin_pct * eff_lev
    notional     = min(notional_raw, max_notional)

    if notional < info["minNotional"]:
        min_qty_notional = info["minQty"] * entry
        if min_qty_notional >= info["minNotional"]:
            margin_needed = min_qty_notional / eff_lev
            if margin_needed <= balance * eff_margin_pct:
                actual_risk_pct = info["minQty"] * abs(entry - sl) / balance
                if actual_risk_pct <= 0.12:
                    return info["minQty"], min_qty_notional, ""
        return 0.0, 0.0, (
            f"Notionnel ${notional:.2f} < min ${info['minNotional']} "
            f"(balance=${balance:.2f} lev={eff_lev}x)"
        )

    qty = round_step(notional / entry, info["stepSize"])
    if qty < info["minQty"]:
        min_qty_notional = info["minQty"] * entry
        if min_qty_notional >= info["minNotional"]:
            margin_needed = min_qty_notional / eff_lev
            if margin_needed <= balance * eff_margin_pct:
                return info["minQty"], min_qty_notional, ""
        return 0.0, 0.0, f"Qty {qty} < minQty {info['minQty']}"

    return qty, qty * entry, ""

# ═══════════════════════════════════════════════════════════════
#  📊  MOTEUR DE SIGNAL SMC/CRT v4.0
# ═══════════════════════════════════════════════════════════════
def market_structure(highs, lows):
    ph, pl = [], []
    for i in range(2, len(highs) - 2):
        if (highs[i]>highs[i-1] and highs[i]>highs[i-2]
                and highs[i]>highs[i+1] and highs[i]>highs[i+2]):
            ph.append(highs[i])
        if (lows[i]<lows[i-1] and lows[i]<lows[i-2]
                and lows[i]<lows[i+1] and lows[i]<lows[i+2]):
            pl.append(lows[i])
    if len(ph)<2 or len(pl)<2: return "NEUTRAL"
    if ph[-1]>ph[-2] and pl[-1]>pl[-2]: return "BULLISH"
    if ph[-1]<ph[-2] and pl[-1]<pl[-2]: return "BEARISH"
    return "NEUTRAL"

def bos_choch(closes, highs, lows, opens, structure, atr: float):
    """
    [P1-A] BOS/CHOCH avec filtre displacement.
    Deux conditions obligatoires avant de valider un BOS :
      1. body ≥ 0.6×ATR  → bougie d'impulsion réelle, pas une mèche
      2. close ≥ structure + 0.15% → clôture clairement au-delà
    Élimine la majorité des fake breakouts / stop-hunts 1m.
    """
    if len(closes) < 20: return None
    rh, rl   = max(highs[-20:-1]), min(lows[-20:-1])
    last     = closes[-1]
    body     = abs(closes[-1] - opens[-1])
    min_body = atr * 0.6          # displacement minimum

    if last > rh:
        if body < min_body:           return None   # bougie faible -> rejet
        if last < rh * 1.0015:        return None   # clôture trop rase du niveau
        return "BOS_BULL" if structure == "BULLISH" else "CHOCH_BULL"

    if last < rl:
        if body < min_body:           return None
        if last > rl * 0.9985:        return None
        return "BOS_BEAR" if structure == "BEARISH" else "CHOCH_BEAR"

    return None

def breakout_retest(highs, lows, closes):
    if len(closes)<40: return None, None
    res  = max(highs[-40:-10]); sup = min(lows[-40:-10])
    prev = closes[-6:-1]
    if any(x>res for x in prev) and closes[-1]>res*0.997: return "BULL_RETEST", res
    if any(x<sup for x in prev) and closes[-1]<sup*1.003: return "BEAR_RETEST", sup
    return None, None

def order_block(opens, closes, highs, lows, direction):
    L = len(closes)
    for i in range(L - OB_LOOKBACK - 1, L - 1):
        if direction=="LONG":
            if closes[i]<opens[i]:
                mv=closes[i+1]-opens[i+1]; bd=abs(closes[i]-opens[i])
                if mv>0 and bd>0 and mv>1.5*bd: return highs[i], lows[i]
        else:
            if closes[i]>opens[i]:
                mv=opens[i+1]-closes[i+1]; bd=abs(closes[i]-opens[i])
                if mv>0 and bd>0 and mv>1.5*bd: return highs[i], lows[i]
    return None

def demand_supply(highs, lows, direction):
    rng = max(highs) - min(lows)
    if direction=="LONG": return min(lows)+rng*0.15, min(lows)
    return max(highs), max(highs)-rng*0.15

def fib_check(price, zh, zl, direction):
    rng = zh - zl
    if rng<1e-9: return False, 0.0, "NONE"
    ratio = (zh-price)/rng if direction=="LONG" else (price-zl)/rng
    ratio = max(0.0, min(ratio, 1.0))
    ok    = FIB_MIN <= ratio <= FIB_MAX
    if ratio>=0.85:   zone="90% StopHunt 🎯"
    elif ratio>=0.72: zone="78.6% DeepLiq 🔥"
    elif ratio>=0.55: zone="61.8% Premium *"
    else:             zone="50% Classique"
    return ok, round(ratio*100,1), zone

def imbalance_check(highs, lows, closes, direction):
    n=min(30, len(highs)-2); price=closes[-1]; best=-1.0
    for i in range(len(highs)-n, len(highs)-2):
        if direction=="LONG":
            gl, gh = highs[i], lows[i+2]
            if gh<=gl: continue
            total = gh-gl
            fill  = max(0.0, min((gh-price)/total, 1.0)) if total>1e-9 else 0.0
        else:
            gh, gl = lows[i], highs[i+2]
            if gl>=gh: continue
            total = gh-gl
            fill  = max(0.0, min((price-gl)/total, 1.0)) if total>1e-9 else 0.0
        if fill>best: best=fill
    return best>=IMBALANCE_MIN_FILL, round(best*100,1), best

def detect_crt(opens, closes, highs, lows, direction):
    if len(closes)<3: return False, "NONE"
    o=opens[-1]; c=closes[-1]; h=highs[-1]; l=lows[-1]
    o1=opens[-2]; c1=closes[-2]; h1=highs[-2]; l1=lows[-2]
    rng=h-l; body=abs(c-o); body1=abs(c1-o1)
    if rng<1e-9: return False, "NONE"
    if body/rng>=CRT_BODY_RATIO:
        if direction=="LONG"  and c>o: return True, "BougieForte↑"
        if direction=="SHORT" and c<o: return True, "BougieForte↓"
    lw=min(o,c)-l; uw=h-max(o,c)
    if direction=="LONG"  and lw/rng>=CRT_WICK_RATIO: return True, "Hammer🔨"
    if direction=="SHORT" and uw/rng>=CRT_WICK_RATIO: return True, "ShootingStar⭐"
    if body1>1e-9 and body>body1*CRT_ENGULF_MULT:
        if direction=="LONG"  and c>o and c>max(o1,c1): return True, "Engulfing↑"
        if direction=="SHORT" and c<o and c<min(o1,c1): return True, "Engulfing↓"
    tol=closes[-1]*CRT_TWEEZER_TOL
    if direction=="LONG"  and abs(l-l1)<=tol and c>c1: return True, "TweezerBottom🟢"
    if direction=="SHORT" and abs(h-h1)<=tol and c<c1: return True, "TweezerTop🔴"
    if len(closes)>=3:
        h2, l2=highs[-3], lows[-3]
        inside=(h1<=h2*1.001) and (l1>=l2*0.999)
        if inside:
            if direction=="LONG"  and c>h1: return True, "InsideBreak↑"
            if direction=="SHORT" and c<l1: return True, "InsideBreak↓"
    return False, "NONE"

def vol_regime(highs, lows, closes, n: int = ATR_PERIOD):
    if len(highs)<n+1: return "NORMAL", 0.0, SCORE_THRESH["NORMAL"]
    trs=[max(highs[i]-lows[i],
             abs(highs[i]-closes[i-1]),
             abs(lows[i]-closes[i-1])) for i in range(-n, 0)]
    atr=sum(trs)/len(trs)
    pct=atr/closes[-1] if closes[-1]>0 else 0
    if pct<ATR_LOW_MULT:  return "LOW",    pct, SCORE_THRESH["LOW"]
    if pct>ATR_HIGH_MULT: return "HIGH",   pct, SCORE_THRESH["HIGH"]
    return "NORMAL", pct, SCORE_THRESH["NORMAL"]

# ═══════════════════════════════════════════════════════════════
#  🔍  v5.0 -- FILTRES QUALITÉ D'ENTRÉE (PRIORITÉ 1)
# ═══════════════════════════════════════════════════════════════

# [P1-B] Cache HTF bias M15 par symbole
_htf_cache: Dict[str, dict] = {}

def get_htf_bias(symbol: str) -> str:
    """
    [P1-B] Structure M15 du symbole : BULLISH / BEARISH / NEUTRAL.
    Mise en cache HTF_CACHE_SEC secondes pour limiter les appels API.
    Les trades ne seront autorisés QUE si la direction 1m aligne avec ce biais.
    """
    cached = _htf_cache.get(symbol, {})
    if cached and (time.time() - cached.get("ts", 0)) < HTF_CACHE_SEC:
        return cached["bias"]
    raw = get_klines(symbol, "15m", 80)
    if not isinstance(raw, list) or len(raw) < 30:
        _htf_cache[symbol] = {"bias": "NEUTRAL", "ts": time.time()}
        return "NEUTRAL"
    highs_htf = [float(x[2]) for x in raw]
    lows_htf  = [float(x[3]) for x in raw]
    bias = market_structure(highs_htf, lows_htf)
    _htf_cache[symbol] = {"bias": bias, "ts": time.time()}
    log(f"  HTF M15 {symbol}: {bias}", "INFO")
    return bias

# [P1-D] Cache BTC trend M15
_btc_cache: dict = {"trend": "NEUTRAL", "ts": 0}

def get_btc_trend() -> str:
    """
    [P1-D] Trend BTC M15 pour filtre corrélation.
    Les altcoins ne vont pas LONG si BTC M15 BEARISH (et vice-versa).
    Mise en cache BTC_CACHE_SEC secondes.
    """
    global _btc_cache
    if time.time() - _btc_cache["ts"] < BTC_CACHE_SEC:
        return _btc_cache["trend"]
    raw = get_klines("BTCUSDT", "15m", 60)
    if not isinstance(raw, list) or len(raw) < 30:
        return "NEUTRAL"
    highs_btc = [float(x[2]) for x in raw]
    lows_btc  = [float(x[3]) for x in raw]
    trend = market_structure(highs_btc, lows_btc)
    _btc_cache = {"trend": trend, "ts": time.time()}
    log(f"  BTC M15 trend: {trend}", "INFO")
    return trend

# [P1-C] Session filter
def current_session_utc() -> str:
    """
    [P1-C] Session de trading basée sur l'heure UTC.
    LONDON  : 07h-12h UTC
    NY      : 12h-20h UTC
    ASIA    : 00h-07h UTC  (range, manipulations fréquentes)
    DEAD    : 20h-00h UTC  (faible liquidité, faux signaux)
    """
    h = datetime.utcnow().hour
    if 7  <= h < 12: return "LONDON"
    if 12 <= h < 20: return "NY"
    if 0  <= h < 7:  return "ASIA"
    return "DEAD"

def session_allowed() -> bool:
    """True uniquement pendant London et NY."""
    sess = current_session_utc()
    if sess in SESSION_WHITELIST: return True
    if sess == "ASIA" and SESSION_ASIA_TRADE: return True
    return False

# [P1-E] Volatility spike (news / liquidation cascade)
def is_vol_spike(highs: list, lows: list, atr: float) -> bool:
    """
    [P1-E] Détecte une bougie anormalement grande (news, liquidation BTC, etc.)
    Si la dernière bougie dépasse SPIKE_ATR_MULT × ATR → on saute le cycle.
    Évite les SL instantanés sur événements extrêmes.
    """
    if atr <= 0 or len(highs) < 2: return False
    last_range = highs[-1] - lows[-1]
    return last_range > atr * SPIKE_ATR_MULT

def get_signal(opens, highs, lows, closes, score_thresh: int,
               atr: float = 0.0) -> Optional[dict]:
    """
    [v5.0] Signal SMC/CRT avec score rebalancé :
      base          : 4  (structure + BOS/CHOCH validé)
      imbalance     : +1
      CRT           : +1  (était +2 → trop dominant)
      displacement  : +1  (NEW — BOS avec impulsion réelle)
      SCORE_MAX     : 7   (inchangé)

    Le filtre displacement est désormais DANS bos_choch() [P1-A].
    Un BOS sans corps suffisant ne passe plus jamais.
    """
    if len(closes) < 55: return None
    if atr <= 0:
        atr = calc_atr(highs, lows, closes)

    price     = closes[-1]
    structure = market_structure(highs, lows)
    if structure == "NEUTRAL": return None

    bos = bos_choch(closes, highs, lows, opens, structure, atr)  # [P1-A]
    br, _ = breakout_retest(highs, lows, closes)
    direction = reason = None

    if   structure == "BULLISH" and bos in ("BOS_BULL", "CHOCH_BULL"):
        direction, reason = "LONG",  bos
    elif br == "BULL_RETEST":
        direction, reason = "LONG",  "BullRetest"
    elif structure == "BEARISH" and bos in ("BOS_BEAR", "CHOCH_BEAR"):
        direction, reason = "SHORT", bos
    elif br == "BEAR_RETEST":
        direction, reason = "SHORT", "BearRetest"
    if not direction: return None

    ob  = order_block(opens, closes, highs, lows, direction)
    dz  = demand_supply(highs[-30:], lows[-30:], direction)
    zh  = ob[0] if ob else dz[0]
    zl  = ob[1] if ob else dz[1]
    fib_ok, fib_pct, fib_zone = fib_check(price, zh, zl, direction)
    if not fib_ok: return None

    # ── [P1-F] Score rebalance ────────────────────────────────
    score = 4   # base : structure confirmee + BOS/retest

    imb_ok, imb_fill, _ = imbalance_check(highs, lows, closes, direction)
    if imb_ok: score += 1

    crt_ok, crt_name = detect_crt(opens, closes, highs, lows, direction)
    if crt_ok: score += 1   # etait +2 -- CRT seul ne suffit plus

    # displacement bonus : BOS avec impulsion multi-bougies
    if bos and bos in ("BOS_BULL", "BOS_BEAR", "CHOCH_BULL", "CHOCH_BEAR"):
        # confirmation : 2 dernières bougies vont dans le même sens
        if direction == "LONG"  and closes[-2] > opens[-2]: score += 1
        if direction == "SHORT" and closes[-2] < opens[-2]: score += 1

    if score < score_thresh: return None

    sl_raw = (min(ob[1] if ob else dz[1], price * 0.9970) if direction == "LONG"
              else max(ob[0] if ob else dz[0], price * 1.0030))
    risk = abs(price - sl_raw)
    if risk < 1e-9 or risk > price * 0.05: return None

    tps = []
    for tp_def in TP_SPLIT:
        tp_price = (price + risk * tp_def["r"] if direction == "LONG"
                    else price - risk * tp_def["r"])
        tps.append({"r": tp_def["r"], "pct": tp_def["pct"],
                    "price": tp_price, "hit": False})

    return {
        "direction": direction, "entry": price, "sl_raw": sl_raw,
        "tps": tps, "risk": risk, "fib_pct": fib_pct, "fib_zone": fib_zone,
        "imb_fill": imb_fill if imb_ok else 0.0,
        "crt_name": crt_name if crt_ok else "-",
        "score": score, "reason": reason,
    }

# ═══════════════════════════════════════════════════════════════
#  📦  FETCH KLINES
# ═══════════════════════════════════════════════════════════════
def fetch_klines_parsed(symbol: str) -> Optional[tuple]:
    raw = get_klines(symbol, "1m", KLINES_LIMIT)
    if not isinstance(raw, list) or len(raw) < 60: return None
    return (
        [int(x[0])   for x in raw],
        [float(x[1]) for x in raw],
        [float(x[2]) for x in raw],
        [float(x[3]) for x in raw],
        [float(x[4]) for x in raw],
    )

# ═══════════════════════════════════════════════════════════════
#  📒  JOURNAL CSV
# ═══════════════════════════════════════════════════════════════
JOURNAL_FILE = f"journal_v4_{datetime.now().strftime('%Y%m%d')}.csv"
_HDR = ["time","symbol","direction","entry","sl","sl_source",
        "tp1","tp2","tp3","qty","notional","margin","fees_est",
        "risk_usd","risk_pct","score","crt","fib","reason",
        "vol","ai_confidence","ai_verdict","ai_risk_adj","status"]

def _jw(row):
    hdr = not os.path.exists(JOURNAL_FILE)
    with open(JOURNAL_FILE, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if hdr: w.writerow(_HDR)
        w.writerow(row)

def journal_open(t: dict, ss: "SessionState"):
    fees = t["qty"] * t["entry"] * FEE_RATE * 2
    _jw([datetime.now().isoformat(),
         t["symbol"], t["direction"], t["entry"], t["sl"], t.get("sl_source",""),
         t["tps"][0]["price"], t["tps"][1]["price"], t["tps"][2]["price"],
         t["qty"], round(t["qty"]*t["entry"],2),
         round(t["qty"]*t["entry"]/LEVERAGE,2), round(fees,4),
         t["risk_usd"], t["risk_pct"], t["score"],
         t["crt_name"], t["fib_zone"], t["reason"],
         t.get("vol_regime",""),
         t.get("ai_confidence","N/A"), t.get("ai_verdict","N/A"),
         t.get("ai_risk_adj", 1.0), "OPEN"])

def journal_close(t: dict, pnl: float, reason: str):
    """
    [v5.3] Journal enrichi avec close_reason structuré.
    close_reason vocabulary :
      SL_HARD       — SL Binance exchange déclenché
      SL_MANUAL     — SL failover Python (latence API)
      TP_PARTIAL    — Clôture partielle manuelle (R2)
      TRAIL_STOP    — Trailing stop ATR déclenché
      TIME_EXIT     — Sortie forcée > MAX_TRADE_HOURS
      BE_EXIT       — Break-even touché
      BINANCE_TP    — TP Binance exchange déclenché
      BINANCE_CLOSED — Fermé côté exchange (raison indéterminée)
    """
    fees = t["qty"] * t["entry"] * FEE_RATE * 2
    _jw([datetime.now().isoformat(),
         t["symbol"], t["direction"], t["entry"], t["sl"], t.get("sl_source",""),
         t["tps"][0]["price"], t["tps"][1]["price"], t["tps"][2]["price"],
         t["qty"], round(t["qty"]*t["entry"],2),
         round(t["qty"]*t["entry"]/LEVERAGE,2), round(fees,4),
         t["risk_usd"], t["risk_pct"], t["score"],
         t["crt_name"], t["fib_zone"], t["reason"],
         t.get("vol_regime",""),
         t.get("ai_confidence","N/A"), t.get("ai_verdict","N/A"),
         t.get("ai_risk_adj", 1.0),
         reason,                                   # <- close_reason structure v5.3
         round(pnl, 4),                            # <- pnl colonne dediee
         f"CLOSE|{reason}|PNL:{round(pnl,4)}"])   # <- compat retro

def _infer_close_reason(trade: dict, pnl: float) -> str:
    """
    [v5.3] Déduit le close_reason à partir de l'état du trade.
    Appelé quand Binance a fermé la position sans qu'on l'initie nous-mêmes.
    """
    if trade.get("close_reason"):
        return trade["close_reason"]    # raison explicite dejà posee par le moteur

    sl     = trade.get("sl", 0)
    entry  = trade.get("entry", 0)
    tps    = trade.get("tps", [])
    direct = trade.get("direction", "LONG")

    # BE touche
    if trade.get("sl_source", "").startswith("BE_AUTO") and pnl <= 0:
        return "BE_EXIT"

    # TP touche : PnL positif et mark > TP1
    if pnl > 0:
        tp1_price = tps[0]["price"] if tps else 0
        if (direct == "LONG"  and tp1_price > 0 and pnl > 0):
            return "BINANCE_TP"
        if (direct == "SHORT" and tp1_price > 0 and pnl > 0):
            return "BINANCE_TP"

    # SL touche
    if pnl < 0:
        return "SL_HARD"

    return "BINANCE_CLOSED"

def _banner_close(sym: str, reason: str, pnl: float, direction: str):
    """
    [v5.3] Banner console coloré selon le type de clôture.
    """
    icons = {
        "SL_HARD"       : f"{RED}🔴 SL_HARD{RST}",
        "SL_MANUAL"     : f"{RED}🔴 SL_MANUAL{RST}",
        "TP_PARTIAL"    : f"{GRN}🟡 TP_PARTIAL{RST}",
        "TRAIL_STOP"    : f"{GRN}🟢 TRAIL_STOP{RST}",
        "TIME_EXIT"     : f"{YEL}⏱️  TIME_EXIT{RST}",
        "BE_EXIT"       : f"{CYN}🔒 BE_EXIT{RST}",
        "BINANCE_TP"    : f"{GRN}🏆 BINANCE_TP{RST}",
        "BINANCE_CLOSED": f"{YEL}📦 BINANCE_CLOSED{RST}",
        "CLOSING_CONFIRMED": f"{CYN}[OK] CONFIRMED{RST}",
    }
    label = icons.get(reason, f"{YEL}❓ {reason}{RST}")
    col   = grn if pnl >= 0 else red
    print(
        f"\n{sep('─')}\n"
        f"  {bld(sym)}  {direction}  {label}\n"
        f"  PnL : {col(f'${pnl:+.4f}')}\n"
        f"{sep('─')}"
    )

# ═══════════════════════════════════════════════════════════════
#  🎯  ORDER MANAGER v4 -- BUGS CORRIGÉS
# ═══════════════════════════════════════════════════════════════
def open_trade(symbol: str, sig: dict, ss: "SessionState",
               atr: float, vol_regime_str: str,
               highs: list = None, lows: list = None,
               closes: list = None,
               ai_result: dict = None) -> Optional[dict]:
    """
    Place un trade avec tous les bugs v3.0 corrigés :
      [OK] Bug #1 : SL recalculé sur real_entry avec structural_sl (pas dynamic_sl sur sl_raw)
      [OK] Bug #2 : TP qty remaining recalculé APRÈS correction part_qty
      [OK] Bug #3 : fallback balance avec log warn explicite
      [OK] Bug #4 : sleep(0.5) après set_leverage avant ordre MARKET
    """
    info = _sym_info.get(symbol)
    if not info:
        log(f"{symbol}: infos symbole absentes", "WARN"); return None

    direction  = sig["direction"]
    entry      = sig["entry"]
    entry_side = "BUY"  if direction == "LONG" else "SELL"
    close_side = "SELL" if direction == "LONG" else "BUY"

    # ── SL STRUCTUREL pre-sizing ──────────────────────────────
    if highs and lows:
        sl_presizing, _ = structural_sl(highs, lows, direction, entry, atr)
    else:
        sl_presizing, _ = dynamic_sl(entry, sig["sl_raw"], direction, atr)
    sl_presizing = round_tick(sl_presizing, info["tickSize"])

    # ── Risk ajuste IA ─────────────────────────────────────────
    score    = sig.get("score", 5)
    risk_pct = ss.adaptive_risk_pct(score=score)
    if ai_result:
        risk_pct = min(risk_pct * ai_result.get("risk_adjustment", 1.0), RISK_MAX_CAP)

    # ── Sizing ────────────────────────────────────────────────
    balance  = ss.current_balance
    eff_lev  = get_effective_leverage(balance)
    qty, notional, err = calc_position_size(
        symbol, balance, risk_pct, entry, sl_presizing)
    if err:
        log(f"  {symbol} sizing refuse: {err}", "WARN"); return None

    risk_usd    = abs(entry - sl_presizing) / entry * notional
    margin      = notional / eff_lev
    sl_dist_pct = abs(entry - sl_presizing) / entry * 100

    log(
        f"  {symbol} {direction} | Entry~{entry:.6f} "
        f"SL_pre={sl_presizing:.6f} dist={sl_dist_pct:.3f}% | "
        f"Qty={fmt_qty(qty, info['stepSize'])} "
        f"Not=${notional:.2f} Marge=${margin:.2f}({eff_lev}x) "
        f"Risk={risk_pct*100:.1f}%(${risk_usd:.4f})",
        "TRADE",
    )

    # ── 🔧 BUG #4 FIX : marge + levier avec sleep avant ordre ─
    set_margin_isolated(symbol)
    ok = set_leverage_api(symbol, eff_lev)
    if not ok:
        log(f"  {symbol}: levier {eff_lev}x non confirme -- retry", "WARN")
        time.sleep(1.0)
        set_leverage_api(symbol, eff_lev)
    time.sleep(0.5)   # <- laisser Binance appliquer le levier avant l'ordre

    # ── Entree MARKET avec retry lot ─────────────────────────
    qty_str   = fmt_qty(qty, info["stepSize"])
    entry_ord = None
    _qty_attempt = qty

    for _attempt in range(3):
        _qty_str_try = fmt_qty(_qty_attempt, info["stepSize"])
        entry_ord = place_order(symbol, entry_side, "MARKET", _qty_str_try)
        if entry_ord and entry_ord.get("status") in ("FILLED", "NEW", "PARTIALLY_FILLED"):
            qty     = _qty_attempt
            qty_str = _qty_str_try
            if _attempt > 0:
                log(f"  {symbol}: lot ajuste -> {qty_str} ([OK] après {_attempt+1} essais)", "WARN")
            break
        err_msg = entry_ord.get("msg", "") if entry_ord else "timeout"
        log(f"  {symbol}: ordre entree refusé (essai {_attempt+1}/3) -> {err_msg}", "WARN")
        # Binance erreur -1111 (lot invalide) ou -2019 (marge insuffisante) :
        # on réduit la qty de 20% et on réessaie
        _qty_attempt = round_step(_qty_attempt * 0.80, info["stepSize"])
        if _qty_attempt < info["minQty"]:
            log(f"  {symbol}: qty {_qty_attempt} < minQty après réduction -> abandon", "WARN")
            break
        time.sleep(0.5)

    if not entry_ord or entry_ord.get("status") not in (
            "FILLED", "NEW", "PARTIALLY_FILLED"):
        log(f"  {symbol}: ordre entree echoue apres 3 essais -> {entry_ord}", "ERROR")
        return None

    real_entry = float(entry_ord.get("avgPrice") or entry_ord.get("price") or entry)
    if real_entry < 1e-9: real_entry = entry

    # ── 🔧 BUG #1 FIX : SL recalcule sur real_entry avec structural_sl ──
    # On NE reutilise PAS sig["sl_raw"] (trop serre) ni dynamic_sl.
    # On recalcule structural_sl sur le vrai prix d'execution.
    if highs and lows:
        sl_final, sl_source = structural_sl(highs, lows, direction, real_entry, atr)
    else:
        # Fallback ATR si pas de donnees OHLC (ne devrait pas arriver)
        dist     = atr * SL_ATR_MIN_FACTOR
        sl_final = real_entry - dist if direction == "LONG" else real_entry + dist
        sl_source = "ATR_fallback"

    sl_final = round_tick(sl_final, info["tickSize"])
    risk_actual = abs(real_entry - sl_final)

    if risk_actual < 1e-9:
        log(f"  {symbol}: risque nul après execution reelle", "WARN")
        cancel_all_orders(symbol)
        return None

    # Securite : SL dans le mauvais sens -> annuler
    if direction == "LONG" and sl_final >= real_entry:
        log(f"  {symbol}: SL au-dessus du prix LONG -> annulation", "ERROR")
        cancel_all_orders(symbol)
        return None
    if direction == "SHORT" and sl_final <= real_entry:
        log(f"  {symbol}: SL en-dessous du prix SHORT -> annulation", "ERROR")
        cancel_all_orders(symbol)
        return None

    # ── SL : STOP_MARKET avec retry ──────────────────────────
    sl_ord        = None
    sl_order_id   = None
    sl_binance_ok = False

    for _sl_try in range(3):
        sl_ord = place_order(
            symbol, close_side, "STOP_MARKET", qty_str,
            stop_price=fmt_px(sl_final, info["tickSize"]),
            reduce_only=True,
        )
        if sl_ord and sl_ord.get("orderId"):
            sl_binance_ok = True
            sl_order_id   = sl_ord.get("orderId")
            break
        log(f"  {symbol}: SL Binance echec essai {_sl_try+1}/3 -> retry...", "WARN")
        time.sleep(0.8)

    if not sl_binance_ok:
        # SL non posé sur Binance : on active le watchdog Python-side
        # Le moniteur surveillera le prix et clôturera en MARKET si SL atteint
        log(
            f"  {symbol}: ⚠️ SL Binance non posé après 3 essais -- "
            f"WATCHDOG PYTHON activé @ {sl_final:.6f}",
            "WARN",
        )
        tg_send(
            f"⚠️ <b>{symbol}</b> : SL Binance refusé !\n"
            f"🛡️ <b>Watchdog Python activé</b> @ <code>{sl_final:.6f}</code>\n"
            f"Le bot clôturera en MARKET si le prix atteint ce niveau."
        )

    # ── 🔧 BUG #2 FIX : TP qty avec remaining correct ─────────
    tp_records = []
    remaining  = qty

    for i, tp_def in enumerate(TP_SPLIT):
        tp_price_raw = (real_entry + risk_actual * tp_def["r"]
                        if direction == "LONG"
                        else real_entry - risk_actual * tp_def["r"])
        tp_px_str = fmt_px(
            round_tick(tp_price_raw, info["tickSize"]),
            info["tickSize"],
        )

        if i < len(TP_SPLIT) - 1:
            part_qty = round_step(qty * tp_def["pct"], info["stepSize"])
            part_qty = max(part_qty, info["minQty"])   # clamp minQty
            # <- BUG #2 FIX : on soustrait APRÈS avoir clampe part_qty
            remaining = max(0.0, round_step(remaining - part_qty, info["stepSize"]))
        else:
            # Dernier TP = tout ce qui reste (jamais 0)
            part_qty  = max(remaining, info["minQty"])
            remaining = 0.0

        # ── TP avec retry + watchdog Python si Binance refuse ─
        tp_ord        = None
        tp_binance_ok = False
        for _tp_try in range(3):
            tp_ord = place_order(
                symbol, close_side, "TAKE_PROFIT_MARKET",
                fmt_qty(part_qty, info["stepSize"]),
                stop_price=tp_px_str, reduce_only=True,
            )
            if tp_ord and tp_ord.get("orderId"):
                tp_binance_ok = True
                break
            log(f"  {symbol}: TP{i+1} Binance echec essai {_tp_try+1}/3 -> retry...", "WARN")
            time.sleep(0.6)

        if not tp_binance_ok:
            log(
                f"  {symbol}: ⚠️ TP{i+1} Binance non posé -- watchdog Python @ {tp_px_str}",
                "WARN",
            )
            tg_send(
                f"⚠️ <b>{symbol}</b> : TP{i+1} Binance refusé !\n"
                f"🛡️ <b>Watchdog Python activé</b> @ <code>{tp_px_str}</code>\n"
                f"Le bot clôturera partiellement en MARKET si le prix atteint ce niveau."
            )

        tp_records.append({
            "r"            : tp_def["r"],
            "pct"          : tp_def["pct"],
            "price"        : float(tp_px_str),
            "qty"          : part_qty,
            "hit"          : False,
            "order_id"     : tp_ord.get("orderId") if tp_ord else None,
            "binance_ok"   : tp_binance_ok,   # False = watchdog Python actif
        })

    # ── Build trade object ────────────────────────────────────
    ai_conf    = ai_result.get("confidence", "N/A")    if ai_result else "N/A"
    ai_verdict = "CONFIRME"                             if ai_result and ai_result.get("confirmed") else "N/A"
    ai_adj     = ai_result.get("risk_adjustment", 1.0) if ai_result else 1.0

    trade = {
        "symbol"        : symbol,
        "direction"     : direction,
        "entry"         : real_entry,
        "sl"            : sl_final,
        "sl_source"     : sl_source,
        "sl_order_id"   : sl_order_id,    # [P1-G] break-even
        "sl_binance_ok" : sl_binance_ok,  # [v5.3.3] False = watchdog Python actif
        "be_triggered"  : False,           # [P1-G] flag break-even
        "closing"       : False,           # [v5.3] anti race-condition
        "close_reason"  : None,            # [v5.3] close_reason structure
        "qty"           : qty,
        "tps"           : tp_records,
        "score"         : sig["score"],
        "crt_name"      : sig.get("crt_name", "-"),
        "fib_zone"      : sig.get("fib_zone", "-"),
        "reason"        : sig.get("reason", "-"),
        "htf_bias"      : sig.get("htf_bias", "NEUTRAL"),   # [v5.3] BE adaptatif
        "vol_regime"    : vol_regime_str,
        "_atr_cache"    : atr,             # [v5.3] trailing stop live
        "risk_usd"      : round(risk_usd, 4),
        "risk_pct"      : risk_pct,
        "open_time"     : time.time(),
        "pnl"           : 0.0,
        "ai_confidence" : ai_conf,
        "ai_verdict"    : ai_verdict,
        "ai_risk_adj"   : ai_adj,
    }

    journal_open(trade, ss)
    tg_trade_open(trade, ss)
    return trade

# ═══════════════════════════════════════════════════════════════
#  🔍  SURVEILLANCE POSITIONS
# ═══════════════════════════════════════════════════════════════
def is_position_open(symbol: str) -> Tuple[bool, float]:
    positions = get_open_positions()
    for p in positions:
        if p["symbol"] == symbol:
            return True, float(p.get("unRealizedProfit", 0))
    return False, 0.0

def estimate_pnl(trade: dict) -> float:
    try:
        mk = get_mark_price(trade["symbol"])
        if not mk: return 0.0
        mark = float(mk.get("markPrice", trade["entry"]))
        if trade["direction"] == "LONG":
            return (mark - trade["entry"]) * trade["qty"]
        return (trade["entry"] - mark) * trade["qty"]
    except Exception:
        return 0.0

# ═══════════════════════════════════════════════════════════════
#  🔄  GESTIONNAIRE DE POSITIONS
# ═══════════════════════════════════════════════════════════════
class LivePositionManager:
    def __init__(self):
        self.positions  : Dict[str, dict] = {}
        self.last_close : Dict[str, float] = {}

    def count(self) -> int: return len(self.positions)

    def can_open(self, symbol: str, ss: "SessionState") -> Tuple[bool, str]:
        if self.count() >= MAX_POSITIONS:
            return False, f"max {MAX_POSITIONS} positions"
        if symbol in self.positions:
            return False, "paire dejà ouverte"
        elapsed = (time.time() - self.last_close.get(symbol, 0)) / 60
        if elapsed < COOLDOWN_MIN:
            return False, f"cooldown {round(COOLDOWN_MIN-elapsed,1)}min"
        paused, reason = ss.check_pause()
        if paused:
            return False, f"pause: {reason}"
        return True, "OK"

    def open(self, symbol: str, trade: dict):
        self.positions[symbol] = trade

    def close(self, symbol: str):
        self.positions.pop(symbol, None)
        self.last_close[symbol] = time.time()

    def _try_breakeven(self, sym: str, trade: dict, upnl: float):
        """
        [v5.3] Break-even ADAPTATIF — 3 modes selon contexte.

        Mode sélectionné à l'exécution :
          • vol HIGH               → trigger = BE_FAST_R (0.3R)  — réactif
          • vol NORMAL / LOW       → trigger = BE_TRIGGER_R (0.5R) — standard
          • score=7 + HTF aligné   → fee_buf divisé par BE_TIGHT_BUF_MULT
                                      (BE plus serré, protège plus de profit)

        Anti race-condition : trade["closing"] bloque tout si clôture en cours.
        """
        if not BE_ENABLED: return
        if trade.get("be_triggered"): return
        if trade.get("closing"):      return   # anti race-condition v5.3

        risk_usd = trade.get("risk_usd", 0)
        if risk_usd <= 0: return

        # ── Selection du trigger selon volatilite ─────────────
        vol = trade.get("vol_regime", "NORMAL")
        trigger_r = BE_FAST_R if vol == "HIGH" else BE_TRIGGER_R

        if upnl < trigger_r * risk_usd: return

        info = _sym_info.get(sym)
        if not info: return

        direction  = trade["direction"]
        entry      = trade["entry"]
        qty        = trade["qty"]
        score      = trade.get("score", 5)
        htf_bias   = trade.get("htf_bias", "NEUTRAL")
        close_side = "SELL" if direction == "LONG" else "BUY"

        # ── Fee buffer adaptatif ───────────────────────────────
        # Score 7 + HTF aligne = setup sniper de haute qualite
        # -> on resserre le BE pour verrouiller plus de profit
        sniper_condition = (
            score == 7 and
            ((direction == "LONG"  and htf_bias == "BULLISH") or
             (direction == "SHORT" and htf_bias == "BEARISH"))
        )
        fee_divisor = BE_TIGHT_BUF_MULT if sniper_condition else 1
        fee_buf     = (entry * FEE_RATE * 6) / fee_divisor

        be_price = (entry + fee_buf if direction == "LONG"
                    else entry - fee_buf)
        be_price = round_tick(be_price, info["tickSize"])

        # ── Sanity check directionnel ──────────────────────────
        if direction == "LONG"  and be_price <= entry:
            log(f"  [BE] {sym}: be_price {be_price:.6f} <= entry -> annule", "WARN")
            return
        if direction == "SHORT" and be_price >= entry:
            log(f"  [BE] {sym}: be_price {be_price:.6f} >= entry -> annule", "WARN")
            return

        mode_label = (
            f"FAST({BE_FAST_R}R/vol HIGH)" if vol == "HIGH"
            else f"SNIPER({trigger_r}R/buf/{fee_divisor})" if sniper_condition
            else f"STD({trigger_r}R)"
        )
        log(
            f"  [BE] {sym}: uPnL=${upnl:.4f} >= {trigger_r}R "
            f"[{mode_label}] -> move SL -> {be_price:.6f}",
            "TRADE",
        )

        # ── Annulation SL existant + replacement ──────────────
        cancel_all_orders(sym)
        time.sleep(0.3)

        qty_str = fmt_qty(qty, info["stepSize"])
        be_ord  = place_order(
            sym, close_side, "STOP_MARKET", qty_str,
            stop_price=fmt_px(be_price, info["tickSize"]),
            reduce_only=True,
        )

        if be_ord:
            trade["sl"]           = be_price
            trade["sl_source"]    = f"BE_AUTO_{mode_label}"
            trade["sl_order_id"]  = be_ord.get("orderId")
            trade["be_triggered"] = True
            log(f"  [BE] {sym}: [OK] SL -> {be_price:.6f} [{mode_label}]", "TRADE")
            tg_send(
                f"🔒 <b>Break-Even AUTO -- {sym}</b>\n"
                f"Mode     : <code>{mode_label}</code>\n"
                f"SL deplace -> <code>{be_price:.6f}</code>\n"
                f"uPnL : +${upnl:.4f}  [{direction}]"
            )
        else:
            log(f"  [BE] {sym}: ❌ Ordre BE echoue", "WARN")

    # ═══════════════════════════════════════════════════════════
    #  🚪  v5.3 -- STRATEGIC EXIT ENGINE
    # ═══════════════════════════════════════════════════════════

    def _close_at_sl(self, sym: str, trade: dict) -> bool:
        """
        SL_MANUAL — Clôture manuelle au marché si Binance SL lag/échoue.
        Appelé quand mark < sl (LONG) ou mark > sl (SHORT) sans fermeture auto.
        Anti race-condition : trade["closing"] empêche double exécution.
        """
        if trade.get("closing"): return False
        info = _sym_info.get(sym)
        if not info: return False

        direction  = trade["direction"]
        close_side = "SELL" if direction == "LONG" else "BUY"
        qty_str    = fmt_qty(trade["qty"], info["stepSize"])

        log(f"  [SL_MAN] {sym}: clôture manuelle SL failover", "WARN")
        trade["closing"] = True
        cancel_all_orders(sym)
        time.sleep(0.2)

        ord_ = place_order(sym, close_side, "MARKET", qty_str, reduce_only=True)
        if ord_:
            log(f"  [SL_MAN] {sym}: [OK] ordre MARKET envoye", "TRADE")
            return True
        else:
            trade["closing"] = False   # reset si echec pour retry au prochain cycle
            log(f"  [SL_MAN] {sym}: ❌ ordre echoue", "ERROR")
            return False

    def _close_partial(self, sym: str, trade: dict,
                       pct: float, reason: str) -> bool:
        """
        TP_PARTIAL — Sortie partielle manuelle (Python-side).
        Utilisé si TP2 Binance non touché après 2R atteint côté mark.
        pct = fraction de la qty totale à fermer (ex: 0.30 pour 30%).
        Anti race-condition : skip si closing en cours.
        """
        if trade.get("closing"): return False
        if trade.get(f"partial_{reason}_done"): return False
        info = _sym_info.get(sym)
        if not info: return False

        direction  = trade["direction"]
        close_side = "SELL" if direction == "LONG" else "BUY"
        part_qty   = round_step(trade["qty"] * pct, info["stepSize"])
        part_qty   = max(part_qty, info["minQty"])
        qty_str    = fmt_qty(part_qty, info["stepSize"])

        log(f"  [PARTIAL] {sym}: fermeture {pct*100:.0f}% qty={qty_str} [{reason}]", "TRADE")
        ord_ = place_order(sym, close_side, "MARKET", qty_str, reduce_only=True)
        if ord_:
            trade[f"partial_{reason}_done"] = True
            log(f"  [PARTIAL] {sym}: [OK] {pct*100:.0f}% ferme", "TRADE")
            tg_send(
                f"📤 <b>Clôture Partielle -- {sym}</b>\n"
                f"Raison : <code>{reason}</code>\n"
                f"Qty    : <code>{qty_str}</code>  ({pct*100:.0f}%  [{direction}])"
            )
            return True
        log(f"  [PARTIAL] {sym}: ❌ ordre partiel echoue", "WARN")
        return False

    def _close_trail(self, sym: str, trade: dict,
                     mark: float, atr: float) -> bool:
        """
        TRAIL_STOP — Trailing stop ATR après TRAIL_R_START×R de profit.
        SL = mark ± ATR × TRAIL_ATR_MULT  (mark-based, robuste vs micro-spikes).
        Ne place PAS d'ordre Binance supplémentaire : déplace le SL en mémoire
        et annule/replace le STOP_MARKET Binance.
        Anti race-condition : trade["closing"] bloque si clôture en cours.
        """
        if CLOSE_MODE == "BINANCE_ONLY": return False
        if trade.get("closing"):         return False

        direction = trade["direction"]
        entry     = trade["entry"]
        risk_usd  = trade.get("risk_usd", 0)
        if risk_usd <= 0 or atr <= 0: return False

        # Calcul du profit actuel en R
        r_dist = abs(entry - trade["sl"])
        if r_dist <= 0: return False
        current_r = (mark - entry) / r_dist if direction == "LONG" else (entry - mark) / r_dist
        if current_r < TRAIL_R_START: return False

        # Nouveau SL trail
        new_sl = (mark - atr * TRAIL_ATR_MULT if direction == "LONG"
                  else mark + atr * TRAIL_ATR_MULT)

        info = _sym_info.get(sym)
        if not info: return False
        new_sl = round_tick(new_sl, info["tickSize"])

        # On ne deplace le SL que si c'est FAVORABLE (jamais reculer)
        if direction == "LONG"  and new_sl <= trade["sl"]: return False
        if direction == "SHORT" and new_sl >= trade["sl"]: return False

        log(
            f"  [TRAIL] {sym}: {current_r:.2f}R >= {TRAIL_R_START}R "
            f"-> SL {trade['sl']:.6f} -> {new_sl:.6f}  (ATRx{TRAIL_ATR_MULT})",
            "TRADE",
        )

        close_side = "SELL" if direction == "LONG" else "BUY"
        qty_str    = fmt_qty(trade["qty"], info["stepSize"])

        cancel_all_orders(sym)
        time.sleep(0.2)
        ord_ = place_order(
            sym, close_side, "STOP_MARKET", qty_str,
            stop_price=fmt_px(new_sl, info["tickSize"]),
            reduce_only=True,
        )
        if ord_:
            old_sl = trade["sl"]
            trade["sl"]         = new_sl
            trade["sl_source"]  = f"TRAIL_{current_r:.1f}R"
            trade["sl_order_id"] = ord_.get("orderId")
            log(f"  [TRAIL] {sym}: [OK] SL {old_sl:.6f} -> {new_sl:.6f}", "TRADE")
            tg_send(
                f"🎯 <b>Trailing Stop -- {sym}</b>\n"
                f"Profit : <code>{current_r:.2f}R</code>\n"
                f"SL deplace -> <code>{new_sl:.6f}</code>  [{direction}]"
            )
            return True
        else:
            log(f"  [TRAIL] {sym}: ❌ ordre trailing echoue", "WARN")
            return False

    def _close_time_exit(self, sym: str, trade: dict) -> bool:
        """
        TIME_EXIT — Sortie forcée si trade bloqué > MAX_TRADE_HOURS.
        Évite les positions zombies qui immobilisent du capital inutilement.
        Anti race-condition : trade["closing"] bloque si clôture en cours.
        """
        if CLOSE_MODE == "BINANCE_ONLY": return False
        if trade.get("closing"):         return False

        elapsed_h = (time.time() - trade.get("open_time", time.time())) / 3600
        if elapsed_h < MAX_TRADE_HOURS: return False

        info = _sym_info.get(sym)
        if not info: return False

        direction  = trade["direction"]
        close_side = "SELL" if direction == "LONG" else "BUY"
        qty_str    = fmt_qty(trade["qty"], info["stepSize"])
        upnl       = estimate_pnl(trade)

        log(
            f"  [TIME_EXIT] {sym}: {elapsed_h:.1f}h >= {MAX_TRADE_HOURS}h "
            f"uPnL=${upnl:.4f} -> sortie forcee",
            "WARN",
        )
        trade["closing"] = True
        cancel_all_orders(sym)
        time.sleep(0.2)

        ord_ = place_order(sym, close_side, "MARKET", qty_str, reduce_only=True)
        if ord_:
            log(f"  [TIME_EXIT] {sym}: [OK] sortie forcee envoyee", "TRADE")
            tg_send(
                f"⏱️ <b>Time Exit -- {sym}</b>\n"
                f"Duree  : <code>{elapsed_h:.1f}h</code>  (max {MAX_TRADE_HOURS}h)\n"
                f"uPnL   : <code>${upnl:.4f}</code>  [{direction}]"
            )
            return True
        else:
            trade["closing"] = False
            log(f"  [TIME_EXIT] {sym}: ❌ ordre echoue", "ERROR")
            return False

    def _handle_strategic_exits(self, sym: str, trade: dict,
                                 mark: float, upnl: float, atr: float) -> bool:
        """
        Orchestrateur des sorties stratégiques v5.3.
        Appelé depuis monitor_all() pour chaque position OUVERTE.

        Pipeline :
          1. SL failover  — si mark franchit SL et position encore ouverte
          2. Partial R2   — si mark ≥ TP2 et PARTIAL_CLOSE_R2 activé
          3. Trailing     — si profit ≥ TRAIL_R_START × R
          4. Time exit    — si trade vieux > MAX_TRADE_HOURS

        Retourne True si une clôture complète a été initiée (→ to_close).
        """
        if trade.get("closing"): return False

        direction = trade["direction"]
        sl        = trade["sl"]
        entry     = trade["entry"]
        risk_dist = abs(entry - sl)

        # ── 1. SL failover ────────────────────────────────────
        sl_breached = (
            (direction == "LONG"  and mark <= sl) or
            (direction == "SHORT" and mark >= sl)
        )
        if sl_breached:
            log(f"  [EXIT] {sym}: mark {mark:.6f} franchit SL {sl:.6f} -> failover", "WARN")
            closed = self._close_at_sl(sym, trade)
            if closed:
                return True   # sera retire de positions dans monitor_all

        # ── 1b. Watchdog TP Python (si Binance a refusé le TP) ──
        # Pour chaque TP dont binance_ok=False, on surveille le prix
        # et on clôture en MARKET si atteint
        if CLOSE_MODE != "BINANCE_ONLY":
            for _tp in trade.get("tps", []):
                if _tp.get("hit"):        continue
                if _tp.get("binance_ok", True): continue  # Binance gère
                tp_price = _tp["price"]
                tp_hit = (
                    (direction == "LONG"  and mark >= tp_price) or
                    (direction == "SHORT" and mark <= tp_price)
                )
                if tp_hit:
                    _tp_label = f"R{_tp['r']}"
                    log(
                        f"  [WATCHDOG_TP] {sym}: mark {mark:.6f} atteint TP "
                        f"{tp_price:.6f} ({_tp_label}) -> clôture Python",
                        "TRADE",
                    )
                    _tp["hit"] = True
                    ok = self._close_partial(
                        sym, trade,
                        pct=_tp["pct"],
                        reason=f"WATCHDOG_TP_{_tp_label}",
                    )
                    if ok:
                        tg_send(
                            f"🎯 <b>Watchdog TP {_tp_label} -- {sym}</b>\n"
                            f"Mark  : <code>{mark:.6f}</code>\n"
                            f"TP    : <code>{tp_price:.6f}</code>\n"
                            f"Qty   : {_tp['pct']*100:.0f}%  [{direction}]"
                        )

        # ── 2. Partial close à 2R ─────────────────────────────
        if PARTIAL_CLOSE_R2 and risk_dist > 0 and CLOSE_MODE != "BINANCE_ONLY":
            r2_price = (entry + risk_dist * 2.0 if direction == "LONG"
                        else entry - risk_dist * 2.0)
            r2_hit = (
                (direction == "LONG"  and mark >= r2_price) or
                (direction == "SHORT" and mark <= r2_price)
            )
            if r2_hit:
                self._close_partial(sym, trade, pct=0.30, reason="R2_PARTIAL")

        # ── 3. Trailing stop ──────────────────────────────────
        if CLOSE_MODE in ("STRATEGIC", "HYBRID") and atr > 0:
            self._close_trail(sym, trade, mark, atr)

        # ── 4. Time exit ──────────────────────────────────────
        closed = self._close_time_exit(sym, trade)
        if closed:
            return True

        return False

    # ═══════════════════════════════════════════════════════════
    #  🔄  MONITOR_ALL v5.3 -- orchestrateur leger
    # ═══════════════════════════════════════════════════════════
    def monitor_all(self, ss: "SessionState"):
        """
        [v5.3] Orchestrateur de surveillance — logique métier externalisée.

        Pipeline par position :
          [1] Sync état exchange (position ouverte ou non)
          [2] Break-even adaptatif
          [3] Sorties stratégiques (SL failover / partials / trailing / time)
          [4] Cleanup des positions fermées + stats session
        """
        to_close: List[Tuple[str, float, str]] = []

        for sym, trade in list(self.positions.items()):
            if trade.get("closing"):
                # Position dejà en cours de fermeture -- on attend confirmation
                open_, upnl = is_position_open(sym)
                if not open_:
                    pnl = estimate_pnl(trade)
                    to_close.append((sym, pnl, trade.get("close_reason", "CLOSING_CONFIRMED")))
                continue

            open_, upnl = is_position_open(sym)

            if not open_:
                # ── Position fermee côte Binance (TP/SL exchange) ──
                pnl    = estimate_pnl(trade)
                reason = _infer_close_reason(trade, pnl)
                to_close.append((sym, pnl, reason))
                result = "WIN" if pnl >= 0 else "LOSS"
                log(
                    f"{sym} CLÔTURÉ [{reason}] | PnL~${pnl:.4f} | {result}",
                    "TRADE",
                )
            else:
                # ── Position ouverte -- surveillance active ──────────
                col = grn if upnl >= 0 else red
                dur = round((time.time() - trade.get("open_time", time.time())) / 60, 1)
                log(f"  {sym} OPEN | uPnL: {col(f'${upnl:.4f}')} | {dur}min", "INFO")

                # ATR courant pour trailing (best-effort -- 0 si fetch impossible)
                atr_live = trade.get("_atr_cache", 0.0)

                # [1] Break-even adaptatif
                self._try_breakeven(sym, trade, upnl)

                # [2] Sorties strategiques
                if CLOSE_MODE != "BINANCE_ONLY":
                    mark_data = get_mark_price(sym)
                    mark = float(mark_data.get("markPrice", 0)) if mark_data else 0.0
                    if mark > 0:
                        force_close = self._handle_strategic_exits(
                            sym, trade, mark, upnl, atr_live)
                        if force_close:
                            pnl    = estimate_pnl(trade)
                            reason = trade.get("close_reason", "SL_MANUAL")
                            to_close.append((sym, pnl, reason))

        # ── Cleanup + mise à jour session ───────────────────────
        for sym, pnl, reason in to_close:
            trade = self.positions.get(sym, {})
            if not trade:
                continue

            journal_close(trade, pnl, reason)
            _banner_close(sym, reason, pnl, trade.get("direction", ""))
            r_dist    = abs(trade.get("entry", 0) - trade.get("sl", 0))
            r_real    = round(abs(pnl) / (r_dist * trade.get("qty", 1)), 2) if r_dist > 0 else 0
            direction = trade.get("direction", "")

            if pnl >= 0:
                ss.record_win(pnl, direction=direction, rr=r_real)
            else:
                ss.record_loss(pnl, direction=direction, rr=-r_real)

            # Refresh balance avec fallback (bug #3 v4)
            new_bal = get_balance_usdt()
            if new_bal > 0:
                ss.current_balance = new_bal
            else:
                log("⚠️  Balance API indisponible -- fallback PnL approximatif", "WARN")
                ss.current_balance = max(0.0, ss.current_balance + pnl)

            tg_trade_close(trade, pnl, reason, ss)

            paused, reason_p = ss.check_pause()
            if paused and ss.consecutive_sl >= MAX_CONSEC_SL:
                log(f"⏸️  PAUSE -- {reason_p}", "PAUSE")
                tg_pause(reason_p, ss)

            self.close(sym)

    def reconcile(self):
        for p in get_open_positions():
            sym = p["symbol"]
            if sym in SYMBOLS and sym not in self.positions:
                d  = "LONG" if float(p.get("positionAmt", 0)) > 0 else "SHORT"
                ep = float(p.get("entryPrice", 0))
                log(f"Position externe {sym} {d} @ {ep}", "WARN")
                tg_send(f"⚠️ Position {sym} {d} @ {ep} detectee hors bot. Gestion manuelle.")

# ═══════════════════════════════════════════════════════════════
#  🔭  SCANNER MULTI-MARCHÉS v4.0
# ═══════════════════════════════════════════════════════════════
def scan_and_rank_symbols(pm: LivePositionManager,
                          ss: SessionState) -> List[dict]:
    results : List[dict] = []
    skipped : List[dict] = []

    log(f"🔭 Scan {len(SYMBOLS)} marches...", "INFO")

    # ── [P1-C] Session filter -- verification globale ──────────
    sess = current_session_utc()
    if not session_allowed():
        log(f"  ⛔ Session {sess} bloquee (hors London/NY) -- scan annule", "WARN")
        tg_send(f"⏸️ Session <b>{sess}</b> bloquee -- AlphaBot v5 attend London/NY.")
        return []

    log(f"  [OK] Session {sess} autorisee", "INFO")

    # ── [P1-D] BTC trend -- recupere une seule fois pour tout le scan
    btc_trend = get_btc_trend() if BTC_CORR_ENABLED else "NEUTRAL"
    log(f"  📡 BTC M15 trend: {btc_trend}", "INFO")

    for symbol in SYMBOLS:
        can, reason = pm.can_open(symbol, ss)
        if not can:
            skipped.append({"symbol": symbol, "skip_reason": reason})
            continue

        data = fetch_klines_parsed(symbol)
        if not data:
            skipped.append({"symbol": symbol, "skip_reason": "fetch echoue"})
            continue

        _, opens, highs, lows, closes = data
        regime, atr_pct, thresh = vol_regime(highs, lows, closes)
        atr = calc_atr(highs, lows, closes)

        # ── [P1-E] Volatility spike (news/liquidation) ────────
        if is_vol_spike(highs, lows, atr):
            skipped.append({"symbol": symbol,
                            "skip_reason": f"vol spike >3.5xATR (news?)"})
            log(f"  ⚡ {symbol}: spike detecte -> skip", "WARN")
            continue

        if regime == "HIGH" and ss.consecutive_sl > 0:
            skipped.append({"symbol": symbol,
                            "skip_reason": f"vol HIGH + {ss.consecutive_sl} SL recent"})
            continue

        sig = get_signal(opens, highs, lows, closes, thresh, atr=atr)
        if not sig:
            skipped.append({"symbol": symbol,
                            "skip_reason": f"pas de signal [{regime}]"})
            continue

        direction = sig["direction"]

        # ── [P1-B] HTF M15 bias -- alignement obligatoire ──────
        if HTF_GATE_ENABLED:
            htf = get_htf_bias(symbol)
            htf_ok = (
                (direction == "LONG"  and htf == "BULLISH") or
                (direction == "SHORT" and htf == "BEARISH") or
                htf == "NEUTRAL"   # NEUTRAL = pas de filtre
            )
            if not htf_ok:
                skipped.append({"symbol": symbol,
                                "skip_reason": f"HTF M15 {htf} ≠ {direction}"})
                log(f"  🚫 {symbol}: HTF {htf} contre {direction} -> rejet", "WARN")
                continue
            sig["htf_bias"] = htf
        else:
            sig["htf_bias"] = "N/A"

        # ── [P1-D] BTC correlation gate ───────────────────────
        if BTC_CORR_ENABLED and symbol != "BTCUSDT":
            btc_block = (
                (direction == "LONG"  and btc_trend == "BEARISH") or
                (direction == "SHORT" and btc_trend == "BULLISH")
            )
            if btc_block:
                skipped.append({"symbol": symbol,
                                "skip_reason": f"BTC M15 {btc_trend} ≠ {direction}"})
                log(f"  🚫 {symbol}: BTC {btc_trend} bloque {direction} -> rejet", "WARN")
                continue

        results.append({
            "symbol" : symbol,
            "sig"    : sig,
            "atr"    : atr,
            "regime" : regime,
            "score"  : sig["score"],
            "highs"  : highs,
            "lows"   : lows,
            "closes" : closes,
            "session": sess,
            "btc_trend": btc_trend,
        })

        log(
            f"  [OK] {symbol:12s} {sig['direction']:5s} "
            f"Score:{sig['score']}/{SCORE_MAX} "
            f"HTF:{sig.get('htf_bias','?'):8s} "
            f"CRT:{sig['crt_name']:15s} "
            f"Fib:{sig['fib_zone']:20s} [{regime}]",
            "TRADE",
        )
        time.sleep(0.3)

    log(
        f"  Resultat scan : {len(results)} signal(s) sur {len(SYMBOLS)} "
        f"({len(skipped)} ignores)",
        "INFO",
    )

    results.sort(key=lambda x: x["score"], reverse=True)
    top_n = results[:TOP_N_SYMBOLS]

    if top_n:
        log("  🏆 Classement :", "INFO")
        for i, r in enumerate(top_n, 1):
            log(
                f"    #{i} {r['symbol']:12s} Score:{r['score']} "
                f"HTF:{r['sig'].get('htf_bias','?')} "
                f"[{r['regime']}] {r['sig']['direction']}",
                "INFO",
            )

    return top_n


def tg_scan_summary(ranked: List[dict], total: int):
    if not ranked:
        tg_send(
            f"🔭 <b>Scan multi-marches</b> -- {total} paires\n"
            f"<i>Aucun signal valide ce cycle.</i>"
        )
        return
    lines = ""
    for i, r in enumerate(ranked):
        sig    = r["sig"]
        entry  = sig["entry"]
        sl_raw = sig["sl_raw"]
        sl_pct = round(abs(entry - sl_raw) / entry * 100, 3)
        tp1    = sig["tps"][0]["price"] if sig["tps"] else 0
        rr1    = round(abs(tp1 - entry) / abs(entry - sl_raw), 2) if abs(entry - sl_raw) > 0 else 0
        lines += (
            f"\n#{i+1} <b>{r['symbol']}</b> -- {sig['direction']} "
            f"Score:<b>{r['score']}/{SCORE_MAX}</b> [{r['regime']}]\n"
            f"   Entree: <code>{entry:.5f}</code>  SL: ({sl_pct}%)\n"
            f"   TP1: <code>{tp1:.5f}</code>  R:R~1:{rr1}  {sig['fib_zone']}\n"
        )
    tg_send(
        f"🔭 <b>Scan {total} marches -- {len(ranked)} signal(s)</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━"
        f"{lines}"
    )

# ═══════════════════════════════════════════════════════════════
#  📊  DASHBOARD CONSOLE
# ═══════════════════════════════════════════════════════════════
def print_dashboard(pm: LivePositionManager, ss: SessionState, cycle: int):
    now      = datetime.now().strftime("%H:%M:%S")
    risk_pct = ss.adaptive_risk_pct(score=5) * 100
    pnl_col  = grn if ss.session_pnl >= 0 else red
    paused, pause_reason = ss.check_pause()
    pause_str = f" | {mag('PAUSE: '+pause_reason[:30])}" if paused else ""
    ai_str    = f" | IA[OK]{ss.ai_confirmed}/❌{ss.ai_rejected}"

    print(f"\n{sep('═')}")
    print(
        f"  {cyn(bld(f'CYCLE #{cycle}  {now}'))}  "
        f"Pos:{yel(str(pm.count()))}/{MAX_POSITIONS}  "
        f"Balance:{grn(f'${ss.current_balance:.2f}')}  "
        f"PnL:{pnl_col(f'${ss.session_pnl:+.4f}')}  "
        f"WR:{yel(f'{ss.win_rate}%')}"
        f"{ai_str}{pause_str}"
    )
    print(sep('═'))

    if pm.positions:
        print(f"  {yel('POSITIONS ACTIVES :')}")
        for sym, t in pm.positions.items():
            mk_data = get_mark_price(sym)
            mark    = float(mk_data.get("markPrice", t["entry"])) if mk_data else t["entry"]
            upnl    = ((mark-t["entry"])*t["qty"] if t["direction"]=="LONG"
                       else (t["entry"]-mark)*t["qty"])
            col     = grn if upnl>=0 else red
            dur     = round((time.time()-t.get("open_time",time.time()))/60, 1)
            print(
                f"  [{yel(sym)}] {t['direction']}  "
                f"Entry:{t['entry']:.6f}  Mark:{mark:.6f}  "
                f"SL:{t['sl']:.6f}[{t.get('sl_source','-')}]  "
                f"uPnL:{col(f'${upnl:.4f}')}  "
                f"IA:{t.get('ai_confidence','?')}%  {dur}min"
            )

# ═══════════════════════════════════════════════════════════════
#  🚀  BOUCLE PRINCIPALE
# ═══════════════════════════════════════════════════════════════
def main():
    print(cyn(bld("""
╔═══════════════════════════════════════════════════════════════╗
║   ALPHABOT FUTURES v5.3.4 -- ANTI-BAN + WATCHDOG            ║
║   21 marches | HTF M15 | BTC corr | Session | BE auto       ║
╚═══════════════════════════════════════════════════════════════╝""")))
    log("🚀 main() démarré -- initialisation en cours...", "INFO")

    if not API_KEY or "COLLE" in (API_KEY or ""):
        log("⛔ API_KEY non renseignee.", "ERROR")
        log("   export BINANCE_KEY='ta_cle'", "ERROR")
        log("   export BINANCE_SECRET='ton_secret'", "ERROR")
        log("   export ANTHROPIC_API_KEY='ta_cle_anthropic'", "ERROR")
        return

    # Anti-ban Binance : delais entre appels au demarrage
    # Chaque restart trop rapide flood IP -> ban 418
    log("Demarrage dans 30s (anti-ban Binance -- protection IP)...", "INFO")
    time.sleep(30)

    log("⏱️  Synchronisation horloge Binance...", "INFO")
    sync_server_time()
    time.sleep(1)

    log("📡 Chargement exchange info...", "INFO")
    if not load_symbol_info(): return
    time.sleep(2)

    log("💰 Récupération balance USDT...", "INFO")

    balance = get_balance_usdt()
    if balance <= 0:
        log("Balance USDT = 0 ou cle API invalide.", "ERROR"); return
    if balance < MIN_BALANCE_USD:
        log(f"Balance ${balance:.2f} < seuil ${MIN_BALANCE_USD}.", "ERROR"); return

    log(f"💰 Balance: ${balance:.2f} USDT", "INFO")

    ss = SessionState(start_balance=balance)
    ss.current_balance = balance

    tg_check()
    tg_startup(ss)

    # Test agent IA au demarrage
    if AI_ENABLED and ANTHROPIC_API_KEY not in ("COLLE_TA_CLE_ANTHROPIC", "", None):
        log("🤖 Test agent IA Anthropic...", "AI")
        # Test leger : juste verifier que la cle est valide
        try:
            test_data = json.dumps({
                "model": ANTHROPIC_MODEL,
                "max_tokens": 10,
                "messages": [{"role": "user", "content": "Reponds juste: OK"}],
            }).encode()
            req = urlreq.Request(
                "https://api.anthropic.com/v1/messages", data=test_data,
                headers={"Content-Type": "application/json",
                         "x-api-key": ANTHROPIC_API_KEY,
                         "anthropic-version": "2023-06-01"},
                method="POST",
            )
            with urlreq.urlopen(req, timeout=15) as r:
                json.loads(r.read())
            log("🤖 Agent IA Anthropic : [OK] OK", "AI")
            tg_send("🤖 <b>Agent IA Anthropic</b> : [OK] Connecte et operationnel")
        except Exception as e:
            log(f"🤖 Agent IA : ❌ {e}", "WARN")
            tg_send(f"⚠️ Agent IA Anthropic : erreur connexion ({e})\nBot continue sans IA.")
    else:
        log("🤖 Agent IA desactive ou cle absente -- mode sans validation IA", "WARN")

    pm         = LivePositionManager()
    pm.reconcile()
    cycle      = 0
    was_paused = False

    while True:
        cycle += 1

        bal = get_balance_usdt()
        if bal > 0:
            ss.current_balance = bal

        print_dashboard(pm, ss, cycle)

        if ss.current_balance < MIN_BALANCE_USD:
            log(f"⛔ Balance ${ss.current_balance:.2f} < ${MIN_BALANCE_USD}.", "ERROR")
            tg_send(f"⛔ Balance trop faible (${ss.current_balance:.4f}). Bot arrête.")
            break

        paused, pause_reason = ss.check_pause()
        if paused:
            if not was_paused:
                log(f"⏸️  PAUSE: {pause_reason}", "PAUSE")
            was_paused = True
            log(f"  En pause ({pause_reason}). Surveillance...", "PAUSE")
            if pm.positions:
                pm.monitor_all(ss)
            time.sleep(SCAN_INTERVAL_SEC)
            continue
        else:
            if was_paused:
                log("▶️  Reprise du bot", "INFO")
                tg_resume(ss)
                was_paused = False

        # ── Surveiller positions ──────────────────────────────
        if pm.positions:
            pm.monitor_all(ss)
            bal = get_balance_usdt()
            if bal > 0: ss.current_balance = bal

        # ── Scan + ranking ───────────────────────────────────
        max_pos = adaptive_max_positions(ss.current_balance)
        if pm.count() < max_pos:
            ranked = scan_and_rank_symbols(pm, ss)

            if cycle - ss.last_summary_cycle >= TG_SUMMARY_CYCLES:
                tg_scan_summary(ranked, len(SYMBOLS))
                tg_hourly_summary(ss, pm.positions)
                ss.last_summary_cycle = cycle

            sniper_ok, sniper_reason = ss.sniper_can_trade()
            if not sniper_ok:
                log(f"  🎯 Sniper: {sniper_reason} -> attente", "INFO")
            else:
                if SNIPER_MODE:
                    ranked = [r for r in ranked if r["score"] >= SNIPER_MIN_SCORE]

                for candidate in ranked:
                    if pm.count() >= max_pos:
                        break

                    symbol = candidate["symbol"]
                    sig    = candidate["sig"]
                    atr    = candidate["atr"]
                    regime = candidate["regime"]
                    highs  = candidate.get("highs")
                    lows   = candidate.get("lows")
                    closes = candidate.get("closes")

                    can, reason = pm.can_open(symbol, ss)
                    if not can:
                        log(f"  {symbol}: skip ({reason})", "INFO")
                        continue

                    # ── 🤖 VALIDATION AGENT IA ────────────────
                    ai_result = None
                    if AI_ENABLED:
                        log(f"  🤖 Agent IA -> validation {symbol}...", "AI")
                        ai_result = _ai_verifier.verify(
                            symbol, sig, highs or [], lows or [],
                            closes or [], regime, ss,
                            btc_trend=candidate.get("btc_trend", "NEUTRAL"),
                        )
                        tg_ai_verdict(symbol, sig, ai_result)

                        if not ai_result["confirmed"]:
                            ss.ai_rejected += 1
                            log(
                                f"  🤖 IA REJETTE {symbol} -- "
                                f"confiance {ai_result['confidence']}% "
                                f"(< {AI_MIN_CONFIDENCE}%) : {ai_result['reasoning']}",
                                "AI",
                            )
                            continue

                        ss.ai_confirmed += 1
                        log(
                            f"  🤖 IA CONFIRME {symbol} -- "
                            f"{ai_result['confidence']}% confiance "
                            f"riskx{ai_result['risk_adjustment']:.1f}",
                            "AI",
                        )

                    log(
                        f"  {symbol}: TRADE {sig['direction']} "
                        f"Score:{sig['score']}/{SCORE_MAX} "
                        f"CRT:{sig['crt_name']} Fib:{sig['fib_zone']} [{regime}]",
                        "TRADE",
                    )

                    trade = open_trade(
                        symbol, sig, ss, atr, regime,
                        highs=highs, lows=lows, closes=closes,
                        ai_result=ai_result,
                    )
                    if trade:
                        pm.open(symbol, trade)
                        # 🔧 BUG #3 FIX : refresh avec fallback dejà dans open_trade
                        bal = get_balance_usdt()
                        if bal > 0:
                            ss.current_balance = bal
                        else:
                            log("⚠️  Balance API indisponible après trade -- fallback", "WARN")
                        ss.sniper_record_trade()
                        log(f"  [OK] {symbol}: ouvert. Balance: ${ss.current_balance:.2f}", "TRADE")
                    else:
                        log(f"  ❌ {symbol}: ouverture echouee", "WARN")

                    time.sleep(2)
                    if SNIPER_MODE:
                        break

        else:
            log(f"Max positions ({max_pos}) atteint. Surveillance uniquement.", "INFO")
            if cycle - ss.last_summary_cycle >= TG_SUMMARY_CYCLES:
                tg_hourly_summary(ss, pm.positions)
                ss.last_summary_cycle = cycle

        log(f"Prochain cycle dans {SCAN_INTERVAL_SEC}s...", "INFO")
        time.sleep(SCAN_INTERVAL_SEC)


# ═══════════════════════════════════════════════════════════════
#  🌐  FLASK KEEPALIVE
# ═══════════════════════════════════════════════════════════════
import threading

try:
    from flask import Flask, jsonify
    _flask_ok = True
except ImportError:
    _flask_ok = False

if _flask_ok:
    _app        = Flask(__name__)
    _bot_status = {"running": False, "cycle": 0, "started_at": None}

    @_app.route("/")
    def index():
        return jsonify({
            "bot"       : "AlphaBot Futures v5.0",
            "status"    : "running" if _bot_status["running"] else "stopped",
            "started_at": _bot_status["started_at"],
            "symbols"   : len(SYMBOLS),
            "top_n"     : TOP_N_SYMBOLS,
            "ai_enabled": AI_ENABLED,
        })

    @_app.route("/health")
    def health():
        return "OK", 200

    def _run_flask():
        port = int(os.environ.get("PORT", 8080))
        log(f"🌐 Flask keepalive sur port {port}", "INFO")
        _app.run(host="0.0.0.0", port=port, use_reloader=False)

# ═══════════════════════════════════════════════════════════════
if __name__ == "__main__":
    if _flask_ok:
        t = threading.Thread(target=_run_flask, daemon=True)
        t.start()
        _bot_status["started_at"] = datetime.now().isoformat()
        _bot_status["running"]    = True
        # ── FIX RENDER : laisser Flask binder le port et passer le health-check
        # avant de lancer main() qui fait des appels réseau longs (Binance, Anthropic).
        # Sans ce sleep, Render peut redéployer si le GET / n'arrive pas assez vite.
        log("⏳ Attente 5s -- Flask health-check Render...", "INFO")
        time.sleep(5)
    else:
        log("Flask non installe -- mode standalone", "WARN")

    try:
        log("🚀 main() démarré", "INFO")
        main()
    except KeyboardInterrupt:
        print(grn(bld("\n  ✋ Bot arrête manuellement.")))
    except Exception as e:
        log(f"ERREUR CRITIQUE: {e}", "ERROR")
        tg_send(f"🚨 <b>AlphaBot v5.3 CRASH</b>\n{e}")
        raise
    finally:
        if _flask_ok:
            _bot_status["running"] = False
