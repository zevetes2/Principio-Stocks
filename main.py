################################-----------------------PORTAFOLIO E.T.H-----------------------##########################################
# VERSIÓN PROFESIONAL 6: Ultra-optimizada - cache único, APIs muertas skip instantáneo,
# sanitización NaN, scoring robusto, 8 workers, timeouts agresivos, sesión HTTP persistente.
# Fuentes: yfinance → Alpha Vantage → FMP → finnhub
# Instalar: pip install yfinance gspread google-auth requests pandas numpy finnhub-python

import gspread
from google.oauth2.service_account import Credentials
import yfinance as yf
import pandas as pd
import datetime
import numpy as np
import requests
import time
import os
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FuturesTimeoutError
from dotenv import load_dotenv
from collections import deque, defaultdict
from typing import Optional, Dict, Any, List, Tuple

# ==============================================================
# 📋 CONFIGURACIÓN DE LOGGING
# ==============================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger("PortafolioETH")

# ==============================================================
# ⚙️  CONFIGURACIÓN DE APIs Y ENTORNO
# ==============================================================
load_dotenv()
JSON_KEY_FILE = "principios.json"

def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise EnvironmentError(f"Variable de entorno '{name}' no encontrada.")
    return value

ALPHA_VANTAGE_KEY = _require_env("ALPHA_VANTAGE_KEY")
FMP_KEY           = _require_env("FMP_KEY")
FINNHUB_KEY       = _require_env("FINNHUB_KEY")

# Google Sheets
SPREADSHEET_NAME = "Portafolio Financiero"
WORKSHEET_NAME   = "7 PRINCIPIOS"
SCORESHEET_NAME  = "SCORES"
START_ROW = 7
END_ROW   = 190

# ==============================================================
# 🔑 AUTENTICACIÓN GOOGLE
# ==============================================================
try:
    scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    creds  = Credentials.from_service_account_file(JSON_KEY_FILE, scopes=scopes)
    gc     = gspread.authorize(creds)
    logger.info("✅ Autenticación con Google exitosa.")
except Exception as e:
    logger.error(f"❌ Error de autenticación Google: {e}")
    raise

# ==============================================================
# 🔄 CIRCUIT BREAKER + RATE LIMITER
# ==============================================================
class CircuitBreaker:
    def __init__(self, max_failures: int = 3, name: str = "API"):
        self.max_failures = max_failures
        self.name = name
        self.failures = 0
        self._open = False
        self._permanent = False
        self._lock = threading.Lock()

    def record_success(self):
        with self._lock:
            self.failures = 0

    def record_failure(self, status_code: int = 0):
        with self._lock:
            if status_code == 403:
                self._permanent = True
                self._open = True
                logger.error(f"🔴 {self.name}: 403 Forbidden - API key inválida o expirada. Desactivada permanentemente.")
                return
            if status_code == 429:
                logger.warning(f"⏱️  {self.name}: 429 Rate Limit - esperando...")
                return
            self.failures += 1
            if self.failures >= self.max_failures:
                self._open = True
                logger.warning(f"🔴 CIRCUIT BREAKER ABIERTO para {self.name} ({self.failures} fallos)")

    def is_open(self) -> bool:
        with self._lock:
            return self._open or self._permanent

    def is_permanent(self) -> bool:
        with self._lock:
            return self._permanent

class RateLimiter:
    def __init__(self, max_calls: int, period: int, name: str = "API"):
        self.max_calls = max_calls
        self.period = period
        self.name = name
        self.calls = deque()
        self._lock = threading.Lock()

    def wait_if_needed(self):
        with self._lock:
            now = time.time()
            while self.calls and now - self.calls[0] > self.period:
                self.calls.popleft()
            if len(self.calls) >= self.max_calls:
                sleep_time = self.period - (now - self.calls[0]) + 1
                if sleep_time > 0:
                    logger.info(f"⏱️  {self.name} rate limit: durmiendo {sleep_time:.1f}s")
                    time.sleep(sleep_time)
            self.calls.append(time.time())

av_limiter   = RateLimiter(max_calls=5,  period=60, name="AlphaVantage")
fmp_limiter  = RateLimiter(max_calls=300, period=60, name="FMP")
finn_limiter = RateLimiter(max_calls=60,  period=60, name="Finnhub")

# ==============================================================
# ⚡ CACHE DE ALPHAVANTAGE POR TICKER
# Evita llamadas duplicadas: INCOME_STATEMENT y BALANCE_SHEET
# se llaman 2 veces cada uno por ticker cuando yfinance falla.
# Con cache, cada endpoint se descarga UNA sola vez por ticker.
# ==============================================================
_av_cache: Dict[str, Any] = {}
_av_cache_lock = threading.Lock()

def av_get_cached(function: str, symbol: str, extra_params=None):
    """Wrapper cacheado de av_get. Misma llamada = respuesta instantánea."""
    key = f"{function}:{symbol}"
    with _av_cache_lock:
        if key in _av_cache:
            return _av_cache[key]
    result = av_get(function, symbol, extra_params)
    with _av_cache_lock:
        _av_cache[key] = result
    return result

av_cb   = CircuitBreaker(max_failures=2, name="AlphaVantage")
fmp_cb  = CircuitBreaker(max_failures=2, name="FMP")
finn_cb = CircuitBreaker(max_failures=2, name="Finnhub")

# ==============================================================
# 🌐 CLIENTE HTTP PERSISTENTE (Keep-Alive)
# ==============================================================
_session = requests.Session()
_session.headers.update({
    "User-Agent": "PortafolioETH/6.0 (Python; contact: user@example.com)",
    "Accept": "application/json",
    "Connection": "keep-alive",
})

AV_BASE   = "https://www.alphavantage.co/query"
FMP_BASE  = "https://financialmodelingprep.com/api/v3"
FMP_V4    = "https://financialmodelingprep.com/api/v4"
FINN_BASE = "https://finnhub.io/api/v1"

USE_FMP = os.getenv("USE_FMP", "true").lower() in ("true", "1", "yes")
if not USE_FMP:
    logger.warning("⚠️ FMP desactivado manualmente via USE_FMP=false")
    fmp_cb._permanent = True

def _safe_request(url: str, params: dict, timeout: int = 8, cb: CircuitBreaker = None,
                  limiter: RateLimiter = None, headers: dict = None) -> Optional[dict]:
    if cb and cb.is_open():
        return None
    if limiter:
        limiter.wait_if_needed()
    try:
        r = _session.get(url, params=params, timeout=timeout)
        status = r.status_code
        if status == 403:
            if cb:
                cb.record_failure(403)
            logger.error(f"🔴 {cb.name if cb else 'API'} 403 Forbidden")
            return None
        if status == 429:
            if cb:
                cb.record_failure(429)
            return None
        r.raise_for_status()
        data = r.json()
        if cb:
            cb.record_success()
        return data
    except requests.exceptions.HTTPError as e:
        status = e.response.status_code if e.response else 0
        if cb:
            cb.record_failure(status)
        return None
    except Exception as e:
        if cb:
            cb.record_failure(0)
        return None

def av_get(function, symbol, extra_params=None):
    if av_cb.is_open():
        return None
    params = {"function": function, "symbol": symbol, "apikey": ALPHA_VANTAGE_KEY}
    if extra_params:
        params.update(extra_params)
    data = _safe_request(AV_BASE, params, cb=av_cb, limiter=av_limiter)
    if data and ("Note" in data or "Information" in data):
        av_cb.record_failure()
        return None
    return data

def fmp_get(endpoint, version="v3", params=None):
    if fmp_cb.is_permanent() or fmp_cb.is_open():
        return None
    p = params or {}
    p["apikey"] = FMP_KEY
    if version == "v3":
        url = f"https://financialmodelingprep.com/api/v3{endpoint}"
    else:
        url = f"https://financialmodelingprep.com/api/v4{endpoint}"
    data = _safe_request(url, p, cb=fmp_cb, limiter=fmp_limiter)
    return data

def finn_get(endpoint, params=None):
    if finn_cb.is_open():
        return None
    p = params or {}
    p["token"] = FINNHUB_KEY
    return _safe_request(f"{FINN_BASE}{endpoint}", p, cb=finn_cb, limiter=finn_limiter)

# ==============================================================
# 📦 CACHE DE SESIÓN
# ==============================================================
_fmp_profile_cache: Dict[str, Optional[dict]] = {}
_fmp_profile_lock = threading.Lock()
_ticker_data_cache: Dict[str, Tuple] = {}
_ticker_data_lock = threading.Lock()

def fmp_profile(symbol: str) -> Optional[dict]:
    with _fmp_profile_lock:
        if symbol in _fmp_profile_cache:
            return _fmp_profile_cache[symbol]
    if fmp_cb.is_permanent():
        return None
    data = fmp_get(f"/company/profile/{symbol}")
    if data is None:
        data = fmp_get(f"/profile/{symbol}")
    result = data[0] if data and isinstance(data, list) and data else None
    with _fmp_profile_lock:
        _fmp_profile_cache[symbol] = result
    return result

# ==============================================================
# 🔧 NORMALIZACIÓN DE TICKERS
# ==============================================================
TICKER_REPLACEMENTS = {
    "BRK.B": "BRK-B", "BRK.A": "BRK-A",
    "BF.B": "BF-B", "BF.A": "BF-A",
}

def normalize_ticker(symbol: str) -> str:
    s = symbol.strip().upper()
    for old, new in TICKER_REPLACEMENTS.items():
        if s == old:
            return new
    return s

def fmp_ticker(symbol: str) -> str:
    s = symbol.strip().upper()
    for old, new in TICKER_REPLACEMENTS.items():
        if s == old:
            return new
    return s.replace(".", "-")

# ==============================================================
# 🔄 HELPER DE FALLBACK GENÉRICO
# ==============================================================

def fetch_with_fallbacks(metric_name: str, primary_val: Any, *sources: Tuple[str, callable]) -> Any:
    if primary_val not in (None, 0, "", 0.0, "N/A", np.nan):
        return primary_val
    for src_name, fn in sources:
        try:
            result = fn()
            if result not in (None, 0, "", 0.0, "N/A", np.nan):
                logger.info(f"{metric_name}: fallback exitoso desde {src_name}")
                return result
        except Exception:
            pass
    logger.warning(f"{metric_name}: no disponible en ninguna fuente")
    return None

# ==============================================================
# 📊 FUNCIONES DE MÉTRICAS
# ==============================================================

def get_target_price(info, ticker_yf, symbol):
    val = info.get("targetMeanPrice")
    if val:
        return val
    try:
        data = fmp_get(f"/price-target-consensus/{fmp_ticker(symbol)}")
        if data and isinstance(data, list) and data:
            return data[0].get("targetConsensus")
    except Exception:
        pass
    try:
        data = finn_get("/stock/price-target", {"symbol": symbol})
        if data:
            return data.get("targetMean")
    except Exception:
        pass
    try:
        data = av_get("ANALYST_PRICE_TARGET", symbol)
        if data and "data" in data:
            targets = [float(d.get("price_target", 0)) for d in data["data"] if d.get("price_target")]
            if targets:
                return round(sum(targets)/len(targets), 2)
    except Exception:
        pass
    return None

def get_analyst_count(info, symbol):
    val = info.get("numberOfAnalystOpinions")
    if val:
        return val
    try:
        data = finn_get("/stock/recommendation", {"symbol": symbol})
        if data and isinstance(data, list) and data:
            d = data[0]
            return (d.get("buy",0) + d.get("hold",0) + d.get("sell",0) +
                    d.get("strongBuy",0) + d.get("strongSell",0))
    except Exception:
        pass
    return 0

def get_rev_growth(info, symbol):
    val = info.get("revenueGrowth")
    if val:
        return val
    try:
        data = fmp_get(f"/income-statement/{fmp_ticker(symbol)}", params={"limit": 2})
        if data and isinstance(data, list) and len(data) >= 2:
            r1, r2 = data[0].get("revenue",0), data[1].get("revenue",0)
            if r2 and r2 != 0:
                return (r1 - r2) / abs(r2)
    except Exception:
        pass
    return 0

def get_margins(info, symbol):
    gross = info.get("grossMargins")
    oper  = info.get("operatingMargins")
    if gross and oper:
        return gross, oper
    try:
        data = fmp_get(f"/ratios/{fmp_ticker(symbol)}", params={"limit": 1})
        if data and isinstance(data, list) and data:
            gross = gross or data[0].get("grossProfitMargin")
            oper  = oper  or data[0].get("operatingProfitMargin")
    except Exception:
        pass
    try:
        data = av_get_cached("INCOME_STATEMENT", symbol)
        if data and "annualReports" in data and data["annualReports"]:
            r = data["annualReports"][0]
            rev = float(r.get("totalRevenue", 0) or 0)
            if rev > 0:
                gp = float(r.get("grossProfit", 0) or 0)
                oi = float(r.get("operatingIncome", 0) or 0)
                gross = gross or (gp / rev)
                oper  = oper  or (oi / rev)
    except Exception:
        pass
    return gross or 0, oper or 0

def get_forward_pe(info, symbol):
    def _fmp():
        data = fmp_get(f"/ratios/{fmp_ticker(symbol)}", params={"limit": 1})
        if data and isinstance(data, list) and data:
            return data[0].get("priceEarningsRatio")
        return None
    def _finn():
        data = finn_get("/stock/metric", {"symbol": symbol, "metric": "all"})
        if data and "metric" in data:
            return data["metric"].get("peForward")
        return None
    return fetch_with_fallbacks("Forward PE", info.get("forwardPE"), ("FMP", _fmp), ("Finnhub", _finn))

def get_peg(info, symbol):
    peg = info.get("pegRatio") or info.get("trailingPegRatio")
    def _fmp():
        data = fmp_get(f"/ratios/{fmp_ticker(symbol)}", params={"limit": 1})
        if data and isinstance(data, list) and data:
            return data[0].get("priceEarningsToGrowthRatio")
        return None
    def _finn():
        data = finn_get("/stock/metric", {"symbol": symbol, "metric": "all"})
        if data and "metric" in data:
            return data["metric"].get("pegRatio")
        return None
    return fetch_with_fallbacks("PEG", peg if peg and peg > 0 else None, ("FMP", _fmp), ("Finnhub", _finn))

def get_fcf(info, ticker_yf, symbol):
    fcf = info.get("freeCashflow")
    if fcf:
        return fcf
    try:
        cf = ticker_yf.cashflow
        if not cf.empty:
            if "Free Cash Flow" in cf.index:
                return cf.loc["Free Cash Flow"].iloc[0]
            elif "Operating Cash Flow" in cf.index:
                ocf = cf.loc["Operating Cash Flow"].iloc[0]
                capex = cf.loc["Capital Expenditure"].iloc[0] if "Capital Expenditure" in cf.index else 0
                return ocf - abs(capex)
    except Exception:
        pass
    try:
        data = fmp_get(f"/cash-flow-statement/{fmp_ticker(symbol)}", params={"limit": 1})
        if data and isinstance(data, list) and data:
            return data[0].get("freeCashFlow")
    except Exception:
        pass
    try:
        data = av_get_cached("CASH_FLOW", symbol)
        if data and "annualReports" in data and data["annualReports"]:
            r = data["annualReports"][0]
            ocf = float(r.get("operatingCashflow", 0) or 0)
            capex = abs(float(r.get("capitalExpenditures", 0) or 0))
            return ocf - capex
    except Exception:
        pass
    return 0

def get_total_debt(info, symbol):
    def _fmp():
        data = fmp_get(f"/balance-sheet-statement/{fmp_ticker(symbol)}", params={"limit": 1})
        if data and isinstance(data, list) and data:
            return data[0].get("totalDebt")
        return None
    def _av():
        data = av_get_cached("BALANCE_SHEET", symbol)
        if data and "annualReports" in data and data["annualReports"]:
            r = data["annualReports"][0]
            st = float(r.get("shortTermDebt", 0) or 0)
            lt = float(r.get("longTermDebt", 0) or 0)
            return st + lt
        return None
    return fetch_with_fallbacks("Total Debt", info.get("totalDebt"), ("FMP", _fmp), ("AlphaVantage", _av)) or 0

def get_ebitda(info, symbol):
    def _fmp():
        data = fmp_get(f"/income-statement/{fmp_ticker(symbol)}", params={"limit": 1})
        if data and isinstance(data, list) and data:
            return data[0].get("ebitda")
        return None
    def _finn():
        data = finn_get("/stock/metric", {"symbol": symbol, "metric": "all"})
        if data and "metric" in data:
            return data["metric"].get("ebitdaPerShare")
        return None
    return fetch_with_fallbacks("EBITDA", info.get("ebitda"), ("FMP", _fmp), ("Finnhub", _finn)) or 0

def get_net_income(info, ticker_yf, symbol):
    val = info.get("netIncomeToCommon") or info.get("netIncome")
    if val:
        return val
    try:
        return ticker_yf.financials.loc['Net Income'].iloc[0]
    except Exception:
        pass
    try:
        data = fmp_get(f"/income-statement/{fmp_ticker(symbol)}", params={"limit": 1})
        if data and isinstance(data, list) and data:
            return data[0].get("netIncome")
    except Exception:
        pass
    try:
        data = av_get_cached("INCOME_STATEMENT", symbol)
        if data and "annualReports" in data and data["annualReports"]:
            return float(data["annualReports"][0].get("netIncome", 0) or 0)
    except Exception:
        pass
    return 0

def get_profit_margin(info, symbol):
    def _fmp():
        data = fmp_get(f"/ratios/{fmp_ticker(symbol)}", params={"limit": 1})
        if data and isinstance(data, list) and data:
            return data[0].get("netProfitMargin")
        return None
    def _finn():
        data = finn_get("/stock/metric", {"symbol": symbol, "metric": "all"})
        if data and "metric" in data:
            return data["metric"].get("netProfitMarginTTM")
        return None
    return fetch_with_fallbacks("Profit Margin", info.get("profitMargins"), ("FMP", _fmp), ("Finnhub", _finn)) or 0

def get_beta(info, symbol):
    def _fmp():
        prof = fmp_profile(fmp_ticker(symbol))
        return prof.get("beta") if prof else None
    def _finn():
        data = finn_get("/stock/metric", {"symbol": symbol, "metric": "all"})
        if data and "metric" in data:
            return data["metric"].get("beta")
        return None
    return fetch_with_fallbacks("Beta", info.get("beta"), ("FMP", _fmp), ("Finnhub", _finn))

def get_sector(info, symbol):
    def _fmp():
        prof = fmp_profile(fmp_ticker(symbol))
        return prof.get("sector") if prof else None
    def _finn():
        data = finn_get("/stock/profile2", {"symbol": symbol})
        if data:
            return data.get("finnhubIndustry")
        return None
    return fetch_with_fallbacks("Sector", info.get("sector"), ("FMP", _fmp), ("Finnhub", _finn)) or "N/A"

def get_website(info, symbol):
    def _fmp():
        prof = fmp_profile(fmp_ticker(symbol))
        return prof.get("website") if prof else None
    def _finn():
        data = finn_get("/stock/profile2", {"symbol": symbol})
        if data:
            return data.get("weburl")
        return None
    return fetch_with_fallbacks("Website", info.get("website"), ("FMP", _fmp), ("Finnhub", _finn)) or "N/A"

def get_earnings_history(ticker_yf, symbol):
    try:
        eh = ticker_yf.earnings_history
        if eh is not None and not eh.empty and "surprisePercent" in eh.columns:
            clean = eh.head(12)["surprisePercent"].dropna().tolist()
            if clean:
                return clean
    except Exception:
        pass
    try:
        data = finn_get("/stock/earnings", {"symbol": symbol, "limit": 12})
        if data and isinstance(data, list):
            surprises = []
            for d in data:
                actual = d.get("actual")
                est    = d.get("estimate")
                if actual is not None and est and est != 0:
                    surprises.append((actual - est) / abs(est))
            if surprises:
                return surprises
    except Exception:
        pass
    try:
        data = fmp_get(f"/earnings-surprises/{fmp_ticker(symbol)}")
        if data and isinstance(data, list):
            surprises = []
            for d in data[:12]:
                actual = d.get("actualEarningResult")
                est    = d.get("estimatedEarning")
                if actual is not None and est and est != 0:
                    surprises.append((actual - est) / abs(est))
            if surprises:
                return surprises
    except Exception:
        pass
    return None

def get_revenue_estimate(ticker_yf, symbol):
    try:
        df = ticker_yf.revenue_estimate
        if not df.empty and df.shape[0] > 3:
            return df.iloc[3, 0]
    except Exception:
        pass
    try:
        data = finn_get("/stock/revenue-estimate", {"symbol": symbol, "freq": "annual"})
        if data and "revenueEstimate" in data:
            ests = data["revenueEstimate"]
            if len(ests) > 1:
                return ests[1].get("revenueAvg")
    except Exception:
        pass
    try:
        data = fmp_get(f"/analyst-estimates/{fmp_ticker(symbol)}", params={"limit": 2})
        if data and isinstance(data, list) and len(data) >= 2:
            return data[1].get("estimatedRevenueAvg")
    except Exception:
        pass
    return None

def get_eps_estimate(ticker_yf, symbol):
    try:
        df = ticker_yf.earnings_estimate
        if not df.empty and df.shape[0] > 3:
            return df.iloc[3, 0]
    except Exception:
        pass
    try:
        data = finn_get("/stock/eps-estimate", {"symbol": symbol, "freq": "annual"})
        if data and "epsEstimate" in data:
            ests = data["epsEstimate"]
            if len(ests) > 1:
                return ests[1].get("epsAvg")
    except Exception:
        pass
    try:
        data = fmp_get(f"/analyst-estimates/{fmp_ticker(symbol)}", params={"limit": 2})
        if data and isinstance(data, list) and len(data) >= 2:
            return data[1].get("estimatedEpsAvg")
    except Exception:
        pass
    return None

def get_total_cash(info, symbol):
    def _fmp():
        data = fmp_get(f"/balance-sheet-statement/{fmp_ticker(symbol)}", params={"limit": 1})
        if data and isinstance(data, list) and data:
            return data[0].get("cashAndCashEquivalents")
        return None
    def _av():
        data = av_get_cached("BALANCE_SHEET", symbol)
        if data and "annualReports" in data and data["annualReports"]:
            return float(data["annualReports"][0].get("cashAndCashEquivalentsAtCarryingValue", 0) or 0)
        return None
    return fetch_with_fallbacks("Total Cash", info.get("totalCash"), ("FMP", _fmp), ("AlphaVantage", _av)) or 0

def get_earning_estimate_avg(ticker_yf, symbol):
    try:
        df = ticker_yf.earnings_estimate
        if not df.empty:
            return df.iloc[0, 0]
    except Exception:
        pass
    try:
        data = finn_get("/stock/eps-estimate", {"symbol": symbol, "freq": "annual"})
        if data and "epsEstimate" in data and data["epsEstimate"]:
            return data["epsEstimate"][0].get("epsAvg")
    except Exception:
        pass
    return None

# ==============================================================
# 🎯 WINSORIZACIÓN DE OUTLIERS
# ==============================================================

def winsorize(values: List[float], lower_pct: float = 0.05, upper_pct: float = 0.95) -> List[float]:
    if not values or len(values) < 3:
        return values
    arr = np.array(values, dtype=float)
    lower = np.percentile(arr, lower_pct * 100)
    upper = np.percentile(arr, upper_pct * 100)
    return np.clip(arr, lower, upper).tolist()

# ==============================================================
# ⚙️ CONFIGURACIÓN POR SECTOR
# ==============================================================

SECTOR_CONFIG = {
    "Technology": {
        "weights": {"precio_objetivo": 0.12, "crecimiento": 0.28, "tendencia": 0.08, "consistencia": 0.15, "valoracion": 0.18, "soportes": 0.05, "williams": 0.04, "volumen": 0.10},
        "thresholds": {"fcf_yield_min": 0.025, "debt_ebitda_max": 2.0, "peg_target": 1.5, "forward_pe_max": 30},
    },
    "Financial Services": {
        "ignore_ebitda": True, "use_roe": True,
        "weights": {"precio_objetivo": 0.18, "crecimiento": 0.12, "tendencia": 0.10, "consistencia": 0.18, "valoracion": 0.22, "soportes": 0.08, "williams": 0.04, "volumen": 0.08},
        "thresholds": {"fcf_yield_min": 0.04, "debt_ebitda_max": 999, "roe_min": 0.10, "pb_max": 2.0},
    },
    "Healthcare": {
        "weights": {"precio_objetivo": 0.15, "crecimiento": 0.15, "tendencia": 0.08, "consistencia": 0.20, "valoracion": 0.18, "soportes": 0.08, "williams": 0.06, "volumen": 0.10},
        "thresholds": {"fcf_yield_min": 0.035, "debt_ebitda_max": 3.0, "pe_max": 25},
    },
    "Energy": {
        "weights": {"precio_objetivo": 0.10, "crecimiento": 0.10, "tendencia": 0.15, "consistencia": 0.10, "valoracion": 0.25, "soportes": 0.12, "williams": 0.08, "volumen": 0.10},
        "thresholds": {"fcf_yield_min": 0.08, "debt_ebitda_max": 2.5, "ev_ebitda_max": 8.0},
    },
    "Real Estate": {
        "use_ffo": True, "ignore_ebitda": True,
        "weights": {"precio_objetivo": 0.12, "crecimiento": 0.10, "tendencia": 0.12, "consistencia": 0.15, "valoracion": 0.25, "soportes": 0.10, "williams": 0.06, "volumen": 0.10},
        "thresholds": {"fcf_yield_min": 0.04, "debt_ebitda_max": 6.0, "p_ffo_max": 20, "dividend_yield_min": 0.03},
    },
    "Utilities": {
        "weights": {"precio_objetivo": 0.10, "crecimiento": 0.08, "tendencia": 0.12, "consistencia": 0.20, "valoracion": 0.22, "soportes": 0.10, "williams": 0.08, "volumen": 0.10},
        "thresholds": {"fcf_yield_min": 0.04, "debt_ebitda_max": 5.0, "dividend_yield_min": 0.03, "pe_max": 20},
    },
    "Consumer Cyclical": {
        "weights": {"precio_objetivo": 0.15, "crecimiento": 0.18, "tendencia": 0.12, "consistencia": 0.12, "valoracion": 0.18, "soportes": 0.10, "williams": 0.07, "volumen": 0.08},
        "thresholds": {"fcf_yield_min": 0.035, "debt_ebitda_max": 3.0, "pe_max": 25},
    },
    "Consumer Defensive": {
        "weights": {"precio_objetivo": 0.12, "crecimiento": 0.10, "tendencia": 0.08, "consistencia": 0.22, "valoracion": 0.20, "soportes": 0.10, "williams": 0.08, "volumen": 0.10},
        "thresholds": {"fcf_yield_min": 0.035, "debt_ebitda_max": 3.0, "dividend_yield_min": 0.025, "pe_max": 22},
    },
    "Industrials": {
        "weights": {"precio_objetivo": 0.15, "crecimiento": 0.15, "tendencia": 0.12, "consistencia": 0.15, "valoracion": 0.18, "soportes": 0.10, "williams": 0.07, "volumen": 0.08},
        "thresholds": {"fcf_yield_min": 0.04, "debt_ebitda_max": 3.5, "pe_max": 22},
    },
    "Communication Services": {
        "weights": {"precio_objetivo": 0.12, "crecimiento": 0.18, "tendencia": 0.10, "consistencia": 0.15, "valoracion": 0.20, "soportes": 0.08, "williams": 0.07, "volumen": 0.10},
        "thresholds": {"fcf_yield_min": 0.03, "debt_ebitda_max": 3.5, "pe_max": 20},
    },
    "Basic Materials": {
        "weights": {"precio_objetivo": 0.10, "crecimiento": 0.10, "tendencia": 0.15, "consistencia": 0.10, "valoracion": 0.25, "soportes": 0.12, "williams": 0.08, "volumen": 0.10},
        "thresholds": {"fcf_yield_min": 0.06, "debt_ebitda_max": 2.5, "ev_ebitda_max": 10},
    },
    "default": {
        "weights": {"precio_objetivo": 0.15, "crecimiento": 0.15, "tendencia": 0.10, "consistencia": 0.15, "valoracion": 0.20, "soportes": 0.10, "williams": 0.05, "volumen": 0.10},
        "thresholds": {"fcf_yield_min": 0.04, "debt_ebitda_max": 3.0},
    }
}

def get_sector_config(sector: str) -> dict:
    sector = sector.lower().strip()
    aliases = {
        "reit": "Real Estate", "real estate": "Real Estate", "reits": "Real Estate",
        "financial": "Financial Services", "financials": "Financial Services", "bank": "Financial Services", "banks": "Financial Services",
        "tech": "Technology", "information technology": "Technology",
        "health": "Healthcare", "health care": "Healthcare", "pharma": "Healthcare", "biotech": "Healthcare",
        "energy": "Energy", "oil": "Energy", "gas": "Energy",
        "utilities": "Utilities", "utility": "Utilities",
        "consumer cyclical": "Consumer Cyclical", "consumer discretionary": "Consumer Cyclical",
        "consumer defensive": "Consumer Defensive", "consumer staples": "Consumer Defensive",
        "industrials": "Industrials", "industrial": "Industrials",
        "communication": "Communication Services", "communication services": "Communication Services", "telecom": "Communication Services", "telecommunications": "Communication Services",
        "materials": "Basic Materials", "basic materials": "Basic Materials",
    }
    if sector in aliases:
        return SECTOR_CONFIG[aliases[sector]]
    for key, config in SECTOR_CONFIG.items():
        if key.lower() in sector or sector in key.lower():
            return config
    return SECTOR_CONFIG["default"]

# ==============================================================
# 📊 SISTEMA DE SCORING (0-100 por principio, 0-100 final)
# ==============================================================

def safe_float(val, default=0.0):
    try:
        if isinstance(val, str):
            val = val.replace("$", "").replace(",", "").replace("%", "").strip()
        return float(val)
    except (TypeError, ValueError):
        return default

def score_precio_objetivo(current_price: float, target_price: float, analyst_count: int) -> float:
    if not target_price or not current_price or current_price <= 0:
        return 50.0
    upside = (target_price - current_price) / current_price
    confidence = min(analyst_count / 10, 1.0)
    raw_score = 50 + (upside * 200)
    return max(0, min(100, raw_score * confidence + 50 * (1 - confidence)))

def score_crecimiento(rev_growth: float, gross_margin: float, operating_margin: float, momentum: str) -> float:
    score = 50.0
    rev_growth = safe_float(rev_growth)
    gross_margin = safe_float(gross_margin)
    operating_margin = safe_float(operating_margin)
    if rev_growth > 0.20: score += 25
    elif rev_growth > 0.10: score += 15
    elif rev_growth > 0.05: score += 5
    elif rev_growth < 0: score -= 15
    if gross_margin > 0.50: score += 10
    elif gross_margin > 0.30: score += 5
    if operating_margin > 0.20: score += 10
    elif operating_margin > 0.10: score += 5
    if momentum == "ACELERANDO": score += 5
    elif momentum == "DESACELERANDO": score -= 5
    return max(0, min(100, score))

def score_tendencia(sma_trend: str, vol_atr: float) -> float:
    trend_scores = {"ALCISTA FUERTE": 95, "ALCISTA": 80, "LATERAL": 50, "BAJISTA": 30, "BAJISTA FUERTE": 15}
    base = trend_scores.get(sma_trend, 50)
    vol_atr = safe_float(vol_atr)
    if vol_atr and vol_atr > 0.05:
        base -= 5
    return max(0, min(100, base))

def score_consistencia(beat_rate: float, weighted_cons: float, surprise_trend: str, worst_miss: float) -> float:
    score = 50.0
    beat_rate = safe_float(beat_rate)
    weighted_cons = safe_float(weighted_cons)
    worst_miss = safe_float(worst_miss)
    if beat_rate > 0.85: score += 25
    elif beat_rate > 0.70: score += 15
    elif beat_rate > 0.50: score += 5
    else: score -= 10
    if weighted_cons > 1.0: score += 15
    elif weighted_cons > 0.5: score += 5
    elif weighted_cons < 0: score -= 10
    if "MEJORANDO" in str(surprise_trend): score += 5
    elif "DETERIORANDO" in str(surprise_trend): score -= 5
    if worst_miss and worst_miss < -0.20: score -= 10
    return max(0, min(100, score))

def score_valoracion(peg: float, forward_pe: float, fcf_yield: float, interest_coverage: float,
                     debt_ebitda: float, fcf_growth: float, sector_cfg: dict) -> float:
    score = 50.0
    peg = safe_float(peg)
    forward_pe = safe_float(forward_pe)
    fcf_yield = safe_float(fcf_yield)
    interest_coverage = safe_float(interest_coverage)
    debt_ebitda = safe_float(debt_ebitda, 999)
    fcf_growth = safe_float(fcf_growth)
    if peg and peg < 1.0: score += 15
    elif peg and peg < 1.5: score += 5
    elif peg and peg > 2.5: score -= 10
    if forward_pe and forward_pe < 15: score += 10
    elif forward_pe and forward_pe > 30: score -= 10
    min_fcf = sector_cfg["thresholds"]["fcf_yield_min"]
    if fcf_yield and fcf_yield > min_fcf + 0.02: score += 15
    elif fcf_yield and fcf_yield > min_fcf: score += 5
    elif fcf_yield and fcf_yield < 0.01: score -= 10
    if interest_coverage and interest_coverage > 5: score += 5
    elif interest_coverage and interest_coverage < 1: score -= 10
    max_debt = sector_cfg["thresholds"]["debt_ebitda_max"]
    if debt_ebitda < max_debt * 0.5: score += 10
    elif debt_ebitda > max_debt: score -= 15
    if fcf_growth and fcf_growth > 0.20: score += 10
    elif fcf_growth and fcf_growth < 0: score -= 5
    return max(0, min(100, score))

def score_soportes(dist_support: float, dist_resistance: float, posicion_sr: str) -> float:
    score = 50.0
    dist_support = safe_float(dist_support)
    dist_resistance = safe_float(dist_resistance)
    if dist_support and dist_support < 0.02: score += 15
    elif dist_support and dist_support < 0.05: score += 5
    if dist_resistance and dist_resistance < 0.02: score -= 10
    if "Rompimiento al alza" in str(posicion_sr): score += 20
    elif "Rompimiento bajista" in str(posicion_sr): score -= 20
    elif "Más cerca de soporte" in str(posicion_sr): score += 5
    elif "Más cerca de resistencia" in str(posicion_sr): score -= 5
    return max(0, min(100, score))

def score_williams(wr_current: float, wr_daily: float) -> float:
    score = 50.0
    wr_current = safe_float(wr_current)
    wr_daily = safe_float(wr_daily)
    if wr_current < -80: score += 20
    elif wr_current < -60: score += 10
    elif wr_current > -20: score -= 15
    elif wr_current > -40: score -= 5
    if wr_daily < -80: score += 5
    elif wr_daily > -20: score -= 5
    return max(0, min(100, score))

def score_volumen(vol_ratio: float, obv_trend: str, price_vol_div: str, mfi_level: str) -> float:
    score = 50.0
    vol_ratio = safe_float(vol_ratio)
    if vol_ratio and vol_ratio > 2.0: score += 10
    elif vol_ratio and vol_ratio > 1.5: score += 5
    if str(obv_trend) == "ACUMULACIÓN": score += 10
    elif str(obv_trend) == "DISTRIBUCIÓN": score -= 10
    if "ALCISTA" in str(price_vol_div): score += 10
    elif "BAJISTA" in str(price_vol_div): score -= 10
    if str(mfi_level) == "SOBREVENDIDO": score += 5
    elif str(mfi_level) == "SOBRECOMPRADO": score -= 5
    return max(0, min(100, score))

def compute_final_score(scores: Dict[str, float], weights: Dict[str, float]) -> Tuple[float, str]:
    total_weight = sum(weights.values())
    if total_weight == 0:
        return 50.0, "C"
    weighted = sum(scores.get(k, 50) * weights.get(k, 0) for k in weights) / total_weight
    if weighted >= 90: grade = "A+"
    elif weighted >= 80: grade = "A"
    elif weighted >= 70: grade = "B+"
    elif weighted >= 60: grade = "B"
    elif weighted >= 50: grade = "C+"
    elif weighted >= 40: grade = "C"
    elif weighted >= 30: grade = "D"
    else: grade = "F"
    return round(weighted, 2), grade

# ==============================================================
# 📊 RANGOS EN GOOGLE SHEETS
# ==============================================================
ticker_range = f'A{START_ROW}:A{END_ROW}'

ranges = {
    'Target Mean Price': f'B{START_ROW}:B{END_ROW}',
    'Analyst Count': f'E{START_ROW}:E{END_ROW}',
    'Target Dispersion': f'F{START_ROW}:F{END_ROW}',
    'Earning Estimate AVG': f'G{START_ROW}:G{END_ROW}',
    'Rev_Growth_YoY': f'H{START_ROW}:H{END_ROW}',
    'Gross_Margin': f'I{START_ROW}:I{END_ROW}',
    'Operating_Margin': f'K{START_ROW}:K{END_ROW}',
    'Growth_Momentum': f'L{START_ROW}:L{END_ROW}',
    'SMA_200': f'M{START_ROW}:M{END_ROW}',
    'SMA_Trend': f'N{START_ROW}:N{END_ROW}',
    'Volatility_ATR': f'O{START_ROW}:O{END_ROW}',
    'Weighted Consistency': f'Q{START_ROW}:Q{END_ROW}',
    'Beat Rate': f'R{START_ROW}:R{END_ROW}',
    'Recent 4Q Avg': f'S{START_ROW}:S{END_ROW}',
    'Revenue Surprise 4Q': f'T{START_ROW}:T{END_ROW}',
    'Surprise Trend': f'U{START_ROW}:U{END_ROW}',
    'Earnings Window': f'V{START_ROW}:V{END_ROW}',
    'Worst Miss': f'W{START_ROW}:W{END_ROW}',
    'PEG': f'Y{START_ROW}:Y{END_ROW}',
    'Interest Coverage': f'Z{START_ROW}:Z{END_ROW}',
    'Forward PE': f'AA{START_ROW}:AA{END_ROW}',
    'FCF Yield': f'AC{START_ROW}:AC{END_ROW}',
    'FCF Growth YoY': f'AD{START_ROW}:AD{END_ROW}',
    'FCF/NI Ratio': f'AE{START_ROW}:AE{END_ROW}',
    'FCF Margin': f'AF{START_ROW}:AF{END_ROW}',
    'Total Cash': f'AI{START_ROW}:AI{END_ROW}',
    'Operating Expense TTM': f'AJ{START_ROW}:AJ{END_ROW}',
    'Total Debt (mrq)': f'AN{START_ROW}:AN{END_ROW}',
    'Interest Expense': f'AO{START_ROW}:AO{END_ROW}',
    'Debt/Equity': f'AQ{START_ROW}:AQ{END_ROW}',
    'Debt/EBITDA': f'AR{START_ROW}:AR{END_ROW}',
    'Years to Pay Debt': f'AS{START_ROW}:AS{END_ROW}',
    'Revenue Estimate AVG': f'AW{START_ROW}:AW{END_ROW}',
    'Profit Margin': f'AX{START_ROW}:AX{END_ROW}',
    'P/E Promedio 6 meses': f'AY{START_ROW}:AY{END_ROW}',
    'Future EPS': f'BF{START_ROW}:BF{END_ROW}',
    'Expected PE': f'BG{START_ROW}:BG{END_ROW}',
    'Expected Return (EPS)': f'BH{START_ROW}:BH{END_ROW}',
    'Expected Return (Rev)': f'BI{START_ROW}:BI{END_ROW}',
    'Expected Return (Analyst)': f'BJ{START_ROW}:BJ{END_ROW}',
    'Expected Return (Consensus)': f'BK{START_ROW}:BK{END_ROW}',
    'Min 200d': f'BL{START_ROW}:BL{END_ROW}',
    'Max 200d': f'BT{START_ROW}:BT{END_ROW}',
    'Soportes': f'BM{START_ROW}:BM{END_ROW}',
    'Resistencias': f'BN{START_ROW}:BN{END_ROW}',
    'Posición S/R': f'BU{START_ROW}:BU{END_ROW}',
    'Soporte Cercano': f'BQ{START_ROW}:BQ{END_ROW}',
    'Resistencia Cercana': f'BP{START_ROW}:BP{END_ROW}',
    'Dist a Soporte %': f'BS{START_ROW}:BS{END_ROW}',
    'Dist a Resistencia %': f'BR{START_ROW}:BR{END_ROW}',
    'Williams %R (Current)': f'BV{START_ROW}:BV{END_ROW}',
    'Williams %R (1w ago)': f'BW{START_ROW}:BW{END_ROW}',
    'Williams %R (2w ago)': f'BX{START_ROW}:BX{END_ROW}',
    'Williams %R (Daily)': f'BY{START_ROW}:BY{END_ROW}',
    'Volume Ratio': f'CB{START_ROW}:CB{END_ROW}',
    'Volume Level': f'CC{START_ROW}:CC{END_ROW}',
    'OBV Trend': f'CD{START_ROW}:CD{END_ROW}',
    'Price-Volume Div': f'CE{START_ROW}:CE{END_ROW}',
    'MFI': f'CF{START_ROW}:CF{END_ROW}',
    'MFI Level': f'CG{START_ROW}:CG{END_ROW}',
    'Sector': f'DA{START_ROW}:DA{END_ROW}',
    'Days Public': f'DC{START_ROW}:DC{END_ROW}',
    'Beta': f'DD{START_ROW}:DD{END_ROW}',
    'Official URL': f'DE{START_ROW}:DE{END_ROW}',
    'Score_Precio': f'DG{START_ROW}:DG{END_ROW}',
    'Score_Crecimiento': f'DH{START_ROW}:DH{END_ROW}',
    'Score_Tendencia': f'DI{START_ROW}:DI{END_ROW}',
    'Score_Consistencia': f'DJ{START_ROW}:DJ{END_ROW}',
    'Score_Valoracion': f'DK{START_ROW}:DK{END_ROW}',
    'Score_Soportes': f'DL{START_ROW}:DL{END_ROW}',
    'Score_Williams': f'DM{START_ROW}:DM{END_ROW}',
    'Score_Volumen': f'DN{START_ROW}:DN{END_ROW}',
    'Score_Final': f'DO{START_ROW}:DO{END_ROW}',
    'Grade': f'DP{START_ROW}:DP{END_ROW}',
    'Alertas': f'DQ{START_ROW}:DQ{END_ROW}',
}

defaults = {
    'Target Mean Price': "N/A", 'Analyst Count': 0, 'Target Dispersion': "N/A",
    'Earning Estimate AVG': "N/A", 'Rev_Growth_YoY': 0, 'Gross_Margin': 0,
    'Operating_Margin': 0, 'Growth_Momentum': "N/A", 'SMA_200': "N/A",
    'SMA_Trend': "N/A", 'Volatility_ATR': 0, 'Weighted Consistency': "N/A",
    'Beat Rate': 0, 'Recent 4Q Avg': 0, 'Revenue Surprise 4Q': "N/A",
    'Surprise Trend': "N/A", 'Earnings Window': "N/A", 'Worst Miss': 0,
    'PEG': "N/A", 'Interest Coverage': 0, 'Forward PE': "N/A",
    'FCF Yield': 0, 'FCF Growth YoY': 0, 'FCF/NI Ratio': 0, 'FCF Margin': 0,
    'Total Cash': 0, 'Operating Expense TTM': 0, 'Total Debt (mrq)': "$0",
    'Interest Expense': "N/A", 'Debt/Equity': "N/A", 'Debt/EBITDA': "N/A",
    'Years to Pay Debt': "N/A", 'Revenue Estimate AVG': "N/A", 'Profit Margin': 0,
    'P/E Promedio 6 meses': "N/A", 'Future EPS': "N/A", 'Expected PE': "N/A",
    'Expected Return (EPS)': "N/A", 'Expected Return (Rev)': "N/A",
    'Expected Return (Analyst)': "N/A", 'Expected Return (Consensus)': "N/A",
    'Sector': "N/A", 'Days Public': "N/A", 'Beta': "N/A", 'Official URL': "N/A",
    'Min 200d': 0, 'Max 200d': 0, 'Soportes': "N/A", 'Resistencias': "N/A",
    'Posición S/R': "N/A", 'Soporte Cercano': 0, 'Resistencia Cercana': 0,
    'Dist a Soporte %': 0, 'Dist a Resistencia %': 0,
    'Williams %R (Current)': 0, 'Williams %R (1w ago)': 0, 'Williams %R (2w ago)': 0, 'Williams %R (Daily)': 0,
    'Volume Ratio': 1, 'Volume Level': "N/A", 'OBV Trend': "N/A", 'Price-Volume Div': "N/A",
    'MFI': 50, 'MFI Level': "N/A",
    'Score_Precio': 50, 'Score_Crecimiento': 50, 'Score_Tendencia': 50,
    'Score_Consistencia': 50, 'Score_Valoracion': 50, 'Score_Soportes': 50,
    'Score_Williams': 50, 'Score_Volumen': 50, 'Score_Final': 50, 'Grade': "C", 'Alertas': "",
}

# ==============================================================
# 🛠️ UTILIDADES
# ==============================================================

def validate_ticker(symbol: str) -> bool:
    if not symbol or not isinstance(symbol, str):
        return False
    symbol = symbol.strip().upper()
    if not symbol.replace(".", "").replace("-", "").isalnum() or len(symbol) > 10:
        return False
    return True

def sanitize_for_sheets(value):
    """Convierte NaN/inf a None para evitar errores JSON en Google Sheets."""
    if value is None:
        return None
    if isinstance(value, float):
        if np.isnan(value) or np.isinf(value):
            return None
    if isinstance(value, (np.integer, np.floating)):
        if np.isnan(value) or np.isinf(value):
            return None
        return float(value)
    return value

def calc_levels(prices, vols, cluster_size, n=4):
    if cluster_size <= 0:
        cluster_size = max(np.mean(prices) * 0.005, 0.01) if len(prices) > 0 else 1.0
    rounded = np.round(prices / cluster_size) * cluster_size
    data_d = {}
    for price_r, vol in zip(rounded, vols):
        if price_r not in data_d:
            data_d[price_r] = {'count': 0, 'volume': 0}
        data_d[price_r]['count']  += 1
        data_d[price_r]['volume'] += vol
    if not data_d:
        return []
    mx_c = max(d['count']  for d in data_d.values())
    mx_v = max(d['volume'] for d in data_d.values())
    scores = {
        p: (d['count'] / mx_c * 0.6 + d['volume'] / mx_v * 0.4)
        for p, d in data_d.items()
    }
    top_n = sorted(scores.items(), key=lambda x: x[1], reverse=True)[:n]
    return sorted([round(p, 2) for p, _ in top_n])

# ==============================================================
# 🧠 CACHE ÚNICO DE DATOS POR TICKER (CLAVE DE VELOCIDAD)
# ==============================================================

def get_cached_ticker_data(symbol: str) -> Tuple[Any, dict, pd.DataFrame]:
    """Descarga UN SOLO history() por ticker y lo cachea."""
    with _ticker_data_lock:
        if symbol in _ticker_data_cache:
            return _ticker_data_cache[symbol]

    norm_sym = normalize_ticker(symbol)
    try:
        # yfinance >= 0.2.x usa curl_cffi internamente; NO admite
        # requests.Session. Para limitar el tiempo de ticker.info (que
        # podía bloquear 30s) usamos un future con timeout de 15s.
        ticker = yf.Ticker(norm_sym)
        with ThreadPoolExecutor(max_workers=1) as _info_ex:
            _fut = _info_ex.submit(lambda: ticker.info or {})
            try:
                info = _fut.result(timeout=15)
            except FuturesTimeoutError:
                logger.warning(f"{symbol}: ticker.info timeout (15s), continuando sin datos de info")
                info = {}
        hist = ticker.history(period="2y", interval="1d", auto_adjust=True, timeout=10)
    except Exception as e:
        logger.error(f"{symbol}: yfinance crítico: {e}")
        ticker = None
        info = {}
        hist = pd.DataFrame()

    result = (ticker, info, hist)
    with _ticker_data_lock:
        _ticker_data_cache[symbol] = result
    return result

# ==============================================================
# 🧠 PROCESAMIENTO DE UN TICKER (FUNCIÓN ATÓMICA)
# ==============================================================

def process_ticker(symbol: str) -> Tuple[str, Dict[str, Any], List[str]]:
    logger.info(f"{'='*50}  {symbol}  {'='*50}")
    results = {key: defaults[key] for key in ranges.keys()}
    alerts = []

    ticker, info, hist = get_cached_ticker_data(symbol)
    current_price = info.get("currentPrice", 0)
    market_cap_yf = info.get("marketCap", 0)

    # Validación cruzada rápida (solo si FMP vivo)
    if market_cap_yf and current_price and not fmp_cb.is_permanent():
        try:
            fmp_prof = fmp_profile(fmp_ticker(symbol))
            if fmp_prof:
                mc_fmp = fmp_prof.get("mktCap", 0)
                price_fmp = fmp_prof.get("price", 0)
                if mc_fmp and market_cap_yf > 0:
                    ratio = abs(mc_fmp - market_cap_yf) / market_cap_yf
                    if ratio > 0.5:
                        alerts.append(f"DISCREPANCIA MC: YF={market_cap_yf:,} vs FMP={mc_fmp:,}")
                if price_fmp and current_price > 0:
                    ratio_p = abs(price_fmp - current_price) / current_price
                    if ratio_p > 0.10:
                        alerts.append(f"DISCREPANCIA PRECIO: YF=${current_price} vs FMP=${price_fmp}")
        except Exception:
            pass

    sector = get_sector(info, symbol)
    results['Sector'] = sector
    sector_cfg = get_sector_config(sector)

    # ═══════════════════════════════════════════════════════
    # PRINCIPIO 1: PRECIO OBJETIVO
    # ═══════════════════════════════════════════════════════
    try:
        tp = get_target_price(info, ticker, symbol)
        results['Target Mean Price'] = tp if tp is not None else defaults['Target Mean Price']
        ac = get_analyst_count(info, symbol)
        results['Analyst Count'] = ac if ac is not None else defaults['Analyst Count']

        disp_val = None
        if ticker:
            try:
                pts = ticker.analyst_price_targets
                if pts and 'current' in pts:
                    th, tl, tm = pts.get('high',0), pts.get('low',0), pts.get('mean',0)
                    if tm > 0 and th > 0 and tl > 0:
                        disp_val = (th - tl) / tm
            except Exception:
                pass
        if disp_val is None and not fmp_cb.is_permanent():
            try:
                data = fmp_get(f"/price-target/{fmp_ticker(symbol)}")
                if data and isinstance(data, list) and data:
                    targets = [d.get("priceTarget", 0) for d in data if d.get("priceTarget")]
                    if len(targets) > 1:
                        mean_t = sum(targets) / len(targets)
                        if mean_t > 0:
                            disp_val = (max(targets) - min(targets)) / mean_t
            except Exception:
                pass
        results['Target Dispersion'] = f"{disp_val:.2%}" if disp_val is not None else defaults['Target Dispersion']
    except Exception as e:
        logger.error(f"{symbol} error P1: {e}")

    # ═══════════════════════════════════════════════════════
    # PRINCIPIO 2: CRECIMIENTO
    # ═══════════════════════════════════════════════════════
    try:
        rev_growth = get_rev_growth(info, symbol)
        gross_margin, operating_margin = get_margins(info, symbol)
        growth_momentum = "ESTABLE"
        try:
            if ticker:
                financials = ticker.quarterly_financials
                if "Total Revenue" in financials.index and financials.shape[1] >= 2:
                    recent_rev = financials.loc["Total Revenue"].iloc[0]
                    older_rev  = financials.loc["Total Revenue"].iloc[1]
                    qoq_growth = (recent_rev - older_rev) / abs(older_rev) if older_rev != 0 else 0
                    if rev_growth and qoq_growth:
                        growth_momentum = "ACELERANDO" if (qoq_growth * 4) > rev_growth else "DESACELERANDO"
        except Exception:
            pass

        eea = get_earning_estimate_avg(ticker, symbol) if ticker else None
        results['Earning Estimate AVG'] = f"${eea:.2f}" if eea is not None else defaults['Earning Estimate AVG']
        results['Rev_Growth_YoY'] = rev_growth
        results['Gross_Margin'] = gross_margin
        results['Operating_Margin'] = operating_margin
        results['Growth_Momentum'] = growth_momentum
    except Exception as e:
        logger.error(f"{symbol} error P2: {e}")

    # ═══════════════════════════════════════════════════════
    # PRINCIPIO 3: TENDENCIA (SMA / ATR) - REUSA hist CACHED
    # ═══════════════════════════════════════════════════════
    try:
        if hist is not None and not hist.empty and len(hist) >= 250:
            sma_200_current = hist['Close'].tail(200).mean()
            sma_200_50d_ago = hist['Close'].iloc[-250:-50].mean()
            sma_slope = (sma_200_current - sma_200_50d_ago) / sma_200_50d_ago if sma_200_50d_ago > 0 else 0

            if sma_slope > 0.05:    sma_trend = "ALCISTA FUERTE"
            elif sma_slope > 0.02:  sma_trend = "ALCISTA"
            elif sma_slope > -0.02: sma_trend = "LATERAL"
            elif sma_slope > -0.05: sma_trend = "BAJISTA"
            else:                   sma_trend = "BAJISTA FUERTE"

            hist['High_Low'] = hist['High'] - hist['Low']
            atr_14 = hist['High_Low'].tail(14).mean()
            cp = hist['Close'].iloc[-1]
            vol_pct = atr_14 / cp if cp > 0 else 0

            results['SMA_200'] = f"${sma_200_current:.2f}"
            results['SMA_Trend'] = sma_trend
            results['Volatility_ATR'] = vol_pct
        else:
            logger.info(f"{symbol}: históricos insuficientes SMA/ATR")
    except Exception as e:
        logger.error(f"{symbol} error P3: {e}")

    # ═══════════════════════════════════════════════════════
    # PRINCIPIO 4: CONSISTENCIA DE EARNINGS
    # ═══════════════════════════════════════════════════════
    try:
        surprise_data = get_earnings_history(ticker, symbol) if ticker else None
        if surprise_data and len(surprise_data) > 0:
            surprise_data = winsorize(surprise_data, lower_pct=0.05, upper_pct=0.95)
            total_quarters = len(surprise_data)
            total_beats = sum(1 for s in surprise_data if s > 0)
            win_rate = total_beats / total_quarters
            num_recent = min(4, len(surprise_data))
            recent_avg = sum(surprise_data[:num_recent]) / num_recent
            worst_miss = min(surprise_data)

            weighted_score = 0
            for s in surprise_data:
                if   s >  0.10: weighted_score += 1.5
                elif s >  0.05: weighted_score += 1.2
                elif s >  0:    weighted_score += 1.0
                elif s > -0.05: weighted_score += 0.3
                elif s > -0.10: weighted_score += 0.0
                else:           weighted_score -= 0.5
            weighted_consistency = weighted_score / total_quarters

            if len(surprise_data) >= 8:
                r4 = sum(surprise_data[:4]) / 4
                o4 = sum(surprise_data[4:8]) / 4
                if   r4 > o4 + 0.02: surprise_trend = "MEJORANDO"
                elif r4 > o4:         surprise_trend = "MEJORANDO LEVE"
                elif r4 < o4 - 0.02: surprise_trend = "DETERIORANDO"
                elif r4 < o4:         surprise_trend = "DETERIORANDO LEVE"
                else:                 surprise_trend = "ESTABLE"
            elif len(surprise_data) >= 4:
                r2 = sum(surprise_data[:2]) / 2
                o2 = sum(surprise_data[2:4]) / 2
                if   r2 > o2 + 0.03: surprise_trend = "MEJORANDO**"
                elif r2 > o2:         surprise_trend = "MEJORANDO LEVE**"
                elif r2 < o2 - 0.03: surprise_trend = "DETERIORANDO**"
                elif r2 < o2:         surprise_trend = "DETERIORANDO LEVE**"
                else:                 surprise_trend = "ESTABLE**"
            else:
                surprise_trend = "DATOS INSUFICIENTES"

            racha_miss = False
            if len(surprise_data) >= 2:
                racha_miss = (surprise_data[0] < 0 and surprise_data[1] < 0)
            if racha_miss and "MEJORANDO" in surprise_trend:
                surprise_trend = "ÚLTIMO DETERIORO"
            elif racha_miss and surprise_trend == "ESTABLE":
                surprise_trend = "DETERIORANDO LEVE"

            results['Beat Rate'] = win_rate
            results['Recent 4Q Avg'] = recent_avg
            results['Worst Miss'] = worst_miss
            results['Weighted Consistency'] = round(weighted_consistency, 4)
            results['Surprise Trend'] = surprise_trend
        else:
            logger.info(f"{symbol}: sin earnings history")
    except Exception as e:
        logger.error(f"{symbol} error P4 (earnings): {e}")

    try:
        rev_surp_val = None
        try:
            data = finn_get("/stock/earnings", {"symbol": symbol, "limit": 8})
            if data and isinstance(data, list):
                rsurps = []
                for d in data[:4]:
                    rev_actual = d.get("revenueActual")
                    rev_est = d.get("revenueEstimate")
                    if rev_actual is not None and rev_est is not None and rev_est != 0:
                        rsurps.append((rev_actual - rev_est) / abs(rev_est))
                if rsurps:
                    rev_surp_val = sum(rsurps) / len(rsurps)
        except Exception:
            pass
        if rev_surp_val is None and not fmp_cb.is_permanent():
            try:
                actual_data = fmp_get(f"/income-statement/{fmp_ticker(symbol)}", params={"limit": 4, "period": "quarter"})
                est_data = fmp_get(f"/analyst-estimates/{fmp_ticker(symbol)}", params={"limit": 8, "period": "quarter"})
                if actual_data and isinstance(actual_data, list) and est_data and isinstance(est_data, list):
                    rsurps = []
                    for actual_q in actual_data[:4]:
                        q_date = actual_q.get("date", "")[:7]
                        rev_real = actual_q.get("revenue", 0)
                        est_match = next((e for e in est_data if e.get("date", "")[:7] == q_date), None)
                        if est_match:
                            rev_est = est_match.get("estimatedRevenueAvg", 0)
                            if rev_real and rev_est and rev_est != 0:
                                rsurps.append((rev_real - rev_est) / abs(rev_est))
                    if rsurps:
                        rev_surp_val = sum(rsurps) / len(rsurps)
            except Exception:
                pass
        results['Revenue Surprise 4Q'] = rev_surp_val if rev_surp_val is not None else defaults['Revenue Surprise 4Q']
    except Exception as e:
        logger.error(f"{symbol} RevSurp general: {e}")

    try:
        ew_val = "N/A"
        timestamp = info.get("earningsTimestampStart")
        if not timestamp:
            try:
                data = finn_get("/calendar/earnings", {"symbol": symbol})
                if data and "earningsCalendar" in data and data["earningsCalendar"]:
                    date_str = data["earningsCalendar"][0].get("date")
                    if date_str:
                        timestamp = datetime.datetime.strptime(date_str, "%Y-%m-%d").timestamp()
            except Exception:
                pass
        if isinstance(timestamp, (int, float)):
            dt = datetime.datetime.fromtimestamp(timestamp)
            days = (dt - datetime.datetime.today()).days
            if days < 0:       ew_val = "PASADO"
            elif days <= 7:    ew_val = "ESTA SEMANA"
            elif days <= 21:   ew_val = "ESTE MES"
            elif days <= 45:   ew_val = "PRÓXIMO MES"
            else:              ew_val = "LEJANO"
            if days <= 7 and days >= 0:
                alerts.append(f"EARNINGS EN {days} DÍAS")
        results['Earnings Window'] = ew_val
    except Exception as e:
        logger.error(f"{symbol} Earnings Window: {e}")

    # ═══════════════════════════════════════════════════════
    # PRINCIPIO 5: VALORACIÓN FINANCIERA
    # ═══════════════════════════════════════════════════════
    try:
        peg_val = get_peg(info, symbol)
        forward_pe = get_forward_pe(info, symbol)
        results['PEG'] = peg_val if peg_val is not None else defaults['PEG']
        results['Forward PE'] = forward_pe if forward_pe is not None else defaults['Forward PE']

        try:
            cov_val = 0
            ebit = None
            interest = None
            if ticker:
                try:
                    f = ticker.financials
                    for label in ['Ebit', 'Operating Income', 'Operating Profit']:
                        if label in f.index:
                            ebit = f.loc[label].iloc[0]; break
                    il = [i for i in f.index if 'Interest Expense' in i]
                    if il:
                        interest = abs(f.loc[il[0]].iloc[0])
                except Exception:
                    pass
            if ebit is None or interest is None:
                data = fmp_get(f"/ratios/{fmp_ticker(symbol)}", params={"limit": 1})
                if data and isinstance(data, list) and data:
                    cov_val = data[0].get("interestCoverage", 0) or 0
            else:
                if ebit is not None and interest and interest > 0:
                    cov_val = round(ebit / interest, 2)
                elif ebit is not None:
                    cov_val = 100
            results['Interest Coverage'] = cov_val
        except Exception as e:
            logger.error(f"{symbol} Interest Coverage: {e}")
    except Exception as e:
        logger.error(f"{symbol} error P5 (parte 1): {e}")

    try:
        market_cap = info.get("marketCap", 0)
        revenue = info.get("totalRevenue", 0)
        if not revenue and not fmp_cb.is_permanent():
            try:
                data = fmp_get(f"/income-statement/{fmp_ticker(symbol)}", params={"limit": 1})
                if data and isinstance(data, list) and data:
                    revenue = data[0].get("revenue", 0)
            except Exception:
                pass

        net_income = get_net_income(info, ticker, symbol) if ticker else 0
        fcf = get_fcf(info, ticker, symbol) if ticker else 0

        if fcf and market_cap and market_cap > 0:
            results['FCF Yield'] = fcf / market_cap
        else:
            results['FCF Yield'] = defaults['FCF Yield']

        try:
            fcf_growth = 0
            if ticker:
                try:
                    cf = ticker.cashflow
                    if not cf.empty and "Free Cash Flow" in cf.index and cf.shape[1] >= 2:
                        fcf_c = cf.loc["Free Cash Flow"].iloc[0]
                        fcf_p = cf.loc["Free Cash Flow"].iloc[1]
                        if fcf_p and fcf_p != 0:
                            fcf_growth = (fcf_c - fcf_p) / abs(fcf_p)
                except Exception:
                    pass
            if fcf_growth == 0 and not fmp_cb.is_permanent():
                data = fmp_get(f"/cash-flow-statement/{fmp_ticker(symbol)}", params={"limit": 2})
                if data and isinstance(data, list) and len(data) >= 2:
                    fc1, fc2 = data[0].get("freeCashFlow",0), data[1].get("freeCashFlow",0)
                    if fc2 and fc2 != 0:
                        fcf_growth = (fc1 - fc2) / abs(fc2)
            results['FCF Growth YoY'] = fcf_growth
        except Exception as e:
            logger.error(f"{symbol} FCF Growth: {e}")

        results['FCF/NI Ratio'] = fcf / net_income if net_income and net_income != 0 and fcf else defaults['FCF/NI Ratio']
        results['FCF Margin'] = fcf / revenue if fcf and revenue and revenue > 0 else defaults['FCF Margin']
    except Exception as e:
        logger.error(f"{symbol} error P5 (FCF): {e}")

    try:
        cash_val = get_total_cash(info, symbol)
        results['Total Cash'] = cash_val if cash_val else defaults['Total Cash']
        try:
            op_exp = None
            if ticker:
                try:
                    qis = ticker.quarterly_income_stmt
                    if "Operating Expense" in qis.index and qis.shape[1] >= 4:
                        op_exp = abs(qis.loc["Operating Expense"].iloc[0:4].sum())
                except Exception:
                    pass
            if op_exp is None and not fmp_cb.is_permanent():
                data = fmp_get(f"/income-statement/{fmp_ticker(symbol)}", params={"limit": 1})
                if data and isinstance(data, list) and data:
                    op_exp = abs(data[0].get("operatingExpenses", 0) or 0)
            results['Operating Expense TTM'] = op_exp if op_exp is not None else defaults['Operating Expense TTM']
        except Exception as e:
            logger.error(f"{symbol} Operating Expense: {e}")
    except Exception as e:
        logger.error(f"{symbol} error P5 (Cash): {e}")

    try:
        total_debt = get_total_debt(info, symbol)
        ebitda = get_ebitda(info, symbol)
        fcf_debt = info.get("freeCashflow") or fcf or 0

        total_equity = info.get("totalStockholderEquity") or info.get("totalEquity")
        if not total_equity:
            try:
                if ticker:
                    bs = ticker.quarterly_balance_sheet
                    for k in ['Stockholders Equity', 'Total Equity Gross Minority Interest', 'Common Stock Equity']:
                        if k in bs.index:
                            total_equity = bs.loc[k].iloc[0]; break
            except Exception:
                pass
        if not total_equity and not fmp_cb.is_permanent():
            try:
                data = fmp_get(f"/balance-sheet-statement/{fmp_ticker(symbol)}", params={"limit": 1})
                if data and isinstance(data, list) and data:
                    total_equity = data[0].get("totalStockholdersEquity")
            except Exception:
                pass

        results['Total Debt (mrq)'] = f"${int(total_debt):,}" if isinstance(total_debt,(int,float)) and total_debt else defaults['Total Debt (mrq)']

        ie_val = defaults['Interest Expense']
        try:
            if ticker:
                try:
                    is_stmt = ticker.income_stmt
                    if "Interest Expense" in is_stmt.index:
                        ie = is_stmt.loc["Interest Expense"].iloc[0]
                        if ie is not None and isinstance(ie, (int, float)) and ie != 0:
                            ie_val = f"${int(abs(ie)):,}"
                except Exception:
                    pass
            if ie_val == defaults['Interest Expense'] and not fmp_cb.is_permanent():
                data = fmp_get(f"/income-statement/{fmp_ticker(symbol)}", params={"limit": 1})
                if data and isinstance(data, list) and data:
                    ie = data[0].get("interestExpense", 0)
                    if ie:
                        ie_val = f"${int(abs(ie)):,}"
        except Exception:
            pass
        results['Interest Expense'] = ie_val

        results['Debt/Equity'] = f"{total_debt/total_equity:.2f}" if total_equity and total_equity > 0 and total_debt else defaults['Debt/Equity']
        results['Debt/EBITDA'] = f"{total_debt/ebitda:.2f}" if ebitda and ebitda > 0 and total_debt else defaults['Debt/EBITDA']
        results['Years to Pay Debt'] = f"{total_debt/fcf_debt:.1f}" if fcf_debt and fcf_debt > 0 and total_debt else defaults['Years to Pay Debt']
    except Exception as e:
        logger.error(f"{symbol} error P5 (Deuda): {e}")

    try:
        trailing_eps = info.get("trailingEps", 0)
        current_pe = info.get("trailingPE", 0)
        forward_pe_val = info.get("forwardPE", 0)
        profit_margin = get_profit_margin(info, symbol)
        revenue_next_year = get_revenue_estimate(ticker, symbol) if ticker else None
        results['Revenue Estimate AVG'] = f"{int(revenue_next_year):,.0f}" if revenue_next_year else defaults['Revenue Estimate AVG']
        results['Profit Margin'] = f"{profit_margin:.2%}" if profit_margin else defaults['Profit Margin']

        try:
            avg_pe_val = defaults['P/E Promedio 6 meses']
            if trailing_eps and trailing_eps > 0 and hist is not None and not hist.empty:
                # Reutiliza hist cacheado (2y diario) en lugar de descargar interval="1mo"
                cutoff = pd.Timestamp.today(tz=hist.index.tz) - pd.Timedelta(days=180)
                hist_pe = hist[hist.index >= cutoff].resample("ME")["Close"].mean().to_frame()
                if not hist_pe.empty:
                    hist_pe["P/E"] = np.divide(hist_pe["Close"], trailing_eps)
                    avg_pe_val = round(hist_pe["P/E"].mean(), 2)
            results['P/E Promedio 6 meses'] = avg_pe_val
        except Exception:
            pass

        eps_next_year = get_eps_estimate(ticker, symbol) if ticker else None
        results['Future EPS'] = f"${eps_next_year:.2f}" if eps_next_year is not None else defaults['Future EPS']

        if current_pe and forward_pe_val:
            if current_pe > 30:      expected_pe = current_pe * 0.85
            elif current_pe < 10:    expected_pe = current_pe * 1.10
            else:                    expected_pe = current_pe * 0.6 + forward_pe_val * 0.4
        elif forward_pe_val:         expected_pe = forward_pe_val
        elif current_pe:             expected_pe = current_pe
        else:                        expected_pe = 15
        results['Expected PE'] = f"{expected_pe:.1f}"

        has_a = has_b = has_c = False
        method_a = method_b = method_c = 0

        if eps_next_year and current_price > 0:
            method_a = (eps_next_year * expected_pe / current_price) - 1
            has_a = True
            results['Expected Return (EPS)'] = f"{method_a:.2%}"
        else:
            results['Expected Return (EPS)'] = defaults['Expected Return (EPS)']

        if revenue_next_year and profit_margin and market_cap and market_cap > 0:
            method_b = (revenue_next_year * profit_margin * expected_pe / market_cap) - 1
            has_b = True
            results['Expected Return (Rev)'] = f"{method_b:.2%}"
        else:
            results['Expected Return (Rev)'] = defaults['Expected Return (Rev)']

        analyst_target = get_target_price(info, ticker, symbol)
        if analyst_target and current_price > 0:
            method_c = (analyst_target - current_price) / current_price
            has_c = True
            results['Expected Return (Analyst)'] = f"{method_c:.2%}"
        else:
            results['Expected Return (Analyst)'] = defaults['Expected Return (Analyst)']

        rets, wgts = [], []
        if has_a: rets.append(method_a); wgts.append(0.5)
        if has_b: rets.append(method_b); wgts.append(0.3)
        if has_c: rets.append(method_c); wgts.append(0.2)
        if rets:
            consensus = sum(r*w for r,w in zip(rets,wgts)) / sum(wgts)
            results['Expected Return (Consensus)'] = f"{consensus:.2%}"
        else:
            results['Expected Return (Consensus)'] = defaults['Expected Return (Consensus)']
    except Exception as e:
        logger.error(f"{symbol} error P5 (Expected): {e}")

    # ═══════════════════════════════════════════════════════
    # PRINCIPIO 6: SOPORTES Y RESISTENCIAS - REUSA hist CACHED
    # ═══════════════════════════════════════════════════════
    try:
        if hist is not None and not hist.empty and len(hist) >= 50:
            cp = hist['Close'].iloc[-1]
            min_200d = hist['Low'].min()
            max_200d = hist['High'].max()
            results['Min 200d'] = round(min_200d, 4)
            results['Max 200d'] = round(max_200d, 4)

            if len(hist) >= 14:
                hist['HL'] = hist['High'] - hist['Low']
                atr_p6 = hist['HL'].tail(14).mean()
                cluster_size = max(atr_p6 * 0.5, cp * 0.003)
            else:
                cluster_size = cp * 0.005

            lows = hist['Low'].values
            highs = hist['High'].values
            volumes = hist['Volume'].values
            support_levels = calc_levels(lows, volumes, cluster_size)
            resistance_levels = calc_levels(highs, volumes, cluster_size)

            s_below = [s for s in support_levels if s < cp]
            r_above = [r for r in resistance_levels if r > cp]
            nearest_support = max(s_below) if s_below else min_200d
            nearest_resistance = min(r_above) if r_above else max_200d
            dist_to_support = (cp - nearest_support) / cp if nearest_support else 1
            dist_to_resistance = (nearest_resistance - cp) / cp if nearest_resistance else 1

            if   cp > max_200d:                pos = "Rompimiento al alza"
            elif cp < min_200d:                pos = "Rompimiento bajista"
            elif dist_to_resistance < 0.02:    pos = "Cerca de resistencia"
            elif dist_to_support < 0.02:     pos = "Cerca del soporte"
            elif dist_to_support < dist_to_resistance: pos = "Más cerca de soporte"
            elif dist_to_resistance < dist_to_support: pos = "Más cerca de resistencia"
            else:                              pos = "En rango"

            results['Soportes'] = ", ".join([f"{s:.2f}" for s in support_levels])
            results['Resistencias'] = ", ".join([f"{r:.2f}" for r in resistance_levels])
            results['Posición S/R'] = pos
            results['Soporte Cercano'] = round(nearest_support, 4)
            results['Resistencia Cercana'] = round(nearest_resistance, 4)
            results['Dist a Soporte %'] = round(dist_to_support, 6)
            results['Dist a Resistencia %'] = round(dist_to_resistance, 6)
        else:
            logger.info(f"{symbol}: históricos insuficientes S/R")
    except Exception as e:
        logger.error(f"{symbol} error P6 (S/R): {e}")

    # ═══════════════════════════════════════════════════════
    # PRINCIPIO 7: WILLIAMS %R - REUSA hist CACHED
    # ═══════════════════════════════════════════════════════
    try:
        if hist is not None and not hist.empty and len(hist) >= 70:
            cp = hist['Close'].iloc[-1]
            hist_copy = hist.copy()
            hist_copy['Date'] = pd.to_datetime(hist_copy.index)
            hist_copy['Week'] = hist_copy['Date'].dt.isocalendar().week
            hist_copy['Year'] = hist_copy['Date'].dt.isocalendar().year
            hist_copy['YearWeek'] = hist_copy['Year'].astype(str) + '-W' + hist_copy['Week'].astype(str).str.zfill(2)
            unique_weeks = hist_copy['YearWeek'].unique()

            if len(unique_weeks) >= 14:
                lb_data = hist_copy[hist_copy['YearWeek'].isin(unique_weeks[-14:])]
                hh = lb_data['High'].max()
                ll = lb_data['Low'].min()
                wr_curr = ((hh - cp) / (hh - ll)) * -100 if hh != ll else 0

                def wr_for_week(wk_idx):
                    wk_data = hist_copy[hist_copy['YearWeek'] == unique_weeks[wk_idx]]
                    if not wk_data.empty:
                        p = wk_data['Close'].iloc[-1]
                        return ((hh - p) / (hh - ll)) * -100 if hh != ll else 0
                    return 0

                wr_1w = wr_for_week(-2) if len(unique_weeks) >= 15 else 0
                wr_2w = wr_for_week(-3) if len(unique_weeks) >= 16 else 0
                last14d = hist_copy.tail(14)
                hh14 = last14d['High'].max(); ll14 = last14d['Low'].min()
                wr_daily = ((hh14 - cp) / (hh14 - ll14)) * -100 if hh14 != ll14 else 0

                results['Williams %R (Current)'] = wr_curr
                results['Williams %R (1w ago)'] = wr_1w
                results['Williams %R (2w ago)'] = wr_2w
                results['Williams %R (Daily)'] = wr_daily
            else:
                logger.info(f"{symbol}: semanas insuficientes Williams")
        else:
            logger.info(f"{symbol}: históricos insuficientes Williams")
    except Exception as e:
        logger.error(f"{symbol} error P7 (Williams): {e}")

    # ═══════════════════════════════════════════════════════
    # PRINCIPIO 8: VOLUMEN Y MOMENTUM - REUSA hist CACHED
    # ═══════════════════════════════════════════════════════
    try:
        if hist is not None and not hist.empty and len(hist) >= 50:
            avg_vol_20 = hist['Volume'].tail(20).mean()
            avg_vol_50 = hist['Volume'].tail(50).mean()
            cur_vol = hist['Volume'].iloc[-1]
            vol_ratio = cur_vol / avg_vol_50 if avg_vol_50 > 0 else 1

            if   vol_ratio > 2.0: vol_level = "MUY ALTO"
            elif vol_ratio > 1.5: vol_level = "ALTO"
            elif vol_ratio > 0.8: vol_level = "NORMAL"
            elif vol_ratio > 0.5: vol_level = "BAJO"
            else:                 vol_level = "MUY BAJO"

            results['Volume Ratio'] = round(vol_ratio, 4)
            results['Volume Level'] = vol_level

            obv = [0]
            for i in range(1, len(hist)):
                if   hist['Close'].iloc[i] > hist['Close'].iloc[i-1]:
                    obv.append(obv[-1] + hist['Volume'].iloc[i])
                elif hist['Close'].iloc[i] < hist['Close'].iloc[i-1]:
                    obv.append(obv[-1] - hist['Volume'].iloc[i])
                else:
                    obv.append(obv[-1])
            hist_copy = hist.copy()
            hist_copy['OBV'] = obv
            obv_sma30 = hist_copy['OBV'].tail(30).mean()
            obv_cur = hist_copy['OBV'].iloc[-1]

            if   obv_cur > obv_sma30 * 1.05: obv_trend = "ACUMULACIÓN"
            elif obv_cur < obv_sma30 * 0.95: obv_trend = "DISTRIBUCIÓN"
            else:                             obv_trend = "NEUTRAL"
            results['OBV Trend'] = obv_trend

            pc20 = (hist['Close'].iloc[-1] - hist['Close'].iloc[-20]) / hist['Close'].iloc[-20] if len(hist) >= 20 else 0
            vc20 = (avg_vol_20 - avg_vol_50) / avg_vol_50 if avg_vol_50 > 0 else 0
            if   pc20 >  0.05 and vc20 >  0.20: div = "ALCISTA FUERTE"
            elif pc20 >  0.05 and vc20 < -0.20: div = "ALCISTA DÉBIL"
            elif pc20 < -0.05 and vc20 >  0.20: div = "BAJISTA FUERTE"
            elif pc20 < -0.05 and vc20 < -0.20: div = "BAJISTA DÉBIL"
            else:                                div = "NEUTRAL"
            results['Price-Volume Div'] = div

            tp_series = (hist['High'] + hist['Low'] + hist['Close']) / 3
            mf_series = tp_series * hist['Volume']
            pos_flow, neg_flow = [], []
            for i in range(1, len(hist)):
                if tp_series.iloc[i] > tp_series.iloc[i-1]:
                    pos_flow.append(mf_series.iloc[i]); neg_flow.append(0)
                else:
                    pos_flow.append(0); neg_flow.append(mf_series.iloc[i])
            if len(pos_flow) >= 14:
                pmf = sum(pos_flow[-14:])
                nmf = sum(neg_flow[-14:])
                if nmf == 0 and pmf == 0: mfi_val = 50
                elif nmf == 0: mfi_val = 100
                else: mfi_val = 100 - (100 / (1 + pmf / nmf))
                if   mfi_val > 80: mfi_level = "SOBRECOMPRADO"
                elif mfi_val > 60: mfi_level = "COMPRADO"
                elif mfi_val > 40: mfi_level = "NEUTRAL"
                elif mfi_val > 20: mfi_level = "VENDIDO"
                else:              mfi_level = "SOBREVENDIDO"
                results['MFI'] = round(mfi_val, 2)
                results['MFI Level'] = mfi_level
            else:
                results['MFI'] = 50
                results['MFI Level'] = "N/A"
        else:
            logger.info(f"{symbol}: históricos insuficientes Volumen")
    except Exception as e:
        logger.error(f"{symbol} error P8 (Volumen): {e}")

    # ═══════════════════════════════════════════════════════
    # DATOS ADICIONALES
    # ═══════════════════════════════════════════════════════
    try:
        dp_val = defaults['Days Public']
        if hist is not None and not hist.empty:
            try:
                first_date = hist.index.min().date()
                # hist es 2y, así que estimamos desde ahí; si no hay datos previos
                # el valor será conservador pero evitamos una descarga de 30+ años
                dp_val = int((datetime.date.today() - first_date).days)
            except Exception:
                pass
        results['Days Public'] = dp_val
    except Exception as e:
        logger.error(f"{symbol} Days Public: {e}")

    beta_val = get_beta(info, symbol)
    results['Beta'] = beta_val if beta_val is not None else defaults['Beta']
    results['Official URL'] = get_website(info, symbol)

    # ═══════════════════════════════════════════════════════
    # SCORING AUTOMÁTICO - ROBUSTO A STRINGS
    # ═══════════════════════════════════════════════════════
    try:
        scores = {}
        scores['precio_objetivo'] = score_precio_objetivo(
            safe_float(current_price), safe_float(results['Target Mean Price']), safe_float(results['Analyst Count'])
        )
        scores['crecimiento'] = score_crecimiento(
            results['Rev_Growth_YoY'], results['Gross_Margin'],
            results['Operating_Margin'], results['Growth_Momentum']
        )
        scores['tendencia'] = score_tendencia(
            results['SMA_Trend'], results['Volatility_ATR']
        )
        scores['consistencia'] = score_consistencia(
            results['Beat Rate'], results['Weighted Consistency'],
            results['Surprise Trend'], results['Worst Miss']
        )
        de_val = results['Debt/EBITDA']
        de_num = safe_float(de_val, 999)
        scores['valoracion'] = score_valoracion(
            results['PEG'], results['Forward PE'],
            results['FCF Yield'], results['Interest Coverage'],
            de_num, results['FCF Growth YoY'], sector_cfg
        )
        scores['soportes'] = score_soportes(
            results['Dist a Soporte %'], results['Dist a Resistencia %'],
            results['Posición S/R']
        )
        scores['williams'] = score_williams(
            results['Williams %R (Current)'], results['Williams %R (Daily)']
        )
        scores['volumen'] = score_volumen(
            results['Volume Ratio'], results['OBV Trend'],
            results['Price-Volume Div'], results['MFI Level']
        )

        final_score, grade = compute_final_score(scores, sector_cfg['weights'])

        results['Score_Precio'] = scores['precio_objetivo']
        results['Score_Crecimiento'] = scores['crecimiento']
        results['Score_Tendencia'] = scores['tendencia']
        results['Score_Consistencia'] = scores['consistencia']
        results['Score_Valoracion'] = scores['valoracion']
        results['Score_Soportes'] = scores['soportes']
        results['Score_Williams'] = scores['williams']
        results['Score_Volumen'] = scores['volumen']
        results['Score_Final'] = final_score
        results['Grade'] = grade

        if scores['tendencia'] < 30 and scores['soportes'] < 30:
            alerts.append("SEÑAL BAJISTA FUERTE")
        if scores['williams'] > 80 and scores['volumen'] > 70:
            alerts.append("SEÑAL ALCISTA FUERTE")
        if scores['consistencia'] < 30:
            alerts.append("CONSISTENCIA DÉBIL")
        if grade in ("A+", "A") and scores['volumen'] < 40:
            alerts.append("ALTO POTENCIAL, VOLUMEN BAJO")
    except Exception as e:
        logger.error(f"{symbol} error en Scoring: {e}")

    if fmp_cb.is_permanent():
        alerts.append("FMP INACTIVO: datos de perfil/ratios incompletos")
    results['Alertas'] = " | ".join(alerts) if alerts else "OK"
    logger.info(f"✅ {symbol} procesado. Score: {results.get('Score_Final', 'N/A')} | Grade: {results.get('Grade', 'N/A')}")
    return symbol, results, alerts


# ==============================================================
# 🚀 ORQUESTACIÓN PRINCIPAL (PARALELO CON 8 WORKERS)
# ==============================================================

def write_to_sheets(worksheet, all_results: Dict[str, List]):
    logger.info("--- Escribiendo datos en Google Sheets ---")
    # Sanitizar todo antes de enviar
    clean_results = {}
    for metric, data_list in all_results.items():
        clean_results[metric] = [[sanitize_for_sheets(v) for v in row] for row in data_list]

    try:
        batch_data = [{'range': ranges[m], 'values': clean_results[m]} for m in clean_results if m in ranges]
        worksheet.batch_update(batch_data, value_input_option='USER_ENTERED')
        logger.info(f"✅ {len(batch_data)} rangos actualizados exitosamente.")
    except Exception as e:
        logger.error(f"❌ Error en batch: {e}. Intentando individual...")
        for metric, data_list in clean_results.items():
            if metric not in ranges:
                continue
            try:
                worksheet.update(range_name=ranges[metric], values=data_list)
                logger.info(f"  ✓ '{metric}' ({len(data_list)} filas)")
                time.sleep(1.2)
            except Exception as e2:
                logger.error(f"  ✗ '{metric}': {e2}")

def test_fmp_connectivity():
    if fmp_cb.is_permanent():
        return False
    try:
        test_url = "https://financialmodelingprep.com/stable/company/profile/AAPL"
        r = _session.get(test_url, params={"apikey": FMP_KEY}, timeout=8)
        if r.status_code == 200:
            logger.info("✅ FMP conectividad OK")
            return True
        elif r.status_code == 403:
            logger.error(f"🔴 FMP 403 Forbidden - Clave inválida")
            return False
        elif r.status_code == 401:
            logger.error("🔴 FMP 401 Unauthorized")
            return False
        else:
            logger.warning(f"⚠️ FMP status {r.status_code}")
            return False
    except Exception as e:
        logger.warning(f"⚠️ FMP conectividad falló: {e}")
        return False

def main():
    # Limpiar caches de sesión en cada ejecución
    with _av_cache_lock:
        _av_cache.clear()
    with _ticker_data_lock:
        _ticker_data_cache.clear()
    with _fmp_profile_lock:
        _fmp_profile_cache.clear()

    fmp_available = test_fmp_connectivity()
    if not fmp_available:
        logger.warning("⚠️ FMP no disponible. Modo DEGRADADO (yfinance + Finnhub only).")

    try:
        sh = gc.open(SPREADSHEET_NAME)
        worksheet = sh.worksheet(WORKSHEET_NAME)

        tickers_list = worksheet.get(ticker_range)
        raw_symbols = [item[0] for item in tickers_list if item and item[0]]
        symbols = [s.strip().upper() for s in raw_symbols if validate_ticker(s)]
        invalid = [s for s in raw_symbols if not validate_ticker(s)]
        if invalid:
            logger.warning(f"Tickers inválidos omitidos: {invalid}")

        if not symbols:
            logger.error("No se encontraron tickers válidos.")
            return

        logger.info(f"🔍 Tickers válidos: {symbols}")
        all_results = {key: [] for key in ranges.keys()}

        ticker_results = {}
        with ThreadPoolExecutor(max_workers=4) as executor:
            future_to_idx = {
                executor.submit(process_ticker, sym): idx
                for idx, sym in enumerate(symbols)
            }
            for future in as_completed(future_to_idx):
                idx = future_to_idx[future]
                try:
                    symbol, results, alerts = future.result()
                    ticker_results[idx] = (symbol, results, alerts)
                    logger.info(f"✅ {symbol} completado (posición {idx})")
                except Exception as e:
                    logger.error(f"{symbols[idx]}: Error fatal: {e}")
                    default_results = {key: defaults[key] for key in ranges.keys()}
                    default_results['Alertas'] = f"ERROR: {str(e)[:50]}"
                    ticker_results[idx] = (symbols[idx], default_results, [])

        for idx in range(len(symbols)):
            if idx in ticker_results:
                _, results, _ = ticker_results[idx]
            else:
                logger.error(f"Ticker posición {idx} ({symbols[idx]}) sin resultados")
                results = {key: defaults[key] for key in ranges.keys()}
                results['Alertas'] = "ERROR: Sin resultados"

            for key in ranges.keys():
                all_results[key].append([sanitize_for_sheets(results[key])])

        write_to_sheets(worksheet, all_results)

        try:
            score_sheet = sh.worksheet(SCORESHEET_NAME)
        except gspread.exceptions.WorksheetNotFound:
            score_sheet = sh.add_worksheet(title=SCORESHEET_NAME, rows=50, cols=20)
            headers = ["Ticker", "Score_Final", "Grade", "P1_Precio", "P2_Crecimiento",
                       "P3_Tendencia", "P4_Consistencia", "P5_Valoracion",
                       "P6_Soportes", "P7_Williams", "P8_Volumen", "Alertas"]
            score_sheet.update('A1:L1', [headers])

        score_data = []
        for i, sym in enumerate(symbols):
            score_data.append([
                sym,
                sanitize_for_sheets(all_results['Score_Final'][i][0]),
                sanitize_for_sheets(all_results['Grade'][i][0]),
                sanitize_for_sheets(all_results['Score_Precio'][i][0]),
                sanitize_for_sheets(all_results['Score_Crecimiento'][i][0]),
                sanitize_for_sheets(all_results['Score_Tendencia'][i][0]),
                sanitize_for_sheets(all_results['Score_Consistencia'][i][0]),
                sanitize_for_sheets(all_results['Score_Valoracion'][i][0]),
                sanitize_for_sheets(all_results['Score_Soportes'][i][0]),
                sanitize_for_sheets(all_results['Score_Williams'][i][0]),
                sanitize_for_sheets(all_results['Score_Volumen'][i][0]),
                sanitize_for_sheets(all_results['Alertas'][i][0]),
            ])
        score_sheet.update(range_name=f'A2:L{1+len(score_data)}', values=score_data)
        logger.info(f"✅ Pestaña '{SCORESHEET_NAME}' actualizada con {len(score_data)} tickers.")
        logger.info("🎉 ¡Proceso completado!")

    except gspread.exceptions.SpreadsheetNotFound:
        logger.error(f'❌ Hoja "{SPREADSHEET_NAME}" no encontrada.')
    except gspread.exceptions.WorksheetNotFound:
        logger.error(f'❌ Pestaña "{WORKSHEET_NAME}" no encontrada.')
    except Exception as e:
        import traceback
        logger.error(f"❌ Error inesperado: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    main()
