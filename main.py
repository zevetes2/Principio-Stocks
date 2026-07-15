################################-----------------------PORTAFOLIO E.T.H. v7.0-----------------------##########################################
# VERSIÓN INSTITUCIONAL: Arquitectura de 3 Capas Independientes
# Business Quality (50%) | Valuation (30%) | Timing (20%)
# Fuentes: yfinance → Alpha Vantage → FMP → finnhub
# Instalar: pip install yfinance gspread google-auth requests pandas numpy finnhub-python scipy

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


try:
    from main_V6 import (
        get_rev_growth, get_margins, get_profit_margin, get_fcf,
        get_net_income, get_total_debt, get_ebitda, get_peg,
        get_forward_pe, get_target_price, get_analyst_count,
        get_sector, get_revenue_estimate, get_eps_estimate,
        test_fmp_connectivity, get_total_cash
    )
except Exception as _import_err:
    logging.Logger.error(f"❌ Fallo importando de main_V6: {_import_err}")
    raise

# ==============================================================
# 📋 CONFIGURACIÓN DE LOGGING
# ==============================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger("PortafolioETH_v7")

# ==============================================================
# ⚙️  CONFIGURACIÓN DE APIs Y ENTORNO
# ==============================================================
load_dotenv()
IS_GITHUB_ACTIONS = os.getenv('GITHUB_ACTIONS') == 'true'
JSON_KEY_FILE = "principios.json"
SERVICE_ACCOUNT_PATH = "firebase-service-key.json"

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
WORKSHEET_NAME   = "8 PRINCIPIOS"      # Hoja principal (se mantiene nombre por compatibilidad)
SCORESHEET_NAME  = "SCORES_v7"         # Nueva pestaña para v7
START_ROW = 7
END_ROW   = 189

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
                logger.error(f"🔴 {self.name}: 403 Forbidden - API key inválida o expirada.")
                return
            if status_code == 429:
                logger.warning(f"⏱️  {self.name}: 429 Rate Limit")
                return
            self.failures += 1
            if self.failures >= self.max_failures:
                self._open = True
                logger.warning(f"🔴 CIRCUIT BREAKER ABIERTO para {self.name}")

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

av_cb   = CircuitBreaker(max_failures=2, name="AlphaVantage")
fmp_cb  = CircuitBreaker(max_failures=2, name="FMP")
finn_cb = CircuitBreaker(max_failures=2, name="Finnhub")
# Desactivar AlphaVantage permanentemente
av_cb._permanent = True
finn_cb._permanent = True 
logger.info("⚠️ AlphaVantage desactivado — modo FMP + Finnhub + yfinance")

# ==============================================================
# 🌐 CLIENTE HTTP PERSISTENTE
# ==============================================================
_session = requests.Session()
_session.headers.update({
    "User-Agent": "PortafolioETH/7.0 (Python; contact: user@example.com)",
    "Accept": "application/json",
    "Connection": "keep-alive",
})

AV_BASE   = "https://www.alphavantage.co/query"
FMP_BASE  = "https://financialmodelingprep.com/api/v3"
FMP_V4    = "https://financialmodelingprep.com/api/v4"
FINN_BASE = "https://finnhub.io/api/v1"

USE_FMP = os.getenv("USE_FMP", "true").lower() in ("true", "1", "yes")
if not USE_FMP:
    logger.warning("⚠️ FMP desactivado manualmente")
    fmp_cb._permanent = True

# ==============================================================
# 📦 CACHES
# ==============================================================
_av_cache: Dict[str, Any] = {}
_av_cache_lock = threading.Lock()
_fmp_profile_cache: Dict[str, Optional[dict]] = {}
_fmp_profile_lock = threading.Lock()
_ticker_data_cache: Dict[str, Tuple] = {}
_ticker_data_lock = threading.Lock()
_historical_pe_cache: Dict[str, List[float]] = {}
_historical_pe_lock = threading.Lock()

def av_get_cached(function: str, symbol: str, extra_params=None):
    key = f"{function}:{symbol}"
    with _av_cache_lock:
        if key in _av_cache:
            return _av_cache[key]
    result = av_get(function, symbol, extra_params)
    with _av_cache_lock:
        _av_cache[key] = result
    return result

def _safe_request(url: str, params: dict, timeout: int = 8, cb: CircuitBreaker = None,
                  limiter: RateLimiter = None) -> Optional[dict]:
    if cb and cb.is_open():
        return None
    if limiter:
        limiter.wait_if_needed()
    try:
        r = _session.get(url, params=params, timeout=timeout)
        status = r.status_code
        if status == 403:
            if cb: cb.record_failure(403)
            return None
        if status == 429:
            if cb: cb.record_failure(429)
            return None
        r.raise_for_status()
        data = r.json()
        if cb: cb.record_success()
        return data
    except requests.exceptions.HTTPError as e:
        status = e.response.status_code if e.response else 0
        if cb: cb.record_failure(status)
        return None
    except Exception:
        if cb: cb.record_failure(0)
        return None

def av_get(function, symbol, extra_params=None):
    if av_cb.is_open(): return None
    params = {"function": function, "symbol": symbol, "apikey": ALPHA_VANTAGE_KEY}
    if extra_params: params.update(extra_params)
    data = _safe_request(AV_BASE, params, cb=av_cb, limiter=av_limiter)
    if data and ("Note" in data or "Information" in data):
        av_cb.record_failure(); return None
    return data

def fmp_get(endpoint, version="v3", params=None):
    if fmp_cb.is_permanent() or fmp_cb.is_open(): return None
    p = params or {}; p["apikey"] = FMP_KEY
    url = f"{FMP_BASE if version=='v3' else FMP_V4}{endpoint}"
    return _safe_request(url, p, cb=fmp_cb, limiter=fmp_limiter)

def finn_get(endpoint, params=None):
    if finn_cb.is_open(): return None
    p = params or {}; p["token"] = FINNHUB_KEY
    return _safe_request(f"{FINN_BASE}{endpoint}", p, cb=finn_cb, limiter=finn_limiter)

def fmp_profile(symbol: str) -> Optional[dict]:
    with _fmp_profile_lock:
        if symbol in _fmp_profile_cache: return _fmp_profile_cache[symbol]
    if fmp_cb.is_permanent(): return None
    data = fmp_get(f"/company/profile/{symbol}")
    if data is None: data = fmp_get(f"/profile/{symbol}")
    result = data[0] if data and isinstance(data, list) and data else None
    with _fmp_profile_lock:
        _fmp_profile_cache[symbol] = result
    return result

# ==============================================================
# 🔧 NORMALIZACIÓN DE TICKERS
# ==============================================================
TICKER_REPLACEMENTS = {"BRK.B": "BRK-B", "BRK.A": "BRK-A", "BF.B": "BF-B", "BF.A": "BF-A"}

def normalize_ticker(symbol: str) -> str:
    s = symbol.strip().upper()
    return TICKER_REPLACEMENTS.get(s, s)

def fmp_ticker(symbol: str) -> str:
    s = symbol.strip().upper()
    return TICKER_REPLACEMENTS.get(s, s).replace(".", "-")

# ==============================================================
# 🛠️ UTILIDADES
# ==============================================================
def safe_float(val, default=0.0):
    try:
        if isinstance(val, str):
            val = val.replace("$", "").replace(",", "").replace("%", "").strip()
        f = float(val)
        return f if not (np.isnan(f) or np.isinf(f)) else default
    except (TypeError, ValueError):
        return default

def sanitize_for_sheets(value):
    if value is None: return None
    if isinstance(value, float):
        if np.isnan(value) or np.isinf(value): return None
    if isinstance(value, (np.integer, np.floating)):
        if np.isnan(value) or np.isinf(value): return None
        return float(value)
    return value

def validate_ticker(symbol: str) -> bool:
    if not symbol or not isinstance(symbol, str): return False
    s = symbol.strip().upper()
    return s.replace(".", "").replace("-", "").isalnum() and len(s) <= 10

def winsorize(values: List[float], lower_pct: float = 0.05, upper_pct: float = 0.95) -> List[float]:
    if not values or len(values) < 3: return values
    arr = np.array(values, dtype=float)
    lower = np.percentile(arr, lower_pct * 100)
    upper = np.percentile(arr, upper_pct * 100)
    return np.clip(arr, lower, upper).tolist()

# ==============================================================
# 📊 NUEVAS FUNCIONES DE MÉTRICAS v7.0
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
        except Exception: pass
    logger.warning(f"{metric_name}: no disponible en ninguna fuente")
    return None

def get_cached_ticker_data(symbol: str) -> Tuple[Any, dict, pd.DataFrame]:
    with _ticker_data_lock:
        if symbol in _ticker_data_cache: return _ticker_data_cache[symbol]
    norm_sym = normalize_ticker(symbol)
    try:
        ticker = yf.Ticker(norm_sym)
        with ThreadPoolExecutor(max_workers=1) as _info_ex:
            _fut = _info_ex.submit(lambda: ticker.info or {})
            try: info = _fut.result(timeout=15)
            except FuturesTimeoutError:
                logger.warning(f"{symbol}: ticker.info timeout")
                info = {}
        hist = ticker.history(period="5y", interval="1d", auto_adjust=True, timeout=10)
    except Exception as e:
        logger.error(f"{symbol}: yfinance crítico: {e}")
        ticker, info, hist = None, {}, pd.DataFrame()
    result = (ticker, info, hist)
    with _ticker_data_lock:
        _ticker_data_cache[symbol] = result
    return result

# ═══════════════════════════════════════════════════════════════
# CAPA 1 — BUSINESS QUALITY: NUEVAS MÉTRICAS
# ═══════════════════════════════════════════════════════════════

def get_3y_cagr(values: List[float]) -> Optional[float]:
    """Calcula CAGR de 3 años a partir de lista de valores anuales (más reciente primero)"""
    if not values or len(values) < 4: return None
    # values[0] = más reciente, values[3] = hace 3 años
    try:
        end_val = safe_float(values[0])
        start_val = safe_float(values[3])
        if start_val and start_val > 0 and end_val and end_val > 0:
            return (end_val / start_val) ** (1/3) - 1
    except Exception: pass
    return None

def get_revenue_3y_cagr(ticker, symbol) -> Optional[float]:
    """Revenue CAGR 3Y desde yfinance financials o FMP"""
    try:
        if ticker:
            fin = ticker.financials
            if fin is not None and not fin.empty and "Total Revenue" in fin.index:
                revs = fin.loc["Total Revenue"].dropna().tolist()
                return get_3y_cagr(revs)
    except Exception: pass
    try:
        data = fmp_get(f"/income-statement/{fmp_ticker(symbol)}", params={"limit": 5})
        if data and isinstance(data, list) and len(data) >= 4:
            revs = [d.get("revenue", 0) for d in data]
            return get_3y_cagr(revs)
    except Exception: pass
    try:
        data = av_get_cached("INCOME_STATEMENT", symbol)
        if data and "annualReports" in data and len(data["annualReports"]) >= 4:
            revs = [float(r.get("totalRevenue", 0) or 0) for r in data["annualReports"]]
            return get_3y_cagr(revs)
    except Exception: pass
    return None

def get_eps_3y_cagr(ticker, symbol) -> Optional[float]:
    """EPS CAGR 3Y"""
    try:
        if ticker:
            fin = ticker.financials
            if fin is not None and not fin.empty:
                for label in ['Diluted EPS', 'Basic EPS', 'Eps']:
                    if label in fin.index:
                        eps_vals = fin.loc[label].dropna().tolist()
                        return get_3y_cagr(eps_vals)
    except Exception: pass
    try:
        data = fmp_get(f"/income-statement/{fmp_ticker(symbol)}", params={"limit": 5})
        if data and isinstance(data, list) and len(data) >= 4:
            eps_vals = [d.get("epsdiluted", d.get("eps", 0)) for d in data]
            return get_3y_cagr(eps_vals)
    except Exception: pass
    return None

def get_margin_stability(ticker, symbol, margin_type="operating", info=None) -> Optional[float]:
    """
    Calcula estabilidad de márgenes como 1 / (1 + std_5y)
    Valor más alto = márgenes más estables
    """
    # 1. Intentar con yfinance (datos más completos)
    try:
        if ticker:
            fin = ticker.financials
            if fin is not None and not fin.empty:
                label_map = {
                    "gross": "Gross Profit",
                    "operating": "Operating Income",
                    "profit": "Net Income"
                }
                rev_label = "Total Revenue"
                margin_label = label_map.get(margin_type)

                if margin_label and margin_label in fin.index and rev_label in fin.index:
                    profits = fin.loc[margin_label].dropna().tail(5)
                    revs = fin.loc[rev_label].dropna().tail(5)
                    common_idx = profits.index.intersection(revs.index)
                    if len(common_idx) >= 3:
                        margins = (profits[common_idx] / revs[common_idx]).dropna()
                        if len(margins) >= 3:
                            std_margin = margins.std()
                            mean_margin = margins.mean()
                            if mean_margin and mean_margin != 0:
                                cv = std_margin / abs(mean_margin)
                                return max(0, 1 - cv)
    except Exception as e:
        logger.debug(f"{symbol}: Margin stability yfinance error: {e}")

    # 2. Fallback a FMP ratios
    try:
        data = fmp_get(f"/ratios/{fmp_ticker(symbol)}", params={"limit": 5})
        if data and isinstance(data, list) and len(data) >= 3:
            key_map = {
                "gross": "grossProfitMargin",
                "operating": "operatingProfitMargin",
                "profit": "netProfitMargin"
            }
            key = key_map.get(margin_type)
            if key:
                margins = []
                for d in data[:5]:
                    val = d.get(key)
                    if val is not None:
                        margins.append(val / 100 if val > 1 else val)
                if len(margins) >= 3:
                    std_m = np.std(margins)
                    mean_m = np.mean(margins)
                    if mean_m and mean_m != 0:
                        return max(0, 1 - (std_m / abs(mean_m)))
    except Exception as e:
        logger.debug(f"{symbol}: Margin stability FMP error: {e}")

        # 3. FALLBACK: Usar info
    try:
        if info:
            info_key_map = {
                "gross": ["grossMargins", "grossMargin"],
                "operating": ["operatingMargins", "operatingMargin"],
                "profit": ["profitMargins", "profitMargin"]
            }
            keys = info_key_map.get(margin_type, [])
            margin_val = None
            for key in keys:
                if key in info and info[key] is not None:
                    margin_val = safe_float(info[key])
                    break
            
            if margin_val and margin_val > 0:
                if margin_val > 0.30:
                    return 0.6
                elif margin_val > 0.15:
                    return 0.5
                else:
                    return 0.4
    except Exception as e:
        logger.debug(f"{symbol}: Margin stability info error: {e}")

    return None

def get_fcf_consistency(ticker, symbol, info=None) -> Optional[float]:
    """
    Ratio de quarters/periods con FCF positivo en últimos 5 años
    1.0 = FCF positivo siempre, 0.0 = FCF negativo siempre
    """
    # 1. Intentar con yfinance
    try:
        if ticker:
            cf = ticker.cashflow
            if cf is not None and not cf.empty and "Free Cash Flow" in cf.index:
                fcf_vals = cf.loc["Free Cash Flow"].dropna()
                positive = sum(1 for v in fcf_vals if safe_float(v, 0) > 0)
                total = len(fcf_vals)
                if total > 0:
                    return positive / total
    except Exception as e:
        logger.debug(f"{symbol}: FCF consistency yfinance error: {e}")

    # 2. Fallback a FMP cash flow statement
    try:
        data = fmp_get(f"/cash-flow-statement/{fmp_ticker(symbol)}", params={"limit": 5})
        if data and isinstance(data, list):
            fcf_vals = [safe_float(d.get("freeCashFlow", 0)) for d in data]
            positive = sum(1 for v in fcf_vals if v > 0)
            total = len(fcf_vals)
            if total > 0:
                return positive / total
    except Exception as e:
        logger.debug(f"{symbol}: FCF consistency FMP error: {e}")

    # 3. FALLBACK: Calcular FCF actual desde info
    try:
        if info:
            ocf = safe_float(info.get("operatingCashflow"), 0)
            if not ocf:
                ocf = safe_float(info.get("operatingCashFlow"), 0)
            
            capex = abs(safe_float(info.get("capitalExpenditures"), 0))
            if not capex:
                capex = abs(safe_float(info.get("capitalExpenditure"), 0))
            
            fcf = ocf - capex
            
            if fcf != 0:
                return 1.0 if fcf > 0 else 0.0
    except Exception as e:
        logger.debug(f"{symbol}: FCF consistency info error: {e}")

    return None
def get_current_ratio(ticker, symbol) -> Optional[float]:
    try:
        if ticker:
            bs = ticker.quarterly_balance_sheet
            if bs is not None and not bs.empty:
                ca = None
                for label in ['Total Current Assets', 'Current Assets']:
                    if label in bs.index:
                        ca = safe_float(bs.loc[label].iloc[0]); break
                cl = None
                for label in ['Total Current Liabilities', 'Current Liabilities']:
                    if label in bs.index:
                        cl = safe_float(bs.loc[label].iloc[0]); break
                if ca and cl and cl > 0: return ca / cl
    except Exception: pass
    try:
        data = fmp_get(f"/balance-sheet-statement/{fmp_ticker(symbol)}", params={"limit": 1, "period": "quarter"})
        if data and isinstance(data, list) and data:
            ca = safe_float(data[0].get("totalCurrentAssets", 0))
            cl = safe_float(data[0].get("totalCurrentLiabilities", 0))
            if ca and cl and cl > 0: return ca / cl
    except Exception: pass
    return None

def get_quick_ratio(ticker, symbol) -> Optional[float]:
    try:
        if ticker:
            bs = ticker.quarterly_balance_sheet
            if bs is not None and not bs.empty:
                ca = None
                for label in ['Total Current Assets', 'Current Assets']:
                    if label in bs.index:
                        ca = safe_float(bs.loc[label].iloc[0]); break
                inv = 0
                for label in ['Inventory', 'Inventories']:
                    if label in bs.index:
                        inv = safe_float(bs.loc[label].iloc[0]); break
                cl = None
                for label in ['Total Current Liabilities', 'Current Liabilities']:
                    if label in bs.index:
                        cl = safe_float(bs.loc[label].iloc[0]); break
                if ca and cl and cl > 0: return (ca - inv) / cl
    except Exception: pass
    try:
        data = fmp_get(f"/balance-sheet-statement/{fmp_ticker(symbol)}", params={"limit": 1, "period": "quarter"})
        if data and isinstance(data, list) and data:
            ca = safe_float(data[0].get("totalCurrentAssets", 0))
            inv = safe_float(data[0].get("inventory", 0))
            cl = safe_float(data[0].get("totalCurrentLiabilities", 0))
            if ca and cl and cl > 0: return (ca - inv) / cl
    except Exception: pass
    return None

def get_net_debt_to_fcf(ticker, symbol, fcf, info=None) -> Optional[float]:
    try:
        total_debt = get_total_debt(info, symbol)  # FIX: Reusa función existente con info
        cash = get_total_cash(info, symbol)
        net_debt = total_debt - cash if total_debt and cash else total_debt
        if fcf and fcf > 0 and net_debt is not None:
            return net_debt / fcf
    except Exception: pass
    return None

def get_debt_growth_yoy(ticker, symbol) -> Optional[float]:
    try:
        data = fmp_get(f"/balance-sheet-statement/{fmp_ticker(symbol)}", params={"limit": 2})
        if data and isinstance(data, list) and len(data) >= 2:
            d1 = safe_float(data[0].get("totalDebt", 0))
            d2 = safe_float(data[1].get("totalDebt", 0))
            if d2 and d2 > 0: return (d1 - d2) / d2
    except Exception: pass
    try:
        data = av_get_cached("BALANCE_SHEET", symbol)
        if data and "annualReports" in data and len(data["annualReports"]) >= 2:
            d1 = float(data["annualReports"][0].get("shortTermDebt", 0) or 0) + float(data["annualReports"][0].get("longTermDebt", 0) or 0)
            d2 = float(data["annualReports"][1].get("shortTermDebt", 0) or 0) + float(data["annualReports"][1].get("longTermDebt", 0) or 0)
            if d2 and d2 > 0: return (d1 - d2) / d2
    except Exception: pass
    return None

def get_roic(ticker, symbol, info=None) -> Optional[float]:
    """
    ROIC = NOPAT / Invested Capital
    NOPAT ≈ EBIT * (1 - tax_rate)
    Invested Capital = Total Equity + Total Debt - Cash
    """
    # 1. Intentar con yfinance (datos más completos)
    try:
        if ticker:
            fin = ticker.financials
            bs = ticker.balance_sheet
            if fin is not None and not fin.empty and bs is not None and not bs.empty:
                ebit = None
                for label in ['Ebit', 'Operating Income', 'Operating Profit']:
                    if label in fin.index:
                        ebit = safe_float(fin.loc[label].iloc[0])
                        break

                tax_rate = 0.21
                for label in ['Tax Rate For Calcs', 'Effective Tax Rate']:
                    if label in fin.index:
                        tr = safe_float(fin.loc[label].iloc[0])
                        if tr and 0 < tr < 1: 
                            tax_rate = tr
                        elif tr and tr > 1:
                            tax_rate = tr / 100
                        break

                nopat = ebit * (1 - tax_rate) if ebit else None

                equity = None
                for label in ['Stockholders Equity', 'Total Equity Gross Minority Interest', 'Common Stock Equity']:
                    if label in bs.index:
                        equity = safe_float(bs.loc[label].iloc[0])
                        break

                # FIX: Pasar info en lugar de None
                debt = get_total_debt(info, symbol)
                cash = get_total_cash(info, symbol)

                if nopat and equity:
                    invested_cap = equity + (debt or 0) - (cash or 0)
                    if invested_cap and invested_cap > 0:
                        return nopat / invested_cap
    except Exception as e:
        logger.debug(f"{symbol}: ROIC yfinance calc error: {e}")

       # 2. FALLBACK: Calcular desde info
    try:
        if info:
            ebitda = safe_float(info.get("ebitda"), 0)
            depreciation = safe_float(info.get("depreciation"), 0)
            ebit = ebitda - depreciation if ebitda and depreciation else ebitda
            
            if not ebit:
                ebit = safe_float(info.get("operatingIncome"), 0)
            
            if ebit:
                tax_rate = 0.21
                tr = info.get("taxRate")
                if tr:
                    tr_val = safe_float(tr, 0)
                    if tr_val > 1:
                        tax_rate = tr_val / 100
                    elif 0 < tr_val < 1:
                        tax_rate = tr_val
                
                nopat = ebit * (1 - tax_rate)
                
                total_equity = safe_float(info.get("totalStockholderEquity"), 0)
                if not total_equity:
                    total_equity = safe_float(info.get("totalStockholdersEquity"), 0)
                if not total_equity:
                    book_value = safe_float(info.get("bookValue"), 0)
                    shares = safe_float(info.get("sharesOutstanding"), 0)
                    if book_value and shares:
                        total_equity = book_value * shares
                
                debt = get_total_debt(info, symbol) or 0
                cash = get_total_cash(info, symbol) or 0
                
                if total_equity:
                    invested_cap = total_equity + debt - cash
                    if invested_cap > 0:
                        return nopat / invested_cap
    except Exception as e:
        logger.debug(f"{symbol}: ROIC info fallback error: {e}")

    # 3. Fallback a FMP ratios
    try:
        data = fmp_get(f"/ratios/{fmp_ticker(symbol)}", params={"limit": 1})
        if data and isinstance(data, list) and data:
            roic_val = data[0].get("returnOnInvestedCapital")
            if roic_val:
                if roic_val > 1:
                    return roic_val / 100
                return roic_val
    except Exception as e:
        logger.debug(f"{symbol}: ROIC FMP fallback error: {e}")

    # 4. Fallback a Finnhub
    try:
        data = finn_get("/stock/metric", {"symbol": symbol, "metric": "all"})
        if data and "metric" in data:
            roic_val = data["metric"].get("roicTTM")
            if roic_val:
                if roic_val > 1:
                    return roic_val / 100
                return roic_val
    except Exception as e:
        logger.debug(f"{symbol}: ROIC Finnhub fallback error: {e}")

    return None


def get_roe(ticker, symbol, info=None) -> Optional[float]:
    """
    Return on Equity = Net Income / Stockholders Equity
    Fallback a info cuando yfinance financials no disponibles
    """
    # 1. Intentar con yfinance
    try:
        if ticker:
            fin = ticker.financials
            if fin is not None and not fin.empty:
                for label in ['Net Income', 'Net Income Common Stockholders']:
                    if label in fin.index:
                        ni = safe_float(fin.loc[label].iloc[0])
                        bs = ticker.balance_sheet
                        if bs is not None and not bs.empty:
                            for eq_label in ['Stockholders Equity', 'Total Equity Gross Minority Interest']:
                                if eq_label in bs.index:
                                    equity = safe_float(bs.loc[eq_label].iloc[0])
                                    if equity and equity > 0:
                                        return ni / equity
    except Exception as e:
        logger.debug(f"{symbol}: ROE yfinance error: {e}")
    
    # 2. FALLBACK: Calcular desde info
    try:
        if info:
            net_income = safe_float(info.get("netIncome"), 0)
            if not net_income:
                net_income = safe_float(info.get("netIncomeToCommon"), 0)
            
            total_equity = safe_float(info.get("totalStockholderEquity"), 0)
            if not total_equity:
                total_equity = safe_float(info.get("totalStockholdersEquity"), 0)
            if not total_equity:
                book_value = safe_float(info.get("bookValue"), 0)
                shares = safe_float(info.get("sharesOutstanding"), 0)
                if book_value and shares:
                    total_equity = book_value * shares
            
            if net_income and total_equity and total_equity > 0:
                return net_income / total_equity
    except Exception as e:
        logger.debug(f"{symbol}: ROE info fallback error: {e}")
    
    # 3. Fallback a FMP
    try:
        data = fmp_get(f"/ratios/{fmp_ticker(symbol)}", params={"limit": 1})
        if data and isinstance(data, list) and data:
            roe_val = data[0].get("returnOnEquity")
            if roe_val:
                if roe_val > 1:
                    return roe_val / 100
                return roe_val
    except Exception:
        pass
    
    return None


def get_moat_indicators(ticker, symbol, info, hist) -> Dict[str, Any]:
    """
    Calcula indicadores de moat competitivo - VERSIÓN CORREGIDA CON FALLBACKS
    """
    indicators = {
        "gross_margin_stable": False,
        "gross_margin_high": False,
        "roic_stable": False,
        "operating_margin_stable": False,
        "market_share_growing": False,
        "fcf_stable": False,
        "moat_score": 0
    }

    try:
        # === 1. Gross Margin (yfinance preferido, info fallback) ===
        gm_calculated = False

        if ticker:
            try:
                fin = ticker.financials
                if fin is not None and not fin.empty and "Gross Profit" in fin.index and "Total Revenue" in fin.index:
                    gms = (fin.loc["Gross Profit"] / fin.loc["Total Revenue"]).dropna().tail(5)
                    if len(gms) >= 3:
                        avg_gm = gms.mean()
                        std_gm = gms.std()
                        indicators["gross_margin_high"] = avg_gm > 0.40
                        indicators["gross_margin_stable"] = std_gm < 0.03
                        gm_calculated = True
            except Exception as e:
                logger.debug(f"{symbol}: GM yfinance error: {e}")

        # Fallback a info para gross margin
        if not gm_calculated and info:
            gm = safe_float(info.get("grossMargins"), 0)
            if not gm:
                gm = safe_float(info.get("grossMargin"), 0)
            if gm > 0:
                indicators["gross_margin_high"] = gm > 0.40
                indicators["gross_margin_stable"] = gm > 0.35

        # === 2. ROIC (FIX: pasa info) ===
        roic = get_roic(ticker, symbol, info)
        if roic is not None:
            roic_norm = roic / 100 if roic > 1 else roic
            indicators["roic_stable"] = roic_norm > 0.15

        # === 3. Operating Margin Stability (FIX: pasa info) ===
        op_stability = get_margin_stability(ticker, symbol, "operating", info)
        if op_stability is not None:
            indicators["operating_margin_stable"] = op_stability > 0.7

        # === 4. FCF Consistency (FIX: pasa info) ===
        fcf_cons = get_fcf_consistency(ticker, symbol, info)
        if fcf_cons is not None:
            indicators["fcf_stable"] = fcf_cons > 0.8

        # === Calcular Moat Score ===
        score = 0
        if indicators["gross_margin_high"]: score += 25
        if indicators["gross_margin_stable"]: score += 15
        if indicators["roic_stable"]: score += 25
        if indicators["operating_margin_stable"]: score += 15
        if indicators["fcf_stable"]: score += 20
        indicators["moat_score"] = min(100, score)

    except Exception as e:
        logger.warning(f"{symbol}: Error moat indicators: {e}")

    return indicators
def get_historical_pe_ratio(symbol: str, hist: pd.DataFrame, trailing_eps: float) -> Dict[str, Any]:
    """
    Calcula PE histórico de 5 años usando datos de precio y EPS
    Retorna: {current_pe, avg_pe_5y, min_pe_5y, max_pe_5y, relative_pe}
    """
    result = {
        "current_pe": None,
        "avg_pe_5y": None,
        "min_pe_5y": None,
        "max_pe_5y": None,
        "relative_pe": None
    }
    
    try:
        if hist is None or hist.empty or len(hist) < 200:
            return result
        
        current_price = hist['Close'].iloc[-1]
        if not current_price or not trailing_eps or trailing_eps <= 0:
            return result
        
        result["current_pe"] = current_price / trailing_eps
        
        # Calcular PE histórico mensual
        with _historical_pe_lock:
            cache_key = f"{symbol}_pe"
            if cache_key in _historical_pe_cache:
                pe_history = _historical_pe_cache[cache_key]
            else:
                # Resamplear a mensual y calcular PE para cada mes
                monthly = hist.resample('ME')['Close'].mean().dropna()
                pe_history = []
                # Necesitamos EPS histórico — usamos trailing EPS como aproximación
                # Para algo más preciso, usaríamos datos de earnings históricos
                for price in monthly:
                    if price and trailing_eps > 0:
                        pe_history.append(price / trailing_eps)
                _historical_pe_cache[cache_key] = pe_history
        
        if pe_history:
            result["avg_pe_5y"] = np.mean(pe_history)
            result["min_pe_5y"] = np.min(pe_history)
            result["max_pe_5y"] = np.max(pe_history)
            if result["avg_pe_5y"] and result["avg_pe_5y"] > 0:
                result["relative_pe"] = result["current_pe"] / result["avg_pe_5y"]
                
    except Exception as e:
        logger.warning(f"{symbol}: Error historical PE: {e}")
    
    return result

def get_historical_ev_ebitda(symbol: str, hist: pd.DataFrame, info: dict) -> Dict[str, Any]:
    """Similar a PE pero para EV/EBITDA"""
    result = {"current": None, "avg_5y": None, "relative": None}
    try:
        market_cap = info.get("marketCap", 0)
        ebitda = info.get("ebitda", 0)
        total_debt = get_total_debt(info, symbol)
        cash = get_total_cash(info, symbol)
        
        if market_cap and ebitda and ebitda > 0:
            ev = market_cap + (total_debt or 0) - (cash or 0)
            current_ev_ebitda = ev / ebitda
            result["current"] = current_ev_ebitda
            
            # Aproximación histórica usando cambio de market cap
            if hist is not None and not hist.empty and len(hist) >= 200:
                monthly_mc = hist.resample('ME')['Close'].mean().dropna() * (market_cap / hist['Close'].iloc[-1])
                hist_ev_ebitda = []
                for mc in monthly_mc:
                    ev_h = mc + (total_debt or 0) - (cash or 0)
                    if ebitda > 0:
                        hist_ev_ebitda.append(ev_h / ebitda)
                if hist_ev_ebitda:
                    result["avg_5y"] = np.mean(hist_ev_ebitda)
                    if result["avg_5y"] and result["avg_5y"] > 0:
                        result["relative"] = current_ev_ebitda / result["avg_5y"]
    except Exception as e:
        logger.warning(f"{symbol}: Error historical EV/EBITDA: {e}")
    return result

def get_historical_fcf_yield(symbol: str, info: dict, fcf: float) -> Dict[str, Any]:
    result = {"current": None, "avg_5y": None, "relative": None}
    try:
        market_cap = info.get("marketCap", 0)
        if market_cap and market_cap > 0 and fcf:
            current_fy = fcf / market_cap
            result["current"] = current_fy
            # FCF yield histórico aproximado
            # En una implementación completa, necesitaríamos FCF histórico
            result["avg_5y"] = current_fy  # Placeholder
            result["relative"] = 1.0
    except Exception: pass
    return result

# ═══════════════════════════════════════════════════════════════
# CAPA 3 — TIMING: MÉTRICAS TÉCNICAS (reusa funciones v6.x)
# ═══════════════════════════════════════════════════════════════

# Reusa: calc_williams_r_adaptive, find_williams_divergences, etc. del v6.x
# Reusa: calc_levels_pro, find_pivot_levels, etc.

# ==============================================================
# 📊 SISTEMA DE SCORING v7.0 — 3 CAPAS INDEPENDIENTES
# ==============================================================

# ── CAPA 1: BUSINESS QUALITY SCORING ──
def score_capa1_growth(rev_growth_yoy: float, rev_cagr_3y: Optional[float], 
                       eps_cagr_3y: Optional[float], revenue_estimate: Optional[float],
                       eps_estimate: Optional[float]) -> float:
    score = 50.0
    rev_growth_yoy = safe_float(rev_growth_yoy)
    
    # Revenue Growth YoY
    if rev_growth_yoy > 0.30: score += 20
    elif rev_growth_yoy > 0.20: score += 15
    elif rev_growth_yoy > 0.10: score += 10
    elif rev_growth_yoy > 0.05: score += 5
    elif rev_growth_yoy < 0: score -= 15
    
    # 3Y Revenue CAGR (más importante que un solo trimestre)
    if rev_cagr_3y:
        rc = safe_float(rev_cagr_3y)
        if rc > 0.25: score += 15
        elif rc > 0.15: score += 10
        elif rc > 0.10: score += 5
        elif rc < 0: score -= 10
    
    # 3Y EPS CAGR
    if eps_cagr_3y:
        ec = safe_float(eps_cagr_3y)
        if ec > 0.30: score += 15
        elif ec > 0.20: score += 10
        elif ec > 0.10: score += 5
        elif ec < 0: score -= 10
    
    # Revenue/EPS Estimates
    if revenue_estimate and revenue_estimate > 0:
        score += 5  # Tiene estimado = cobertura analista
    
    return max(0, min(100, score))

def score_capa1_consistency(beat_rate: float, weighted_cons: float, 
                            surprise_trend: str, worst_miss: float) -> float:
    score = 50.0
    beat_rate = safe_float(beat_rate)
    weighted_cons = safe_float(weighted_cons)
    worst_miss = safe_float(worst_miss)
    
    if beat_rate > 0.90: score += 25
    elif beat_rate > 0.80: score += 20
    elif beat_rate > 0.70: score += 15
    elif beat_rate > 0.50: score += 5
    else: score -= 15
    
    if weighted_cons > 1.5: score += 15
    elif weighted_cons > 1.0: score += 10
    elif weighted_cons > 0.5: score += 5
    elif weighted_cons < 0: score -= 10
    
    if "MEJORANDO" in str(surprise_trend): score += 5
    elif "DETERIORANDO" in str(surprise_trend): score -= 5
    
    if worst_miss and worst_miss < -0.30: score -= 15
    elif worst_miss and worst_miss < -0.15: score -= 5
    
    return max(0, min(100, score))

def score_capa1_margins(gross_margin: float, operating_margin: float, 
                        profit_margin: float, margin_stability: Optional[float]) -> float:
    score = 50.0
    gm = safe_float(gross_margin)
    om = safe_float(operating_margin)
    pm = safe_float(profit_margin)
    
    if gm > 0.60: score += 15
    elif gm > 0.40: score += 10
    elif gm > 0.25: score += 5
    elif gm < 0.10: score -= 5
    
    if om > 0.25: score += 10
    elif om > 0.15: score += 5
    elif om < 0.05: score -= 5
    
    if pm > 0.20: score += 10
    elif pm > 0.10: score += 5
    elif pm < 0.05: score -= 5
    
    # Margin Stability (nuevo en v7)
    if margin_stability:
        ms = safe_float(margin_stability)
        score += ms * 10  # Hasta +10 puntos por estabilidad
    
    return max(0, min(100, score))

def score_capa1_fcf_quality(fcf_yield: float, fcf_margin: float, fcf_growth: float,
                            fcf_ni_ratio: float, fcf_consistency: Optional[float]) -> float:
    score = 50.0
    fy = safe_float(fcf_yield)
    fm = safe_float(fcf_margin)
    fg = safe_float(fcf_growth)
    fni = safe_float(fcf_ni_ratio)
    
    if fy > 0.08: score += 20
    elif fy > 0.05: score += 15
    elif fy > 0.03: score += 10
    elif fy > 0.01: score += 5
    elif fy < 0: score -= 10
    
    if fm > 0.20: score += 10
    elif fm > 0.10: score += 5
    elif fm < 0: score -= 5
    
    if fg > 0.30: score += 10
    elif fg > 0.15: score += 5
    elif fg < 0: score -= 5
    
    # FCF / Net Income (calidad de earnings)
    if fni > 1.2: score += 10  # FCF > NI = earnings de alta calidad
    elif fni > 0.8: score += 5
    elif fni < 0.5: score -= 10  # FCF mucho menor que NI = posible manipulación
    
    # FCF Consistency
    if fcf_consistency:
        fc = safe_float(fcf_consistency)
        score += fc * 10  # Hasta +10
    
    return max(0, min(100, score))

def score_capa1_debt_health(debt_ebitda: float, interest_coverage: float,
                            debt_equity: float, current_ratio: Optional[float],
                            quick_ratio: Optional[float], net_debt_fcf: Optional[float],
                            debt_growth_yoy: Optional[float]) -> float:
    score = 50.0
    de = safe_float(debt_ebitda, 999)
    ic = safe_float(interest_coverage)
    
    if de < 1.0: score += 20
    elif de < 2.0: score += 15
    elif de < 3.0: score += 10
    elif de < 4.0: score += 5
    elif de > 6.0: score -= 15
    elif de > 4.0: score -= 5
    
    if ic > 10: score += 15
    elif ic > 5: score += 10
    elif ic > 2: score += 5
    elif ic < 1: score -= 15
    elif ic < 2: score -= 5
    
    # Current Ratio
    if current_ratio:
        cr = safe_float(current_ratio)
        if cr > 2.0: score += 5
        elif cr < 1.0: score -= 5
    
    # Quick Ratio
    if quick_ratio:
        qr = safe_float(quick_ratio)
        if qr > 1.5: score += 5
        elif qr < 0.8: score -= 5
    
    # Net Debt / FCF
    if net_debt_fcf:
        ndf = safe_float(net_debt_fcf)
        if ndf < 2: score += 5
        elif ndf > 5: score -= 5
    
    # Debt Growth YoY
    if debt_growth_yoy:
        dg = safe_float(debt_growth_yoy)
        if dg > 0.50: score -= 10  # Deuda creciendo rápido
        elif dg < -0.20: score += 5  # Deuda reduciéndose
    
    return max(0, min(100, score))

def score_capa1_roic_roe(roic: Optional[float], roe: Optional[float]) -> float:
    score = 50.0
    r = safe_float(roic) if roic else 0
    e = safe_float(roe) if roe else 0
    
    # ROIC scoring (más importante)
    if r > 0.20: score += 30
    elif r > 0.15: score += 20
    elif r > 0.10: score += 10
    elif r > 0.05: score += 5
    elif r > 0: score += 0
    else: score -= 10
    
    # ROE como complemento
    if e > 0.25: score += 10
    elif e > 0.15: score += 5
    elif e < 0.05: score -= 5
    
    return max(0, min(100, score))

def score_capa1_moat(moat_score: float) -> float:
    return max(0, min(100, moat_score))

def compute_capa1_business_quality(growth_score: float, consistency_score: float,
                                   margins_score: float, fcf_score: float,
                                   debt_score: float, roic_score: float,
                                   moat_score: float) -> float:
    """
    Business Quality = ponderación de los 6 sub-scores
    """
    total = (growth_score * 0.20 +
             consistency_score * 0.20 +
             margins_score * 0.15 +
             fcf_score * 0.15 +
             debt_score * 0.15 +
             roic_score * 0.10 +
             moat_score * 0.05)
    return round(total, 2)

# ── CAPA 2: VALUATION SCORING ──
def score_capa2_peg(peg: float) -> float:
    score = 50.0
    p = safe_float(peg)
    if p and p < 0.8: score += 25
    elif p and p < 1.0: score += 20
    elif p and p < 1.5: score += 10
    elif p and p < 2.0: score += 0
    elif p and p > 3.0: score -= 20
    elif p and p > 2.0: score -= 10
    return max(0, min(100, score))

def score_capa2_forward_pe(forward_pe: float, sector: str) -> float:
    score = 50.0
    pe = safe_float(forward_pe)
    
    # Umbrales por sector
    sector_pe_max = {
        "Technology": 30, "Healthcare": 25, "Financial Services": 15,
        "Energy": 12, "Utilities": 18, "Consumer Cyclical": 20,
        "Consumer Defensive": 22, "Industrials": 20, "Communication Services": 20,
        "Basic Materials": 15, "Real Estate": 20, "default": 20
    }
    max_pe = sector_pe_max.get(sector, sector_pe_max["default"])
    
    if pe and pe < max_pe * 0.5: score += 25
    elif pe and pe < max_pe * 0.7: score += 15
    elif pe and pe < max_pe: score += 5
    elif pe and pe > max_pe * 2: score -= 20
    elif pe and pe > max_pe * 1.5: score -= 10
    
    return max(0, min(100, score))

def score_capa2_fcf_yield(fcf_yield: float) -> float:
    score = 50.0
    fy = safe_float(fcf_yield)
    if fy > 0.10: score += 25
    elif fy > 0.07: score += 20
    elif fy > 0.05: score += 15
    elif fy > 0.03: score += 5
    elif fy < 0: score -= 15
    return max(0, min(100, score))

def score_capa2_ev_ebitda(ev_ebitda: float) -> float:
    score = 50.0
    ev = safe_float(ev_ebitda)
    if ev and ev < 8: score += 25
    elif ev and ev < 12: score += 15
    elif ev and ev < 15: score += 5
    elif ev and ev > 25: score -= 20
    elif ev and ev > 20: score -= 10
    return max(0, min(100, score))

def score_capa2_analyst_upside(current_price: float, target_price: float, analyst_count: int) -> float:
    if not target_price or not current_price or current_price <= 0 or analyst_count < 3:
        return 50.0
    upside = (target_price - current_price) / current_price
    confidence = min(analyst_count / 15, 1.0)
    raw = 50 + upside * 200
    return max(0, min(100, raw * confidence + 50 * (1 - confidence)))

def score_capa2_historical_valuation(relative_pe: Optional[float], 
                                      relative_ev_ebitda: Optional[float],
                                      relative_fcf_yield: Optional[float]) -> float:
    score = 50.0
    # Relative PE: < 1 = más barato que histórico = bueno
    if relative_pe:
        rp = safe_float(relative_pe)
        if rp < 0.7: score += 25
        elif rp < 0.9: score += 15
        elif rp < 1.0: score += 5
        elif rp > 1.5: score -= 20
        elif rp > 1.2: score -= 10
    
    # Relative EV/EBITDA
    if relative_ev_ebitda:
        rev = safe_float(relative_ev_ebitda)
        if rev < 0.8: score += 10
        elif rev > 1.3: score -= 10
    
    return max(0, min(100, score))

def compute_capa2_valuation(peg_score: float, pe_score: float, fcf_yield_score: float,
                            ev_ebitda_score: float, analyst_score: float,
                            historical_score: float) -> float:
    total = (peg_score * 0.20 +
             pe_score * 0.15 +
             fcf_yield_score * 0.20 +
             ev_ebitda_score * 0.15 +
             analyst_score * 0.10 +
             historical_score * 0.20)
    return round(total, 2)

# ── CAPA 3: TIMING SCORING ──
def score_capa3_tendencia(sma_trend: str, sma_slope: float) -> float:
    trend_scores = {
        "ALCISTA FUERTE": 95, "ALCISTA": 80, "LATERAL": 50,
        "BAJISTA": 30, "BAJISTA FUERTE": 15
    }
    base = trend_scores.get(sma_trend, 50)
    # Ajuste por pendiente
    slope = safe_float(sma_slope)
    if slope > 0.10: base += 5
    elif slope < -0.10: base -= 5
    return max(0, min(100, base))

def score_capa3_soportes(dist_support: float, dist_resistance: float,
                         fuerza_soporte: float, fuerza_resistencia: float,
                         atr_threshold: float, posicion_sr: str) -> float:
    score = 50.0
    ds = safe_float(dist_support, 1.0)
    dr = safe_float(dist_resistance, 1.0)
    fs = safe_float(fuerza_soporte, 0)
    fr = safe_float(fuerza_resistencia, 0)
    th = safe_float(atr_threshold, 0.02)
    influence = th * 2
    
    if ds < influence and fs > 50:
        score += min(30, (1 - ds/influence) * 30)
    if dr < influence and fr > 50:
        score -= min(25, (1 - dr/influence) * 25)
    
    if "Rompimiento al alza" in str(posicion_sr): score += 15
    elif "Rompimiento bajista" in str(posicion_sr): score -= 15
    
    return max(0, min(100, score))

def score_capa3_williams(wr_current: float, div_signal: str, div_strength: float,
                         combined_signal: str) -> float:
    score = 50.0
    wr = safe_float(wr_current)
    
    if wr < -90: score += 20
    elif wr < -80: score += 15
    elif wr < -70: score += 10
    elif wr > -10: score -= 20
    elif wr > -20: score -= 15
    elif wr > -30: score -= 10
    
    if "ALCISTA" in str(div_signal): score += min(15, div_strength / 5)
    elif "BAJISTA" in str(div_signal): score -= min(15, div_strength / 5)
    
    if "ALCISTA FUERTE" in str(combined_signal): score += 10
    elif "BAJISTA FUERTE" in str(combined_signal): score -= 10
    
    return max(0, min(100, score))

def score_capa3_volumen(vol_ratio: float, obv_trend: str, price_vol_div: str) -> float:
    score = 50.0
    vr = safe_float(vol_ratio)
    if vr > 2.0: score += 10
    elif vr > 1.5: score += 5
    if str(obv_trend) == "ACUMULACIÓN": score += 10
    elif str(obv_trend) == "DISTRIBUCIÓN": score -= 10
    if "ALCISTA" in str(price_vol_div): score += 10
    elif "BAJISTA" in str(price_vol_div): score -= 10
    return max(0, min(100, score))

def score_capa3_volatilidad(vol_atr: float) -> float:
    score = 50.0
    va = safe_float(vol_atr)
    # Menor volatilidad = mejor timing (más predecible)
    if va < 0.01: score += 15
    elif va < 0.02: score += 10
    elif va < 0.03: score += 5
    elif va > 0.08: score -= 15
    elif va > 0.05: score -= 5
    return max(0, min(100, score))

def compute_capa3_timing(tendencia_score: float, soportes_score: float,
                         williams_score: float, volumen_score: float,
                         volatilidad_score: float) -> float:
    total = (tendencia_score * 0.30 +
             soportes_score * 0.25 +
             williams_score * 0.20 +
             volumen_score * 0.15 +
             volatilidad_score * 0.10)
    return round(total, 2)

# ═══════════════════════════════════════════════════════════════
# SCORE FINAL Y MATRIZ DE DECISIONES
# ═══════════════════════════════════════════════════════════════

def compute_final_score_v7(bq_score: float, val_score: float, timing_score: float,
                           strategy: str = "largo_plazo") -> Tuple[float, str, str, str, str]:
    """
    Calcula score final según estrategia y genera clasificación
    """
    strategy_weights = {
        "largo_plazo":  (0.50, 0.35, 0.15),
        "swing":        (0.25, 0.25, 0.50),
        "crisis":       (0.40, 0.40, 0.20),
        "default":      (0.50, 0.30, 0.20)
    }
    
    w_bq, w_val, w_tim = strategy_weights.get(strategy, strategy_weights["default"])
    final = bq_score * w_bq + val_score * w_val + timing_score * w_tim
    
    # Clasificación de cada capa
    def classify(score: float) -> str:
        if score >= 85: return "MUY ALTA"
        elif score >= 70: return "ALTA"
        elif score >= 55: return "MEDIA"
        elif score >= 40: return "BAJA"
        else: return "MUY BAJA"
    
    bq_class = classify(bq_score)
    val_class = classify(val_score)
    tim_class = classify(timing_score)
    
    # Entry Status basado en Timing
    if timing_score >= 85: entry_status = "IDEAL ENTRY"
    elif timing_score >= 70: entry_status = "GOOD ENTRY"
    elif timing_score >= 50: entry_status = "NEUTRAL"
    elif timing_score >= 30: entry_status = "EXTENDED"
    else: entry_status = "AVOID ENTRY"
    
    # Conviction basado en matriz
    conviction = determine_conviction(bq_class, val_class, tim_class)
    
    return round(final, 2), bq_class, val_class, tim_class, entry_status, conviction

def determine_conviction(bq: str, val: str, tim: str) -> str:
    """Matriz de decisiones v7.0"""
    # Combinaciones especiales
    if bq in ["MUY ALTA", "ALTA"] and val in ["MUY ALTA", "ALTA"] and tim in ["MUY ALTA", "ALTA"]:
        return "COMPRA AGRESIVA"
    if bq in ["MUY ALTA", "ALTA"] and val in ["MUY ALTA", "ALTA"] and tim in ["BAJA", "MUY BAJA"]:
        return "WATCHLIST — Esperar mejor entrada"
    if bq in ["MUY ALTA", "ALTA"] and val in ["BAJA", "MUY BAJA"] and tim in ["MUY ALTA", "ALTA"]:
        return "PEQUEÑA POSICIÓN — Oportunidad con riesgo"
    if bq in ["BAJA", "MUY BAJA"] and val in ["ALTA", "MUY ALTA"] and tim in ["MUY ALTA", "ALTA"]:
        return "SWING ONLY — No para largo plazo"
    if bq in ["BAJA", "MUY BAJA"] and val in ["BAJA", "MUY BAJA"] and tim in ["MUY ALTA", "ALTA"]:
        return "ESPECULACIÓN — Alto riesgo"
    if bq in ["MUY ALTA", "ALTA"] and val == "MEDIA" and tim == "MEDIA":
        return "DCA — Dollar Cost Averaging"
    if bq == "MUY ALTA" and val == "MUY ALTA" and tim in ["BAJA", "MUY BAJA"]:
        return "BUFFETT ZONE — Empresa excelente, esperar timing"
    
    # Default por capa dominante
    if bq in ["MUY BAJA", "BAJA"]:
        return "EVITAR — Calidad insuficiente"
    if val in ["MUY BAJA", "BAJA"] and bq not in ["MUY ALTA"]:
        return "SOBREVALORADA — Esperar corrección"
    if tim in ["MUY BAJA", "BAJA"]:
        return "ESPERAR — Mal momento técnico"
    
    return "NEUTRAL — Revisar métricas"

def determine_allocation(final_score: float, conviction: str) -> str:
    """Recomendación de asignación de capital"""
    if "COMPRA AGRESIVA" in conviction: return "5-10% del portafolio"
    if "PEQUEÑA POSICIÓN" in conviction: return "2-3% del portafolio"
    if "DCA" in conviction: return "1-2% mensual"
    if "WATCHLIST" in conviction: return "0% — Monitorear"
    if "SWING ONLY" in conviction: return "1-2% máximo, stop estricto"
    if "ESPECULACIÓN" in conviction: return "0.5-1% máximo"
    if "BUFFETT" in conviction: return "0% — Esperar entry ideal"
    
    if final_score >= 80: return "3-5% del portafolio"
    elif final_score >= 65: return "2-3% del portafolio"
    elif final_score >= 50: return "1-2% del portafolio"
    elif final_score >= 35: return "0.5-1% máximo"
    else: return "0% — No invertir"

# ==============================================================
# 📊 RANGOS EN GOOGLE SHEETS v7.0
# ==============================================================

# Nuevos rangos para la arquitectura de 3 capas
v7_ranges = {
        # Métricas base de v6 que el uploader necesita
    'Gross_Margin': f'EG{START_ROW}:EG{END_ROW}',
    'Operating_Margin': f'EH{START_ROW}:EH{END_ROW}',
    'FCF_Margin': f'EI{START_ROW}:EI{END_ROW}',
    'Net_Income': f'EJ{START_ROW}:EJ{END_ROW}',
    'Total_Debt': f'EK{START_ROW}:EK{END_ROW}',
    'EBITDA': f'EL{START_ROW}:EL{END_ROW}',
    'Profit_Margin': f'EM{START_ROW}:EM{END_ROW}',
    'Trailing_EPS': f'EN{START_ROW}:EN{END_ROW}',
    'Current_Price': f'EO{START_ROW}:EO{END_ROW}',
    
    # Métricas de valoración actual
    'Current_PE': f'EP{START_ROW}:EP{END_ROW}',
    'EV_EBITDA_Current': f'EQ{START_ROW}:EQ{END_ROW}',
    'FCF_Yield_Current': f'ER{START_ROW}:ER{END_ROW}',
    'FCF_Yield_Avg_5Y': f'ES{START_ROW}:ES{END_ROW}',
    'Analyst_Target': f'ET{START_ROW}:ET{END_ROW}',
    'Analyst_Count': f'EU{START_ROW}:EU{END_ROW}',
    'Upside': f'EV{START_ROW}:EV{END_ROW}',
    
    # Métricas técnicas de v6
    'SMA_Trend': f'EW{START_ROW}:EW{END_ROW}',
    'SMA_Slope': f'EX{START_ROW}:EX{END_ROW}',
    'Volume_Ratio': f'EY{START_ROW}:EY{END_ROW}',
    'OBV_Trend': f'EZ{START_ROW}:EZ{END_ROW}',
    'Price_Volume_Div': f'FA{START_ROW}:FA{END_ROW}',
    'Dist_Soporte': f'FB{START_ROW}:FB{END_ROW}',
    'Dist_Resistencia': f'FC{START_ROW}:FC{END_ROW}',
    'Fuerza_Soporte': f'FD{START_ROW}:FD{END_ROW}',
    'Fuerza_Resistencia': f'FE{START_ROW}:FE{END_ROW}',
    'Posicion_SR': f'FF{START_ROW}:FF{END_ROW}',
    'Williams_Current': f'FG{START_ROW}:FG{END_ROW}',
    'Williams_Divergence': f'FH{START_ROW}:FH{END_ROW}',
    'Williams_Div_Strength': f'FI{START_ROW}:FI{END_ROW}',
    'Williams_Combined': f'FJ{START_ROW}:FJ{END_ROW}',
    'Volatility_ATR': f'FK{START_ROW}:FK{END_ROW}',



    # ── CAPA 1: BUSINESS QUALITY ──
    'BQ_Growth_Score': f'GA{START_ROW}:GA{END_ROW}',
    'BQ_Consistency_Score': f'GB{START_ROW}:GB{END_ROW}',
    'BQ_Margins_Score': f'GC{START_ROW}:GC{END_ROW}',
    'BQ_FCF_Score': f'GD{START_ROW}:GD{END_ROW}',
    'BQ_Debt_Score': f'GE{START_ROW}:GE{END_ROW}',
    'BQ_ROIC_Score': f'GF{START_ROW}:GF{END_ROW}',
    'BQ_Moat_Score': f'GG{START_ROW}:GG{END_ROW}',
    'BQ_Final': f'GH{START_ROW}:GH{END_ROW}',
    
    # Métricas raw de Capa 1
    'Rev_CAGR_3Y': f'GI{START_ROW}:GI{END_ROW}',
    'EPS_CAGR_3Y': f'GJ{START_ROW}:GJ{END_ROW}',
    'Margin_Stability': f'GK{START_ROW}:GK{END_ROW}',
    'FCF_Consistency': f'GL{START_ROW}:GL{END_ROW}',
    'Current_Ratio': f'GM{START_ROW}:GM{END_ROW}',
    'Quick_Ratio': f'GN{START_ROW}:GN{END_ROW}',
    'Net_Debt_FCF': f'GO{START_ROW}:GO{END_ROW}',
    'Debt_Growth_YoY': f'GP{START_ROW}:GP{END_ROW}',
    'ROIC': f'GQ{START_ROW}:GQ{END_ROW}',
    'ROE': f'GR{START_ROW}:GR{END_ROW}',
    'Moat_Score_Raw': f'GS{START_ROW}:GS{END_ROW}',
    
    # ── CAPA 2: VALUATION ──
    'VAL_PEG_Score': f'GT{START_ROW}:GT{END_ROW}',
    'VAL_PE_Score': f'GU{START_ROW}:GU{END_ROW}',
    'VAL_FCFYield_Score': f'GV{START_ROW}:GV{END_ROW}',
    'VAL_EVEBITDA_Score': f'GW{START_ROW}:GW{END_ROW}',
    'VAL_Analyst_Score': f'GX{START_ROW}:GX{END_ROW}',
    'VAL_Historical_Score': f'GY{START_ROW}:GY{END_ROW}',
    'VAL_Final': f'GZ{START_ROW}:GZ{END_ROW}',
    
    # Métricas raw de Capa 2
    'Relative_PE': f'HA{START_ROW}:HA{END_ROW}',
    'Relative_EV_EBITDA': f'HB{START_ROW}:HB{END_ROW}',
    'Historical_PE_Avg': f'HC{START_ROW}:HC{END_ROW}',
    'Historical_PE_Min': f'HD{START_ROW}:HD{END_ROW}',
    'Historical_PE_Max': f'HE{START_ROW}:HE{END_ROW}',
    
    # ── CAPA 3: TIMING ──
    'TIM_Tendencia_Score': f'HF{START_ROW}:HF{END_ROW}',
    'TIM_Soportes_Score': f'HG{START_ROW}:HG{END_ROW}',
    'TIM_Williams_Score': f'HH{START_ROW}:HH{END_ROW}',
    'TIM_Volumen_Score': f'HI{START_ROW}:HI{END_ROW}',
    'TIM_Volatilidad_Score': f'HJ{START_ROW}:HJ{END_ROW}',
    'TIM_Final': f'HK{START_ROW}:HK{END_ROW}',
    
    # ── OUTPUT FINAL ──
    'Final_Score': f'HL{START_ROW}:HL{END_ROW}',
    'BQ_Class': f'HM{START_ROW}:HM{END_ROW}',
    'VAL_Class': f'HN{START_ROW}:HN{END_ROW}',
    'TIM_Class': f'HO{START_ROW}:HO{END_ROW}',
    'Entry_Status': f'HP{START_ROW}:HP{END_ROW}',
    'Conviction': f'HQ{START_ROW}:HQ{END_ROW}',
    'Allocation': f'HR{START_ROW}:HR{END_ROW}',
    'Strategy': f'HS{START_ROW}:HS{END_ROW}',
    'Alertas': f'HT{START_ROW}:HT{END_ROW}',
    
    # ── DATOS LEGACY (compatibilidad con columnas anteriores) ──
    'Ticker': f'A{START_ROW}:A{END_ROW}',
    'Price_Actual': f'C{START_ROW}:C{END_ROW}',
    'Sector': f'DA{START_ROW}:DA{END_ROW}',
}

v7_defaults = {
    'Gross_Margin': 0, 'Operating_Margin': 0, 'FCF_Margin': 0,
    'Net_Income': "N/A", 'Total_Debt': "N/A", 'EBITDA': "N/A",
    'Profit_Margin': 0, 'Trailing_EPS': "N/A", 'Current_Price': 0,
    'Current_PE': "N/A", 'EV_EBITDA_Current': "N/A",
    'FCF_Yield_Current': "N/A", 'FCF_Yield_Avg_5Y': "N/A",
    'Analyst_Target': "N/A", 'Analyst_Count': 0, 'Upside': 0,
    'SMA_Trend': "LATERAL", 'SMA_Slope': 0, 'Volume_Ratio': 1.0,
    'OBV_Trend': "NEUTRAL", 'Price_Volume_Div': "NEUTRAL",
    'Dist_Soporte': 1.0, 'Dist_Resistencia': 1.0,
    'Fuerza_Soporte': 0, 'Fuerza_Resistencia': 0, 'Posicion_SR': "N/A",
    'Williams_Current': 0, 'Williams_Divergence': "NEUTRAL",
    'Williams_Div_Strength': 0, 'Williams_Combined': "NEUTRAL",
    'Volatility_ATR': 0,
    'BQ_Growth_Score': 50, 'BQ_Consistency_Score': 50, 'BQ_Margins_Score': 50,
    'BQ_FCF_Score': 50, 'BQ_Debt_Score': 50, 'BQ_ROIC_Score': 50, 'BQ_Moat_Score': 50,
    'BQ_Final': 50,
    'Rev_CAGR_3Y': "N/A", 'EPS_CAGR_3Y': "N/A", 'Margin_Stability': "N/A",
    'FCF_Consistency': "N/A", 'Current_Ratio': "N/A", 'Quick_Ratio': "N/A",
    'Net_Debt_FCF': "N/A", 'Debt_Growth_YoY': "N/A", 'ROIC': "N/A", 'ROE': "N/A",
    'Moat_Score_Raw': 0,
    'VAL_PEG_Score': 50, 'VAL_PE_Score': 50, 'VAL_FCFYield_Score': 50,
    'VAL_EVEBITDA_Score': 50, 'VAL_Analyst_Score': 50, 'VAL_Historical_Score': 50,
    'VAL_Final': 50,
    'Relative_PE': "N/A", 'Relative_EV_EBITDA': "N/A",
    'Historical_PE_Avg': "N/A", 'Historical_PE_Min': "N/A", 'Historical_PE_Max': "N/A",
    'TIM_Tendencia_Score': 50, 'TIM_Soportes_Score': 50, 'TIM_Williams_Score': 50,
    'TIM_Volumen_Score': 50, 'TIM_Volatilidad_Score': 50, 'TIM_Final': 50,
    'Final_Score': 50, 'BQ_Class': "MEDIA", 'VAL_Class': "MEDIA", 'TIM_Class': "MEDIA",
    'Entry_Status': "NEUTRAL", 'Conviction': "NEUTRAL", 'Allocation': "0%",
    'Strategy': "largo_plazo", 'Price_Actual': 0, 'Sector': "N/A", 'Ticker':"", 'Alertas': "OK"
}


# En write_to_sheets_v7, excluir columnas legacy
EXCLUDE_FROM_WRITE = {'Ticker', 'Price_Actual', 'Sector'}

    


# ==============================================================
# 🧠 PROCESAMIENTO v7.0 — FUNCIÓN ATÓMICA POR TICKER
# ==============================================================

def process_ticker_v7(symbol: str, strategy: str = "largo_plazo") -> Tuple[str, Dict[str, Any], List[str]]:
    """
    Procesa un ticker con la nueva arquitectura de 3 capas
    """
    results = {key: v7_defaults[key] for key in v7_ranges.keys()}
    alerts = []
    results['Alertas'] = "OK"  # Default, will be overwritten at end
    
    ticker, info, hist = get_cached_ticker_data(symbol)
    current_price = info.get("currentPrice", 0)
    results['Price_Actual'] = current_price if current_price else 0
    
    sector = info.get("sector") or get_sector(info, symbol)
    results['Sector'] = sector
    
    # ═══════════════════════════════════════════════════════
    # RECOLECCIÓN DE DATOS (reusa funciones v6.x)
    # ═══════════════════════════════════════════════════════
    
    # Datos básicos
    rev_growth = get_rev_growth(info, symbol) or 0
    gross_margin, operating_margin = get_margins(info, symbol)
    profit_margin = get_profit_margin(info, symbol) or 0
    fcf = get_fcf(info, ticker, symbol) or 0
    net_income = get_net_income(info, ticker, symbol) or 0
    total_debt = get_total_debt(info, symbol) or 0
    ebitda = get_ebitda(info, symbol) or 0
    market_cap = info.get("marketCap", 0)
    peg = get_peg(info, symbol)
    forward_pe = get_forward_pe(info, symbol)
    trailing_eps = info.get("trailingEps", 0)
    analyst_target = get_target_price(info, ticker, symbol)
    analyst_count = get_analyst_count(info, symbol)

    # FCF metrics
    fcf_yield = fcf / market_cap if market_cap and market_cap > 0 and fcf else 0
    fcf_margin = fcf / info.get("totalRevenue", 0) if info.get("totalRevenue") and fcf else 0
    fcf_ni_ratio = fcf / net_income if net_income and net_income != 0 and fcf else 0
    
    # Debt metrics
    interest_coverage = 0
    try:
        if ticker:
            f = ticker.financials
            for label in ['Ebit', 'Operating Income']:
                if label in f.index:
                    ebit = safe_float(f.loc[label].iloc[0])
                    il = [i for i in f.index if 'Interest Expense' in i]
                    if il:
                        interest = abs(safe_float(f.loc[il[0]].iloc[0]))
                        if interest > 0:
                            interest_coverage = ebit / interest
    except Exception:
        pass

    # Debt/Equity
    debt_equity = safe_float(info.get("debtToEquity"), 999) / 100 if info.get("debtToEquity") else 999

    # Debt/EBITDA
    debt_ebitda = (total_debt / ebitda) if ebitda and ebitda > 0 else 999

    # FCF Growth YoY
    fcf_growth = 0
    try:
        if ticker and ticker.cashflow is not None and not ticker.cashflow.empty and "Free Cash Flow" in ticker.cashflow.index:
            cf_vals = ticker.cashflow.loc["Free Cash Flow"].dropna()
            if len(cf_vals) >= 2:
                fcf_growth = (cf_vals.iloc[0] - cf_vals.iloc[1]) / abs(cf_vals.iloc[1]) if cf_vals.iloc[1] != 0 else 0
    except Exception:
        pass

    # Ensure v6.x metric keys exist for consistency score fallback
    results.setdefault('Beat Rate', 0)
    results.setdefault('Weighted Consistency', 0)
    results.setdefault('Surprise Trend', "N/A")
    results.setdefault('Worst Miss', 0)
    results.setdefault('Debt/EBITDA', debt_ebitda)
    results.setdefault('Interest Coverage', interest_coverage)
    results.setdefault('Debt/Equity', debt_equity)
    results.setdefault('FCF Growth', fcf_growth)
    results.setdefault('SMA_Trend', 'LATERAL')
    results.setdefault('Dist a Soporte %', 1.0)
    results.setdefault('Dist a Resistencia %', 1.0)
    results.setdefault('Fuerza Soporte', 0)
    results.setdefault('Fuerza Resistencia', 0)
    results.setdefault('ATR Threshold %', 0.02)
    results.setdefault('Posición S/R', 'N/A')
    results.setdefault('Williams %R (Current)', 0)
    results.setdefault('Williams Divergence', 'NEUTRAL')
    results.setdefault('Williams Div Strength', 0)
    results.setdefault('Williams Combined Signal', 'NEUTRAL')
    results.setdefault('Volume Ratio', 1.0)
    results.setdefault('OBV Trend', 'NEUTRAL')
    results.setdefault('Price-Volume Div', 'NEUTRAL')
    results.setdefault('Volatility_ATR', 0)
        # Guardar métricas base para que el uploader las encuentre
    results['Gross_Margin'] = gross_margin if gross_margin else 0
    results['Operating_Margin'] = operating_margin if operating_margin else 0
    results['FCF_Margin'] = fcf_margin if fcf_margin else 0
    results['Net_Income'] = net_income if net_income else "N/A"
    results['Total_Debt'] = total_debt if total_debt else "N/A"
    results['EBITDA'] = ebitda if ebitda else "N/A"
    results['Profit_Margin'] = profit_margin if profit_margin else 0
    results['Trailing_EPS'] = trailing_eps if trailing_eps else "N/A"
    results['Current_Price'] = current_price if current_price else 0
    results['FCF_Growth'] = fcf_growth if fcf_growth else 0
    results['Debt_Equity'] = debt_equity if debt_equity != 999 else "N/A"
    results['Market_Cap'] = market_cap if market_cap else "N/A"
 # ═══════════════════════════════════════════════════════
    # CAPA 1 — BUSINESS QUALITY: CÁLCULO DE SUB-SCORES
    # ═══════════════════════════════════════════════════════
    
    # 1. Growth Score
    rev_cagr_3y = get_revenue_3y_cagr(ticker, symbol)
    eps_cagr_3y = get_eps_3y_cagr(ticker, symbol)
    revenue_estimate = get_revenue_estimate(ticker, symbol)
    eps_estimate = get_eps_estimate(ticker, symbol)
    
    results['Rev_CAGR_3Y'] = rev_cagr_3y if rev_cagr_3y is not None else "N/A"
    results['EPS_CAGR_3Y'] = eps_cagr_3y if eps_cagr_3y is not None else "N/A"
    
    growth_score = score_capa1_growth(rev_growth, rev_cagr_3y, eps_cagr_3y, revenue_estimate, eps_estimate)
    results['BQ_Growth_Score'] = growth_score
    
    # 2. Consistency Score (reusa datos de earnings history ya calculados)
    beat_rate = results.get('Beat Rate', 0)
    weighted_cons = results.get('Weighted Consistency', 0)
    surprise_trend = results.get('Surprise Trend', "N/A")
    worst_miss = results.get('Worst Miss', 0)
    consistency_score = score_capa1_consistency(beat_rate, weighted_cons, surprise_trend, worst_miss)
    results['BQ_Consistency_Score'] = consistency_score
    
    # 3. Margins Score
    margin_stability = get_margin_stability(ticker, symbol, "operating", info)
    results['Margin_Stability'] = margin_stability if margin_stability is not None else "N/A"
    margins_score = score_capa1_margins(gross_margin, operating_margin, profit_margin, margin_stability)
    results['BQ_Margins_Score'] = margins_score
    
    # 4. FCF Quality Score
    fcf_consistency = get_fcf_consistency(ticker, symbol)
    results['FCF_Consistency'] = fcf_consistency if fcf_consistency is not None else "N/A"
    fcf_score = score_capa1_fcf_quality(fcf_yield, fcf_margin, fcf_growth, fcf_ni_ratio, fcf_consistency)
    results['BQ_FCF_Score'] = fcf_score
    
    # 5. Debt Health Score
    current_ratio = get_current_ratio(ticker, symbol)
    quick_ratio = get_quick_ratio(ticker, symbol)
    net_debt_fcf = get_net_debt_to_fcf(ticker, symbol, fcf, info)
    debt_growth_yoy = get_debt_growth_yoy(ticker, symbol)
    
    results['Current_Ratio'] = current_ratio if current_ratio is not None else "N/A"
    results['Quick_Ratio'] = quick_ratio if quick_ratio is not None else "N/A"
    results['Net_Debt_FCF'] = net_debt_fcf if net_debt_fcf is not None else "N/A"
    results['Debt_Growth_YoY'] = debt_growth_yoy if debt_growth_yoy is not None else "N/A"
    
    # Calcular debt/ebitda para el score
    debt_ebitda_val = safe_float(results.get('Debt/EBITDA', "999"), 999)
    interest_coverage_val = safe_float(results.get('Interest Coverage', 0))
    debt_equity_val = safe_float(results.get('Debt/Equity', "999"), 999)
    
    debt_score = score_capa1_debt_health(debt_ebitda_val, interest_coverage_val, debt_equity_val,
                                          current_ratio, quick_ratio, net_debt_fcf, debt_growth_yoy)
    results['BQ_Debt_Score'] = debt_score
    
    # 6. ROIC/ROE Score
    roic = get_roic(ticker, symbol, info)
    roe = get_roe(ticker, symbol, info)
    results['ROIC'] = roic if roic is not None else "N/A"
    results['ROE'] = roe if roe is not None else "N/A"
    roic_score = score_capa1_roic_roe(roic, roe)
    results['BQ_ROIC_Score'] = roic_score
    
    # 7. Moat Score
    moat_indicators = get_moat_indicators(ticker, symbol, info, hist)
    moat_score = moat_indicators.get('moat_score', 0)
    results['Moat_Score_Raw'] = moat_score
    moat_score_final = score_capa1_moat(moat_score)
    results['BQ_Moat_Score'] = moat_score_final
    
    # Business Quality Final
    bq_final = compute_capa1_business_quality(growth_score, consistency_score, margins_score,
                                               fcf_score, debt_score, roic_score, moat_score_final)
    results['BQ_Final'] = bq_final
    
    # ═══════════════════════════════════════════════════════
    # CAPA 2 — VALUATION: CÁLCULO DE SUB-SCORES
    # ═══════════════════════════════════════════════════════
    
    # Métricas históricas
    hist_pe = get_historical_pe_ratio(symbol, hist, trailing_eps)
    hist_ev_ebitda = get_historical_ev_ebitda(symbol, hist, info)
    hist_fcf_yield = get_historical_fcf_yield(symbol, info, fcf)
    # Guardar métricas de valoración actual
    results['Current_PE'] = hist_pe.get('current_pe') if hist_pe.get('current_pe') is not None else "N/A"
    results['EV_EBITDA_Current'] = hist_ev_ebitda.get('current') if hist_ev_ebitda.get('current') is not None else "N/A"
    results['FCF_Yield_Current'] = hist_fcf_yield.get('current') if hist_fcf_yield.get('current') is not None else "N/A"
    results['FCF_Yield_Avg_5Y'] = hist_fcf_yield.get('avg_5y') if hist_fcf_yield.get('avg_5y') is not None else "N/A"
    results['Analyst_Target'] = analyst_target if analyst_target else "N/A"
    results['Analyst_Count'] = analyst_count if analyst_count else 0
    results['Upside'] = (analyst_target - current_price) / current_price if analyst_target and current_price else 0
    results['Relative_PE'] = hist_pe.get('relative_pe') if hist_pe.get('relative_pe') is not None else "N/A"
    results['Relative_EV_EBITDA'] = hist_ev_ebitda.get('relative') if hist_ev_ebitda.get('relative') is not None else "N/A"
    results['Historical_PE_Avg'] = hist_pe.get('avg_pe_5y') if hist_pe.get('avg_pe_5y') is not None else "N/A"
    results['Historical_PE_Min'] = hist_pe.get('min_pe_5y') if hist_pe.get('min_pe_5y') is not None else "N/A"
    results['Historical_PE_Max'] = hist_pe.get('max_pe_5y') if hist_pe.get('max_pe_5y') is not None else "N/A"
    
    # 1. PEG Score
    peg_score = score_capa2_peg(peg)
    results['VAL_PEG_Score'] = peg_score
    
    # 2. Forward PE Score
    pe_score = score_capa2_forward_pe(forward_pe, sector)
    results['VAL_PE_Score'] = pe_score
    
    # 3. FCF Yield Score
    fcf_yield_score = score_capa2_fcf_yield(fcf_yield)
    results['VAL_FCFYield_Score'] = fcf_yield_score
    
    # 4. EV/EBITDA Score
    ev_ebitda = hist_ev_ebitda.get('current')
    ev_ebitda_score = score_capa2_ev_ebitda(ev_ebitda)
    results['VAL_EVEBITDA_Score'] = ev_ebitda_score
    
    # 5. Analyst Upside Score
    analyst_score = score_capa2_analyst_upside(current_price, analyst_target, analyst_count)
    results['VAL_Analyst_Score'] = analyst_score
    
    # 6. Historical Valuation Score
    historical_score = score_capa2_historical_valuation(
        hist_pe.get('relative_pe'),
        hist_ev_ebitda.get('relative'),
        hist_fcf_yield.get('relative')
    )
    results['VAL_Historical_Score'] = historical_score
    
    # Valuation Final
    val_final = compute_capa2_valuation(peg_score, pe_score, fcf_yield_score, 
                                         ev_ebitda_score, analyst_score, historical_score)
    results['VAL_Final'] = val_final
    
    # ═══════════════════════════════════════════════════════
    # CAPA 3 — TIMING: CÁLCULO DE SUB-SCORES
    # ═══════════════════════════════════════════════════════
    
    # Calcular métricas técnicas primero
    sma_trend = "LATERAL"
    sma_slope = 0
    vol_atr = 0
    vol_ratio = 1.0
    obv_trend = "NEUTRAL"
    price_vol_div = "NEUTRAL"
    dist_support = 1.0
    dist_resistance = 1.0
    fuerza_soporte = 0
    fuerza_resistencia = 0
    posicion_sr = "N/A"
    wr_current = 0
    wr_div = "NEUTRAL"
    wr_div_strength = 0
    wr_combined = "NEUTRAL"
    
    try:
        if hist is not None and not hist.empty and len(hist) >= 250:
            # SMA Trend
            sma_50 = hist['Close'].tail(50).mean()
            sma_200 = hist['Close'].tail(200).mean()
            if sma_50 > sma_200 * 1.05:
                sma_trend = "ALCISTA FUERTE"
            elif sma_50 > sma_200:
                sma_trend = "ALCISTA"
            elif sma_50 < sma_200 * 0.95:
                sma_trend = "BAJISTA FUERTE"
            elif sma_50 < sma_200:
                sma_trend = "BAJISTA"
            
            # SMA Slope
            sma_200_current = hist['Close'].tail(200).mean()
            sma_200_50d_ago = hist['Close'].iloc[-250:-50].mean()
            sma_slope = (sma_200_current - sma_200_50d_ago) / sma_200_50d_ago if sma_200_50d_ago > 0 else 0
            
            # Volatility ATR
            high_low = hist['High'] - hist['Low']
            high_close = abs(hist['High'] - hist['Close'].shift())
            low_close = abs(hist['Low'] - hist['Close'].shift())
            tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
            vol_atr = tr.tail(14).mean() / hist['Close'].iloc[-1] if len(tr) >= 14 else 0
            
            # Volume Ratio
            avg_vol = hist['Volume'].tail(20).mean()
            vol_ratio = hist['Volume'].iloc[-1] / avg_vol if avg_vol > 0 else 1.0
    except Exception as e:
        logger.warning(f"{symbol}: Error calculando métricas técnicas: {e}")
    
    # Guardar métricas técnicas en results
    results['SMA_Trend'] = sma_trend
    results['SMA_Slope'] = sma_slope
    results['Volatility_ATR'] = vol_atr
    results['Volume_Ratio'] = vol_ratio
    results['OBV_Trend'] = obv_trend
    results['Price_Volume_Div'] = price_vol_div
    results['Dist_Soporte'] = dist_support
    results['Dist_Resistencia'] = dist_resistance
    results['Fuerza_Soporte'] = fuerza_soporte
    results['Fuerza_Resistencia'] = fuerza_resistencia
    results['Posicion_SR'] = posicion_sr
    results['Williams_Current'] = wr_current
    results['Williams_Divergence'] = wr_div
    results['Williams_Div_Strength'] = wr_div_strength
    results['Williams_Combined'] = wr_combined
    
    # 1. Tendencia Score
    tendencia_score = score_capa3_tendencia(sma_trend, sma_slope)
    results['TIM_Tendencia_Score'] = tendencia_score
    
    # 2. Soportes/Resistencias Score
    dist_support = results.get('Dist a Soporte %', 1.0)
    dist_resistance = results.get('Dist a Resistencia %', 1.0)
    fuerza_soporte = results.get('Fuerza Soporte', 0)
    fuerza_resistencia = results.get('Fuerza Resistencia', 0)
    atr_threshold = results.get('ATR Threshold %', 0.02)
    posicion_sr = results.get('Posición S/R', 'N/A')
    
    soportes_score = score_capa3_soportes(dist_support, dist_resistance, fuerza_soporte,
                                          fuerza_resistencia, atr_threshold, posicion_sr)
    results['TIM_Soportes_Score'] = soportes_score
    
    # 3. Williams %R Score
    wr_current = results.get('Williams %R (Current)', 0)
    div_signal = results.get('Williams Divergence', 'NEUTRAL')
    div_strength = results.get('Williams Div Strength', 0)
    combined_signal = results.get('Williams Combined Signal', 'NEUTRAL')
    
    williams_score = score_capa3_williams(wr_current, div_signal, div_strength, combined_signal)
    results['TIM_Williams_Score'] = williams_score
    
    # 4. Volumen Score
    vol_ratio = results.get('Volume Ratio', 1.0)
    obv_trend = results.get('OBV Trend', 'NEUTRAL')
    price_vol_div = results.get('Price-Volume Div', 'NEUTRAL')
    
    volumen_score = score_capa3_volumen(vol_ratio, obv_trend, price_vol_div)
    results['TIM_Volumen_Score'] = volumen_score
    
    # 5. Volatilidad Score
    vol_atr = results.get('Volatility_ATR', 0)
    volatilidad_score = score_capa3_volatilidad(vol_atr)
    results['TIM_Volatilidad_Score'] = volatilidad_score
    
    # Timing Final
    timing_final = compute_capa3_timing(tendencia_score, soportes_score, williams_score,
                                         volumen_score, volatilidad_score)
    results['TIM_Final'] = timing_final
    
    # ═══════════════════════════════════════════════════════
    # OUTPUT FINAL: MATRIZ DE DECISIONES v7.0
    # ═══════════════════════════════════════════════════════
    
    final_score, bq_class, val_class, tim_class, entry_status, conviction = compute_final_score_v7(
        bq_final, val_final, timing_final, strategy
    )
    
    results['Final_Score'] = final_score
    results['BQ_Class'] = bq_class
    results['VAL_Class'] = val_class
    results['TIM_Class'] = tim_class
    results['Entry_Status'] = entry_status
    results['Conviction'] = conviction
    results['Allocation'] = determine_allocation(final_score, conviction)
    results['Strategy'] = strategy
    
    # Alertas v7.0
    if bq_class in ["MUY ALTA", "ALTA"] and val_class in ["MUY ALTA", "ALTA"] and tim_class in ["MUY ALTA", "ALTA"]:
        alerts.append("🎯 SETUP IDEAL: Compra agresiva")
    elif "WATCHLIST" in conviction:
        alerts.append("⏳ WATCHLIST: Empresa excelente, esperar mejor timing")
    elif "BUFFETT" in conviction:
        alerts.append("🦉 BUFFETT ZONE: Calidad excepcional, timing desfavorable")
    elif "SWING ONLY" in conviction:
        alerts.append("⚡ SWING: Solo para trading corto plazo")
    elif "ESPECULACIÓN" in conviction:
        alerts.append("🎲 ALTO RIESGO: Especulación pura")
    
    if bq_class in ["MUY BAJA", "BAJA"]:
        alerts.append("⚠️ CALIDAD BAJA: Revisar fundamentales")
    
    results['Alertas'] = " | ".join(alerts) if alerts else "OK"
    
    logger.info(f"✅ {symbol} v7.0 | BQ:{bq_final:.1f} | VAL:{val_final:.1f} | TIM:{timing_final:.1f} | FINAL:{final_score:.1f} | {conviction}")
    
    return symbol, results, alerts


# ==============================================================
# 🚀 ORQUESTACIÓN PRINCIPAL v7.0
# ==============================================================

def write_to_sheets_v7(worksheet, all_results: Dict[str, List]):
    """Escribe los resultados v7.0 en Google Sheets con la nueva arquitectura de 3 capas"""
    logger.info("--- Escribiendo datos v7.0 en Google Sheets ---")

    # Sanitizar todo antes de enviar
    clean_results = {}
    for metric, data_list in all_results.items():
        clean_results[metric] = [[sanitize_for_sheets(v) for v in row] for row in data_list]

    try:
        batch_data = [{'range': v7_ranges[m], 'values': clean_results[m]} 
                      for m in clean_results 
                      if m in v7_ranges and m not in EXCLUDE_FROM_WRITE]
        worksheet.batch_update(batch_data, value_input_option='USER_ENTERED')
        logger.info(f"✅ {len(batch_data)} rangos v7.0 actualizados exitosamente.")
    except Exception as e:
        logger.error(f"❌ Error en batch v7.0: {e}. Intentando individual...")
        for metric, data_list in clean_results.items():
            if metric not in v7_ranges or metric in EXCLUDE_FROM_WRITE:
                continue
            try:
                worksheet.update(range_name=v7_ranges[metric], values=data_list)
                logger.info(f"  ✓ '{metric}' ({len(data_list)} filas)")
                time.sleep(1.2)
            except Exception as e2:
                logger.error(f"  ✗ '{metric}': {e2}")

def main_v7():
    """Orquestación principal de la arquitectura v7.0"""
    # Limpiar caches de sesión
    with _av_cache_lock:
        _av_cache.clear()
    with _ticker_data_lock:
        _ticker_data_cache.clear()
    with _fmp_profile_lock:
        _fmp_profile_cache.clear()
    with _historical_pe_lock:
        _historical_pe_cache.clear()

    fmp_available = test_fmp_connectivity()
    if not fmp_available:
        logger.warning("⚠️ FMP no disponible. Modo DEGRADADO (yfinance + Finnhub only).")

    try:
        sh = gc.open(SPREADSHEET_NAME)
        
        # Usar la hoja principal (compatibilidad) o crear nueva pestaña v7
        try:
            worksheet = sh.worksheet(WORKSHEET_NAME)
        except gspread.exceptions.WorksheetNotFound:
            worksheet = sh.add_worksheet(title=WORKSHEET_NAME, rows=200, cols=50)
        
        # Headers v7.0
        headers_v7 = [
            "Ticker", "BQ_Final", "VAL_Final", "TIM_Final", "Final_Score",
            "BQ_Class", "VAL_Class", "TIM_Class", "Entry_Status", "Conviction",
            "Allocation", "Strategy", "Alertas"
        ]
        # Headers written below after data processing

        # Crear/obtener pestaña de scores v7
        try:
            scores_v7_sheet = sh.worksheet(SCORESHEET_NAME)
        except gspread.exceptions.WorksheetNotFound:
            scores_v7_sheet = sh.add_worksheet(title=SCORESHEET_NAME, rows=200, cols=30)
            scores_v7_sheet.update(range_name='A1:M1', values=[headers_v7])
            

        tickers_list = worksheet.get(v7_ranges['Ticker'])
        raw_symbols = [item[0] for item in tickers_list if item and item[0]]
        symbols = [s.strip().upper() for s in raw_symbols if validate_ticker(s)]
        invalid = [s for s in raw_symbols if not validate_ticker(s)]
        if invalid:
            logger.warning(f"Tickers inválidos omitidos: {invalid}")

        if not symbols:
            logger.error("No se encontraron tickers válidos.")
            return

        logger.info(f"🔍 v7.0 | Tickers válidos: {symbols}")
        logger.info(f"🏗️  Arquitectura: Business Quality 50% | Valuation 30% | Timing 20%")
        
        all_results = {key: [] for key in v7_ranges.keys()}

        ticker_results = {}
        
        # Leer estrategia por ticker (columna HS) si existe
        strategy_column = worksheet.get(f'HS{START_ROW}:HS{END_ROW}')
        strategy_map = {}
        for i, val in enumerate(strategy_column):
            if val and val[0] and val[0].strip().lower() in ["largo_plazo", "swing", "crisis"]:
                strategy_map[i] = val[0].strip().lower()
        
        with ThreadPoolExecutor(max_workers=4) as executor:
            future_to_idx = {
                executor.submit(process_ticker_v7, sym, strategy_map.get(idx, "largo_plazo")): idx
                for idx, sym in enumerate(symbols)
            }
            for future in as_completed(future_to_idx):
                idx = future_to_idx[future]
                try:
                    symbol, results, alerts = future.result()
                    ticker_results[idx] = (symbol, results, alerts)
                    logger.info(f"✅ {symbol} v7.0 completado (posición {idx})")
                except Exception as e:
                    logger.error(f"{symbols[idx]}: Error fatal v7.0: {e}")
                    # Fallback a defaults v7
                    default_results = {key: v7_defaults[key] for key in v7_ranges.keys()}
                    default_results['Ticker'] = symbols[idx]
                    default_results['Alertas'] = f"ERROR: {str(e)[:50]}"
                    ticker_results[idx] = (symbols[idx], default_results, [])

        for idx in range(len(symbols)):
            if idx in ticker_results:
                _, results, _ = ticker_results[idx]
            else:
                logger.error(f"Ticker posición {idx} ({symbols[idx]}) sin resultados v7.0")
                results = {key: v7_defaults[key] for key in v7_ranges.keys()}
                results['Ticker'] = symbols[idx]
                results['Alertas'] = "ERROR: Sin resultados"
            
            for key in v7_ranges.keys():
                all_results[key].append([sanitize_for_sheets(results[key])])

        # Escribir en hoja principal (nuevas columnas GA-HS)
        write_to_sheets_v7(worksheet, all_results)

        # Escribir en pestaña de scores v7
        score_data_v7 = []
        for i, sym in enumerate(symbols):
            score_data_v7.append([
                sym,
                sanitize_for_sheets(all_results['BQ_Final'][i][0]),
                sanitize_for_sheets(all_results['VAL_Final'][i][0]),
                sanitize_for_sheets(all_results['TIM_Final'][i][0]),
                sanitize_for_sheets(all_results['Final_Score'][i][0]),
                sanitize_for_sheets(all_results['BQ_Class'][i][0]),
                sanitize_for_sheets(all_results['VAL_Class'][i][0]),
                sanitize_for_sheets(all_results['TIM_Class'][i][0]),
                sanitize_for_sheets(all_results['Entry_Status'][i][0]),
                sanitize_for_sheets(all_results['Conviction'][i][0]),
                sanitize_for_sheets(all_results['Allocation'][i][0]),
                sanitize_for_sheets(all_results['Strategy'][i][0]),
                sanitize_for_sheets(ticker_results[i][1].get('Alertas', 'OK') if i in ticker_results else 'OK'),
            ])
        
        scores_v7_sheet.update(range_name='A1:M1', values=[headers_v7])
        logger.info(f"✅ Pestaña '{SCORESHEET_NAME}' actualizada con {len(score_data_v7)} tickers v7.0.")
        
    
        # Backup a Firestore v7.0 (nueva arquitectura 3 capas)
        try:
            from firebase_uploader_v7 import upload_v7_to_firestore
            upload_v7_to_firestore(all_results, symbols)
        except Exception as e:
            logger.error(f"❌ Error subiendo v7.0 a Firestore: {e}")

        logger.info("🎉 ¡Proceso v7.0 completado! Arquitectura de 3 capas activa.")

    except gspread.exceptions.SpreadsheetNotFound:
        logger.error(f'❌ Hoja "{SPREADSHEET_NAME}" no encontrada.')
    except Exception as e:
        import traceback
        logger.error(f"❌ Error inesperado v7.0: {e}")
        traceback.print_exc()


# ==============================================================
# 🔄 COMPATIBILIDAD: MODO HÍBRIDO v6.x + v7.0
# ==============================================================

def main_hybrid():
    """
    Modo híbrido: Ejecuta v6.x (8 principios) Y v7.0 (3 capas) en paralelo.
    Cada uno sube a su propia colección de Firestore.
    """
    logger.info("🔄 MODO HÍBRIDO: Ejecutando v6.x + v7.0")
    
    # Ejecutar v6.x primero → sube a 'portafolio' (P1-P8)
    try:
        from main_V6 import main as main_v6
        main_v6()
        logger.info("✅ v6.x completado — datos en colección 'portafolio'")
    except Exception as e:
        logger.error(f"❌ v6.x falló: {e}")
    
    # Luego ejecutar v7.0 → sube a 'portafolio_v7' (BQ/VAL/TIM)
    try:
        main_v7()
        logger.info("✅ v7.0 completado — datos en colección 'portafolio_v7'")
    except Exception as e:
        logger.error(f"❌ v7.0 falló: {e}")

# ==============================================================
# 📋 ENTRY POINT
# ==============================================================

if __name__ == "__main__":
    # Por defecto ejecuta v7.0. Para modo híbrido, cambiar a main_hybrid()
    main_v7()