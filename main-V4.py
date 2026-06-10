################################-----------------------PORTAFOLIO E.T.H-----------------------##########################################
# VERSIÓN MEJORADA 4: Eliminación de Polygon.io, fallback refactorizado, logging estructurado,
# rate limiting, validación de tickers y tipos de datos consistentes.
# Fuentes: yfinance → Alpha Vantage → FMP → finnhub
# Instalar dependencias: pip install yfinance gspread google-auth requests pandas numpy finnhub-python

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
from dotenv import load_dotenv
from collections import deque

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
# ⚙️  CONFIGURACIÓN DE APIs
# ==============================================================
load_dotenv()

JSON_KEY_FILE = "principios.json"

def _require_env(name: str) -> str:
    """Falla explícitamente si la variable de entorno no está definida."""
    value = os.getenv(name)
    if not value:
        raise EnvironmentError(
            f"\n❌ Variable de entorno '{name}' no encontrada.\n"
            f"   → En GitHub Actions: agregala como Secret en Settings > Secrets.\n"
            f"   → En local: ejecutá `export {name}=tu_clave` antes de correr el script.\n"
        )
    return value

ALPHA_VANTAGE_KEY = _require_env("ALPHA_VANTAGE_KEY")
FMP_KEY           = _require_env("FMP_KEY")
FINNHUB_KEY       = _require_env("FINNHUB_KEY")

# Google Sheets
spreadsheet_name = "Portafolio Financiero"
worksheet_name   = "7 PRINCIPIOS"
start_row = 7
end_row   = 27

# ==============================================================
# 🔑 AUTENTICACIÓN GOOGLE
# ==============================================================
try:
    scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    creds  = Credentials.from_service_account_file(JSON_KEY_FILE, scopes=scopes)
    gc     = gspread.authorize(creds)
    logger.info("✅ Autenticación con Google exitosa.")
except FileNotFoundError:
    logger.error(f"❌ No se encontró el archivo '{JSON_KEY_FILE}'.")
    raise
except Exception as e:
    logger.error(f"❌ Error de autenticación: {e}")
    raise

# ==============================================================
# ⏱️ RATE LIMITER
# ==============================================================
class RateLimiter:
    """Limitador simple basado en ventana de tiempo."""
    def __init__(self, max_calls: int, period: int, name: str = "API"):
        self.max_calls = max_calls
        self.period = period
        self.name = name
        self.calls = deque()

    def wait_if_needed(self):
        now = time.time()
        # Limpiar llamadas antiguas
        while self.calls and now - self.calls[0] > self.period:
            self.calls.popleft()
        if len(self.calls) >= self.max_calls:
            sleep_time = self.period - (now - self.calls[0]) + 1
            if sleep_time > 0:
                logger.info(f"⏱️  {self.name} rate limit: durmiendo {sleep_time:.1f}s")
                time.sleep(sleep_time)
        self.calls.append(time.time())

# Limitadores por API (ajustar según plan contratado)
av_limiter   = RateLimiter(max_calls=5,  period=60, name="AlphaVantage")
fmp_limiter  = RateLimiter(max_calls=300, period=60, name="FMP")
finn_limiter = RateLimiter(max_calls=60,  period=60, name="Finnhub")

# ==============================================================
# 🌐 CLIENTES DE APIs EXTERNAS
# ==============================================================

AV_BASE   = "https://www.alphavantage.co/query"
FMP_BASE  = "https://financialmodelingprep.com/api/v3"
FMP_V4    = "https://financialmodelingprep.com/api/v4"
FINN_BASE = "https://finnhub.io/api/v1"

def av_get(function, symbol, extra_params=None):
    """Consulta Alpha Vantage con rate limiting y manejo de errores."""
    av_limiter.wait_if_needed()
    try:
        params = {"function": function, "symbol": symbol, "apikey": ALPHA_VANTAGE_KEY}
        if extra_params:
            params.update(extra_params)
        r = requests.get(AV_BASE, params=params, timeout=15)
        data = r.json()
        if "Note" in data or "Information" in data:
            logger.warning(f"Alpha Vantage rate limit mensaje para {symbol}: {data.get('Note', data.get('Information'))}")
            return None
        return data
    except Exception as e:
        logger.warning(f"Alpha Vantage error ({function}/{symbol}): {e}")
        return None

def fmp_get(endpoint, version="v3", params=None):
    """Consulta FMP con rate limiting y manejo de errores."""
    fmp_limiter.wait_if_needed()
    try:
        base = FMP_BASE if version == "v3" else FMP_V4
        p = params or {}
        p["apikey"] = FMP_KEY
        r = requests.get(f"{base}{endpoint}", params=p, timeout=15)
        return r.json()
    except Exception as e:
        logger.warning(f"FMP error ({endpoint}): {e}")
        return None

def finn_get(endpoint, params=None):
    """Consulta Finnhub con rate limiting y manejo de errores."""
    finn_limiter.wait_if_needed()
    try:
        p = params or {}
        p["token"] = FINNHUB_KEY
        r = requests.get(f"{FINN_BASE}{endpoint}", params=p, timeout=15)
        return r.json()
    except Exception as e:
        logger.warning(f"Finnhub error ({endpoint}): {e}")
        return None

# ==============================================================
# 🔄 HELPER DE FALLBACK GENÉRICO
# ==============================================================

def fetch_with_fallbacks(metric_name, primary_val, *sources):
    """
    Intenta obtener un valor desde fuentes secuenciales.

    Args:
        metric_name: nombre de la métrica para logging
        primary_val: valor primario (ej: desde yfinance info)
        sources: tuplas de (source_name, callable)

    Returns:
        El primer valor válido encontrado, o None.
    """
    if primary_val not in (None, 0, "", 0.0):
        return primary_val
    for src_name, fn in sources:
        try:
            result = fn()
            if result not in (None, 0, "", 0.0):
                logger.info(f"{metric_name}: fallback exitoso desde {src_name}")
                return result
        except Exception as e:
            logger.debug(f"{metric_name}: {src_name} falló: {e}")
    logger.warning(f"{metric_name}: no disponible en ninguna fuente")
    return None

# ==============================================================
# 📊 FUNCIONES DE MÉTRICAS (REFACTORIZADAS, SIN POLY_GET)
# ==============================================================

# ── Target Price ──────────────────────────────────────────────
def get_target_price(info, ticker_yf, symbol):
    val = info.get("targetMeanPrice")
    if val:
        return val
    try:
        data = fmp_get(f"/price-target-consensus/{symbol}")
        if data and isinstance(data, list) and data:
            return data[0].get("targetConsensus")
    except Exception as e:
        logger.warning(f"get_target_price FMP fallback: {e}")
    try:
        data = finn_get("/stock/price-target", {"symbol": symbol})
        if data:
            return data.get("targetMean")
    except Exception as e:
        logger.warning(f"get_target_price Finnhub fallback: {e}")
    try:
        data = av_get("ANALYST_PRICE_TARGET", symbol)
        if data and "data" in data:
            targets = [float(d.get("price_target", 0)) for d in data["data"] if d.get("price_target")]
            if targets:
                return round(sum(targets)/len(targets), 2)
    except Exception as e:
        logger.warning(f"get_target_price AV fallback: {e}")
    return None

# ── Analyst Count ─────────────────────────────────────────────
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
    except Exception as e:
        logger.warning(f"get_analyst_count Finnhub fallback: {e}")
    return 0

# ── Revenue Growth YoY ────────────────────────────────────────
def get_rev_growth(info, symbol):
    val = info.get("revenueGrowth")
    if val:
        return val
    try:
        data = fmp_get(f"/income-statement/{symbol}", params={"limit": 2})
        if data and isinstance(data, list) and len(data) >= 2:
            r1, r2 = data[0].get("revenue",0), data[1].get("revenue",0)
            if r2 and r2 != 0:
                return (r1 - r2) / abs(r2)
    except Exception as e:
        logger.warning(f"get_rev_growth FMP fallback: {e}")
    return 0

# ── Gross & Operating Margins ─────────────────────────────────
def get_margins(info, symbol):
    gross = info.get("grossMargins")
    oper  = info.get("operatingMargins")
    if gross and oper:
        return gross, oper
    try:
        data = fmp_get(f"/ratios/{symbol}", params={"limit": 1})
        if data and isinstance(data, list) and data:
            gross = gross or data[0].get("grossProfitMargin")
            oper  = oper  or data[0].get("operatingProfitMargin")
    except Exception as e:
        logger.warning(f"get_margins FMP fallback: {e}")
    try:
        data = av_get("INCOME_STATEMENT", symbol)
        if data and "annualReports" in data and data["annualReports"]:
            r = data["annualReports"][0]
            rev = float(r.get("totalRevenue", 0) or 0)
            if rev > 0:
                gp = float(r.get("grossProfit", 0) or 0)
                oi = float(r.get("operatingIncome", 0) or 0)
                gross = gross or (gp / rev)
                oper  = oper  or (oi / rev)
    except Exception as e:
        logger.warning(f"get_margins AV fallback: {e}")
    return gross or 0, oper or 0

# ── Forward PE ────────────────────────────────────────────────
def _fmp_forward_pe(symbol):
    data = fmp_get(f"/ratios/{symbol}", params={"limit": 1})
    if data and isinstance(data, list) and data:
        return data[0].get("priceEarningsRatio")
    return None

def _finnhub_forward_pe(symbol):
    data = finn_get("/stock/metric", {"symbol": symbol, "metric": "all"})
    if data and "metric" in data:
        return data["metric"].get("peForward")
    return None

def get_forward_pe(info, symbol):
    return fetch_with_fallbacks(
        "Forward PE", info.get("forwardPE"),
        ("FMP", lambda: _fmp_forward_pe(symbol)),
        ("Finnhub", lambda: _finnhub_forward_pe(symbol))
    )

# ── PEG Ratio ─────────────────────────────────────────────────
def _fmp_peg(symbol):
    data = fmp_get(f"/ratios/{symbol}", params={"limit": 1})
    if data and isinstance(data, list) and data:
        return data[0].get("priceEarningsToGrowthRatio")
    return None

def _finnhub_peg(symbol):
    data = finn_get("/stock/metric", {"symbol": symbol, "metric": "all"})
    if data and "metric" in data:
        return data["metric"].get("pegRatio")
    return None

def get_peg(info, symbol):
    peg = info.get("pegRatio") or info.get("trailingPegRatio")
    return fetch_with_fallbacks(
        "PEG", peg if peg and peg > 0 else None,
        ("FMP", lambda: _fmp_peg(symbol)),
        ("Finnhub", lambda: _finnhub_peg(symbol))
    )

# ── Free Cash Flow ────────────────────────────────────────────
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
    except Exception as e:
        logger.warning(f"get_fcf yfinance CF fallback: {e}")
    try:
        data = fmp_get(f"/cash-flow-statement/{symbol}", params={"limit": 1})
        if data and isinstance(data, list) and data:
            return data[0].get("freeCashFlow")
    except Exception as e:
        logger.warning(f"get_fcf FMP fallback: {e}")
    try:
        data = av_get("CASH_FLOW", symbol)
        if data and "annualReports" in data and data["annualReports"]:
            r = data["annualReports"][0]
            ocf = float(r.get("operatingCashflow", 0) or 0)
            capex = abs(float(r.get("capitalExpenditures", 0) or 0))
            return ocf - capex
    except Exception as e:
        logger.warning(f"get_fcf AV fallback: {e}")
    return 0

# ── Total Debt ────────────────────────────────────────────────
def _fmp_total_debt(symbol):
    data = fmp_get(f"/balance-sheet-statement/{symbol}", params={"limit": 1})
    if data and isinstance(data, list) and data:
        return data[0].get("totalDebt")
    return None

def _av_total_debt(symbol):
    data = av_get("BALANCE_SHEET", symbol)
    if data and "annualReports" in data and data["annualReports"]:
        r = data["annualReports"][0]
        st = float(r.get("shortTermDebt", 0) or 0)
        lt = float(r.get("longTermDebt", 0) or 0)
        return st + lt
    return None

def get_total_debt(info, symbol):
    return fetch_with_fallbacks(
        "Total Debt", info.get("totalDebt"),
        ("FMP", lambda: _fmp_total_debt(symbol)),
        ("AlphaVantage", lambda: _av_total_debt(symbol))
    ) or 0

# ── EBITDA ────────────────────────────────────────────────────
def _fmp_ebitda(symbol):
    data = fmp_get(f"/income-statement/{symbol}", params={"limit": 1})
    if data and isinstance(data, list) and data:
        return data[0].get("ebitda")
    return None

def _finnhub_ebitda(symbol):
    data = finn_get("/stock/metric", {"symbol": symbol, "metric": "all"})
    if data and "metric" in data:
        return data["metric"].get("ebitdaPerShare")
    return None

def get_ebitda(info, symbol):
    return fetch_with_fallbacks(
        "EBITDA", info.get("ebitda"),
        ("FMP", lambda: _fmp_ebitda(symbol)),
        ("Finnhub", lambda: _finnhub_ebitda(symbol))
    ) or 0

# ── Net Income ────────────────────────────────────────────────
def get_net_income(info, ticker_yf, symbol):
    val = info.get("netIncomeToCommon") or info.get("netIncome")
    if val:
        return val
    try:
        return ticker_yf.financials.loc['Net Income'].iloc[0]
    except Exception as e:
        logger.warning(f"get_net_income yfinance fallback: {e}")
    try:
        data = fmp_get(f"/income-statement/{symbol}", params={"limit": 1})
        if data and isinstance(data, list) and data:
            return data[0].get("netIncome")
    except Exception as e:
        logger.warning(f"get_net_income FMP fallback: {e}")
    try:
        data = av_get("INCOME_STATEMENT", symbol)
        if data and "annualReports" in data and data["annualReports"]:
            return float(data["annualReports"][0].get("netIncome", 0) or 0)
    except Exception as e:
        logger.warning(f"get_net_income AV fallback: {e}")
    return 0

# ── Profit Margin ─────────────────────────────────────────────
def _fmp_profit_margin(symbol):
    data = fmp_get(f"/ratios/{symbol}", params={"limit": 1})
    if data and isinstance(data, list) and data:
        return data[0].get("netProfitMargin")
    return None

def _finnhub_profit_margin(symbol):
    data = finn_get("/stock/metric", {"symbol": symbol, "metric": "all"})
    if data and "metric" in data:
        return data["metric"].get("netProfitMarginTTM")
    return None

def get_profit_margin(info, symbol):
    return fetch_with_fallbacks(
        "Profit Margin", info.get("profitMargins"),
        ("FMP", lambda: _fmp_profit_margin(symbol)),
        ("Finnhub", lambda: _finnhub_profit_margin(symbol))
    ) or 0

# ── Beta ──────────────────────────────────────────────────────
def _fmp_beta(symbol):
    data = fmp_get(f"/profile/{symbol}")
    if data and isinstance(data, list) and data:
        return data[0].get("beta")
    return None

def _finnhub_beta(symbol):
    data = finn_get("/stock/metric", {"symbol": symbol, "metric": "all"})
    if data and "metric" in data:
        return data["metric"].get("beta")
    return None

def get_beta(info, symbol):
    return fetch_with_fallbacks(
        "Beta", info.get("beta"),
        ("FMP", lambda: _fmp_beta(symbol)),
        ("Finnhub", lambda: _finnhub_beta(symbol))
    )

# ── Sector ────────────────────────────────────────────────────
def _fmp_sector(symbol):
    data = fmp_get(f"/profile/{symbol}")
    if data and isinstance(data, list) and data:
        return data[0].get("sector")
    return None

def _finnhub_sector(symbol):
    data = finn_get("/stock/profile2", {"symbol": symbol})
    if data:
        return data.get("finnhubIndustry")
    return None

def get_sector(info, symbol):
    return fetch_with_fallbacks(
        "Sector", info.get("sector"),
        ("FMP", lambda: _fmp_sector(symbol)),
        ("Finnhub", lambda: _finnhub_sector(symbol))
    ) or "N/A"

# ── Website ───────────────────────────────────────────────────
def _fmp_website(symbol):
    data = fmp_get(f"/profile/{symbol}")
    if data and isinstance(data, list) and data:
        return data[0].get("website")
    return None

def _finnhub_website(symbol):
    data = finn_get("/stock/profile2", {"symbol": symbol})
    if data:
        return data.get("weburl")
    return None

def get_website(info, symbol):
    return fetch_with_fallbacks(
        "Website", info.get("website"),
        ("FMP", lambda: _fmp_website(symbol)),
        ("Finnhub", lambda: _finnhub_website(symbol))
    ) or "N/A"

# ── Earnings History (Surprise %) ─────────────────────────────
def get_earnings_history(ticker_yf, symbol):
    try:
        eh = ticker_yf.earnings_history
        if eh is not None and not eh.empty and "surprisePercent" in eh.columns:
            clean = eh.head(12)["surprisePercent"].dropna().tolist()
            if clean:
                return clean
    except Exception as e:
        logger.warning(f"get_earnings_history yfinance: {e}")
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
    except Exception as e:
        logger.warning(f"get_earnings_history Finnhub: {e}")
    try:
        data = fmp_get(f"/earnings-surprises/{symbol}")
        if data and isinstance(data, list):
            surprises = []
            for d in data[:12]:
                actual = d.get("actualEarningResult")
                est    = d.get("estimatedEarning")
                if actual is not None and est and est != 0:
                    surprises.append((actual - est) / abs(est))
            if surprises:
                return surprises
    except Exception as e:
        logger.warning(f"get_earnings_history FMP: {e}")
    return None

# ── Revenue Estimate ──────────────────────────────────────────
def get_revenue_estimate(ticker_yf, symbol):
    try:
        df = ticker_yf.revenue_estimate
        if not df.empty and df.shape[0] > 3:
            return df.iloc[3, 0]
    except Exception as e:
        logger.warning(f"get_revenue_estimate yfinance: {e}")
    try:
        data = finn_get("/stock/revenue-estimate", {"symbol": symbol, "freq": "annual"})
        if data and "revenueEstimate" in data:
            ests = data["revenueEstimate"]
            if len(ests) > 1:
                return ests[1].get("revenueAvg")
    except Exception as e:
        logger.warning(f"get_revenue_estimate Finnhub: {e}")
    try:
        data = fmp_get(f"/analyst-estimates/{symbol}", params={"limit": 2})
        if data and isinstance(data, list) and len(data) >= 2:
            return data[1].get("estimatedRevenueAvg")
    except Exception as e:
        logger.warning(f"get_revenue_estimate FMP: {e}")
    return None

# ── EPS Estimate (Future) ─────────────────────────────────────
def get_eps_estimate(ticker_yf, symbol):
    try:
        df = ticker_yf.earnings_estimate
        if not df.empty and df.shape[0] > 3:
            return df.iloc[3, 0]
    except Exception as e:
        logger.warning(f"get_eps_estimate yfinance: {e}")
    try:
        data = finn_get("/stock/eps-estimate", {"symbol": symbol, "freq": "annual"})
        if data and "epsEstimate" in data:
            ests = data["epsEstimate"]
            if len(ests) > 1:
                return ests[1].get("epsAvg")
    except Exception as e:
        logger.warning(f"get_eps_estimate Finnhub: {e}")
    try:
        data = fmp_get(f"/analyst-estimates/{symbol}", params={"limit": 2})
        if data and isinstance(data, list) and len(data) >= 2:
            return data[1].get("estimatedEpsAvg")
    except Exception as e:
        logger.warning(f"get_eps_estimate FMP: {e}")
    return None

# ── Total Cash ────────────────────────────────────────────────
def _fmp_total_cash(symbol):
    data = fmp_get(f"/balance-sheet-statement/{symbol}", params={"limit": 1})
    if data and isinstance(data, list) and data:
        return data[0].get("cashAndCashEquivalents")
    return None

def _av_total_cash(symbol):
    data = av_get("BALANCE_SHEET", symbol)
    if data and "annualReports" in data and data["annualReports"]:
        return float(data["annualReports"][0].get("cashAndCashEquivalentsAtCarryingValue", 0) or 0)
    return None

def get_total_cash(info, symbol):
    return fetch_with_fallbacks(
        "Total Cash", info.get("totalCash"),
        ("FMP", lambda: _fmp_total_cash(symbol)),
        ("AlphaVantage", lambda: _av_total_cash(symbol))
    ) or 0

# ── Earning Estimate AVG ──────────────────────────────────────
def get_earning_estimate_avg(ticker_yf, symbol):
    try:
        df = ticker_yf.earnings_estimate
        if not df.empty:
            return df.iloc[0, 0]
    except Exception as e:
        logger.warning(f"get_earning_estimate_avg yfinance: {e}")
    try:
        data = finn_get("/stock/eps-estimate", {"symbol": symbol, "freq": "annual"})
        if data and "epsEstimate" in data and data["epsEstimate"]:
            return data["epsEstimate"][0].get("epsAvg")
    except Exception as e:
        logger.warning(f"get_earning_estimate_avg Finnhub: {e}")
    return None

# ==============================================================
# 📊 RANGOS EN GOOGLE SHEETS
# ==============================================================
ticker_range = f'A{start_row}:A{end_row}'

ranges = {
    'Target Mean Price': f'B{start_row}:B{end_row}',
    'Analyst Count': f'E{start_row}:E{end_row}',
    'Target Dispersion': f'F{start_row}:F{end_row}',
    'Earning Estimate AVG': f'G{start_row}:G{end_row}',
    'Rev_Growth_YoY': f'H{start_row}:H{end_row}',
    'Gross_Margin': f'I{start_row}:I{end_row}',
    'Operating_Margin': f'K{start_row}:K{end_row}',
    'Growth_Momentum': f'L{start_row}:L{end_row}',
    'SMA_200': f'M{start_row}:M{end_row}',
    'SMA_Trend': f'N{start_row}:N{end_row}',
    'Volatility_ATR': f'O{start_row}:O{end_row}',
    'Weighted Consistency': f'Q{start_row}:Q{end_row}',
    'Beat Rate': f'R{start_row}:R{end_row}',
    'Recent 4Q Avg': f'S{start_row}:S{end_row}',
    'Revenue Surprise 4Q': f'T{start_row}:T{end_row}',
    'Surprise Trend': f'U{start_row}:U{end_row}',
    'Earnings Window': f'V{start_row}:V{end_row}',
    'Worst Miss': f'W{start_row}:W{end_row}',
    'PEG': f'Y{start_row}:Y{end_row}',
    'Interest Coverage': f'Z{start_row}:Z{end_row}',
    'Forward PE': f'AA{start_row}:AA{end_row}',
    'FCF Yield': f'AC{start_row}:AC{end_row}',
    'FCF Growth YoY': f'AD{start_row}:AD{end_row}',
    'FCF/NI Ratio': f'AE{start_row}:AE{end_row}',
    'FCF Margin': f'AF{start_row}:AF{end_row}',
    'Total Cash': f'AI{start_row}:AI{end_row}',
    'Operating Expense TTM': f'AJ{start_row}:AJ{end_row}',
    'Total Debt (mrq)': f'AN{start_row}:AN{end_row}',
    'Interest Expense': f'AO{start_row}:AO{end_row}',
    'Debt/Equity': f'AQ{start_row}:AQ{end_row}',
    'Debt/EBITDA': f'AR{start_row}:AR{end_row}',
    'Years to Pay Debt': f'AS{start_row}:AS{end_row}',
    'Revenue Estimate AVG': f'AW{start_row}:AW{end_row}',
    'Profit Margin': f'AX{start_row}:AX{end_row}',
    'P/E Promedio 6 meses': f'AY{start_row}:AY{end_row}',
    'Future EPS': f'BF{start_row}:BF{end_row}',
    'Expected PE': f'BG{start_row}:BG{end_row}',
    'Expected Return (EPS)': f'BH{start_row}:BH{end_row}',
    'Expected Return (Rev)': f'BI{start_row}:BI{end_row}',
    'Expected Return (Analyst)': f'BJ{start_row}:BJ{end_row}',
    'Expected Return (Consensus)': f'BK{start_row}:BK{end_row}',
    'Min 200d': f'BL{start_row}:BL{end_row}',
    'Max 200d': f'BT{start_row}:BT{end_row}',
    'Soportes': f'BM{start_row}:BM{end_row}',
    'Resistencias': f'BN{start_row}:BN{end_row}',
    'Posición S/R': f'BU{start_row}:BU{end_row}',
    'Soporte Cercano': f'BQ{start_row}:BQ{end_row}',
    'Resistencia Cercana': f'BP{start_row}:BP{end_row}',
    'Dist a Soporte %': f'BS{start_row}:BS{end_row}',
    'Dist a Resistencia %': f'BR{start_row}:BR{end_row}',
    'Williams %R (Current)': f'BV{start_row}:BV{end_row}',
    'Williams %R (1w ago)': f'BW{start_row}:BW{end_row}',
    'Williams %R (2w ago)': f'BX{start_row}:BX{end_row}',
    'Williams %R (Daily)': f'BY{start_row}:BY{end_row}',
    'Volume Ratio': f'CB{start_row}:CB{end_row}',
    'Volume Level': f'CC{start_row}:CC{end_row}',
    'OBV Trend': f'CD{start_row}:CD{end_row}',
    'Price-Volume Div': f'CE{start_row}:CE{end_row}',
    'MFI': f'CF{start_row}:CF{end_row}',
    'MFI Level': f'CG{start_row}:CG{end_row}',
    'Sector': f'DA{start_row}:DA{end_row}',
    'Days Public': f'DC{start_row}:DC{end_row}',
    'Beta': f'DD{start_row}:DD{end_row}',
    'Official URL': f'DE{start_row}:DE{end_row}'
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
    'MFI': 50, 'MFI Level': "N/A"
}

# ==============================================================
# 🚀 LÓGICA PRINCIPAL
# ==============================================================

def validate_ticker(symbol):
    """Valida que el ticker tenga formato razonable."""
    if not symbol or not isinstance(symbol, str):
        return False
    symbol = symbol.strip().upper()
    if not symbol.isalnum() or len(symbol) > 10:
        return False
    return True

def calc_levels(prices, vols, cluster_size, n=4):
    """Calcula niveles de soporte/resistencia por clustering."""
    if cluster_size <= 0:
        logger.warning(f"cluster_size inválido ({cluster_size}), usando fallback")
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

try:
    sh        = gc.open(spreadsheet_name)
    worksheet = sh.worksheet(worksheet_name)

    tickers_list = worksheet.get(ticker_range)
    raw_symbols = [item[0] for item in tickers_list if item and item[0]]
    symbols = [s.strip().upper() for s in raw_symbols if validate_ticker(s)]
    invalid = [s for s in raw_symbols if not validate_ticker(s)]
    if invalid:
        logger.warning(f"Tickers inválidos omitidos: {invalid}")

    if not symbols:
        logger.error("No se encontraron tickers válidos.")
    else:
        logger.info(f"🔍 Tickers válidos a procesar: {symbols}")
        all_results = {key: [] for key in ranges.keys()}

        for symbol in symbols:
            logger.info(f"{'='*60}  Procesando: {symbol}  {'='*60}")

            # Inicializar resultados del ticker con defaults
            results = {key: defaults[key] for key in ranges.keys()}

            try:
                ticker = yf.Ticker(symbol)
                info   = ticker.info or {}
            except Exception as e:
                logger.error(f"{symbol}: yfinance error crítico: {e}")
                info   = {}
                ticker = None

            # ═══════════════════════════════════════════════════════
            # PRINCIPIO 1: PRECIO OBJETIVO
            # ═══════════════════════════════════════════════════════
            try:
                tp = get_target_price(info, ticker, symbol)
                results['Target Mean Price'] = tp if tp is not None else defaults['Target Mean Price']

                ac = get_analyst_count(info, symbol)
                results['Analyst Count'] = ac if ac is not None else defaults['Analyst Count']

                # Target Dispersion
                disp_val = None
                if ticker:
                    try:
                        pts = ticker.analyst_price_targets
                        if pts and 'current' in pts:
                            th, tl, tm = pts.get('high',0), pts.get('low',0), pts.get('mean',0)
                            if tm > 0 and th > 0 and tl > 0:
                                disp_val = (th - tl) / tm
                    except Exception as e:
                        logger.warning(f"{symbol} Target Dispersion yfinance: {e}")

                if disp_val is None:
                    try:
                        data = fmp_get(f"/price-target/{symbol}")
                        if data and isinstance(data, list) and data:
                            targets = [d.get("priceTarget", 0) for d in data if d.get("priceTarget")]
                            if len(targets) > 1:
                                mean_t = sum(targets) / len(targets)
                                if mean_t > 0:
                                    disp_val = (max(targets) - min(targets)) / mean_t
                    except Exception as e:
                        logger.warning(f"{symbol} Target Dispersion FMP: {e}")

                results['Target Dispersion'] = f"{disp_val:.2%}" if disp_val is not None else defaults['Target Dispersion']
            except Exception as e:
                logger.error(f"{symbol} error en Principio 1: {e}")

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
                except Exception as e:
                    logger.warning(f"{symbol} Growth Momentum: {e}")

                eea = get_earning_estimate_avg(ticker, symbol) if ticker else None
                results['Earning Estimate AVG'] = f"${eea:.2f}" if eea is not None else defaults['Earning Estimate AVG']
                results['Rev_Growth_YoY'] = rev_growth
                results['Gross_Margin'] = gross_margin
                results['Operating_Margin'] = operating_margin
                results['Growth_Momentum'] = growth_momentum
            except Exception as e:
                logger.error(f"{symbol} error en Principio 2: {e}")

            # ═══════════════════════════════════════════════════════
            # PRINCIPIO 3: TENDENCIA (SMA / ATR)
            # ═══════════════════════════════════════════════════════
            try:
                hist_p3 = None
                if ticker:
                    try:
                        end_date = datetime.datetime.today()
                        start_date = end_date - datetime.timedelta(days=365)
                        hist_p3 = ticker.history(start=start_date, end=end_date, interval="1d")
                    except Exception as e:
                        logger.warning(f"{symbol} yfinance hist_p3: {e}")

                if hist_p3 is not None and not hist_p3.empty and len(hist_p3) >= 250:
                    sma_200_current = hist_p3['Close'].tail(200).mean()
                    sma_200_50d_ago = hist_p3['Close'].iloc[-250:-50].mean()
                    sma_slope = (sma_200_current - sma_200_50d_ago) / sma_200_50d_ago if sma_200_50d_ago > 0 else 0

                    if sma_slope > 0.05:    sma_trend = "ALCISTA FUERTE"
                    elif sma_slope > 0.02:  sma_trend = "ALCISTA"
                    elif sma_slope > -0.02: sma_trend = "LATERAL"
                    elif sma_slope > -0.05: sma_trend = "BAJISTA"
                    else:                   sma_trend = "BAJISTA FUERTE"

                    hist_p3['High_Low'] = hist_p3['High'] - hist_p3['Low']
                    atr_14 = hist_p3['High_Low'].tail(14).mean()
                    cp     = hist_p3['Close'].iloc[-1]
                    vol_pct = atr_14 / cp if cp > 0 else 0

                    results['SMA_200'] = f"${sma_200_current:.2f}"
                    results['SMA_Trend'] = sma_trend
                    results['Volatility_ATR'] = vol_pct
                else:
                    logger.info(f"{symbol}: datos históricos insuficientes para SMA/ATR")
            except Exception as e:
                logger.error(f"{symbol} error en Principio 3: {e}")

            # ═══════════════════════════════════════════════════════
            # PRINCIPIO 4: CONSISTENCIA DE EARNINGS
            # ═══════════════════════════════════════════════════════
            try:
                surprise_data = get_earnings_history(ticker, symbol) if ticker else None
                if surprise_data and len(surprise_data) > 0:
                    total_quarters = len(surprise_data)
                    total_beats    = sum(1 for s in surprise_data if s > 0)
                    win_rate       = total_beats / total_quarters
                    num_recent     = min(4, len(surprise_data))
                    recent_avg     = sum(surprise_data[:num_recent]) / num_recent
                    worst_miss     = min(surprise_data)

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
                    logger.info(f"{symbol}: sin datos de earnings history")
            except Exception as e:
                logger.error(f"{symbol} error en Principio 4 (earnings): {e}")

            try:
                rev_surp_val = None

                # Fuente 1: Finnhub
                try:
                    data = finn_get("/stock/earnings", {"symbol": symbol, "limit": 8})
                    if data and isinstance(data, list):
                        rsurps = []
                        for d in data[:4]:
                            rev_actual = d.get("revenueActual")
                            rev_est    = d.get("revenueEstimate")
                            if (rev_actual is not None and rev_est is not None and rev_est != 0):
                                rsurps.append((rev_actual - rev_est) / abs(rev_est))
                        if rsurps:
                            rev_surp_val = sum(rsurps) / len(rsurps)
                            logger.info(f"{symbol} Revenue Surprise (Finnhub): {rev_surp_val:.2%}")
                except Exception as e:
                    logger.warning(f"{symbol} Revenue Surprise Finnhub: {e}")

                # Fuente 2: FMP
                if rev_surp_val is None:
                    try:
                        actual_data = fmp_get(f"/income-statement/{symbol}", params={"limit": 4, "period": "quarter"})
                        est_data    = fmp_get(f"/analyst-estimates/{symbol}", params={"limit": 8, "period": "quarter"})
                        if (actual_data and isinstance(actual_data, list) and
                            est_data   and isinstance(est_data,   list)):
                            rsurps = []
                            for actual_q in actual_data[:4]:
                                q_date  = actual_q.get("date", "")[:7]
                                rev_real = actual_q.get("revenue", 0)
                                est_match = next((e for e in est_data if e.get("date", "")[:7] == q_date), None)
                                if est_match:
                                    rev_est = est_match.get("estimatedRevenueAvg", 0)
                                    if rev_real and rev_est and rev_est != 0:
                                        rsurps.append((rev_real - rev_est) / abs(rev_est))
                            if rsurps:
                                rev_surp_val = sum(rsurps) / len(rsurps)
                                logger.info(f"{symbol} Revenue Surprise (FMP): {rev_surp_val:.2%}")
                    except Exception as e:
                        logger.warning(f"{symbol} Revenue Surprise FMP: {e}")

                results['Revenue Surprise 4Q'] = rev_surp_val if rev_surp_val is not None else defaults['Revenue Surprise 4Q']
            except Exception as e:
                logger.error(f"{symbol} Revenue Surprise general: {e}")

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
                    except Exception as e:
                        logger.warning(f"{symbol} Earnings Window Finnhub: {e}")
                if isinstance(timestamp, (int, float)):
                    dt   = datetime.datetime.fromtimestamp(timestamp)
                    days = (dt - datetime.datetime.today()).days
                    if days < 0:       ew_val = "PASADO"
                    elif days <= 7:    ew_val = "ESTA SEMANA"
                    elif days <= 21:   ew_val = "ESTE MES"
                    elif days <= 45:   ew_val = "PRÓXIMO MES"
                    else:              ew_val = "LEJANO"
                results['Earnings Window'] = ew_val
            except Exception as e:
                logger.error(f"{symbol} Earnings Window: {e}")

            # ═══════════════════════════════════════════════════════
            # PRINCIPIO 5: VALORACIÓN FINANCIERA
            # ═══════════════════════════════════════════════════════
            try:
                peg_val     = get_peg(info, symbol)
                forward_pe  = get_forward_pe(info, symbol)
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
                        except Exception as e:
                            logger.warning(f"{symbol} Interest Coverage yfinance: {e}")
                    if ebit is None or interest is None:
                        data = fmp_get(f"/ratios/{symbol}", params={"limit": 1})
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
                logger.error(f"{symbol} error en Principio 5 (parte 1): {e}")

            try:
                market_cap = info.get("marketCap", 0)
                revenue    = info.get("totalRevenue", 0)

                if not revenue:
                    try:
                        data = fmp_get(f"/income-statement/{symbol}", params={"limit": 1})
                        if data and isinstance(data, list) and data:
                            revenue = data[0].get("revenue", 0)
                    except Exception as e:
                        logger.warning(f"{symbol} Revenue FMP fallback: {e}")

                net_income = get_net_income(info, ticker, symbol) if ticker else 0
                fcf        = get_fcf(info, ticker, symbol) if ticker else 0

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
                        except Exception as e:
                            logger.warning(f"{symbol} FCF Growth yfinance: {e}")
                    if fcf_growth == 0:
                        data = fmp_get(f"/cash-flow-statement/{symbol}", params={"limit": 2})
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
                logger.error(f"{symbol} error en Principio 5 (parte 2 FCF): {e}")

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
                        except Exception as e:
                            logger.warning(f"{symbol} OpExp yfinance: {e}")
                    if op_exp is None:
                        data = fmp_get(f"/income-statement/{symbol}", params={"limit": 1})
                        if data and isinstance(data, list) and data:
                            op_exp = abs(data[0].get("operatingExpenses", 0) or 0)
                    results['Operating Expense TTM'] = op_exp if op_exp is not None else defaults['Operating Expense TTM']
                except Exception as e:
                    logger.error(f"{symbol} Operating Expense: {e}")
            except Exception as e:
                logger.error(f"{symbol} error en Principio 5 (parte 3 Cash): {e}")

            try:
                total_debt = get_total_debt(info, symbol)
                ebitda     = get_ebitda(info, symbol)
                fcf_debt   = info.get("freeCashflow") or fcf or 0

                total_equity = info.get("totalStockholderEquity") or info.get("totalEquity")
                if not total_equity:
                    try:
                        if ticker:
                            bs = ticker.quarterly_balance_sheet
                            for k in ['Stockholders Equity', 'Total Equity Gross Minority Interest', 'Common Stock Equity']:
                                if k in bs.index:
                                    total_equity = bs.loc[k].iloc[0]; break
                    except Exception as e:
                        logger.warning(f"{symbol} Equity yfinance: {e}")
                if not total_equity:
                    try:
                        data = fmp_get(f"/balance-sheet-statement/{symbol}", params={"limit": 1})
                        if data and isinstance(data, list) and data:
                            total_equity = data[0].get("totalStockholdersEquity")
                    except Exception as e:
                        logger.warning(f"{symbol} Equity FMP: {e}")

                results['Total Debt (mrq)'] = f"${int(total_debt):,}" if isinstance(total_debt,(int,float)) and total_debt else defaults['Total Debt (mrq)']

                ie_val = defaults['Interest Expense']
                try:
                    if ticker:
                        try:
                            is_stmt = ticker.income_stmt
                            if "Interest Expense" in is_stmt.index:
                                ie = is_stmt.loc["Interest Expense"].iloc[0]
                                ie_val = f"${int(ie):,}" if isinstance(ie,(int,float)) else defaults['Interest Expense']
                        except Exception as e:
                            logger.warning(f"{symbol} Interest Expense yfinance: {e}")
                    if ie_val == defaults['Interest Expense']:
                        data = fmp_get(f"/income-statement/{symbol}", params={"limit": 1})
                        if data and isinstance(data, list) and data:
                            ie = data[0].get("interestExpense", 0)
                            if ie:
                                ie_val = f"${int(ie):,}"
                except Exception as e:
                    logger.warning(f"{symbol} Interest Expense FMP: {e}")
                results['Interest Expense'] = ie_val

                results['Debt/Equity'] = f"{total_debt/total_equity:.2f}" if total_equity and total_equity > 0 and total_debt else defaults['Debt/Equity']
                results['Debt/EBITDA'] = f"{total_debt/ebitda:.2f}" if ebitda and ebitda > 0 and total_debt else defaults['Debt/EBITDA']
                results['Years to Pay Debt'] = f"{total_debt/fcf_debt:.1f}" if fcf_debt and fcf_debt > 0 and total_debt else defaults['Years to Pay Debt']
            except Exception as e:
                logger.error(f"{symbol} error en Principio 5 (parte 4 Deuda): {e}")

            try:
                current_price  = info.get("currentPrice", 0)
                market_cap     = info.get("marketCap", 0)
                trailing_eps   = info.get("trailingEps", 0)
                current_pe     = info.get("trailingPE", 0)
                forward_pe_val = info.get("forwardPE", 0)
                profit_margin  = get_profit_margin(info, symbol)

                revenue_next_year = get_revenue_estimate(ticker, symbol) if ticker else None
                results['Revenue Estimate AVG'] = f"{int(revenue_next_year):,.0f}" if revenue_next_year else defaults['Revenue Estimate AVG']
                results['Profit Margin'] = f"{profit_margin:.2%}" if profit_margin else defaults['Profit Margin']

                try:
                    avg_pe_val = defaults['P/E Promedio 6 meses']
                    if trailing_eps and trailing_eps > 0 and ticker:
                        end_d   = datetime.datetime.today()
                        start_d = end_d - datetime.timedelta(days=180)
                        hist_pe = ticker.history(start=start_d, end=end_d, interval="1mo")
                        if not hist_pe.empty:
                            hist_pe["P/E"] = np.divide(hist_pe["Close"], trailing_eps)
                            avg_pe_val = round(hist_pe["P/E"].mean(), 2)
                    results['P/E Promedio 6 meses'] = avg_pe_val
                except Exception as e:
                    logger.warning(f"{symbol} P/E 6m: {e}")

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
                logger.error(f"{symbol} error en Principio 5 (parte 5 Expected): {e}")

            # ═══════════════════════════════════════════════════════
            # PRINCIPIO 6: SOPORTES Y RESISTENCIAS
            # ═══════════════════════════════════════════════════════
            try:
                hist_p6 = None
                if ticker:
                    try:
                        end_d = datetime.datetime.today()
                        start_d = end_d - datetime.timedelta(days=200)
                        hist_p6 = ticker.history(start=start_d, end=end_d, interval="1d")
                    except Exception as e:
                        logger.warning(f"{symbol} yfinance hist_p6: {e}")

                if hist_p6 is not None and not hist_p6.empty and len(hist_p6) >= 50:
                    cp     = hist_p6['Close'].iloc[-1]
                    min_200d = hist_p6['Low'].min()
                    max_200d = hist_p6['High'].max()

                    results['Min 200d'] = round(min_200d, 4)
                    results['Max 200d'] = round(max_200d, 4)

                    if len(hist_p6) >= 14:
                        hist_p6['HL'] = hist_p6['High'] - hist_p6['Low']
                        atr_p6 = hist_p6['HL'].tail(14).mean()
                        cluster_size = max(atr_p6 * 0.5, cp * 0.003)
                    else:
                        cluster_size = cp * 0.005

                    lows    = hist_p6['Low'].values
                    highs   = hist_p6['High'].values
                    closes  = hist_p6['Close'].values
                    volumes = hist_p6['Volume'].values

                    support_levels    = calc_levels(lows,  volumes, cluster_size)
                    resistance_levels = calc_levels(highs, volumes, cluster_size)

                    s_below = [s for s in support_levels    if s < cp]
                    r_above = [r for r in resistance_levels if r > cp]

                    nearest_support    = max(s_below) if s_below else min_200d
                    nearest_resistance = min(r_above) if r_above else max_200d

                    dist_to_support    = (cp - nearest_support)    / cp if nearest_support    else 1
                    dist_to_resistance = (nearest_resistance - cp) / cp if nearest_resistance else 1

                    if   cp > max_200d:                pos = "Rompimiento al alza"
                    elif cp < min_200d:                pos = "Rompimiento bajista"
                    elif dist_to_resistance < 0.02:    pos = "Cerca de resistencia"
                    elif dist_to_support    < 0.02:    pos = "Cerca del soporte"
                    elif dist_to_support    < dist_to_resistance: pos = "Más cerca de soporte"
                    elif dist_to_resistance < dist_to_support:    pos = "Más cerca de resistencia"
                    else:                              pos = "En rango"

                    results['Soportes'] = ", ".join([f"{s:.2f}" for s in support_levels])
                    results['Resistencias'] = ", ".join([f"{r:.2f}" for r in resistance_levels])
                    results['Posición S/R'] = pos
                    results['Soporte Cercano'] = round(nearest_support, 4)
                    results['Resistencia Cercana'] = round(nearest_resistance, 4)
                    results['Dist a Soporte %'] = round(dist_to_support, 6)
                    results['Dist a Resistencia %'] = round(dist_to_resistance, 6)
                else:
                    logger.info(f"{symbol}: datos históricos insuficientes para Soportes/Resistencias")
            except Exception as e:
                logger.error(f"{symbol} error en Principio 6 (S/R): {e}")

            # ═══════════════════════════════════════════════════════
            # PRINCIPIO 7: WILLIAMS %R
            # ═══════════════════════════════════════════════════════
            try:
                hist_p7 = None
                if ticker:
                    try:
                        end_d   = datetime.datetime.today()
                        start_d = end_d - datetime.timedelta(days=110)
                        hist_p7 = ticker.history(start=start_d, end=end_d, interval="1d")
                    except Exception as e:
                        logger.warning(f"{symbol} yfinance hist_p7: {e}")

                if hist_p7 is not None and not hist_p7.empty and len(hist_p7) >= 70:
                    cp = hist_p7['Close'].iloc[-1]
                    # Asegurar índice datetime
                    hist_p7 = hist_p7.copy()
                    hist_p7['Date'] = pd.to_datetime(hist_p7.index)
                    hist_p7['Week'] = hist_p7['Date'].dt.isocalendar().week
                    hist_p7['Year'] = hist_p7['Date'].dt.isocalendar().year
                    hist_p7['YearWeek'] = hist_p7['Year'].astype(str) + '-W' + hist_p7['Week'].astype(str).str.zfill(2)

                    unique_weeks = hist_p7['YearWeek'].unique()
                    if len(unique_weeks) >= 14:
                        lb_data = hist_p7[hist_p7['YearWeek'].isin(unique_weeks[-14:])]
                        hh = lb_data['High'].max()
                        ll = lb_data['Low'].min()
                        wr_curr  = ((hh - cp)  / (hh - ll)) * -100 if hh != ll else 0

                        def wr_for_week(wk_idx):
                            wk_data = hist_p7[hist_p7['YearWeek'] == unique_weeks[wk_idx]]
                            if not wk_data.empty:
                                p = wk_data['Close'].iloc[-1]
                                return ((hh - p) / (hh - ll)) * -100 if hh != ll else 0
                            return 0

                        wr_1w = wr_for_week(-2) if len(unique_weeks) >= 15 else 0
                        wr_2w = wr_for_week(-3) if len(unique_weeks) >= 16 else 0

                        last14d = hist_p7.tail(14)
                        hh14 = last14d['High'].max(); ll14 = last14d['Low'].min()
                        wr_daily = ((hh14 - cp) / (hh14 - ll14)) * -100 if hh14 != ll14 else 0

                        results['Williams %R (Current)'] = wr_curr
                        results['Williams %R (1w ago)'] = wr_1w
                        results['Williams %R (2w ago)'] = wr_2w
                        results['Williams %R (Daily)'] = wr_daily
                    else:
                        logger.info(f"{symbol}: semanas insuficientes para Williams %R")
                else:
                    logger.info(f"{symbol}: datos históricos insuficientes para Williams %R")
            except Exception as e:
                logger.error(f"{symbol} error en Principio 7 (Williams): {e}")

            # ═══════════════════════════════════════════════════════
            # PRINCIPIO 8: VOLUMEN Y MOMENTUM
            # ═══════════════════════════════════════════════════════
            try:
                hist_p8 = None
                if ticker:
                    try:
                        end_d   = datetime.datetime.today()
                        start_d = end_d - datetime.timedelta(days=120)
                        hist_p8 = ticker.history(start=start_d, end=end_d, interval="1d")
                    except Exception as e:
                        logger.warning(f"{symbol} yfinance hist_p8: {e}")

                if hist_p8 is not None and not hist_p8.empty and len(hist_p8) >= 50:
                    avg_vol_20 = hist_p8['Volume'].tail(20).mean()
                    avg_vol_50 = hist_p8['Volume'].tail(50).mean()
                    cur_vol    = hist_p8['Volume'].iloc[-1]

                    vol_ratio = cur_vol / avg_vol_50 if avg_vol_50 > 0 else 1

                    if   vol_ratio > 2.0: vol_level = "MUY ALTO"
                    elif vol_ratio > 1.5: vol_level = "ALTO"
                    elif vol_ratio > 0.8: vol_level = "NORMAL"
                    elif vol_ratio > 0.5: vol_level = "BAJO"
                    else:                 vol_level = "MUY BAJO"

                    results['Volume Ratio'] = round(vol_ratio, 4)
                    results['Volume Level'] = vol_level

                    obv = [0]
                    for i in range(1, len(hist_p8)):
                        if   hist_p8['Close'].iloc[i] > hist_p8['Close'].iloc[i-1]:
                            obv.append(obv[-1] + hist_p8['Volume'].iloc[i])
                        elif hist_p8['Close'].iloc[i] < hist_p8['Close'].iloc[i-1]:
                            obv.append(obv[-1] - hist_p8['Volume'].iloc[i])
                        else:
                            obv.append(obv[-1])

                    hist_p8 = hist_p8.copy()
                    hist_p8['OBV'] = obv

                    obv_sma30 = hist_p8['OBV'].tail(30).mean()
                    obv_cur   = hist_p8['OBV'].iloc[-1]

                    if   obv_cur > obv_sma30 * 1.05: obv_trend = "ACUMULACIÓN"
                    elif obv_cur < obv_sma30 * 0.95: obv_trend = "DISTRIBUCIÓN"
                    else:                             obv_trend = "NEUTRAL"

                    results['OBV Trend'] = obv_trend

                    pc20 = (hist_p8['Close'].iloc[-1] - hist_p8['Close'].iloc[-20]) /                           hist_p8['Close'].iloc[-20] if len(hist_p8) >= 20 else 0
                    vc20 = (avg_vol_20 - avg_vol_50) / avg_vol_50 if avg_vol_50 > 0 else 0

                    if   pc20 >  0.05 and vc20 >  0.20: div = "ALCISTA FUERTE"
                    elif pc20 >  0.05 and vc20 < -0.20: div = "ALCISTA DÉBIL"
                    elif pc20 < -0.05 and vc20 >  0.20: div = "BAJISTA FUERTE"
                    elif pc20 < -0.05 and vc20 < -0.20: div = "BAJISTA DÉBIL"
                    else:                                div = "NEUTRAL"

                    results['Price-Volume Div'] = div

                    tp_series = (hist_p8['High'] + hist_p8['Low'] + hist_p8['Close']) / 3
                    mf_series = tp_series * hist_p8['Volume']
                    pos_flow, neg_flow = [], []
                    for i in range(1, len(hist_p8)):
                        if tp_series.iloc[i] > tp_series.iloc[i-1]:
                            pos_flow.append(mf_series.iloc[i])
                            neg_flow.append(0)
                        else:
                            pos_flow.append(0)
                            neg_flow.append(mf_series.iloc[i])

                    if len(pos_flow) >= 14:
                        pmf = sum(pos_flow[-14:])
                        nmf = sum(neg_flow[-14:])
                        if nmf == 0 and pmf == 0:
                            mfi_val = 50
                        elif nmf == 0:
                            mfi_val = 100
                        else:
                            mfi_val = 100 - (100 / (1 + pmf / nmf))

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
                    logger.info(f"{symbol}: datos históricos insuficientes para Volumen/Momentum")
            except Exception as e:
                logger.error(f"{symbol} error en Principio 8 (Volumen): {e}")

            # ═══════════════════════════════════════════════════════
            # DATOS ADICIONALES
            # ═══════════════════════════════════════════════════════
            results['Sector'] = get_sector(info, symbol)

            try:
                dp_val = defaults['Days Public']
                if ticker:
                    try:
                        hist_max = ticker.history(period="max", auto_adjust=False)
                        if not hist_max.empty:
                            dp_val = int((datetime.date.today() - hist_max.index.min().date()).days)
                    except Exception as e:
                        logger.warning(f"{symbol} Days Public yfinance: {e}")
                results['Days Public'] = dp_val
            except Exception as e:
                logger.error(f"{symbol} Days Public: {e}")

            beta_val = get_beta(info, symbol)
            results['Beta'] = beta_val if beta_val is not None else defaults['Beta']
            results['Official URL'] = get_website(info, symbol)

            logger.info(f"✅ {symbol} procesado.")

            # ── Empujar resultados del ticker a all_results ──
            for key in ranges.keys():
                all_results[key].append([results[key]])

        # ═══════════════════════════════════════════════════════════
        # 📤 ESCRITURA EN GOOGLE SHEETS (batch)
        # ═══════════════════════════════════════════════════════════
        logger.info("--- Escribiendo datos en Google Sheets ---")
        try:
            batch_data = [{'range': ranges[m], 'values': data} for m, data in all_results.items()]
            worksheet.batch_update(batch_data, value_input_option='USER_ENTERED')
            logger.info(f"✅ {len(batch_data)} rangos actualizados exitosamente.")
        except Exception as e:
            logger.error(f"❌ Error en batch: {e}. Intentando individual...")
            for metric, data_list in all_results.items():
                try:
                    worksheet.update(range_name=ranges[metric], values=data_list)
                    logger.info(f"  ✓ '{metric}' actualizado ({len(data_list)} filas)")
                    time.sleep(1.2)
                except Exception as e2:
                    logger.error(f"  ✗ '{metric}': {e2}")

        logger.info("🎉 ¡Proceso completado!")

except gspread.exceptions.SpreadsheetNotFound:
    logger.error(f'❌ Hoja "{spreadsheet_name}" no encontrada.')
except gspread.exceptions.WorksheetNotFound:
    logger.error(f'❌ Pestaña "{worksheet_name}" no encontrada.')
except Exception as e:
    import traceback
    logger.error(f"❌ Error inesperado: {e}")
    traceback.print_exc()
    