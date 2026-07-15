#!/usr/bin/env python3
"""
SCRIPT DE PRUEBA: Diagnóstico de Moat Score v7.0 - VERSIÓN CORREGIDA
Ejecutar con: python test_moat_debug_fixed.py
"""

import logging
import sys

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%H:%M:%S',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("MoatDebug")

def safe_float(val, default=0.0):
    try:
        if isinstance(val, str):
            val = val.replace("$", "").replace(",", "").replace("%", "").strip()
        f = float(val)
        import math
        if math.isnan(f) or math.isinf(f):
            return default
        return f
    except (TypeError, ValueError):
        return default

# Simular funciones de main_V6
def get_total_debt(info, symbol):
    if info and info.get("totalDebt"):
        return safe_float(info.get("totalDebt"))
    return None

def get_total_cash(info, symbol):
    if info and info.get("totalCash"):
        return safe_float(info.get("totalCash"))
    return None

def fmp_get(endpoint, version="v3", params=None):
    return None

def fmp_ticker(symbol):
    return symbol.replace(".", "-")

# ==============================================================
# get_roic - VERSIÓN CON FALLBACK A info
# ==============================================================
def get_roic(ticker, symbol, info=None) -> float:
    """ROIC con fallback a info cuando yfinance financials están vacíos"""

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
                        break

                nopat = ebit * (1 - tax_rate) if ebit else None

                equity = None
                for label in ['Stockholders Equity', 'Total Equity Gross Minority Interest', 'Common Stock Equity']:
                    if label in bs.index:
                        equity = safe_float(bs.loc[label].iloc[0])
                        break

                debt = get_total_debt(info, symbol)
                cash = get_total_cash(info, symbol)

                if nopat and equity:
                    invested_cap = equity + (debt or 0) - (cash or 0)
                    if invested_cap and invested_cap > 0:
                        roic = nopat / invested_cap
                        logger.info(f"[get_roic] Calculado desde yfinance: {roic:.4f}")
                        return roic
    except Exception as e:
        logger.debug(f"[get_roic] yfinance error: {e}")

    # 2. FALLBACK: Calcular desde info (datos del ticker.info de yfinance)
    try:
        if info:
            # yfinance info tiene estos campos comunes
            ebitda = safe_float(info.get("ebitda"), 0)
            depreciation = safe_float(info.get("depreciation"), 0)
            ebit = ebitda - depreciation if ebitda and depreciation else ebitda

            # Si no hay ebitda, intentar con operatingIncome
            if not ebit:
                ebit = safe_float(info.get("operatingIncome"), 0)

            if ebit:
                tax_rate = 0.21
                # Algunos tickers tienen taxRate
                tr = info.get("taxRate")
                if tr:
                    tr_val = safe_float(tr, 0)
                    if tr_val > 1:  # Viene como porcentaje
                        tax_rate = tr_val / 100
                    elif 0 < tr_val < 1:
                        tax_rate = tr_val

                nopat = ebit * (1 - tax_rate)

                # Invested capital desde info
                total_equity = safe_float(info.get("totalStockholderEquity"), 0)
                if not total_equity:
                    total_equity = safe_float(info.get("totalStockholdersEquity"), 0)
                if not total_equity:
                    # Calcular desde bookValue * shares
                    book_value = safe_float(info.get("bookValue"), 0)
                    shares = safe_float(info.get("sharesOutstanding"), 0)
                    if book_value and shares:
                        total_equity = book_value * shares

                debt = get_total_debt(info, symbol) or 0
                cash = get_total_cash(info, symbol) or 0

                if total_equity:
                    invested_cap = total_equity + debt - cash
                    if invested_cap > 0:
                        roic = nopat / invested_cap
                        logger.info(f"[get_roic] Calculado desde info: {roic:.4f} (ebit={ebit}, equity={total_equity}, debt={debt}, cash={cash})")
                        return roic
    except Exception as e:
        logger.debug(f"[get_roic] info fallback error: {e}")

    # 3. Fallback a FMP
    try:
        data = fmp_get(f"/ratios/{fmp_ticker(symbol)}", params={"limit": 1})
        if data and isinstance(data, list) and data:
            roic_val = data[0].get("returnOnInvestedCapital")
            if roic_val:
                if roic_val > 1:
                    roic_val = roic_val / 100
                logger.info(f"[get_roic] Desde FMP: {roic_val:.4f}")
                return roic_val
    except Exception:
        pass

    logger.warning(f"[get_roic] No se pudo calcular ROIC para {symbol}")
    return None

# ==============================================================
# get_margin_stability - VERSIÓN CON FALLBACK A info
# ==============================================================
def get_margin_stability(ticker, symbol, margin_type="operating", info=None) -> float:
    """Estabilidad de márgenes con fallback a info"""

    # 1. Intentar con yfinance
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
                                stability = max(0, 1 - cv)
                                logger.info(f"[get_margin_stability] Desde yfinance ({margin_type}): {stability:.4f}")
                                return stability
    except Exception as e:
        logger.debug(f"[get_margin_stability] yfinance error: {e}")

    # 2. FALLBACK: Usar info (un solo punto, asumimos neutral)
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
                # Solo tenemos un punto, no podemos calcular estabilidad real
                # Asumimos estabilidad neutral (0.5) o usamos un proxy
                # Si el margen es alto (>30%), asumimos cierta estabilidad
                if margin_val > 0.30:
                    stability = 0.6  # Margen alto suele indicar pricing power
                elif margin_val > 0.15:
                    stability = 0.5  # Neutral
                else:
                    stability = 0.4  # Margen bajo = menos estable
                logger.info(f"[get_margin_stability] Desde info ({margin_type}): {stability:.4f} (margin={margin_val:.3f})")
                return stability
    except Exception as e:
        logger.debug(f"[get_margin_stability] info fallback error: {e}")

    # 3. Fallback FMP
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
                    import numpy as np
                    std_m = np.std(margins)
                    mean_m = np.mean(margins)
                    if mean_m and mean_m != 0:
                        stability = max(0, 1 - (std_m / abs(mean_m)))
                        logger.info(f"[get_margin_stability] Desde FMP ({margin_type}): {stability:.4f}")
                        return stability
    except Exception:
        pass

    logger.warning(f"[get_margin_stability] No disponible para {symbol}")
    return None

# ==============================================================
# get_fcf_consistency - VERSIÓN CON FALLBACK A info
# ==============================================================
def get_fcf_consistency(ticker, symbol, info=None) -> float:
    """FCF consistency con fallback a info"""

    # 1. Intentar con yfinance
    try:
        if ticker:
            cf = ticker.cashflow
            if cf is not None and not cf.empty and "Free Cash Flow" in cf.index:
                fcf_vals = cf.loc["Free Cash Flow"].dropna()
                positive = sum(1 for v in fcf_vals if safe_float(v, 0) > 0)
                total = len(fcf_vals)
                if total > 0:
                    ratio = positive / total
                    logger.info(f"[get_fcf_consistency] Desde yfinance: {ratio:.4f} ({positive}/{total})")
                    return ratio
    except Exception as e:
        logger.debug(f"[get_fcf_consistency] yfinance error: {e}")

    # 2. FALLBACK: Calcular FCF actual desde info
    try:
        if info:
            ocf = safe_float(info.get("operatingCashflow"), 0)
            capex = abs(safe_float(info.get("capitalExpenditures"), 0))

            # Alternativas de nombres de campo
            if not ocf:
                ocf = safe_float(info.get("operatingCashFlow"), 0)
            if not ocf:
                ocf = safe_float(info.get("totalCashFromOperatingActivities"), 0)

            if not capex:
                capex = abs(safe_float(info.get("capitalExpenditure"), 0))
            if not capex:
                capex = abs(safe_float(info.get("capitalExpenditures"), 0))

            fcf = ocf - capex

            if fcf != 0:
                # Solo tenemos un período, asumimos positivo = bueno
                ratio = 1.0 if fcf > 0 else 0.0
                logger.info(f"[get_fcf_consistency] Desde info: {ratio:.4f} (fcf={fcf:,.0f})")
                return ratio
    except Exception as e:
        logger.debug(f"[get_fcf_consistency] info fallback error: {e}")

    # 3. Fallback FMP
    try:
        data = fmp_get(f"/cash-flow-statement/{fmp_ticker(symbol)}", params={"limit": 5})
        if data and isinstance(data, list):
            fcf_vals = [safe_float(d.get("freeCashFlow", 0)) for d in data]
            positive = sum(1 for v in fcf_vals if v > 0)
            total = len(fcf_vals)
            if total > 0:
                ratio = positive / total
                logger.info(f"[get_fcf_consistency] Desde FMP: {ratio:.4f}")
                return ratio
    except Exception:
        pass

    logger.warning(f"[get_fcf_consistency] No disponible para {symbol}")
    return None

# ==============================================================
# get_moat_indicators - VERSIÓN FINAL CORREGIDA
# ==============================================================
def get_moat_indicators(ticker, symbol, info, hist) -> dict:
    """VERSIÓN CORREGIDA CON FALLBACKS A info"""
    indicators = {
        "gross_margin_stable": False,
        "gross_margin_high": False,
        "roic_stable": False,
        "operating_margin_stable": False,
        "market_share_growing": False,
        "fcf_stable": False,
        "moat_score": 0
    }

    logger.info(f"\n{'='*60}")
    logger.info(f"CALCULANDO MOAT PARA {symbol}")
    logger.info(f"{'='*60}")
    logger.info(f"ticker.financials empty: {ticker.financials.empty if ticker else 'N/A (ticker=None)'}")
    logger.info(f"info disponible: {info is not None and len(info) > 0}")

    try:
        # === 1. Gross Margin ===
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
                        logger.info(f"[Moat] Gross Margin (yfinance): avg={avg_gm:.3f}, std={std_gm:.3f}, high={indicators['gross_margin_high']}, stable={indicators['gross_margin_stable']}")
            except Exception as e:
                logger.debug(f"[Moat] GM yfinance error: {e}")

        if not gm_calculated and info:
            # Fallback a info
            gm = safe_float(info.get("grossMargins"), 0)
            if not gm:
                gm = safe_float(info.get("grossMargin"), 0)
            if gm > 0:
                indicators["gross_margin_high"] = gm > 0.40
                # Sin datos históricos, asumimos estable si es alto
                indicators["gross_margin_stable"] = gm > 0.35
                logger.info(f"[Moat] Gross Margin (info): {gm:.3f}, high={indicators['gross_margin_high']}, stable={indicators['gross_margin_stable']}")

        # === 2. ROIC ===
        roic = get_roic(ticker, symbol, info)
        if roic is not None:
            roic_norm = roic / 100 if roic > 1 else roic
            indicators["roic_stable"] = roic_norm > 0.15
            logger.info(f"[Moat] ROIC: {roic_norm:.3f}, stable={indicators['roic_stable']}")
        else:
            logger.warning(f"[Moat] ROIC: NO DISPONIBLE")

        # === 3. Operating Margin Stability ===
        op_stability = get_margin_stability(ticker, symbol, "operating", info)
        if op_stability is not None:
            indicators["operating_margin_stable"] = op_stability > 0.7
            logger.info(f"[Moat] Op Margin Stability: {op_stability:.3f}, stable={indicators['operating_margin_stable']}")
        else:
            logger.warning(f"[Moat] Op Margin Stability: NO DISPONIBLE")

        # === 4. FCF Consistency ===
        fcf_cons = get_fcf_consistency(ticker, symbol, info)
        if fcf_cons is not None:
            indicators["fcf_stable"] = fcf_cons > 0.8
            logger.info(f"[Moat] FCF Consistency: {fcf_cons:.3f}, stable={indicators['fcf_stable']}")
        else:
            logger.warning(f"[Moat] FCF Consistency: NO DISPONIBLE")

        # === Score Final ===
        score = 0
        if indicators["gross_margin_high"]: score += 25
        if indicators["gross_margin_stable"]: score += 15
        if indicators["roic_stable"]: score += 25
        if indicators["operating_margin_stable"]: score += 15
        if indicators["fcf_stable"]: score += 20
        indicators["moat_score"] = min(100, score)

        logger.info(f"\n[Moat] >>> SCORE FINAL: {score} puntos")
        logger.info(f"[Moat] >>> Indicadores activos: {sum([1 for v in [indicators['gross_margin_high'], indicators['gross_margin_stable'], indicators['roic_stable'], indicators['operating_margin_stable'], indicators['fcf_stable']] if v])}/5")

    except Exception as e:
        logger.error(f"[Moat] Error general: {e}")
        import traceback
        traceback.print_exc()

    return indicators


# ==============================================================
# TEST PRINCIPAL
# ==============================================================
if __name__ == "__main__":
    import pandas as pd

    print("\n" + "="*70)
    print("TEST DE MOAT INDICATORS - VERSIÓN CORREGIDA CON FALLBACKS")
    print("="*70)

    # Simular info de AAPL (datos reales aproximados)
    info_aapl = {
        "totalDebt": 111000000000,
        "totalCash": 62000000000,
        "ebitda": 125000000000,
        "operatingIncome": 118000000000,
        "totalStockholderEquity": 57000000000,
        "grossMargins": 0.453,      # 45.3%
        "operatingMargins": 0.303,   # 30.3%
        "profitMargins": 0.253,      # 25.3%
        "operatingCashflow": 110000000000,
        "capitalExpenditures": -11000000000,
        "marketCap": 3000000000000,
        "currentPrice": 185.0,
        "sector": "Technology"
    }

    # Simular info de MSFT
    info_msft = {
        "totalDebt": 48000000000,
        "totalCash": 75000000000,
        "ebitda": 88000000000,
        "operatingIncome": 86000000000,
        "totalStockholderEquity": 168000000000,
        "grossMargins": 0.689,      # 68.9%
        "operatingMargins": 0.442,   # 44.2%
        "profitMargins": 0.355,      # 35.5%
        "operatingCashflow": 87000000000,
        "capitalExpenditures": -44000000000,
        "marketCap": 2800000000000,
        "currentPrice": 380.0,
        "sector": "Technology"
    }

    # Simular info de JPM (banco - márgenes bajos)
    info_jpm = {
        "totalDebt": 500000000000,
        "totalCash": 800000000000,
        "ebitda": None,
        "operatingIncome": 58000000000,
        "totalStockholderEquity": 290000000000,
        "grossMargins": None,       # Bancos no reportan GM
        "operatingMargins": 0.38,    # 38%
        "profitMargins": 0.32,       # 32%
        "operatingCashflow": 120000000000,
        "capitalExpenditures": -15000000000,
        "marketCap": 550000000000,
        "currentPrice": 195.0,
        "sector": "Financial Services"
    }

    class DummyTicker:
        def __init__(self):
            self.financials = pd.DataFrame()
            self.balance_sheet = pd.DataFrame()
            self.cashflow = pd.DataFrame()

    ticker = DummyTicker()
    hist = None

    for symbol, info in [("AAPL", info_aapl), ("MSFT", info_msft), ("JPM", info_jpm)]:
        result = get_moat_indicators(ticker, symbol, info, hist)
        print(f"\n{'='*70}")
        print(f"RESULTADO {symbol}: Moat Score = {result['moat_score']}")
        print(f"  gross_margin_high: {result['gross_margin_high']}")
        print(f"  gross_margin_stable: {result['gross_margin_stable']}")
        print(f"  roic_stable: {result['roic_stable']}")
        print(f"  operating_margin_stable: {result['operating_margin_stable']}")
        print(f"  fcf_stable: {result['fcf_stable']}")
        print(f"{'='*70}")