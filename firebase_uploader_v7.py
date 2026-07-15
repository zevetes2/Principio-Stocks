import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime
from typing import Dict, List, Any

SERVICE_ACCOUNT_PATH = "firebase-service-key.json"

_db = None

def get_db():
    global _db
    if _db is None:
        if not firebase_admin._apps:
            cred = credentials.Certificate(SERVICE_ACCOUNT_PATH)
            firebase_admin.initialize_app(cred)
        _db = firestore.client()
    return _db

def safe_float_val(val, default=0.0):
    """Convierte cualquier valor a float de forma segura."""
    try:
        if val is None or val == '' or val == 'N/A':
            return default
        if isinstance(val, str):
            val = val.replace("$", "").replace(",", "").replace("%", "").strip()
        return float(val)
    except (TypeError, ValueError):
        return default

def get_val(arr, idx, default=None):
    """Extrae valor de array bidimensional [[val], [val], ...]"""
    try:
        v = arr[idx][0]
        return default if v is None or v == '' or v == 'N/A' else v
    except (IndexError, TypeError):
        return default

def upload_v7_to_firestore(all_results: Dict[str, List], symbols: List[str]):
    """
    Sube los datos v7.0 (3 Capas) a Firestore en colección 'portafolio_v7'.

    all_results: dict con arrays de cada métrica (formato de main.py v7.0)
    symbols: lista de tickers en orden
    """
    db = get_db()
    batch = db.batch()

    # Metadata v7
    meta_ref = db.collection('portafolio_v7').document('metadata')
    batch.set(meta_ref, {
        'lastUpdate': datetime.now().isoformat(),
        'totalTickers': len(symbols),
        'version': 'v7.0',
        'source': 'main_v7.py',
        'strategy': 'largo_plazo'  # default, se puede parametrizar
    }, merge=True)

    # Subir cada ticker como documento individual
    for i, sym in enumerate(symbols):
        doc_ref = db.collection('portafolio_v7').document(sym)

        ticker_data = {
            'ticker': sym,
            'updatedAt': datetime.now().isoformat(),

            # ── CAPA 1: BUSINESS QUALITY ──
            'bq': {
                'growthScore': safe_float_val(get_val(all_results.get('BQ_Growth_Score', []), i, 50)),
                'consistencyScore': safe_float_val(get_val(all_results.get('BQ_Consistency_Score', []), i, 50)),
                'marginsScore': safe_float_val(get_val(all_results.get('BQ_Margins_Score', []), i, 50)),
                'fcfScore': safe_float_val(get_val(all_results.get('BQ_FCF_Score', []), i, 50)),
                'debtScore': safe_float_val(get_val(all_results.get('BQ_Debt_Score', []), i, 50)),
                'roicScore': safe_float_val(get_val(all_results.get('BQ_ROIC_Score', []), i, 50)),
                'moatScore': safe_float_val(get_val(all_results.get('BQ_Moat_Score', []), i, 50)),
                'final': safe_float_val(get_val(all_results.get('BQ_Final', []), i, 50)),
                'class': str(get_val(all_results.get('BQ_Class', []), i, 'MEDIA')),
            },

            # Raw metrics BQ
            'bqRaw': {
                'revCAGR3Y': str(get_val(all_results.get('Rev_CAGR_3Y', []), i, 'N/A')),
                'epsCAGR3Y': str(get_val(all_results.get('EPS_CAGR_3Y', []), i, 'N/A')),
                'marginStability': str(get_val(all_results.get('Margin_Stability', []), i, 'N/A')),
                'fcfConsistency': str(get_val(all_results.get('FCF_Consistency', []), i, 'N/A')),
                'currentRatio': str(get_val(all_results.get('Current_Ratio', []), i, 'N/A')),
                'quickRatio': str(get_val(all_results.get('Quick_Ratio', []), i, 'N/A')),
                'netDebtFCF': str(get_val(all_results.get('Net_Debt_FCF', []), i, 'N/A')),
                'debtGrowthYoY': str(get_val(all_results.get('Debt_Growth_YoY', []), i, 'N/A')),
                'roic': str(get_val(all_results.get('ROIC', []), i, 'N/A')),
                'roe': str(get_val(all_results.get('ROE', []), i, 'N/A')),
                'moatScoreRaw': safe_float_val(get_val(all_results.get('Moat_Score_Raw', []), i, 0)),
                'grossMargin': str(get_val(all_results.get('Gross_Margin', []), i, 'N/A')),
                'operatingMargin': str(get_val(all_results.get('Operating_Margin', []), i, 'N/A')),
                'fcfMargin': str(get_val(all_results.get('FCF_Margin', []), i, 'N/A')),
                'netIncome': str(get_val(all_results.get('Net_Income', []), i, 'N/A')),
                'totalDebt': str(get_val(all_results.get('Total_Debt', []), i, 'N/A')),
                'ebitda': str(get_val(all_results.get('EBITDA', []), i, 'N/A')),
                'profitMargin': str(get_val(all_results.get('Profit_Margin', []), i, 'N/A')),
                'trailingEPS': str(get_val(all_results.get('Trailing_EPS', []), i, 'N/A')),
            },

            # ── CAPA 2: VALUATION ──
            'val': {
                'pegScore': safe_float_val(get_val(all_results.get('VAL_PEG_Score', []), i, 50)),
                'peScore': safe_float_val(get_val(all_results.get('VAL_PE_Score', []), i, 50)),
                'fcfYieldScore': safe_float_val(get_val(all_results.get('VAL_FCFYield_Score', []), i, 50)),
                'evEbitdaScore': safe_float_val(get_val(all_results.get('VAL_EVEBITDA_Score', []), i, 50)),
                'analystScore': safe_float_val(get_val(all_results.get('VAL_Analyst_Score', []), i, 50)),
                'historicalScore': safe_float_val(get_val(all_results.get('VAL_Historical_Score', []), i, 50)),
                'final': safe_float_val(get_val(all_results.get('VAL_Final', []), i, 50)),
                'class': str(get_val(all_results.get('VAL_Class', []), i, 'MEDIA')),
            },

            # Raw metrics VAL
            'valRaw': {
                'relativePE': str(get_val(all_results.get('Relative_PE', []), i, 'N/A')),
                'relativeEVEBITDA': str(get_val(all_results.get('Relative_EV_EBITDA', []), i, 'N/A')),
                'historicalPEAvg': str(get_val(all_results.get('Historical_PE_Avg', []), i, 'N/A')),
                'historicalPEMin': str(get_val(all_results.get('Historical_PE_Min', []), i, 'N/A')),
                'historicalPEMax': str(get_val(all_results.get('Historical_PE_Max', []), i, 'N/A')),
                'currentPE': str(get_val(all_results.get('Current_PE', []), i, 'N/A')),
                'evEbitdaCurrent': str(get_val(all_results.get('EV_EBITDA_Current', []), i, 'N/A')),
                'fcfYieldCurrent': str(get_val(all_results.get('FCF_Yield_Current', []), i, 'N/A')),
                'fcfYieldAvg5Y': str(get_val(all_results.get('FCF_Yield_Avg_5Y', []), i, 'N/A')),
                'analystTarget': str(get_val(all_results.get('Analyst_Target', []), i, 'N/A')),
                'analystCount': safe_float_val(get_val(all_results.get('Analyst_Count', []), i, 0)),
                'upside': safe_float_val(get_val(all_results.get('Upside', []), i, 0)),
            },

            # ── CAPA 3: TIMING ──
            'tim': {
                'tendenciaScore': safe_float_val(get_val(all_results.get('TIM_Tendencia_Score', []), i, 50)),
                'soportesScore': safe_float_val(get_val(all_results.get('TIM_Soportes_Score', []), i, 50)),
                'williamsScore': safe_float_val(get_val(all_results.get('TIM_Williams_Score', []), i, 50)),
                'volumenScore': safe_float_val(get_val(all_results.get('TIM_Volumen_Score', []), i, 50)),
                'volatilidadScore': safe_float_val(get_val(all_results.get('TIM_Volatilidad_Score', []), i, 50)),
                'final': safe_float_val(get_val(all_results.get('TIM_Final', []), i, 50)),
                'class': str(get_val(all_results.get('TIM_Class', []), i, 'MEDIA')),
            },

                        # Raw metrics TIM (técnicas de v6)
            'timRaw': {
                'smaTrend': str(get_val(all_results.get('SMA_Trend', []), i, 'LATERAL')),
                'smaSlope': safe_float_val(get_val(all_results.get('SMA_Slope', []), i, 0)),
                'volATR': safe_float_val(get_val(all_results.get('Volatility_ATR', []), i, 0.02)),
                'volRatio': safe_float_val(get_val(all_results.get('Volume_Ratio', []), i, 1)),
                'obvTrend': str(get_val(all_results.get('OBV_Trend', []), i, 'NEUTRAL')),
                'priceVolDiv': str(get_val(all_results.get('Price_Volume_Div', []), i, 'NEUTRAL')),
                'distSoporte': safe_float_val(get_val(all_results.get('Dist_Soporte', []), i, 0.05)),
                'distResistencia': safe_float_val(get_val(all_results.get('Dist_Resistencia', []), i, 0.05)),
                'fuerzaSoporte': safe_float_val(get_val(all_results.get('Fuerza_Soporte', []), i, 0)),
                'fuerzaResistencia': safe_float_val(get_val(all_results.get('Fuerza_Resistencia', []), i, 0)),
                'posicionSR': str(get_val(all_results.get('Posicion_SR', []), i, 'N/A')),
                'wrCurrent': safe_float_val(get_val(all_results.get('Williams_Current', []), i, -50)),
                'wrDivergence': str(get_val(all_results.get('Williams_Divergence', []), i, 'NEUTRAL')),
                'wrDivStrength': safe_float_val(get_val(all_results.get('Williams_Div_Strength', []), i, 0)),
                'wrCombined': str(get_val(all_results.get('Williams_Combined', []), i, 'NEUTRAL')),
            },

            # ── OUTPUT FINAL ──
            'finalScore': safe_float_val(get_val(all_results.get('Final_Score', []), i, 50)),
            'entryStatus': str(get_val(all_results.get('Entry_Status', []), i, 'NEUTRAL')),
            'conviction': str(get_val(all_results.get('Conviction', []), i, 'NEUTRAL')),
            'allocation': str(get_val(all_results.get('Allocation', []), i, '0%')),
            'strategy': str(get_val(all_results.get('Strategy', []), i, 'largo_plazo')),
            'alertas': str(get_val(all_results.get('Alertas', []), i, 'OK')),

            # ── DATOS BASE ──
            'priceActual': safe_float_val(get_val(all_results.get('Price_Actual', []), i, 0)),
            'sector': str(get_val(all_results.get('Sector', []), i, 'N/A')),
        }

        batch.set(doc_ref, ticker_data, merge=True)

    batch.commit()
    print(f"✅ Firestore v7.0 actualizado: {len(symbols)} tickers en colección 'portafolio_v7'")
    print(f"   Timestamp: {datetime.now().isoformat()}")