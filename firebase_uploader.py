import logging
import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime
from typing import Dict, List, Any

logger = logging.getLogger(__name__)
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

def upload_to_firestore(all_results: Dict[str, List], symbols: List[str], version: str = '6.3'):
    """
    Sube los datos de los 8 principios a Firestore PRESERVANDO tickers no actualizados.
    Usa merge=True a nivel de campo individual para no borrar datos existentes.
    """
    db = get_db()
    
    now_str = datetime.now().isoformat()
    
    # 1. LEER datos existentes de Firestore para PRESERVAR tickers no en este batch
    existing_tickers_by_p = {}  # p_key -> set de tickers existentes
    existing_data_by_p = {}     # p_key -> {ticker: data_dict}
    
    for p_num in range(1, 9):
        p_key = f'P{p_num}'
        try:
            doc = db.collection('portafolio').document(p_key).get()
            if doc.exists:
                data = doc.to_dict()
                tickers_list = data.get('tickers', [])
                existing_tickers_by_p[p_key] = {t['ticker'] for t in tickers_list if 'ticker' in t}
                existing_data_by_p[p_key] = {t['ticker']: t for t in tickers_list if 'ticker' in t}
                logger.info(f"📖 {p_key}: {len(tickers_list)} tickers existentes cargados")
            else:
                existing_tickers_by_p[p_key] = set()
                existing_data_by_p[p_key] = {}
        except Exception as e:
            logger.warning(f"⚠️ No se pudieron leer datos existentes de {p_key}: {e}")
            existing_tickers_by_p[p_key] = set()
            existing_data_by_p[p_key] = {}
    
    # 2. Construir nuevos datos MERGEADOS (existentes + nuevos)
    batch_symbols = set(s.strip().upper() for s in symbols)
    
    for p_num in range(1, 9):
        p_key = f'P{p_num}'
        
        # Empezar con tickers existentes
        merged_tickers = []
        merged_tickers_dict = {}
        
        # Preservar tickers existentes que NO están en este batch
        for ticker, data in existing_data_by_p.get(p_key, {}).items():
            if ticker not in batch_symbols:
                merged_tickers_dict[ticker] = data
        
        logger.info(f"🔒 {p_key}: {len(merged_tickers_dict)} tickers preservados")
    
    # 3. Construir datos de tickers en este batch
    for i, sym in enumerate(symbols):
        sym_upper = sym.strip().upper()
        
        # ========== P1 - PRECIO OBJETIVO ==========
        tp = get_val(all_results.get('Target Mean Price', []), i)
        pa = get_val(all_results.get('Price Actual', []), i, 0)
        ac = get_val(all_results.get('Analyst Count', []), i, 0)

        tp_float = safe_float_val(tp)
        pa_float = safe_float_val(pa)

        upside = 0
        if tp_float and pa_float and pa_float > 0:
            upside = (tp_float - pa_float) / pa_float

        cartera_val = str(get_val(all_results.get('Cartera', []), i, 'No'))
        if cartera_val.lower() in ('sí', 'si', 'yes', '1', 'true'):
            cartera_val = 'Sí'
        else:
            cartera_val = 'No'
        
        p1_data = {
            'ticker': sym_upper,
            'cartera': cartera_val,
            'priceTarget': tp_float,
            'priceActual': pa_float,
            'numAnalysts': int(safe_float_val(ac, 0)),
            'targetDispersion': str(get_val(all_results.get('Target Dispersion', []), i, 'N/A')),
            'scoreP1': safe_float_val(get_val(all_results.get('Score_Precio', []), i, 50)),
            'upsideCalculado': upside,
            'scoreFinal': safe_float_val(get_val(all_results.get('Score_Final', []), i, 50)),
            'grade': str(get_val(all_results.get('Grade', []), i, 'C')),
            'lastUpdate': now_str,
        }
        merged_tickers_dict[sym_upper] = p1_data

        # ========== P2 - CRECIMIENTO ==========
        rev = safe_float_val(get_val(all_results.get('Rev_Growth_YoY', []), i, 0))
        om = safe_float_val(get_val(all_results.get('Operating_Margin', []), i, 0))
        gm = safe_float_val(get_val(all_results.get('Gross_Margin', []), i, 0))
        est = safe_float_val(get_val(all_results.get('Earning Estimate AVG', []), i, 0))
        est_growth = est
        moat = (gm * 0.5 + om * 0.5) if gm and om else 0
        tipo = 'CRECIMIENTO SANO'
        if rev > 0.5:
            tipo = 'CRECIMIENTO AGRESIVO'
        elif rev < 0:
            tipo = 'CRECIMIENTO NEGATIVO'
        momentum = str(get_val(all_results.get('Growth_Momentum', []), i, 'ESTABLE'))

        p2_data = {
            'ticker': sym_upper,
            'revenueGrowth': rev,
            'estimateGrowth': est_growth,
            'operatingMargin': om,
            'grossMargin': gm,
            'growthMomentum': momentum,
            'moat': moat,
            'tipo': tipo,
            'scoreP2': safe_float_val(get_val(all_results.get('Score_Crecimiento', []), i, 50)),
            'lastUpdate': now_str,
        }
        # Merge con existente si hay más campos
        if sym_upper in existing_data_by_p.get('P2', {}):
            existing_p2 = existing_data_by_p['P2'][sym_upper].copy()
            existing_p2.update(p2_data)
            p2_data = existing_p2
        merged_tickers_dict[sym_upper] = p2_data  # Esto es para P2, necesitamos estructura separada

    # CORRECCIÓN: Necesitamos un dict por principio
    # Rehacemos la estructura correctamente:
    
    # Reiniciar merged por principio
    merged_by_principio = {}
    for p_num in range(1, 9):
        p_key = f'P{p_num}'
        merged_by_principio[p_key] = {}
        # Preservar existentes
        for ticker, data in existing_data_by_p.get(p_key, {}).items():
            if ticker not in batch_symbols:
                merged_by_principio[p_key][ticker] = data

    # Ahora sí, construir cada principio
    for i, sym in enumerate(symbols):
        sym_upper = sym.strip().upper()
        
        # P1
        tp = get_val(all_results.get('Target Mean Price', []), i)
        pa = get_val(all_results.get('Price Actual', []), i, 0)
        ac = get_val(all_results.get('Analyst Count', []), i, 0)
        tp_float = safe_float_val(tp)
        pa_float = safe_float_val(pa)
        upside = (tp_float - pa_float) / pa_float if tp_float and pa_float and pa_float > 0 else 0
        cartera_val = str(get_val(all_results.get('Cartera', []), i, 'No'))
        cartera_val = 'Sí' if cartera_val.lower() in ('sí', 'si', 'yes', '1', 'true') else 'No'
        
        merged_by_principio['P1'][sym_upper] = {
            'ticker': sym_upper, 'cartera': cartera_val,
            'priceTarget': tp_float, 'priceActual': pa_float,
            'numAnalysts': int(safe_float_val(ac, 0)),
            'targetDispersion': str(get_val(all_results.get('Target Dispersion', []), i, 'N/A')),
            'scoreP1': safe_float_val(get_val(all_results.get('Score_Precio', []), i, 50)),
            'upsideCalculado': upside,
            'scoreFinal': safe_float_val(get_val(all_results.get('Score_Final', []), i, 50)),
            'grade': str(get_val(all_results.get('Grade', []), i, 'C')),
            'lastUpdate': now_str,
        }

        # P2
        rev = safe_float_val(get_val(all_results.get('Rev_Growth_YoY', []), i, 0))
        om = safe_float_val(get_val(all_results.get('Operating_Margin', []), i, 0))
        gm = safe_float_val(get_val(all_results.get('Gross_Margin', []), i, 0))
        est = safe_float_val(get_val(all_results.get('Earning Estimate AVG', []), i, 0))
        moat = (gm * 0.5 + om * 0.5) if gm and om else 0
        tipo = 'CRECIMIENTO AGRESIVO' if rev > 0.5 else 'CRECIMIENTO NEGATIVO' if rev < 0 else 'CRECIMIENTO SANO'
        
        merged_by_principio['P2'][sym_upper] = {
            'ticker': sym_upper, 'revenueGrowth': rev,
            'estimateGrowth': est, 'operatingMargin': om,
            'grossMargin': gm,
            'growthMomentum': str(get_val(all_results.get('Growth_Momentum', []), i, 'ESTABLE')),
            'moat': moat, 'tipo': tipo,
            'scoreP2': safe_float_val(get_val(all_results.get('Score_Crecimiento', []), i, 50)),
            'lastUpdate': now_str,
        }

        # P3
        sma = get_val(all_results.get('SMA_200', []), i)
        sma_float = safe_float_val(sma.replace('$', '') if isinstance(sma, str) and '$' in sma else sma, 0)
        trend = str(get_val(all_results.get('SMA_Trend', []), i, 'N/A'))
        ratio = pa_float / sma_float if sma_float and sma_float > 0 else 0
        if pa_float < sma_float:
            estado = 'TENDENCIA BAJISTA'
        elif ratio <= 1.05:
            estado = 'ZONA DE COMPRA (Soporte)'
        elif ratio <= 1.15:
            estado = 'ACUMULAR'
        elif ratio <= 1.30:
            estado = 'SUBIENDO'
        else:
            estado = 'SOBREEXTENDIDO'
            
        merged_by_principio['P3'][sym_upper] = {
            'ticker': sym_upper, 'priceActual': pa_float,
            'sma200': sma_float, 'smaTrend': trend.strip(),
            'volatilityATR': safe_float_val(get_val(all_results.get('Volatility_ATR', []), i, 0)),
            'estado': estado,
            'scoreP3': safe_float_val(get_val(all_results.get('Score_Tendencia', []), i, 50)),
            'lastUpdate': now_str,
        }

        # P4
        beat = safe_float_val(get_val(all_results.get('Beat Rate', []), i, 0))
        wc_raw = get_val(all_results.get('Weighted Consistency', []), i, 0)
        recent4q_raw = get_val(all_results.get('Recent 4Q Avg', []), i, 0)
        
        merged_by_principio['P4'][sym_upper] = {
            'ticker': sym_upper, 'beatRate': beat,
            'weightedConsistency': safe_float_val(wc_raw, 0.0),
            'surpriseTrend': str(get_val(all_results.get('Surprise Trend', []), i, 'N/A')),
            'worstMiss': safe_float_val(get_val(all_results.get('Worst Miss', []), i, 0)),
            'recent4QAvg': safe_float_val(recent4q_raw, 0.0),
            'revenueSurprise4Q': str(get_val(all_results.get('Revenue Surprise 4Q', []), i, 'N/A')),
            'earningsWindow': str(get_val(all_results.get('Earnings Window', []), i, 'N/A')),
            'scoreP4': safe_float_val(get_val(all_results.get('Score_Consistencia', []), i, 50)),
            'lastUpdate': now_str,
        }

        # P5
        peg_raw = get_val(all_results.get('PEG', []), i, 'N/A')
        pe_raw = get_val(all_results.get('Forward PE', []), i, 'N/A')
        pe_hist = get_val(all_results.get('P/E Promedio 6 meses', []), i, 'N/A')
        fcf_yield = safe_float_val(get_val(all_results.get('FCF Yield', []), i, 0))
        debt_raw = str(get_val(all_results.get('Debt/EBITDA', []), i, 'N/A'))
        fcf_growth = safe_float_val(get_val(all_results.get('FCF Growth YoY', []), i, 0))
        interest_cov = safe_float_val(get_val(all_results.get('Interest Coverage', []), i, 0))
        fcf_margin = safe_float_val(get_val(all_results.get('FCF Margin', []), i, 0))
        fcf_ni = safe_float_val(get_val(all_results.get('FCF/NI Ratio', []), i, 0))
        total_cash = safe_float_val(get_val(all_results.get('Total Cash', []), i, 0))
        op_expense = safe_float_val(get_val(all_results.get('Operating Expense TTM', []), i, 0))
        op_expense_monthly = op_expense / 12 if op_expense else 0
        months_cash = total_cash / op_expense_monthly if op_expense_monthly > 0 else 0
        total_debt_str = str(get_val(all_results.get('Total Debt (mrq)', []), i, '$0'))
        total_debt = safe_float_val(total_debt_str.replace('$', '').replace(',', ''), 0)
        interest_exp_str = str(get_val(all_results.get('Interest Expense', []), i, 'N/A'))
        interest_exp = safe_float_val(interest_exp_str.replace('$', '').replace(',', ''), 0)
        interest_rate = (interest_exp / total_debt) * 100 if total_debt > 0 and interest_exp > 0 else 0
        years_pay = str(get_val(all_results.get('Years to Pay Debt', []), i, 'N/A'))
        debt_ebitda_num = safe_float_val(debt_raw.replace('$', '').replace(',', ''), 0)
        
        # Clasificaciones
        peg_num = safe_float_val(peg_raw, 0)
        pe_num = safe_float_val(pe_raw, 0)
        if peg_num > 0:
            pe_clasif = "EXCELENTE (Growth)" if peg_num < 1.2 else "PRECIO JUSTO" if peg_num < 2 else "CARO (Sobrecrecido)"
        elif pe_num > 0:
            pe_clasif = "BARATO" if pe_num < 15 else "MOD. BARATO" if pe_num < 20 else "MODERADO" if pe_num <= 39.9 else "CARO" if pe_num <= 59 else "MUY CARO"
        else:
            pe_clasif = "N/A"
        
        fcf_quality = "N/A" if fcf_ni == 0 else "EXCELENTE" if fcf_ni >= 1.2 else "BUENA" if fcf_ni >= 0.9 else "ACEPTABLE" if fcf_ni >= 0.7 else "CUESTIONABLE"
        cash_clasif = "SIN DATOS" if months_cash == 0 else "Optimizada" if months_cash < 6 else "Ideal" if months_cash < 12 else "Rica" if months_cash >= 12 else "Inestable"
        debt_clasif = "Deuda Excelente" if interest_rate < 2 else "Deuda Razonable" if interest_rate <= 5 else "Deuda Costosa"
        debt_level = "SIN DEUDA" if debt_raw == "N/A" or debt_ebitda_num == 0 else "MUY BAJA" if debt_ebitda_num < 1 else "BAJA" if debt_ebitda_num < 2 else "MODERADA" if debt_ebitda_num < 3 else "ALTA" if debt_ebitda_num < 4 else "MUY ALTA"
        payoff_speed = "N/A" if years_pay == "N/A" else "SIN DEUDA" if total_debt == 0 else "MUY RÁPIDO" if safe_float_val(years_pay, 0) < 2 else "RÁPIDO" if safe_float_val(years_pay, 0) < 4 else "MODERADO" if safe_float_val(years_pay, 0) < 7 else "LENTO" if safe_float_val(years_pay, 0) < 10 else "MUY LENTO"

        merged_by_principio['P5'][sym_upper] = {
            'ticker': sym_upper,
            'peg': str(peg_raw) if peg_raw != 'N/A' else 'N/A',
            'forwardPE': str(pe_raw) if pe_raw != 'N/A' else 'N/A',
            'peHistorico': str(pe_hist) if pe_hist != 'N/A' else 'N/A',
            'fcfYield': fcf_yield,
            'debtEbitda': str(debt_raw) if debt_raw != 'N/A' else 'N/A',
            'fcfGrowth': fcf_growth,
            'interestCoverage': interest_cov,
            'fcfMargin': fcf_margin,
            'fcfNIRatio': fcf_ni,
            'fcfQuality': fcf_quality,
            'peClasificacion': pe_clasif,
            'totalCash': total_cash,
            'opExpenseTTM': op_expense,
            'opExpenseMonthly': op_expense_monthly,
            'monthsCash': months_cash,
            'cashClasificacion': cash_clasif,
            'totalDebt': total_debt,
            'interestExpense': interest_exp,
            'interestRate': interest_rate,
            'debtEquity': str(get_val(all_results.get('Debt/Equity', []), i, 'N/A')),
            'yearsPayDebt': years_pay,
            'debtClasificacion': debt_clasif,
            'debtLevel': debt_level,
            'payoffSpeed': payoff_speed,
            'revenueEstimate': str(get_val(all_results.get('Revenue Estimate AVG', []), i, 'N/A')),
            'profitMargin': safe_float_val(get_val(all_results.get('Profit Margin', []), i, 0)),
            'futureEPS': str(get_val(all_results.get('Future EPS', []), i, 'N/A')),
            'expectedPE': str(get_val(all_results.get('Expected PE', []), i, 'N/A')),
            'expectedReturnEPS': str(get_val(all_results.get('Expected Return (EPS)', []), i, 'N/A')),
            'expectedReturnRev': str(get_val(all_results.get('Expected Return (Rev)', []), i, 'N/A')),
            'expectedReturnAnalyst': str(get_val(all_results.get('Expected Return (Analyst)', []), i, 'N/A')),
            'expectedReturnConsensus': str(get_val(all_results.get('Expected Return (Consensus)', []), i, 'N/A')),
            'scoreP5': safe_float_val(get_val(all_results.get('Score_Valoracion', []), i, 50)),
            'lastUpdate': now_str,
        }

        # P6
        merged_by_principio['P6'][sym_upper] = {
            'ticker': sym_upper,
            'posicionSR': str(get_val(all_results.get('Posición S/R', []), i, 'N/A')),
            'distSoporte': safe_float_val(get_val(all_results.get('Dist a Soporte %', []), i, 0)),
            'distResistencia': safe_float_val(get_val(all_results.get('Dist a Resistencia %', []), i, 0)),
            'soporteCercano': safe_float_val(get_val(all_results.get('Soporte Cercano', []), i, 0)),
            'resistenciaCercana': safe_float_val(get_val(all_results.get('Resistencia Cercana', []), i, 0)),
            'soportes': str(get_val(all_results.get('Soportes', []), i, 'N/A')),
            'resistencias': str(get_val(all_results.get('Resistencias', []), i, 'N/A')),
            'fibonacci': str(get_val(all_results.get('Fibonacci Cerca', []), i, 'N/A')),
            'fuerzaSoporte': safe_float_val(get_val(all_results.get('Fuerza Soporte', []), i, 0)),
            'fuerzaResistencia': safe_float_val(get_val(all_results.get('Fuerza Resistencia', []), i, 0)),
            'atrThreshold': safe_float_val(get_val(all_results.get('ATR Threshold %', []), i, 0.02)),
            'min200d': safe_float_val(get_val(all_results.get('Min 200d', []), i, 0)),
            'max200d': safe_float_val(get_val(all_results.get('Max 200d', []), i, 0)),
            'scoreP6': safe_float_val(get_val(all_results.get('Score_Soportes', []), i, 50)),
            'lastUpdate': now_str,
        }

        # P7
        merged_by_principio['P7'][sym_upper] = {
            'ticker': sym_upper,
            'williamsCurrent': safe_float_val(get_val(all_results.get('Williams %R (Current)', []), i, 0)),
            'williamsDaily': safe_float_val(get_val(all_results.get('Williams %R (Daily)', []), i, 0)),
            'williamsWeekly': safe_float_val(get_val(all_results.get('Williams %R (Weekly)', []), i, 0)),
            'williams1w': safe_float_val(get_val(all_results.get('Williams %R (1w ago)', []), i, 0)),
            'williams2w': safe_float_val(get_val(all_results.get('Williams %R (2w ago)', []), i, 0)),
            'williamsLookback': int(safe_float_val(get_val(all_results.get('Williams Lookback', []), i, 14))),
            'williamsATR': safe_float_val(get_val(all_results.get('Williams ATR%', []), i, 0.02)),
            'williamsDivergence': str(get_val(all_results.get('Williams Divergence', []), i, 'NEUTRAL')),
            'williamsDivStrength': safe_float_val(get_val(all_results.get('Williams Div Strength', []), i, 0)),
            'williamsCombinedSignal': str(get_val(all_results.get('Williams Combined Signal', []), i, 'NEUTRAL')),
            'williamsCombinedStrength': safe_float_val(get_val(all_results.get('Williams Combined Strength', []), i, 0)),
            'williamsSignalStrength': safe_float_val(get_val(all_results.get('Williams Signal Strength', []), i, 0)),
            'williamsSignalQuality': str(get_val(all_results.get('Williams Signal Quality', []), i, 'BAJA')),
            'williamsState': str(get_val(all_results.get('Williams State', []), i, 'NEUTRAL')),
            'scoreP7': safe_float_val(get_val(all_results.get('Score_Williams', []), i, 50)),
            'lastUpdate': now_str,
        }

        # P8
        merged_by_principio['P8'][sym_upper] = {
            'ticker': sym_upper,
            'volumeRatio': safe_float_val(get_val(all_results.get('Volume Ratio', []), i, 1)),
            'volumeLevel': str(get_val(all_results.get('Volume Level', []), i, 'N/A')),
            'obvTrend': str(get_val(all_results.get('OBV Trend', []), i, 'N/A')),
            'mfiLevel': str(get_val(all_results.get('MFI Level', []), i, 'N/A')),
            'mfi': safe_float_val(get_val(all_results.get('MFI', []), i, 50)),
            'priceVolumeDiv': str(get_val(all_results.get('Price-Volume Div', []), i, 'N/A')),
            'scoreP8': safe_float_val(get_val(all_results.get('Score_Volumen', []), i, 50)),
            'priceChange20d': 0,
            'volChange20d': 0,
            'lastUpdate': now_str,
        }

    # 4. Subir a Firestore
    batch = db.batch()
    
    for p_num in range(1, 9):
        p_key = f'P{p_num}'
        tickers_list = list(merged_by_principio[p_key].values())
        
        doc_ref = db.collection('portafolio').document(p_key)
        batch.set(doc_ref, {
            'tickers': tickers_list,
            'count': len(tickers_list),
            'updatedAt': now_str,
        }, merge=True)
        
        logger.info(f"📤 {p_key}: {len(tickers_list)} tickers totales ({len(batch_symbols)} actualizados + {len(tickers_list) - len(batch_symbols)} preservados)")

    # Metadata
    meta_ref = db.collection('portafolio').document('metadata')
    batch.set(meta_ref, {
        'lastUpdate': now_str,
        'totalTickers': sum(len(merged_by_principio[f'P{p}']) for p in range(1, 9)) // 8,
        'version': version,
        'source': 'main_V6.py',
        'lastBatchSize': len(symbols),
        'lastBatchTickers': list(batch_symbols),
    }, merge=True)

    batch.commit()
    logger.info(f"✅ Firestore actualizado: {len(symbols)} tickers actualizados, datos preservados")
    logger.info(f"   Timestamp: {now_str}")