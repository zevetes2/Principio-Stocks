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

def upload_to_firestore(all_results: Dict[str, List], symbols: List[str]):
    """
    Sube los datos de los 8 principios a Firestore.

    all_results: dict con arrays de cada métrica (formato de main.py)
    symbols: lista de tickers en orden
    """
    db = get_db()
    batch = db.batch()

    # Metadata
    meta_ref = db.collection('portafolio').document('metadata')
    batch.set(meta_ref, {
        'lastUpdate': datetime.now().isoformat(),
        'totalTickers': len(symbols),
        'version': '6.3',
        'source': 'main.py'
    }, merge=True)

    # Construir datos por principio
    principios_data = {}
    for p_num in range(1, 9):
        p_key = f'P{p_num}'
        principios_data[p_key] = {
            'tickers': [],
            'count': len(symbols),
            'updatedAt': datetime.now().isoformat()
        }

    for i, sym in enumerate(symbols):
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
        # Normalizar valores
        if cartera_val.lower() in ('sí', 'si', 'yes', '1', 'true'):
            cartera_val = 'Sí'
        else:
            cartera_val = 'No'
        principios_data['P1']['tickers'].append({
            'ticker': sym,
            'cartera': cartera_val,
            'priceTarget': tp_float,
            'priceActual': pa_float,
            'numAnalysts': int(safe_float_val(ac, 0)),
            'targetDispersion': str(get_val(all_results.get('Target Dispersion', []), i, 'N/A')),
            'scoreP1': safe_float_val(get_val(all_results.get('Score_Precio', []), i, 50)),
            'upsideCalculado': upside,
            # NUEVOS: Score final y grade para el dashboard
            'scoreFinal': safe_float_val(get_val(all_results.get('Score_Final', []), i, 50)),
            'grade': str(get_val(all_results.get('Grade', []), i, 'C'))
        })

        # ========== P2 - CRECIMIENTO ==========
        rev = safe_float_val(get_val(all_results.get('Rev_Growth_YoY', []), i, 0))
        om = safe_float_val(get_val(all_results.get('Operating_Margin', []), i, 0))
        gm = safe_float_val(get_val(all_results.get('Gross_Margin', []), i, 0))
        est = safe_float_val(get_val(all_results.get('Earning Estimate AVG', []), i, 0))

        # Estimate Growth: calcular como (Estimate - Actual) / Actual si tenemos datos
        # Por ahora usamos Revenue Growth como proxy o 0
        est_growth = est  # placeholder - puedes mejorar esto

        # Moat: proxy usando Gross Margin + Operating Margin
        moat = (gm * 0.5 + om * 0.5) if gm and om else 0

        # Tipo de crecimiento
        tipo = 'CRECIMIENTO SANO'
        if rev > 0.5:
            tipo = 'CRECIMIENTO AGRESIVO'
        elif rev < 0:
            tipo = 'CRECIMIENTO NEGATIVO'

        momentum = str(get_val(all_results.get('Growth_Momentum', []), i, 'ESTABLE'))

        principios_data['P2']['tickers'].append({
            'ticker': sym,
            'revenueGrowth': rev,
            'estimateGrowth': est_growth,
            'operatingMargin': om,
            'grossMargin': gm,
            'growthMomentum': momentum,
            'moat': moat,
            'tipo': tipo,
            'scoreP2': safe_float_val(get_val(all_results.get('Score_Crecimiento', []), i, 50))
        })

        # ========== P3 - TENDENCIA ==========
        sma = get_val(all_results.get('SMA_200', []), i)
        trend = str(get_val(all_results.get('SMA_Trend', []), i, 'N/A'))
        vol = safe_float_val(get_val(all_results.get('Volatility_ATR', []), i, 0))

        # Parsear SMA (viene como "$123.45" o número)
        sma_float = 0
        if isinstance(sma, str) and '$' in sma:
            sma_float = safe_float_val(sma.replace('$', ''))
        else:
            sma_float = safe_float_val(sma)

        # C7 = priceActual, M7 = sma200
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

        principios_data['P3']['tickers'].append({
            'ticker': sym,
            'priceActual': pa_float,  # <-- AGREGAR ESTA LÍNEA
            'sma200': sma_float,
            'smaTrend': trend.strip(),
            'volatilityATR': vol,
            'estado': estado,
            'scoreP3': safe_float_val(get_val(all_results.get('Score_Tendencia', []), i, 50))
        })

                # ========== P4 - CONSISTENCIA ==========
        beat = safe_float_val(get_val(all_results.get('Beat Rate', []), i, 0))
        wc_raw = get_val(all_results.get('Weighted Consistency', []), i, 0)
        wc = safe_float_val(wc_raw, 0.0)
        worst = safe_float_val(get_val(all_results.get('Worst Miss', []), i, 0))
        recent4q_raw = get_val(all_results.get('Recent 4Q Avg', []), i, 0)
        recent4q = safe_float_val(recent4q_raw, 0.0)
        revSurp = get_val(all_results.get('Revenue Surprise 4Q', []), i, 'N/A')
        earnWindow = str(get_val(all_results.get('Earnings Window', []), i, 'N/A'))
        surpriseTrend = str(get_val(all_results.get('Surprise Trend', []), i, 'N/A'))

        principios_data['P4']['tickers'].append({
            'ticker': sym,
            'beatRate': beat,
            'weightedConsistency': wc,
            'surpriseTrend': surpriseTrend,
            'worstMiss': worst,
            'recent4QAvg': recent4q,
            'revenueSurprise4Q': str(revSurp) if revSurp != 'N/A' else 'N/A',
            'earningsWindow': earnWindow,
            'scoreP4': safe_float_val(get_val(all_results.get('Score_Consistencia', []), i, 50))
        })

                # ========== P5 - VALORACIÓN COMPLETA ==========
        # Datos básicos
        peg_raw = get_val(all_results.get('PEG', []), i, 'N/A')
        pe_raw = get_val(all_results.get('Forward PE', []), i, 'N/A')
        pe_historico_raw = get_val(all_results.get('P/E Promedio 6 meses', []), i, 'N/A')
        fcf_yield = safe_float_val(get_val(all_results.get('FCF Yield', []), i, 0))
        debt_ebitda_raw = get_val(all_results.get('Debt/EBITDA', []), i, 'N/A')
        fcf_growth = safe_float_val(get_val(all_results.get('FCF Growth YoY', []), i, 0))
        interest_cov = safe_float_val(get_val(all_results.get('Interest Coverage', []), i, 0))
        fcf_margin = safe_float_val(get_val(all_results.get('FCF Margin', []), i, 0))
        fcf_ni = safe_float_val(get_val(all_results.get('FCF/NI Ratio', []), i, 0))
        
        # Cash & Burn
        total_cash = safe_float_val(get_val(all_results.get('Total Cash', []), i, 0))
        op_expense = safe_float_val(get_val(all_results.get('Operating Expense TTM', []), i, 0))
        op_expense_monthly = op_expense / 12 if op_expense else 0
        months_cash = total_cash / op_expense_monthly if op_expense_monthly > 0 else 0
        
        # Deuda
        total_debt_str = str(get_val(all_results.get('Total Debt (mrq)', []), i, '$0'))
        total_debt = safe_float_val(total_debt_str.replace('$', '').replace(',', ''), 0)
        interest_exp_str = str(get_val(all_results.get('Interest Expense', []), i, 'N/A'))
        interest_exp = safe_float_val(interest_exp_str.replace('$', '').replace(',', ''), 0)
        debt_equity = str(get_val(all_results.get('Debt/Equity', []), i, 'N/A'))
        years_pay = str(get_val(all_results.get('Years to Pay Debt', []), i, 'N/A'))
        
        # Interest Rate
        interest_rate = 0
        if total_debt > 0 and interest_exp > 0:
            interest_rate = (interest_exp / total_debt) * 100
        
        # Clasificaciones
        # PEG/P/E
        peg_num = safe_float_val(peg_raw, 0)
        pe_num = safe_float_val(pe_raw, 0)
        pe_hist_num = safe_float_val(pe_historico_raw, 0)
        
        if peg_num > 0:
            if peg_num < 1.2: pe_clasif = "EXCELENTE (Growth)"
            elif peg_num < 2: pe_clasif = "PRECIO JUSTO"
            else: pe_clasif = "CARO (Sobrecrecido)"
        elif pe_num > 0:
            if pe_num < 15: pe_clasif = "BARATO"
            elif pe_num < 20: pe_clasif = "MOD. BARATO"
            elif pe_num <= 39.9: pe_clasif = "MODERADO"
            elif pe_num <= 59: pe_clasif = "CARO"
            else: pe_clasif = "MUY CARO"
        else:
            pe_clasif = "N/A"
        
        # FCF Quality
        if fcf_ni == 0: fcf_quality = "N/A"
        elif fcf_ni >= 1.2: fcf_quality = "EXCELENTE"
        elif fcf_ni >= 0.9: fcf_quality = "BUENA"
        elif fcf_ni >= 0.7: fcf_quality = "ACEPTABLE"
        else: fcf_quality = "CUESTIONABLE"
        
        # Cash Classification
        if months_cash == 0: cash_clasif = "SIN DATOS"
        elif months_cash >= 3 and months_cash < 6: cash_clasif = "Optimizada"
        elif months_cash >= 6 and months_cash < 12: cash_clasif = "Ideal"
        elif months_cash >= 12: cash_clasif = "Rica"
        elif months_cash < 3: cash_clasif = "Inestable"
        else: cash_clasif = "N/A"
        
        # Debt Classification
        if interest_rate < 2: debt_clasif = "Deuda Excelente"
        elif interest_rate <= 5: debt_clasif = "Deuda Razonable"
        else: debt_clasif = "Deuda Costosa"
        
        # Debt Level
        debt_ebitda_num = safe_float_val(debt_ebitda_raw.replace('$', '').replace(',', ''), 0)
        if debt_ebitda_raw == "N/A" or debt_ebitda_num == 0: debt_level = "SIN DEUDA"
        elif debt_ebitda_num < 1: debt_level = "MUY BAJA"
        elif debt_ebitda_num < 2: debt_level = "BAJA"
        elif debt_ebitda_num < 3: debt_level = "MODERADA"
        elif debt_ebitda_num < 4: debt_level = "ALTA"
        else: debt_level = "MUY ALTA"
        
        # Payoff Speed
        years_pay_num = safe_float_val(years_pay.replace('$', '').replace(',', ''), 0)
        if years_pay == "N/A": payoff_speed = "N/A"
        elif total_debt == 0: payoff_speed = "SIN DEUDA"
        elif years_pay_num < 2: payoff_speed = "MUY RÁPIDO"
        elif years_pay_num < 4: payoff_speed = "RÁPIDO"
        elif years_pay_num < 7: payoff_speed = "MODERADO"
        elif years_pay_num < 10: payoff_speed = "LENTO"
        else: payoff_speed = "MUY LENTO"
        
        # Future Returns
        rev_est = str(get_val(all_results.get('Revenue Estimate AVG', []), i, 'N/A'))
        profit_m = safe_float_val(get_val(all_results.get('Profit Margin', []), i, 0))
        future_eps = str(get_val(all_results.get('Future EPS', []), i, 'N/A'))
        exp_pe = str(get_val(all_results.get('Expected PE', []), i, 'N/A'))
        ret_eps = str(get_val(all_results.get('Expected Return (EPS)', []), i, 'N/A'))
        ret_rev = str(get_val(all_results.get('Expected Return (Rev)', []), i, 'N/A'))
        ret_analyst = str(get_val(all_results.get('Expected Return (Analyst)', []), i, 'N/A'))
        ret_cons = str(get_val(all_results.get('Expected Return (Consensus)', []), i, 'N/A'))

        principios_data['P5']['tickers'].append({
            'ticker': sym,
            'peg': str(peg_raw) if peg_raw != 'N/A' else 'N/A',
            'forwardPE': str(pe_raw) if pe_raw != 'N/A' else 'N/A',
            'peHistorico': str(pe_historico_raw) if pe_historico_raw != 'N/A' else 'N/A',
            'fcfYield': fcf_yield,
            'debtEbitda': str(debt_ebitda_raw) if debt_ebitda_raw != 'N/A' else 'N/A',
            'fcfGrowth': fcf_growth,
            'interestCoverage': interest_cov,
            'fcfMargin': fcf_margin,
            'fcfNIRatio': fcf_ni,
            'fcfQuality': fcf_quality,
            'peClasificacion': pe_clasif,
            
            # Cash
            'totalCash': total_cash,
            'opExpenseTTM': op_expense,
            'opExpenseMonthly': op_expense_monthly,
            'monthsCash': months_cash,
            'cashClasificacion': cash_clasif,
            
            # Debt
            'totalDebt': total_debt,
            'interestExpense': interest_exp,
            'interestRate': interest_rate,
            'debtEquity': debt_equity,
            'yearsPayDebt': years_pay,
            'debtClasificacion': debt_clasif,
            'debtLevel': debt_level,
            'payoffSpeed': payoff_speed,
            
            # Future Returns
            'revenueEstimate': rev_est,
            'profitMargin': profit_m,
            'futureEPS': future_eps,
            'expectedPE': exp_pe,
            'expectedReturnEPS': ret_eps,
            'expectedReturnRev': ret_rev,
            'expectedReturnAnalyst': ret_analyst,
            'expectedReturnConsensus': ret_cons,
            
            'scoreP5': safe_float_val(get_val(all_results.get('Score_Valoracion', []), i, 50))
        })

                # ========== P6 - SOPORTES PRO ==========
        principios_data['P6']['tickers'].append({
            'ticker': sym,
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
            'scoreP6': safe_float_val(get_val(all_results.get('Score_Soportes', []), i, 50))
        })

                # ========== P7 - WILLIAMS %R PRO ==========
        williams_current = safe_float_val(get_val(all_results.get('Williams %R (Current)', []), i, 0))
        williams_daily = safe_float_val(get_val(all_results.get('Williams %R (Daily)', []), i, 0))
        williams_weekly = safe_float_val(get_val(all_results.get('Williams %R (Weekly)', []), i, 0))
        williams_1w = safe_float_val(get_val(all_results.get('Williams %R (1w ago)', []), i, 0))
        williams_2w = safe_float_val(get_val(all_results.get('Williams %R (2w ago)', []), i, 0))
        
        principios_data['P7']['tickers'].append({
            'ticker': sym,
            'williamsCurrent': williams_current,
            'williamsDaily': williams_daily,
            'williamsWeekly': williams_weekly,
            'williams1w': williams_1w,
            'williams2w': williams_2w,
            'williamsLookback': int(safe_float_val(get_val(all_results.get('Williams Lookback', []), i, 14))),
            'williamsATR': safe_float_val(get_val(all_results.get('Williams ATR%', []), i, 0.02)),
            'williamsDivergence': str(get_val(all_results.get('Williams Divergence', []), i, 'NEUTRAL')),
            'williamsDivStrength': safe_float_val(get_val(all_results.get('Williams Div Strength', []), i, 0)),
            'williamsCombinedSignal': str(get_val(all_results.get('Williams Combined Signal', []), i, 'NEUTRAL')),
            'williamsCombinedStrength': safe_float_val(get_val(all_results.get('Williams Combined Strength', []), i, 0)),
            'williamsSignalStrength': safe_float_val(get_val(all_results.get('Williams Signal Strength', []), i, 0)),
            'williamsSignalQuality': str(get_val(all_results.get('Williams Signal Quality', []), i, 'BAJA')),
            'williamsState': str(get_val(all_results.get('Williams State', []), i, 'NEUTRAL')),
            'scoreP7': safe_float_val(get_val(all_results.get('Score_Williams', []), i, 50))
        })

        # ========== P8 - VOLUMEN ==========
        principios_data['P8']['tickers'].append({
            'ticker': sym,
            'volumeRatio': safe_float_val(get_val(all_results.get('Volume Ratio', []), i, 1)),
            'volumeLevel': str(get_val(all_results.get('Volume Level', []), i, 'N/A')),
            'obvTrend': str(get_val(all_results.get('OBV Trend', []), i, 'N/A')),
            'mfiLevel': str(get_val(all_results.get('MFI Level', []), i, 'N/A')),
            'mfi': safe_float_val(get_val(all_results.get('MFI', []), i, 50)),
            'priceVolumeDiv': str(get_val(all_results.get('Price-Volume Div', []), i, 'N/A')),
            'scoreP8': safe_float_val(get_val(all_results.get('Score_Volumen', []), i, 50)),
            # Campos opcionales para el nuevo diseño
            'priceChange20d': 0,  # Placeholder - agregar en main.py si lo necesitas
            'volChange20d': 0     # Placeholder - agregar en main.py si lo necesitas
        })

    # Subir a Firestore (batch de 500 operaciones máximo)
    for p_key, data in principios_data.items():
        doc_ref = db.collection('portafolio').document(p_key)
        batch.set(doc_ref, data, merge=True)

    batch.commit()
    print(f"✅ Firestore actualizado: {len(symbols)} tickers, 8 principios")
    print(f"   Timestamp: {datetime.now().isoformat()}")

