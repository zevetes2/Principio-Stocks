################################-----------------------PORTAFOLIO E.T.H-----------------------##########################################

# Importa las librerías necesarias
import gspread
from google.oauth2.service_account import Credentials
import yfinance as yf
import pandas as pd
import datetime
import numpy as np

JSON_KEY_FILE = "principios.json"  # <--- ¡IMPORTANTE: CAMBIA ESTO!

spreadsheet_name = "Portafolio Financiero"
worksheet_name = "7 PRINCIPIOS"

start_row = 7
end_row = 187

# --- AUTENTICACIÓN ---
try:
    scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]
    creds = Credentials.from_service_account_file(
        JSON_KEY_FILE,
        scopes=scopes
    )
    gc = gspread.authorize(creds)
    print("Autenticación con Cuenta de Servicio exitosa.")
except FileNotFoundError:
    print(f"Error: No se encontró el archivo '{JSON_KEY_FILE}'.")
    print("Por favor, sube tu archivo JSON al panel de archivos de Colab y verifica el nombre.")
    raise
except Exception as e:
    print(f"Ocurrió un error inesperado durante la autenticación: {e}")
    raise

# --- Rango para leer los tickers ---
ticker_range = f'A{start_row}:A{end_row}'

# --- Rango para escribir cada métrica ---
ranges = {
    # Principio 1
    'Target Mean Price': f'B{start_row}:B{end_row}',
    'Analyst Count': f'E{start_row}:E{end_row}',
    'Target Dispersion': f'F{start_row}:F{end_row}',
    # Principio 2
    'Earning Estimate AVG': f'G{start_row}:G{end_row}',
    'Rev_Growth_YoY': f'H{start_row}:H{end_row}',
    'Gross_Margin': f'I{start_row}:I{end_row}',
    'Operating_Margin': f'K{start_row}:K{end_row}',
    'Growth_Momentum': f'L{start_row}:L{end_row}',
    # Principio 3
    'SMA_200': f'M{start_row}:M{end_row}',
    'SMA_Trend': f'N{start_row}:N{end_row}',
    'Volatility_ATR': f'O{start_row}:O{end_row}',
    # Principio 4
    'Weighted Consistency': f'Q{start_row}:Q{end_row}',
    'Beat Rate': f'R{start_row}:R{end_row}',
    'Recent 4Q Avg': f'S{start_row}:S{end_row}',
    'Revenue Surprise 4Q': f'T{start_row}:T{end_row}',
    'Surprise Trend': f'U{start_row}:U{end_row}',
    'Earnings Window': f'V{start_row}:V{end_row}',
    'Worst Miss': f'W{start_row}:W{end_row}',
    # Principio 5
    # Seccion 1
    'PEG': f'Y{start_row}:Y{end_row}',
    'Interest Coverage': f'Z{start_row}:Z{end_row}',
    'Forward PE': f'AA{start_row}:AA{end_row}',
    # Seccion 2
    'FCF Yield': f'AC{start_row}:AC{end_row}',
    'FCF Growth YoY': f'AD{start_row}:AD{end_row}',
    'FCF/NI Ratio': f'AE{start_row}:AE{end_row}',
    'FCF Margin': f'AF{start_row}:AF{end_row}',
    # Seccion 3
    'Total Cash': f'AI{start_row}:AI{end_row}',
    'Operating Expense TTM': f'AJ{start_row}:AJ{end_row}',
    # Seccion 4
    'Total Debt (mrq)': f'AN{start_row}:AN{end_row}',
    'Interest Expense': f'AO{start_row}:AO{end_row}',
    'Debt/Equity': f'AQ{start_row}:AQ{end_row}',
    'Debt/EBITDA': f'AR{start_row}:AR{end_row}',
    'Years to Pay Debt': f'AS{start_row}:AS{end_row}',
    # Seccion 5
    'Revenue Estimate AVG': f'AW{start_row}:AW{end_row}',
    'Profit Margin': f'AX{start_row}:AX{end_row}',
    'P/E Promedio 6 meses': f'AY{start_row}:AY{end_row}',  # CORREGIDO: era 'Ay'
    # Seccion 6
    'Future EPS': f'BF{start_row}:BF{end_row}',
    'Expected PE': f'BG{start_row}:BG{end_row}',
    'Expected Return (EPS)': f'BH{start_row}:BH{end_row}',
    'Expected Return (Rev)': f'BI{start_row}:BI{end_row}',
    'Expected Return (Analyst)': f'BJ{start_row}:BJ{end_row}',
    'Expected Return (Consensus)': f'BK{start_row}:BK{end_row}',
    # Principio 8
    'Volume Ratio': f'CB{start_row}:CB{end_row}',
    'Volume Level': f'CC{start_row}:CC{end_row}',
    'OBV Trend': f'CD{start_row}:CD{end_row}',
    'Price-Volume Div': f'CE{start_row}:CE{end_row}',
    'MFI': f'CF{start_row}:CF{end_row}',
    'MFI Level': f'CG{start_row}:CG{end_row}',
    # Seccion Final
    'Sector': f'DA{start_row}:DA{end_row}',
    'Days Public': f'DC{start_row}:DC{end_row}',
    'Beta': f'DD{start_row}:DD{end_row}',
    'Official URL': f'DE{start_row}:DE{end_row}'
}

# --- Lógica principal del script ---
try:
    # 1. Abrir la hoja y la pestaña
    sh = gc.open(spreadsheet_name)
    worksheet = sh.worksheet(worksheet_name)

    # 2. Leer los tickers de la hoja
    tickers_list = worksheet.get(ticker_range)
    symbols = [item[0] for item in tickers_list if item and item[0]]

    if not symbols:
        print("No se encontraron tickers en el rango especificado.")
    else:
        print("Tickers encontrados:", symbols)

        # 3. Procesar cada ticker y recopilar los datos
        all_results = {key: [] for key in ranges.keys()}

        for symbol in symbols:
            print(f"\nObteniendo datos para: {symbol}...")

            # Valores por defecto
            defaults = {
                'Target Mean Price': "N/A",
                'Analyst Count': 0,
                'Target Dispersion': "0%",
                'Earning Estimate AVG': "N/A",
                'Rev_Growth_YoY': "0%",
                'Gross_Margin': "0%",
                'Operating_Margin': "0%",
                'Growth_Momentum': "N/A",
                'SMA_200': "N/A",
                'SMA_Trend': "N/A",
                'Volatility_ATR': "N/A",
                'Weighted Consistency': "N/A",
                'Beat Rate': "N/A",
                'Recent 4Q Avg': "N/A",
                'Revenue Surprise 4Q': "N/A",
                'Surprise Trend': "N/A",
                'Earnings Window': "N/A",
                'Worst Miss': "N/A",
                'PEG': "N/A",
                'Interest Coverage': 0,
                'Forward PE': "N/A",
                'FCF Yield': 0,
                'FCF Growth YoY': 0,
                'FCF/NI Ratio': 0,
                'FCF Margin': 0,
                'Total Cash': 0,
                'Operating Expense TTM': 0,
                'Total Debt (mrq)': "0",
                'Interest Expense': "N/A",
                'Debt/Equity': "N/A",
                'Debt/EBITDA': "N/A",
                'Years to Pay Debt': "N/A",
                'Revenue Estimate AVG': "N/A",
                'Profit Margin': "0",
                'P/E Promedio 6 meses': "N/A",
                'Future EPS': "N/A",
                'Expected PE': "N/A",
                'Expected Return (EPS)': "N/A",
                'Expected Return (Rev)': "N/A",
                'Expected Return (Analyst)': "N/A",
                'Expected Return (Consensus)': "N/A",
                'Sector': "N/A",
                'Days Public': "N/A",
                'Beta': "N/A",
                'Official URL': "N/A"
            }

            try:
                ticker = yf.Ticker(symbol)
                info = ticker.info
            except Exception as e:
                print(f"  ❌ Error al inicializar yf.Ticker({symbol}): {e}")
                for key in all_results.keys():
                    all_results[key].append([defaults[key]])
                continue

            #--------------------------------------------PRINCIPIO 1-------------------------------------------------#

            # 1. Target Mean Price
            try:
                target_price = info.get("targetMeanPrice", 0)
                all_results['Target Mean Price'].append([target_price if target_price else "N/A"])
            except:
                all_results['Target Mean Price'].append([defaults['Target Mean Price']])

            # 2. Número de analistas
            try:
                analyst_count = info.get('numberOfAnalystOpinions', 0)
                if analyst_count is None:
                    analyst_count = 0
                all_results['Analyst Count'].append([analyst_count])
            except:
                all_results['Analyst Count'].append([defaults['Analyst Count']])

            # 3. Target Dispersion
            try:
                price_targets = ticker.analyst_price_targets
                if price_targets and 'current' in price_targets:
                    target_high = price_targets.get('high', 0)
                    target_low = price_targets.get('low', 0)
                    target_mean = price_targets.get('mean', 0)

                    if target_mean > 0 and target_high > 0 and target_low > 0:
                        dispersion = (target_high - target_low) / target_mean
                        all_results['Target Dispersion'].append([f"{dispersion:.2%}"])
                    else:
                        all_results['Target Dispersion'].append([defaults['Target Dispersion']])
                else:
                    all_results['Target Dispersion'].append([defaults['Target Dispersion']])
            except:
                all_results['Target Dispersion'].append([defaults['Target Dispersion']])

            #--------------------------------------------PRINCIPIO 2-------------------------------------------------#

            try:
                rev_growth = info.get("revenueGrowth", 0)
                gross_margin = info.get("grossMargins", 0)
                operating_margin = info.get("operatingMargins", 0)

                # Growth Momentum
                try:
                    financials = ticker.quarterly_financials
                    if "Total Revenue" in financials.index and financials.shape[1] >= 2:
                        recent_rev = financials.loc["Total Revenue"].iloc[0]
                        older_rev = financials.loc["Total Revenue"].iloc[1]
                        qoq_growth = (recent_rev - older_rev) / abs(older_rev) if older_rev != 0 else 0

                        if rev_growth and qoq_growth:
                            growth_momentum = "ACELERANDO" if (qoq_growth * 4) > rev_growth else "DESACELERANDO"
                        else:
                            growth_momentum = "ESTABLE"
                    else:
                        growth_momentum = "N/A"
                except:
                    growth_momentum = "N/A"

                all_results['Rev_Growth_YoY'].append([f"{rev_growth:.2%}" if rev_growth else "0%"])
                all_results['Gross_Margin'].append([f"{gross_margin:.2%}" if gross_margin else "0%"])
                all_results['Operating_Margin'].append([f"{operating_margin:.2%}" if operating_margin else "0%"])
                all_results['Growth_Momentum'].append([growth_momentum])
            except:
                all_results['Rev_Growth_YoY'].append([defaults['Rev_Growth_YoY']])
                all_results['Gross_Margin'].append([defaults['Gross_Margin']])
                all_results['Operating_Margin'].append([defaults['Operating_Margin']])
                all_results['Growth_Momentum'].append([defaults['Growth_Momentum']])

            #--------------------------------------------PRINCIPIO 3-------------------------------------------------#

            try:
                end_date = datetime.datetime.today()
                start_date = end_date - datetime.timedelta(days=365)
                hist = ticker.history(start=start_date, end=end_date, interval="1d")

                if not hist.empty and len(hist) >= 250:
                    sma_200_current = hist['Close'].tail(200).mean()
                    sma_200_50d_ago = hist['Close'].iloc[-250:-50].mean()

                    if sma_200_50d_ago > 0:
                        sma_slope = (sma_200_current - sma_200_50d_ago) / sma_200_50d_ago

                        if sma_slope > 0.05:
                            sma_trend = "ALCISTA FUERTE"
                        elif sma_slope > 0.02:
                            sma_trend = "ALCISTA"
                        elif sma_slope > -0.02:
                            sma_trend = "LATERAL"
                        elif sma_slope > -0.05:
                            sma_trend = "BAJISTA"
                        else:
                            sma_trend = "BAJISTA FUERTE"
                    else:
                        sma_trend = "N/A"

                    hist['High_Low'] = hist['High'] - hist['Low']
                    atr_14 = hist['High_Low'].tail(14).mean()
                    current_price = hist['Close'].iloc[-1]
                    volatility_pct = (atr_14 / current_price) if current_price > 0 else 0

                    all_results['SMA_200'].append([f"${sma_200_current:.2f}"])
                    all_results['SMA_Trend'].append([sma_trend])
                    all_results['Volatility_ATR'].append([f"{volatility_pct:.2%}"])
                else:
                    all_results['SMA_200'].append([defaults['SMA_200']])
                    all_results['SMA_Trend'].append([defaults['SMA_Trend']])
                    all_results['Volatility_ATR'].append([defaults['Volatility_ATR']])
            except:
                all_results['SMA_200'].append([defaults['SMA_200']])
                all_results['SMA_Trend'].append([defaults['SMA_Trend']])
                all_results['Volatility_ATR'].append([defaults['Volatility_ATR']])

            #--------------------------------------------PRINCIPIO 4-------------------------------------------------#

            try:
                earnings_history = ticker.earnings_history
                has_data = False
                surprise_data = []

                if earnings_history is not None and not earnings_history.empty:
                    if 'surprisePercent' in earnings_history.columns:
                        history_recent = earnings_history.head(12)
                        history_clean = history_recent[history_recent['surprisePercent'].notna()]

                        if len(history_clean) > 0:
                            has_data = True
                            surprise_data = history_clean['surprisePercent'].tolist()

                if has_data and len(surprise_data) > 0:
                    total_quarters = len(surprise_data)
                    total_beats = sum(1 for s in surprise_data if s > 0)
                    win_rate = total_beats / total_quarters

                    num_recent = min(4, len(surprise_data))
                    recent_surprises = surprise_data[:num_recent]
                    recent_avg = sum(recent_surprises) / len(recent_surprises) if recent_surprises else 0

                    worst_miss = min(surprise_data)

                    weighted_score = 0
                    for surprise in surprise_data:
                        if surprise > 0.10:
                            weighted_score += 1.5
                        elif surprise > 0.05:
                            weighted_score += 1.2
                        elif surprise > 0:
                            weighted_score += 1.0
                        elif surprise > -0.05:
                            weighted_score += 0.3
                        elif surprise > -0.10:
                            weighted_score += 0
                        else:
                            weighted_score -= 0.5

                    weighted_consistency = weighted_score / total_quarters

                    # Surprise Trend
                    if len(surprise_data) >= 8:
                        recent_4q = sum(surprise_data[:4]) / 4
                        older_4q = sum(surprise_data[4:8]) / 4
                        if recent_4q > older_4q + 0.02:
                            surprise_trend = "MEJORANDO"
                        elif recent_4q > older_4q:
                            surprise_trend = "MEJORANDO LEVE"
                        elif recent_4q < older_4q - 0.02:
                            surprise_trend = "DETERIORANDO"
                        elif recent_4q < older_4q:
                            surprise_trend = "DETERIORANDO LEVE"
                        else:
                            surprise_trend = "ESTABLE"
                    elif len(surprise_data) >= 6:
                        recent_3q = sum(surprise_data[:3]) / 3
                        older_3q = sum(surprise_data[3:6]) / 3
                        if recent_3q > older_3q + 0.02:
                            surprise_trend = "MEJORANDO*"
                        elif recent_3q > older_3q:
                            surprise_trend = "MEJORANDO LEVE*"
                        elif recent_3q < older_3q - 0.02:
                            surprise_trend = "DETERIORANDO*"
                        elif recent_3q < older_3q:
                            surprise_trend = "DETERIORANDO LEVE*"
                        else:
                            surprise_trend = "ESTABLE*"
                    elif len(surprise_data) >= 4:
                        recent_2q = sum(surprise_data[:2]) / 2
                        older_2q = sum(surprise_data[2:4]) / 2
                        if recent_2q > older_2q + 0.03:
                            surprise_trend = "MEJORANDO**"
                        elif recent_2q > older_2q:
                            surprise_trend = "MEJORANDO LEVE**"
                        elif recent_2q < older_2q - 0.03:
                            surprise_trend = "DETERIORANDO**"
                        elif recent_2q < older_2q:
                            surprise_trend = "DETERIORANDO LEVE**"
                        else:
                            surprise_trend = "ESTABLE**"
                    elif len(surprise_data) >= 2:
                        if surprise_data[0] > surprise_data[1] + 0.05:
                            surprise_trend = "ÚLTIMA MEJORA***"
                        elif surprise_data[0] < surprise_data[1] - 0.05:
                            surprise_trend = "ÚLTIMO DETERIORO***"
                        else:
                            surprise_trend = "SIN TENDENCIA CLARA***"
                    else:
                        surprise_trend = "DATOS INSUFICIENTES"

                    all_results['Beat Rate'].append([f"{win_rate:.2%}"])
                    all_results['Recent 4Q Avg'].append([f"{recent_avg:.2%}"])
                    all_results['Worst Miss'].append([f"{worst_miss:.2%}"])
                    all_results['Weighted Consistency'].append([f"{weighted_consistency:.2f}"])
                    all_results['Surprise Trend'].append([surprise_trend])
                else:
                    all_results['Beat Rate'].append([defaults['Beat Rate']])
                    all_results['Recent 4Q Avg'].append([defaults['Recent 4Q Avg']])
                    all_results['Worst Miss'].append([defaults['Worst Miss']])
                    all_results['Weighted Consistency'].append([defaults['Weighted Consistency']])
                    all_results['Surprise Trend'].append([defaults['Surprise Trend']])
            except:
                all_results['Beat Rate'].append([defaults['Beat Rate']])
                all_results['Recent 4Q Avg'].append([defaults['Recent 4Q Avg']])
                all_results['Worst Miss'].append([defaults['Worst Miss']])
                all_results['Weighted Consistency'].append([defaults['Weighted Consistency']])
                all_results['Surprise Trend'].append([defaults['Surprise Trend']])

            # Revenue Surprise 4Q
            try:
                revenue_surprise_found = False

                if earnings_history is not None and not earnings_history.empty:
                    if 'revenueEstimate' in earnings_history.columns and 'revenue' in earnings_history.columns:
                        history_4q = earnings_history.head(4)
                        revenue_surprises = []
                        for idx, row in history_4q.iterrows():
                            if pd.notna(row.get('revenue')) and pd.notna(row.get('revenueEstimate')):
                                estimate = row['revenueEstimate']
                                actual = row['revenue']
                                if estimate > 0:
                                    surprise = (actual - estimate) / estimate
                                    revenue_surprises.append(surprise)

                        if revenue_surprises:
                            avg_rev_surprise = sum(revenue_surprises) / len(revenue_surprises)
                            all_results['Revenue Surprise 4Q'].append([f"{avg_rev_surprise:.2%}"])
                            revenue_surprise_found = True

                if not revenue_surprise_found:
                    qf = ticker.quarterly_financials
                    if qf is not None and not qf.empty and "Total Revenue" in qf.index:
                        revenues = qf.loc["Total Revenue"].head(5).tolist()
                        if len(revenues) >= 2:
                            qoq_growth = []
                            for i in range(min(4, len(revenues) - 1)):
                                if revenues[i+1] != 0:
                                    growth = (revenues[i] - revenues[i+1]) / abs(revenues[i+1])
                                    qoq_growth.append(growth)
                            if qoq_growth:
                                avg_qoq = sum(qoq_growth) / len(qoq_growth)
                                all_results['Revenue Surprise 4Q'].append([f"{avg_qoq:.2%}"])
                                revenue_surprise_found = True

                if not revenue_surprise_found:
                    all_results['Revenue Surprise 4Q'].append([defaults['Revenue Surprise 4Q']])
            except:
                all_results['Revenue Surprise 4Q'].append([defaults['Revenue Surprise 4Q']])

            # Earnings Window
            try:
                timestamp = info.get("earningsTimestampStart", None)
                if isinstance(timestamp, (int, float)):
                    date_object = datetime.datetime.fromtimestamp(timestamp)
                    today = datetime.datetime.today()
                    days_to_earnings = (date_object - today).days

                    if days_to_earnings < 0:
                        earnings_window = "PASADO"
                    elif days_to_earnings <= 7:
                        earnings_window = "ESTA SEMANA"
                    elif days_to_earnings <= 21:
                        earnings_window = "ESTE MES"
                    elif days_to_earnings <= 45:
                        earnings_window = "PRÓXIMO MES"
                    else:
                        earnings_window = "LEJANO"

                    all_results['Earnings Window'].append([earnings_window])
                else:
                    all_results['Earnings Window'].append([defaults['Earnings Window']])
            except:
                all_results['Earnings Window'].append([defaults['Earnings Window']])

            #--------------------------------------------PRINCIPIO 5-------------------------------------------------#

            # SECCIÓN 1: PRECIO
            try:
                peg = info.get("pegRatio") or info.get("trailingPegRatio")
                peg_val = peg if (peg and peg > 0) else "N/A"
                all_results['PEG'].append([peg_val])

                # Interest Coverage
                f = ticker.financials
                ebit = None
                for label in ['Ebit', 'Operating Income', 'Operating Profit']:
                    if label in f.index:
                        ebit = f.loc[label].iloc[0]
                        break

                interest = None
                interest_labels = [idx for idx in f.index if 'Interest Expense' in idx]
                if interest_labels:
                    interest = abs(f.loc[interest_labels[0]].iloc[0])

                if ebit is not None and interest and interest > 0:
                    coverage_val = round(ebit / interest, 2)
                elif ebit is not None and (interest == 0 or interest is None):
                    coverage_val = 100
                else:
                    coverage_val = 0

                all_results['Interest Coverage'].append([coverage_val])

                forward_pe = info.get("forwardPE", "N/A")
                all_results['Forward PE'].append([forward_pe])
            except:
                all_results['PEG'].append([defaults['PEG']])
                all_results['Interest Coverage'].append([defaults['Interest Coverage']])
                all_results['Forward PE'].append([defaults['Forward PE']])

            # SECCIÓN 2: FCF
            try:
                market_cap = info.get("marketCap", 0)
                revenue = info.get("totalRevenue", 0)

                net_income = info.get("netIncome")
                if not net_income:
                    try:
                        net_income = ticker.financials.loc['Net Income'].iloc[0]
                    except:
                        net_income = info.get("netIncomeToCommon", 0)

                fcf = info.get("freeCashflow")
                cf_statement = ticker.cashflow
                if (fcf is None or fcf == 0) and not cf_statement.empty:
                    if "Free Cash Flow" in cf_statement.index:
                        fcf = cf_statement.loc["Free Cash Flow"].iloc[0]
                    elif "Operating Cash Flow" in cf_statement.index:
                        ocf = cf_statement.loc["Operating Cash Flow"].iloc[0]
                        capex = cf_statement.loc["Capital Expenditure"].iloc[0] if "Capital Expenditure" in cf_statement.index else 0
                        fcf = ocf + capex

                # FCF Yield
                if fcf and market_cap and market_cap > 0:
                    fcf_yield = fcf / market_cap
                    all_results['FCF Yield'].append([fcf_yield])
                else:
                    all_results['FCF Yield'].append([defaults['FCF Yield']])

                # FCF Growth YoY
                fcf_growth = 0
                try:
                    if not cf_statement.empty and "Free Cash Flow" in cf_statement.index and cf_statement.shape[1] >= 2:
                        fcf_current = cf_statement.loc["Free Cash Flow"].iloc[0]
                        fcf_previous = cf_statement.loc["Free Cash Flow"].iloc[1]
                        if fcf_previous and fcf_previous != 0:
                            fcf_growth = (fcf_current - fcf_previous) / abs(fcf_previous)
                    all_results['FCF Growth YoY'].append([fcf_growth])
                except:
                    all_results['FCF Growth YoY'].append([defaults['FCF Growth YoY']])

                # FCF/NI Ratio
                if net_income and net_income != 0 and fcf:
                    fcf_to_ni = fcf / net_income
                    all_results['FCF/NI Ratio'].append([fcf_to_ni])
                else:
                    all_results['FCF/NI Ratio'].append([defaults['FCF/NI Ratio']])

                # FCF Margin
                if fcf and revenue and revenue > 0:
                    fcf_margin = fcf / revenue
                    all_results['FCF Margin'].append([fcf_margin])
                else:
                    all_results['FCF Margin'].append([defaults['FCF Margin']])
            except:
                all_results['FCF Yield'].append([defaults['FCF Yield']])
                all_results['FCF Growth YoY'].append([defaults['FCF Growth YoY']])
                all_results['FCF/NI Ratio'].append([defaults['FCF/NI Ratio']])
                all_results['FCF Margin'].append([defaults['FCF Margin']])

            # SECCIÓN 3: CASH
            try:
                cash_value = info.get("totalCash", 0)
                if isinstance(cash_value, (int, float)) and cash_value != 0:
                    all_results['Total Cash'].append([cash_value])
                else:
                    all_results['Total Cash'].append([defaults['Total Cash']])

                # Operating Expense TTM
                try:
                    qis = ticker.quarterly_income_stmt
                    if "Operating Expense" in qis.index and qis.shape[1] >= 4:
                        op_exp_qtrs = qis.loc["Operating Expense"].iloc[0:4]
                        op_exp_ttm = abs(op_exp_qtrs.sum())
                        all_results['Operating Expense TTM'].append([op_exp_ttm])
                    else:
                        inc_stmt = ticker.income_stmt
                        if "Operating Expense" in inc_stmt.index:
                            op_exp_ttm = abs(inc_stmt.loc["Operating Expense"].iloc[0])
                            all_results['Operating Expense TTM'].append([op_exp_ttm])
                        else:
                            all_results['Operating Expense TTM'].append([defaults['Operating Expense TTM']])
                except:
                    all_results['Operating Expense TTM'].append([defaults['Operating Expense TTM']])
            except:
                all_results['Total Cash'].append([defaults['Total Cash']])
                all_results['Operating Expense TTM'].append([defaults['Operating Expense TTM']])

            # SECCIÓN 4: DEUDA
            try:
                total_debt = info.get("totalDebt", 0)
                ebitda = info.get("ebitda", 0)
                fcf_for_debt = info.get("freeCashflow", 0)

                # Obtener Total Equity
                total_equity = info.get("totalStockholderEquity") or info.get("totalEquity")
                if total_equity is None or total_equity == 0:
                    balance_sheet = ticker.quarterly_balance_sheet
                    equity_keys = ['Stockholders Equity', 'Total Equity Gross Minority Interest', 'Common Stock Equity']
                    for key in equity_keys:
                        if key in balance_sheet.index:
                            total_equity = balance_sheet.loc[key].iloc[0]
                            break

                # Total Debt
                if isinstance(total_debt, (int, float)) and total_debt != 0:
                    formatted_debt = f"${int(total_debt):,}"
                    all_results['Total Debt (mrq)'].append([formatted_debt])
                else:
                    all_results['Total Debt (mrq)'].append([defaults['Total Debt (mrq)']])

                # Interest Expense
                income_stmt = ticker.income_stmt
                if "Interest Expense" in income_stmt.index and not income_stmt.loc["Interest Expense"].empty:
                    interest = income_stmt.loc["Interest Expense"].iloc[0]
                    all_results['Interest Expense'].append([f"${int(interest):,}" if isinstance(interest, (int, float)) else interest])
                else:
                    all_results['Interest Expense'].append([defaults['Interest Expense']])

                # Debt/Equity
                if total_equity and total_equity > 0:
                    debt_to_equity = total_debt / total_equity if total_debt else 0
                    all_results['Debt/Equity'].append([f"{debt_to_equity:.2f}"])
                else:
                    all_results['Debt/Equity'].append([defaults['Debt/Equity']])

                # Debt/EBITDA
                if ebitda and ebitda > 0:
                    debt_to_ebitda = total_debt / ebitda if total_debt else 0
                    all_results['Debt/EBITDA'].append([f"{debt_to_ebitda:.2f}"])
                else:
                    all_results['Debt/EBITDA'].append([defaults['Debt/EBITDA']])

                # Years to Pay Debt
                if fcf_for_debt and fcf_for_debt > 0 and total_debt:
                    years_to_pay = total_debt / fcf_for_debt
                    all_results['Years to Pay Debt'].append([f"{years_to_pay:.1f}"])
                else:
                    all_results['Years to Pay Debt'].append([defaults['Years to Pay Debt']])
            except:
                all_results['Total Debt (mrq)'].append([defaults['Total Debt (mrq)']])
                all_results['Interest Expense'].append([defaults['Interest Expense']])
                all_results['Debt/Equity'].append([defaults['Debt/Equity']])
                all_results['Debt/EBITDA'].append([defaults['Debt/EBITDA']])
                all_results['Years to Pay Debt'].append([defaults['Years to Pay Debt']])

            # SECCIÓN 5: FUTURO RETORNO
            try:
                current_price = info.get("currentPrice", 0)
                market_cap = info.get("marketCap", 0)
                trailing_eps = info.get("trailingEps", 0)
                current_pe = info.get("trailingPE", 0)
                forward_pe = info.get("forwardPE", 0)
                profit_margin = info.get("profitMargins", 0)

                # Revenue Estimate
                try:
                    revenue_estimate_df = ticker.revenue_estimate
                    if not revenue_estimate_df.empty and revenue_estimate_df.shape[0] > 3:
                        revenue_next_year = revenue_estimate_df.iloc[3, 0]
                        all_results['Revenue Estimate AVG'].append([f"{int(revenue_next_year):,.0f}"])
                    else:
                        revenue_next_year = None
                        all_results['Revenue Estimate AVG'].append([defaults['Revenue Estimate AVG']])
                except:
                    revenue_next_year = None
                    all_results['Revenue Estimate AVG'].append([defaults['Revenue Estimate AVG']])

                # Profit Margin
                all_results['Profit Margin'].append([f"{profit_margin:.2%}" if profit_margin else "0"])

                # P/E Promedio 6 meses
                try:
                    if trailing_eps and trailing_eps > 0:
                        end_date = datetime.datetime.today()
                        start_date = end_date - datetime.timedelta(days=180)
                        hist = ticker.history(start=start_date, end=end_date, interval="1mo")
                        if not hist.empty:
                            hist["P/E"] = np.divide(hist["Close"], trailing_eps)
                            avg_pe = hist["P/E"].mean()
                            all_results['P/E Promedio 6 meses'].append([round(avg_pe, 2)])
                        else:
                            all_results['P/E Promedio 6 meses'].append([defaults['P/E Promedio 6 meses']])
                    else:
                        all_results['P/E Promedio 6 meses'].append([defaults['P/E Promedio 6 meses']])
                except:
                    all_results['P/E Promedio 6 meses'].append([defaults['P/E Promedio 6 meses']])

                # Future EPS Estimate
                try:
                    earnings_estimate_df = ticker.earnings_estimate
                    if not earnings_estimate_df.empty and earnings_estimate_df.shape[0] > 3:
                        eps_next_year = earnings_estimate_df.iloc[3, 0]
                        all_results['Future EPS'].append([f"${eps_next_year:.2f}"])
                    else:
                        eps_next_year = None
                        all_results['Future EPS'].append([defaults['Future EPS']])
                except:
                    eps_next_year = None
                    all_results['Future EPS'].append([defaults['Future EPS']])

                # Expected PE (con mean reversion)
                if current_pe and forward_pe:
                    if current_pe > 30:
                        pe_contraction = 0.15
                        expected_pe = current_pe * (1 - pe_contraction)
                    elif current_pe < 10:
                        pe_expansion = 0.10
                        expected_pe = current_pe * (1 + pe_expansion)
                    else:
                        expected_pe = (current_pe * 0.6 + forward_pe * 0.4)
                elif forward_pe:
                    expected_pe = forward_pe
                elif current_pe:
                    expected_pe = current_pe
                else:
                    expected_pe = 15

                all_results['Expected PE'].append([f"{expected_pe:.1f}"])

                # Expected Return (EPS method)
                if eps_next_year and trailing_eps and eps_next_year > 0 and current_price > 0:
                    future_price_eps = eps_next_year * expected_pe
                    expected_return_eps = (future_price_eps / current_price) - 1
                    all_results['Expected Return (EPS)'].append([f"{expected_return_eps:.2%}"])
                    method_a = expected_return_eps
                    has_method_a = True
                else:
                    all_results['Expected Return (EPS)'].append([defaults['Expected Return (EPS)']])
                    has_method_a = False

                # Expected Return (Revenue method)
                if revenue_next_year and profit_margin and market_cap and market_cap > 0:
                    future_net_income = revenue_next_year * profit_margin
                    future_market_cap = future_net_income * expected_pe
                    expected_return_rev = (future_market_cap / market_cap) - 1
                    all_results['Expected Return (Rev)'].append([f"{expected_return_rev:.2%}"])
                    method_b = expected_return_rev
                    has_method_b = True
                else:
                    all_results['Expected Return (Rev)'].append([defaults['Expected Return (Rev)']])
                    has_method_b = False

                # Expected Return (Analyst target)
                analyst_target = info.get("targetMeanPrice", 0)
                if analyst_target and current_price and current_price > 0:
                    expected_return_analyst = (analyst_target - current_price) / current_price
                    all_results['Expected Return (Analyst)'].append([f"{expected_return_analyst:.2%}"])
                    method_c = expected_return_analyst
                    has_method_c = True
                else:
                    all_results['Expected Return (Analyst)'].append([defaults['Expected Return (Analyst)']])
                    has_method_c = False

                # Expected Return (Consensus) - PROMEDIO PONDERADO
                returns = []
                weights = []

                if has_method_a:
                    returns.append(method_a)
                    weights.append(0.5)
                if has_method_b:
                    returns.append(method_b)
                    weights.append(0.3)
                if has_method_c:
                    returns.append(method_c)
                    weights.append(0.2)

                if returns:
                    weighted_return = sum(r * w for r, w in zip(returns, weights)) / sum(weights)
                    all_results['Expected Return (Consensus)'].append([f"{weighted_return:.2%}"])
                else:
                    all_results['Expected Return (Consensus)'].append([defaults['Expected Return (Consensus)']])
            except:
                all_results['Revenue Estimate AVG'].append([defaults['Revenue Estimate AVG']])
                all_results['Profit Margin'].append([defaults['Profit Margin']])
                all_results['P/E Promedio 6 meses'].append([defaults['P/E Promedio 6 meses']])
                all_results['Future EPS'].append([defaults['Future EPS']])
                all_results['Expected PE'].append([defaults['Expected PE']])
                all_results['Expected Return (EPS)'].append([defaults['Expected Return (EPS)']])
                all_results['Expected Return (Rev)'].append([defaults['Expected Return (Rev)']])
                all_results['Expected Return (Analyst)'].append([defaults['Expected Return (Analyst)']])
                all_results['Expected Return (Consensus)'].append([defaults['Expected Return (Consensus)']])

            # ============================================================
            # PRINCIPIO 8: VOLUMEN Y MOMENTUM (5% del total)
            # ============================================================

            try:
                end_date = datetime.datetime.today()
                start_date = end_date - datetime.timedelta(days=90)
                hist = ticker.history(start=start_date, end=end_date, interval="1d")

                if not hist.empty and len(hist) >= 50:

                    # ═══ COMPONENTE 1: Volumen Relativo ═══
                    avg_volume_20d = hist['Volume'].tail(20).mean()
                    avg_volume_50d = hist['Volume'].tail(50).mean()
                    current_volume = hist['Volume'].iloc[-1]

                    volume_ratio = current_volume / avg_volume_50d if avg_volume_50d > 0 else 1

                    # Clasificar volumen
                    if volume_ratio > 2.0:
                        volume_level = "MUY ALTO"
                    elif volume_ratio > 1.5:
                        volume_level = "ALTO"
                    elif volume_ratio > 0.8:
                        volume_level = "NORMAL"
                    elif volume_ratio > 0.5:
                        volume_level = "BAJO"
                    else:
                        volume_level = "MUY BAJO"

                    all_results['Volume Ratio'].append([volume_ratio])
                    all_results['Volume Level'].append([volume_level])

                    # ═══ COMPONENTE 2: On-Balance Volume (OBV) Trend ═══
                    obv = [0]
                    for i in range(1, len(hist)):
                        if hist['Close'].iloc[i] > hist['Close'].iloc[i-1]:
                            obv.append(obv[-1] + hist['Volume'].iloc[i])
                        elif hist['Close'].iloc[i] < hist['Close'].iloc[i-1]:
                            obv.append(obv[-1] - hist['Volume'].iloc[i])
                        else:
                            obv.append(obv[-1])

                    hist['OBV'] = obv

                    # Tendencia del OBV (últimos 20 días)
                    obv_sma_20 = hist['OBV'].tail(20).mean()
                    obv_current = hist['OBV'].iloc[-1]

                    if obv_current > obv_sma_20 * 1.05:
                        obv_trend = "ACUMULACIÓN"
                    elif obv_current < obv_sma_20 * 0.95:
                        obv_trend = "DISTRIBUCIÓN"
                    else:
                        obv_trend = "NEUTRAL"

                    all_results['OBV Trend'].append([obv_trend])

                    # ═══ COMPONENTE 3: Price-Volume Divergence ═══
                    # ¿El precio sube con volumen creciente o decreciente?
                    price_change_20d = (hist['Close'].iloc[-1] - hist['Close'].iloc[-20]) / hist['Close'].iloc[-20]
                    volume_change_20d = (avg_volume_20d - avg_volume_50d) / avg_volume_50d

                    # Divergencias
                    if price_change_20d > 0.05 and volume_change_20d > 0.2:
                        divergence = "ALCISTA FUERTE"  # Precio sube + volumen aumenta = bueno
                    elif price_change_20d > 0.05 and volume_change_20d < -0.2:
                        divergence = "ALCISTA DÉBIL"   # Precio sube + volumen baja = sospechoso
                    elif price_change_20d < -0.05 and volume_change_20d > 0.2:
                        divergence = "BAJISTA FUERTE"  # Precio baja + volumen alto = malo
                    elif price_change_20d < -0.05 and volume_change_20d < -0.2:
                        divergence = "BAJISTA DÉBIL"   # Precio baja + volumen bajo = puede rebotar
                    else:
                        divergence = "NEUTRAL"

                    all_results['Price-Volume Div'].append([divergence])

                    # ═══ COMPONENTE 4: Money Flow Index (MFI) - RSI con volumen ═══
                    typical_price = (hist['High'] + hist['Low'] + hist['Close']) / 3
                    money_flow = typical_price * hist['Volume']

                    positive_flow = []
                    negative_flow = []

                    for i in range(1, len(hist)):
                        if typical_price.iloc[i] > typical_price.iloc[i-1]:
                            positive_flow.append(money_flow.iloc[i])
                            negative_flow.append(0)
                        else:
                            positive_flow.append(0)
                            negative_flow.append(money_flow.iloc[i])

                    # MFI de últimos 14 días
                    if len(positive_flow) >= 14:
                        positive_mf = sum(positive_flow[-14:])
                        negative_mf = sum(negative_flow[-14:])

                        if negative_mf > 0:
                            mfi = 100 - (100 / (1 + (positive_mf / negative_mf)))
                        else:
                            mfi = 100

                        if mfi > 80:
                            mfi_level = "SOBRECOMPRADO"
                        elif mfi > 60:
                            mfi_level = "COMPRADO"
                        elif mfi > 40:
                            mfi_level = "NEUTRAL"
                        elif mfi > 20:
                            mfi_level = "VENDIDO"
                        else:
                            mfi_level = "SOBREVENDIDO"

                        all_results['MFI'].append([mfi])
                        all_results['MFI Level'].append([mfi_level])
                    else:
                        all_results['MFI'].append([50])
                        all_results['MFI Level'].append(["N/A"])



                else:
                    all_results['Volume Ratio'].append([1])
                    all_results['Volume Level'].append(["N/A"])
                    all_results['OBV Trend'].append(["N/A"])
                    all_results['Price-Volume Div'].append(["N/A"])
                    all_results['MFI'].append([50])
                    all_results['MFI Level'].append(["N/A"])

            except Exception as e:
                print(f"  ⚠️ [ERROR {symbol}] Volume/Momentum: {str(e)}")
                all_results['Volume Ratio'].append([1])
                all_results['Volume Level'].append(["ERROR"])
                all_results['OBV Trend'].append(["ERROR"])
                all_results['Price-Volume Div'].append(["ERROR"])
                all_results['MFI'].append([50])
                all_results['MFI Level'].append(["ERROR"])

            # INFORMACIÓN ADICIONAL
            # Sector
            try:
                all_results['Sector'].append([info.get("sector", "N/A")])
            except:
                all_results['Sector'].append([defaults['Sector']])

            # Days Public
            try:
                hist = ticker.history(period="max", auto_adjust=False)
                if not hist.empty:
                    fecha_inicio = hist.index.min().date()
                    fecha_actual = datetime.date.today()
                    dias_publica = (fecha_actual - fecha_inicio).days
                    all_results['Days Public'].append([int(dias_publica)])
                else:
                    all_results['Days Public'].append([defaults['Days Public']])
            except:
                all_results['Days Public'].append([defaults['Days Public']])

            # Beta
            try:
                all_results['Beta'].append([info.get("beta", "N/A")])
            except:
                all_results['Beta'].append([defaults['Beta']])

            # URL Oficial
            try:
                url = info.get("website", "N/A")
                all_results['Official URL'].append([url])
            except:
                all_results['Official URL'].append([defaults['Official URL']])

        # 4. Escribir los datos en los rangos especificados
        print("\n--- Escribiendo datos en Google Sheets ---")
        for metric, data_list in all_results.items():
            try:
                range_to_update = ranges[metric]
                worksheet.update(range_to_update, data_list)
                print(f"✅ '{metric}' actualizado en {range_to_update}")
            except Exception as e:
                print(f"❌ Error actualizando '{metric}': {e}")

        print("\n🎉 ¡Proceso completado exitosamente!")

except gspread.exceptions.SpreadsheetNotFound:
    print(f'❌ Error: La hoja de cálculo "{spreadsheet_name}" no fue encontrada.')
    print("Verifica el nombre y que la cuenta de servicio tenga acceso.")
except gspread.exceptions.WorksheetNotFound:
    print(f'❌ Error: La pestaña "{worksheet_name}" no fue encontrada.')
except Exception as e:
    print(f"❌ Ocurrió un error inesperado: {e}")
    import traceback

    traceback.print_exc()
