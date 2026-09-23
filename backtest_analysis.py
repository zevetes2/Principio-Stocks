# backtest_analysis.py
"""
Backtesting del sistema de scoring.

Modos:
  1. --mode historical : usa yfinance para simular desde cero (backtest real)
  2. --mode snapshots  : analiza los snapshots reales acumulados
  3. --mode ic         : Information Coefficient por principio

Uso:
  python backtest_analysis.py --mode ic
  python backtest_analysis.py --mode snapshots --horizon 30
  python backtest_analysis.py --mode historical --start 2024-01-01
"""
import argparse
import datetime
import os
import sys
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
import yfinance as yf

# ── Configuración ──
HORIZONS = {
    "1m": 21,   # días de trading
    "3m": 63,
    "6m": 126,
    "1y": 252,
}
SNAPSHOT_CSV = "snapshots.csv"
MIN_OBSERVATIONS = 30  # mínimo para calcular IC


# ══════════════════════════════════════════════════════════════
# CARGA DE DATOS
# ══════════════════════════════════════════════════════════════

def load_snapshots(path: str = SNAPSHOT_CSV) -> pd.DataFrame:
    """Carga el CSV de snapshots."""
    if not os.path.isfile(path):
        print(f"❌ {path} no existe. Corre main_V6.py primero.")
        sys.exit(1)
    df = pd.read_csv(path, parse_dates=["date"])
    print(f"✅ Cargados {len(df)} snapshots ({df['date'].min().date()} → {df['date'].max().date()})")
    print(f"   Tickers únicos: {df['ticker'].nunique()}")
    print(f"   Días únicos: {df['date'].nunique()}")
    return df


def get_forward_returns(
    tickers: List[str],
    start_date: datetime.date,
    end_date: datetime.date,
    horizon_days: int,
) -> pd.DataFrame:
    """
    Descarga precios históricos y calcula retornos forward.

    Retorna DataFrame: index=[date, ticker], columns=['fwd_return']
    """
    print(f"\n📈 Descargando precios para {len(tickers)} tickers...")
    data = yf.download(
        tickers, start=start_date, end=end_date + datetime.timedelta(days=horizon_days + 30),
        progress=False, auto_adjust=True, group_by="ticker", threads=True,
    )

    # Extraer 'Close' de cada ticker
    if isinstance(data.columns, pd.MultiIndex):
        close = data.xs("Close", axis=1, level=1)
    else:
        close = data[["Close"]] if "Close" in data.columns else data

    # Forward return: precio en t+horizon / precio en t - 1
    fwd = close.shift(-horizon_days) / close - 1.0

    # Convertir a long format
    fwd_long = fwd.stack().reset_index()
    fwd_long.columns = ["date", "ticker", "fwd_return"]
    fwd_long["date"] = pd.to_datetime(fwd_long["date"]).dt.normalize()
    return fwd_long


# ══════════════════════════════════════════════════════════════
# INFORMATION COEFFICIENT (IC)
# ══════════════════════════════════════════════════════════════

def compute_ic(df: pd.DataFrame, horizon_days: int) -> pd.DataFrame:
    """
    IC = correlación de Spearman entre score y forward return, por fecha.
    Reporta media, std, IR (IC_mean / IC_std), y t-stat.
    """
    tickers = df["ticker"].unique().tolist()
    start = df["date"].min().date()
    end = df["date"].max().date()

    fwd = get_forward_returns(tickers, start, end, horizon_days)

    # Merge
    merged = df.merge(fwd, on=["date", "ticker"], how="inner").dropna(subset=["fwd_return"])
    if len(merged) < MIN_OBSERVATIONS:
        print(f"⚠️  Solo {len(merged)} observaciones con retorno — insuficiente para IC")
        return pd.DataFrame()

    # Score columns a analizar
    score_cols = [
        "score_final", "score_precio", "score_crecimiento", "score_tendencia",
        "score_consistencia", "score_valoracion", "score_soportes",
        "score_williams", "score_volumen",
    ]

    results = []
    for col in score_cols:
        # IC por fecha (cross-sectional)
        ics_by_date = []
        for date, group in merged.groupby("date"):
            if len(group) < 5:
                continue
            ic = group[col].corr(group["fwd_return"], method="spearman")
            if not pd.isna(ic):
                ics_by_date.append(ic)

        if not ics_by_date:
            continue

        ics = np.array(ics_by_date)
        ic_mean = ics.mean()
        ic_std = ics.std()
        ir = ic_mean / ic_std if ic_std > 0 else 0
        t_stat = ic_mean / (ic_std / np.sqrt(len(ics))) if ic_std > 0 else 0

        results.append({
            "principle": col,
            "IC_mean": round(ic_mean, 4),
            "IC_std": round(ic_std, 4),
            "IR": round(ir, 3),
            "t_stat": round(t_stat, 2),
            "n_dates": len(ics),
            "significant": "✅" if abs(t_stat) > 2 else "❌",
        })

    return pd.DataFrame(results).sort_values("IC_mean", ascending=False)


# ══════════════════════════════════════════════════════════════
# DECILE ANALYSIS
# ══════════════════════════════════════════════════════════════

def decile_analysis(df: pd.DataFrame, horizon_days: int, score_col: str = "score_final") -> pd.DataFrame:
    """
    Ordena tickers por score, los divide en deciles, y calcula el retorno
    promedio de cada decil. Un buen scoring debe mostrar monotonía.
    """
    tickers = df["ticker"].unique().tolist()
    start = df["date"].min().date()
    end = df["date"].max().date()

    fwd = get_forward_returns(tickers, start, end, horizon_days)
    merged = df.merge(fwd, on=["date", "ticker"], how="inner").dropna(subset=["fwd_return"])

    if len(merged) < MIN_OBSERVATIONS:
        print(f"⚠️  Solo {len(merged)} observaciones — insuficiente")
        return pd.DataFrame()

    # Por fecha: asignar decil a cada ticker
    def assign_decile(group):
        if len(group) < 10:
            return pd.Series(index=group.index, data=np.nan)
        try:
            return pd.qcut(group[score_col], 10, labels=False, duplicates="drop")
        except Exception:
            return pd.Series(index=group.index, data=np.nan)

    merged["decile"] = merged.groupby("date", group_keys=False).apply(assign_decile)
    merged = merged.dropna(subset=["decile"])

    # Retorno promedio por decil
    by_decile = merged.groupby("decile")["fwd_return"].agg(["mean", "std", "count"]).reset_index()
    by_decile["mean_pct"] = (by_decile["mean"] * 100).round(2)
    by_decile["std_pct"] = (by_decile["std"] * 100).round(2)

    return by_decile


# ══════════════════════════════════════════════════════════════
# REPORTE
# ══════════════════════════════════════════════════════════════

def print_ic_report(ic_df: pd.DataFrame, horizon: str):
    print("\n" + "═" * 72)
    print(f"  INFORMATION COEFFICIENT — Horizonte: {horizon}")
    print("═" * 72)
    if ic_df.empty:
        print("  Sin datos suficientes.")
        return
    print(ic_df.to_string(index=False))
    print()
    print("  📖 Interpretación:")
    print("     IC_mean > 0.05 → principio predictivo")
    print("     IC_mean > 0.10 → muy fuerte (raro)")
    print("     |t_stat| > 2  → estadísticamente significativo")
    print("     IR > 0.5      → consistente en el tiempo")


def print_decile_report(decile_df: pd.DataFrame, horizon: str):
    print("\n" + "═" * 72)
    print(f"  DECILE ANALYSIS — Horizonte: {horizon}")
    print("═" * 72)
    if decile_df.empty:
        print("  Sin datos suficientes.")
        return
    print(decile_df.to_string(index=False))
    print()
    # Top vs bottom
    if len(decile_df) >= 10:
        top = decile_df.iloc[-1]["mean_pct"]
        bottom = decile_df.iloc[0]["mean_pct"]
        spread = top - bottom
        print(f"  📊 Spread Top-Bottom: {spread:.2f}%")
        print(f"     {'✅' if spread > 0 else '❌'} {'Monotonía positiva' if spread > 0 else 'Sin señal'}")


# ══════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Backtesting del sistema E.T.H")
    parser.add_argument("--mode", choices=["ic", "snapshots", "deciles"], default="ic")
    parser.add_argument("--horizon", choices=["1m", "3m", "6m", "1y"], default="3m")
    parser.add_argument("--csv", default=SNAPSHOT_CSV)
    parser.add_argument("--force", action="store_true",
                        help="Forzar análisis aunque no haya datos suficientes")
    args = parser.parse_args()

    horizon_days = HORIZONS[args.horizon]
    df = load_snapshots(args.csv)

    # ── SIEMPRE mostrar el resumen básico ──
    print("\n" + "═" * 72)
    print("  RESUMEN DE SNAPSHOTS")
    print("═" * 72)
    print(f"  Total: {len(df)}")
    print(f"  Rango: {df['date'].min().date()} → {df['date'].max().date()}")
    print(f"  Tickers: {df['ticker'].nunique()}")
    print(f"  Runs: {df['date'].nunique()}")

    if "sector" in df.columns:
        print("\n  Score_Final por sector:")
        summary = df.groupby("sector")["score_final"].agg(["mean", "std", "count"]).round(2)
        print(summary.to_string())

    if "grade" in df.columns:
        print("\n  Distribución de grades:")
        print(df["grade"].value_counts().sort_index().to_string())

    # ── Filtrar snapshots con antigüedad suficiente ──
    cutoff = pd.Timestamp.today() - pd.Timedelta(days=horizon_days + 30)
    df_usable = df[df["date"] <= cutoff].copy()

    age_days = (pd.Timestamp.today() - df["date"].min()).days
    needed_days = horizon_days + 30 - age_days

    if df_usable.empty and not args.force:
        print("\n" + "═" * 72)
        print(f"  ⚠️  No hay suficientes datos para análisis {args.horizon}")
        print("═" * 72)
        print(f"  Antigüedad actual: {age_days} días")
        print(f"  Necesario: {horizon_days + 30} días")
        print(f"  Faltan: {needed_days} días")
        print()
        print(f"  📅 Podrás correr --mode ic/deciles el "
              f"{(pd.Timestamp.today() + pd.Timedelta(days=needed_days)).date()}")
        print()
        print(f"  💡 Sugerencia: prueba con --horizon 1m (51 días) o usa --force")
        print(f"     para ver el análisis aunque sea incompleto.")
        return

    if args.mode == "ic":
        ic_df = compute_ic(df_usable, horizon_days)
        print_ic_report(ic_df, args.horizon)

    elif args.mode == "deciles":
        decile_df = decile_analysis(df_usable, horizon_days)
        print_decile_report(decile_df, args.horizon)

if __name__ == "__main__":
    main()