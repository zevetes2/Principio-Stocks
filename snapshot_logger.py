# snapshot_logger.py
"""
Registra un snapshot diario de scores + precios por ticker.
Almacenamiento dual: CSV local (backup) + Firestore (queryable).

Uso desde main_V6.py:
    from snapshot_logger import log_snapshot
    log_snapshot(all_results, symbols)
"""
import os
import csv
import datetime
import logging
from typing import Dict, List, Any

logger = logging.getLogger("PortafolioETH.Snapshot")

SNAPSHOT_CSV = "snapshots.csv"

# Columnas del CSV histórico
SNAPSHOT_COLUMNS = [
    "date", "ticker", "sector", "price", "score_final", "grade",
    "score_precio", "score_crecimiento", "score_tendencia", "score_consistencia",
    "score_valoracion", "score_soportes", "score_williams", "score_volumen",
    "risk_cap", "piotroski", "altman_z",
]


def _safe_get(all_results: Dict[str, List], key: str, idx: int, default=None):
    """Extrae all_results[key][idx][0] con fallback seguro."""
    try:
        val = all_results.get(key, [])
        if idx < len(val) and val[idx]:
            return val[idx][0]
        return default
    except Exception:
        return default


def _row_from_results(date: str, ticker: str, all_results: Dict[str, List], idx: int) -> List:
    """Construye una fila para el CSV a partir de all_results."""
    return [
        date,
        ticker,
        _safe_get(all_results, "Sector", idx, "N/A"),
        _safe_get(all_results, "Price Actual", idx, 0),
        _safe_get(all_results, "Score_Final", idx, 50),
        _safe_get(all_results, "Grade", idx, "C"),
        _safe_get(all_results, "Score_Precio", idx, 50),
        _safe_get(all_results, "Score_Crecimiento", idx, 50),
        _safe_get(all_results, "Score_Tendencia", idx, 50),
        _safe_get(all_results, "Score_Consistencia", idx, 50),
        _safe_get(all_results, "Score_Valoracion", idx, 50),
        _safe_get(all_results, "Score_Soportes", idx, 50),
        _safe_get(all_results, "Score_Williams", idx, 50),
        _safe_get(all_results, "Score_Volumen", idx, 50),
        _safe_get(all_results, "Risk Cap", idx, 100),
        _safe_get(all_results, "Piotroski F-Score", idx, "N/A"),
        _safe_get(all_results, "Altman Z-Score", idx, "N/A"),
    ]


def _append_csv(rows: List[List]):
    """Append al CSV local. Crea con header si no existe."""
    file_exists = os.path.isfile(SNAPSHOT_CSV)
    with open(SNAPSHOT_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(SNAPSHOT_COLUMNS)
        writer.writerows(rows)


def _upload_firestore(rows: List[List]):
    """Sube los snapshots a Firestore (colección 'snapshots')."""
    try:
        import firebase_admin
        from firebase_admin import credentials, firestore

        # Inicializar solo una vez
        if not firebase_admin._apps:
            key_path = os.getenv("FIREBASE_KEY_PATH", "firebase-service-key.json")
            if not os.path.isfile(key_path):
                logger.warning(f"Firestore: {key_path} no encontrado, saltando upload")
                return
            cred = credentials.Certificate(key_path)
            firebase_admin.initialize_app(cred)

        db = firestore.client()
        batch = db.batch()
        col = db.collection("snapshots")

        for row in rows:
            doc_data = dict(zip(SNAPSHOT_COLUMNS, row))
            # ID compuesto: date_ticker (evita duplicados si se corre 2x el mismo día)
            doc_id = f"{doc_data['date']}_{doc_data['ticker']}"
            batch.set(col.document(doc_id), doc_data)

        batch.commit()
        logger.info(f"✅ Firestore: {len(rows)} snapshots guardados")
    except Exception as e:
        logger.error(f"❌ Firestore snapshot upload falló: {e}")


def log_snapshot(all_results: Dict[str, List], symbols: List[str]):
    """
    Punto de entrada. Guarda el snapshot del run actual.
    Llamar al final de main(), después de write_to_sheets.
    """
    date_str = datetime.date.today().isoformat()
    rows = []
    for idx, sym in enumerate(symbols):
        rows.append(_row_from_results(date_str, sym, all_results, idx))

    # 1. CSV local (siempre)
    try:
        _append_csv(rows)
        logger.info(f"✅ CSV: {len(rows)} snapshots añadidos a {SNAPSHOT_CSV}")
    except Exception as e:
        logger.error(f"❌ CSV snapshot falló: {e}")

    # 2. Firestore (si hay credenciales)
    _upload_firestore(rows)