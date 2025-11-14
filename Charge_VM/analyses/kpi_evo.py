from __future__ import annotations

from typing import Iterable
from datetime import datetime

import pandas as pd
from sqlalchemy import MetaData, Table, create_engine, text
from sqlalchemy.dialects.mysql import insert


DB_CONFIG_KPI = {
    "host": "141.94.31.144",
    "port": 3306,
    "user": "AdminNidec",
    "password": "u6Ehe987XBSXxa4",
    "database": "indicator",
}


def _build_engine(config: dict):
    return create_engine(
        "mysql+pymysql://{user}:{password}@{host}:{port}/{database}".format(**config)
    )


engine_kpi = _build_engine(DB_CONFIG_KPI)


def get_last_complete_month_end():
    now = datetime.now()
    first_day_current_month = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    last_complete_month_end = first_day_current_month - pd.Timedelta(seconds=1)
    
    return last_complete_month_end


def fetch_sessions() -> pd.DataFrame:
    """
    📅 Récupère les sessions jusqu'au dernier mois complet uniquement
    """
    cutoff_date = get_last_complete_month_end()
    
    query = text(
        """
        SELECT
            `Site`,
            `Datetime start` AS dt_start,
            `is_ok`,
            `moment`
        FROM indicator.kpi_sessions
        WHERE `Datetime start` IS NOT NULL
          AND `Datetime start` <= :cutoff_date
        """
    )

    print(f"📅 Récupération des sessions jusqu'au {cutoff_date.strftime('%d/%m/%Y')}")
    
    df = pd.read_sql_query(query, con=engine_kpi, params={"cutoff_date": cutoff_date})
    if df.empty:
        return df

    df["dt_start"] = pd.to_datetime(df["dt_start"], errors="coerce")
    df = df.dropna(subset=["dt_start"]).copy()
    
    # Double vérification côté Python
    df = df[df["dt_start"] <= cutoff_date]

    site_series = df["Site"].astype(str).str.strip()
    site_series = site_series.replace("", "Unknown")
    site_series = site_series.mask(
        site_series.str.lower().isin({"none", "nan"}), "Unknown"
    )
    df["Site"] = site_series

    print(f"✅ {len(df)} sessions chargées (mois en cours exclu)")
    return df


def classify_success(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    is_ok = pd.to_numeric(df.get("is_ok"), errors="coerce").fillna(0).astype(int)
    moment = df.get("moment").astype(str).str.strip().str.lower()
    fin_de_charge = moment.eq("fin de charge")
    df = df.copy()
    df["is_success"] = is_ok.eq(1) | (~is_ok.eq(1) & fin_de_charge)
    df["mois"] = df["dt_start"].dt.strftime("%m-%Y")
    return df


def aggregate_success(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    grouped = df.groupby("mois", as_index=False)["is_success"].agg(
        total="count", successes="sum"
    )

    grouped["tr"] = (
        grouped["successes"].div(grouped["total"].replace(0, pd.NA)).mul(100).round(2)
    ).fillna(0)

    grouped["Site"] = "Global"

    return grouped[["Site", "mois", "tr"]]


def chunk_records(records: Iterable[dict], chunk_size: int = 500) -> Iterable[list[dict]]:
    chunk: list[dict] = []
    for record in records:
        chunk.append(record)
        if len(chunk) >= chunk_size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def upsert_kpi_evo(data: pd.DataFrame) -> None:
    if data.empty:
        print("ℹ️  Aucun enregistrement à insérer dans indicator.kpi_evo")
        return

    metadata = MetaData()
    kpi_evo_table = Table(
        "kpi_evo", metadata, autoload_with=engine_kpi, schema="indicator"
    )

    with engine_kpi.begin() as conn:
        conn.execute(text("DELETE FROM indicator.kpi_evo"))
        insert_stmt = insert(kpi_evo_table)
        for chunk in chunk_records(data.to_dict("records")):
            conn.execute(
                insert_stmt.on_duplicate_key_update(tr=insert_stmt.inserted.tr),
                chunk,
            )

    print(
        f"✅ Table indicator.kpi_evo mise à jour avec {len(data)} lignes (Fin de charge considéré réussi)"
    )


def main() -> None:
    print("=" * 70)
    print("🚀 CALCUL KPI ÉVOLUTION (DERNIER MOIS COMPLET)")
    print("=" * 70)
    
    cutoff_date = get_last_complete_month_end()
    print(f"\n📅 Période d'analyse: jusqu'au {cutoff_date.strftime('%d/%m/%Y')}")
    print(f"   (Mois actuel exclu car non complet)\n")
    
    sessions = fetch_sessions()
    sessions = classify_success(sessions)
    aggregates = aggregate_success(sessions)
    upsert_kpi_evo(aggregates)
    
    print("\n" + "=" * 70)
    print("✅ SCRIPT TERMINÉ")
    print("=" * 70)


if __name__ == "__main__":
    main()