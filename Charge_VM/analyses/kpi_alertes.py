import pandas as pd
import mysql.connector
from contextlib import contextmanager
from datetime import datetime
from sqlalchemy import create_engine, text

DB_CONFIG = {
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

engine = _build_engine(DB_CONFIG)

@contextmanager
def get_db_connection():
    conn = None
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        yield conn
    except mysql.connector.Error as e:
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()


def fetch_sess_kpi():
    query = text("""
        SELECT 
            Site,
            PDC,
            `Datetime start`,
            is_ok,
            moment,
            type_erreur,
            `EVI Error Code`,
            `Downstream Code PC`
        FROM indicator.kpi_sessions
        WHERE `Datetime start` IS NOT NULL
        ORDER BY `Datetime start` DESC
    """)
    
    df = pd.read_sql_query(query, con=engine)
    
    if df.empty:
        return df
    
    df["Datetime start"] = pd.to_datetime(df["Datetime start"], errors="coerce")
    df = df.dropna(subset=["Datetime start"])
    df["is_ok_filt"] = df["is_ok"].fillna(0).astype(bool)
    
    return df


def get_last_detection_date():
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT MAX(detection) FROM kpi_alertes")
            result = cursor.fetchone()
            cursor.close()
            return result[0] if result[0] else None
    except:
        return None


def save_alerts_to_db(alert_rows):
    if not alert_rows:
        return {"success": True, "rows_affected": 0, "error": None}
    
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            insert_query = """
                INSERT INTO kpi_alertes 
                (Site, PDC, type_erreur, detection, occurrences_12h, moment, evi_code, downstream_code_pc)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    occurrences_12h = VALUES(occurrences_12h),
                    moment = VALUES(moment),
                    evi_code = VALUES(evi_code),
                    downstream_code_pc = VALUES(downstream_code_pc)
            """
            
            data = []
            for row in alert_rows:
                data.append((
                    str(row.get("Site", ""))[:50],
                    str(row.get("PDC", ""))[:50],
                    str(row.get("Type d'erreur", ""))[:100],
                    row["Détection"],
                    int(row.get("Occurrences sur 12h", 0)),
                    str(row.get("Moment", ""))[:20] if row.get("Moment") else None,
                    str(row.get("EVI Code", ""))[:50] if row.get("EVI Code") else None,
                    str(row.get("Downstream Code PC", ""))[:50] if row.get("Downstream Code PC") else None
                ))
            
            cursor.executemany(insert_query, data)
            conn.commit()
            
            rows_affected = cursor.rowcount
            cursor.close()
            
            return {"success": True, "rows_affected": rows_affected, "error": None}
            
    except Exception as e:
        return {"success": False, "rows_affected": 0, "error": str(e)}


def detect_alerts_from_sess_kpi(sess_kpi, date_min=None):
    if date_min:
        sess_kpi = sess_kpi[sess_kpi["Datetime start"] >= date_min].copy()
    
    errors_only = sess_kpi[~sess_kpi["is_ok_filt"]].copy()
    
    if errors_only.empty:
        return []
    
    errors_only = errors_only.dropna(subset=[
        "Datetime start", "Site", "PDC", "type_erreur", 
        "moment", "EVI Error Code", "Downstream Code PC"
    ])

    errors_only = errors_only.sort_values(
        ["Site", "PDC", "type_erreur", "moment", "EVI Error Code", "Downstream Code PC", "Datetime start"]
    ).reset_index()

    alert_rows = []

    group_cols = ["Site", "PDC", "type_erreur", "moment", "EVI Error Code", "Downstream Code PC"]
    
    for group_keys, group in errors_only.groupby(group_cols):
        site, pdc, err_type, moment, evi_code, down_code = group_keys
        
        times = group["Datetime start"].reset_index(drop=True)
        idxs = group["index"].reset_index(drop=True)
        
        processed = set()
        
        for i in range(len(times)):
            if i in processed:
                continue
                
            t0 = times.iloc[i]
            t1 = t0 + pd.Timedelta(hours=12)
            
            window_mask = (times >= t0) & (times <= t1)
            window_indices = times[window_mask].index.tolist()
            
            if len(window_indices) >= 3:
                idx3 = idxs.iloc[i]
                row = sess_kpi.loc[idx3]

                alert_rows.append({
                    "Site": site,
                    "PDC": pdc,
                    "Type d'erreur": err_type,
                    "Détection": t0,
                    "Occurrences sur 12h": len(window_indices),
                    "Moment": moment,
                    "EVI Code": evi_code,
                    "Downstream Code PC": down_code
                })
                
                processed.update(window_indices)
    
    return alert_rows

def main():
    print("=" * 70)
    print("DETECTION ET SAUVEGARDE DES ALERTES KPI")
    print("=" * 70)
    
    try:
        print("\nChargement des sessions...")
        sess_kpi = fetch_sess_kpi()
        
        if sess_kpi.empty:
            print("Aucune session trouvée dans kpi_sessions")
            return
        
        print(f"Sessions chargees: {len(sess_kpi)}")
        
        last_date = get_last_detection_date()
        if last_date:
            print(f"Derniere alerte en BDD: {last_date}")
            print(f"Analyse uniquement depuis: {last_date}")
            date_min = pd.to_datetime(last_date)
        else:
            print("Aucune alerte en BDD, analyse complete")
            date_min = None
        
        alert_rows = detect_alerts_from_sess_kpi(sess_kpi, date_min=date_min)
        
        if not alert_rows:
            print("\nAucune nouvelle alerte a sauvegarder")
            return
        
        print(f"\nNouveaux alertes detectees: {len(alert_rows)}")
        
        for i, alert in enumerate(alert_rows[:5], 1):
            print(f"  {i}. {alert['Site']} | {alert['PDC']} | {alert['Type d\'erreur']} | {alert['Détection']}")
        
        if len(alert_rows) > 5:
            print(f"  ... et {len(alert_rows) - 5} autres")
        
        print(f"\nSauvegarde de {len(alert_rows)} alertes...")
        result = save_alerts_to_db(alert_rows)
        
        if result["success"]:
            print(f"Alertes sauvegardees: {result['rows_affected']}")
        else:
            print(f"Erreur: {result['error']}")
        
    except Exception as e:
        print(f"\nERREUR: {e}")
        import traceback
        traceback.print_exc()
    
    print("\n" + "=" * 70)
    print("TERMINE")
    print("=" * 70)


if __name__ == "__main__":
    main()