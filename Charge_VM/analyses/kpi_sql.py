# kpi_sql.py
from pathlib import Path

import numpy as np
import pandas as pd
from sqlalchemy import MetaData, Table, create_engine
from sqlalchemy.dialects.mysql import insert

# Connexion SQL
DB_CONFIG = {
    "host": "141.94.31.144",
    "port": 3306,
    "user": "AdminNidec",
    "password": "u6Ehe987XBSXxa4",
    "database": "elto",
}

engine = create_engine(
    f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
)

FINAL_PATH = Path("data/charge.csv")
KPIS_XLSX = Path("data/kpis.xlsx")
KPIS_MAC = Path("data/MAC.xlsx")
TMP_XLSX = KPIS_XLSX.with_name("kpis_tmp.xlsx")

SITE_CODE_COL = "Name Project"
SITE_COL = "Site"
PDC_COL = "PDC"
DATE_START = "Datetime start"
DATE_END = "Datetime end"

DS_PC = "Downstream Code PC"
EVI_CODE = "EVI Error Code"
EVI_MOMENT = "EVI Status during error"
SOC_COL = "State of charge(0:good, 1:error)"

SITE_MAP = {
    "001": "Saint-Jean-de-Maurienne",
    "002": "La Rochelle",
    "003": "Pouilly-en-Auxois",
    "004": "Carvin",
    "006": "Pau - Novotel",
}


def classify_errors(df: pd.DataFrame) -> pd.DataFrame:
    soc_series = pd.to_numeric(df.get(SOC_COL), errors="coerce")
    if isinstance(soc_series, pd.Series):
        soc = soc_series.fillna(0).astype(int)
    else:
        soc = pd.Series(0, index=df.index, dtype=int)
    df["is_ok"] = soc.eq(0)

    fail_mask = soc.eq(1)

    ds_pc_val = pd.to_numeric(df.get(DS_PC), errors="coerce")
    ds_pc_val = (
        ds_pc_val.fillna(0).astype(int)
        if isinstance(ds_pc_val, pd.Series)
        else pd.Series(0, index=df.index, dtype=int)
    )
    evi_code_val = pd.to_numeric(df.get(EVI_CODE), errors="coerce")
    evi_code_val = (
        evi_code_val.fillna(0).astype(int)
        if isinstance(evi_code_val, pd.Series)
        else pd.Series(0, index=df.index, dtype=int)
    )

    df["type_erreur"] = np.select(
        [
            fail_mask
            & ((ds_pc_val.eq(8192)) | ((ds_pc_val.eq(0)) & (evi_code_val.ne(0)))),
            fail_mask & ((ds_pc_val.ne(0)) & (ds_pc_val.ne(8192))),
        ],
        ["Erreur_EVI", "Erreur_DownStream"],
        default="Erreur_Unknow_S",
    )

    def map_moment(val):
        try:
            val = int(val)
        except Exception:
            return "Fin de charge"
        if 1 <= val <= 2:
            return "Init"
        if 4 <= val <= 6:
            return "Lock Connector"
        if val == 7:
            return "CableCheck"
        if val == 8:
            return "Charge"
        if val > 8:
            return "Fin de charge"
        return "Fin de charge"

    evi_moment_val = pd.to_numeric(df.get(EVI_MOMENT), errors="coerce")
    evi_moment_val = (
        evi_moment_val.fillna(0).astype(int)
        if isinstance(evi_moment_val, pd.Series)
        else pd.Series(0, index=df.index, dtype=int)
    )

    def map_moment_general(row):
        if row["type_erreur"] in ("Erreur_EVI", "Erreur_DownStream"):
            try:
                val = int(row[EVI_MOMENT])
            except Exception:
                return "Unknown"
            if val == 0:
                return "Fin de charge"
            return map_moment(val)
        return "Unknown"

    df["moment"] = df.apply(map_moment_general, axis=1)
    return df


def _safe_dt(s):
    return pd.to_datetime(s, errors="coerce")


def _date_str_from_rows(idx, dt_end, dt_start):
    if pd.isna(idx):
        return ""
    try:
        d = dt_end.loc[idx]
        if pd.isna(d):
            d = dt_start.loc[idx]
        return d.strftime("%Y-%m-%d %H:%M:%S") if pd.notna(d) else ""
    except Exception:
        return ""


def build_stats_global_tables(df: pd.DataFrame) -> dict:
    soc = pd.to_numeric(df.get(SOC_COL), errors="coerce")
    soc = soc.fillna(0).astype(int) if isinstance(soc, pd.Series) else pd.Series(0, index=df.index, dtype=int)
    df_ok = df[soc.eq(0)].copy()

    dt_s = _safe_dt(df_ok.get(DATE_START))
    dt_e = _safe_dt(df_ok.get(DATE_END))

    energy = pd.to_numeric(df_ok.get("Energy (Kwh)"), errors="coerce")
    pmean = pd.to_numeric(df_ok.get("Mean Power (Kw)"), errors="coerce")
    pmax = pd.to_numeric(df_ok.get("Max Power (Kw)"), errors="coerce")
    soc_s = pd.to_numeric(df_ok.get("SOC Start"), errors="coerce")
    soc_e = pd.to_numeric(df_ok.get("SOC End"), errors="coerce")
    dur_min = (dt_e - dt_s).dt.total_seconds().div(60)

    def safe_idx_min(series, threshold=None):
        if not isinstance(series, pd.Series) or not series.notna().any():
            return np.nan
        if threshold is None:
            return series.idxmin()
        s = series.copy()
        s = s.where(s >= threshold)
        return s.idxmin() if s.notna().any() else np.nan

    e_min_i = safe_idx_min(energy, threshold=4)
    e_max_i = energy.idxmax() if isinstance(energy, pd.Series) and energy.notna().any() else np.nan

    pm_min_i = safe_idx_min(pmean, threshold=4)
    pm_max_i = pmean.idxmax() if isinstance(pmean, pd.Series) and pmean.notna().any() else np.nan

    px_min_i = safe_idx_min(pmax, threshold=4)
    px_max_i = pmax.idxmax() if isinstance(pmax, pd.Series) and pmax.notna().any() else np.nan

    d_min_i = dur_min.idxmin() if isinstance(dur_min, pd.Series) and dur_min.notna().any() else np.nan
    d_max_i = dur_min.idxmax() if isinstance(dur_min, pd.Series) and dur_min.notna().any() else np.nan

    stats_energy_ok = pd.DataFrame(
        {
            "Indicateur": ["Total (kWh)", "Moyenne (kWh)", "Min (kWh)", "Max (kWh)"],
            "Valeur": [
                round(float(energy.sum(skipna=True)) if isinstance(energy, pd.Series) else 0.0, 3),
                round(float(energy.mean(skipna=True)) if isinstance(energy, pd.Series) else 0.0, 3),
                round(float(energy.min(skipna=True)) if isinstance(energy, pd.Series) else 0.0, 3),
                round(float(energy.max(skipna=True)) if isinstance(energy, pd.Series) else 0.0, 3),
            ],
            "Date": ["", "", _date_str_from_rows(e_min_i, dt_e, dt_s), _date_str_from_rows(e_max_i, dt_e, dt_s)],
        }
    )

    stats_pmean_ok = pd.DataFrame(
        {
            "Indicateur": ["Moyenne (kW)", "Min (kW)", "Max (kW)"],
            "Valeur": [
                round(float(pmean.mean(skipna=True)) if isinstance(pmean, pd.Series) else 0.0, 3),
                round(float(pmean.min(skipna=True)) if isinstance(pmean, pd.Series) else 0.0, 3),
                round(float(pmean.max(skipna=True)) if isinstance(pmean, pd.Series) else 0.0, 3),
            ],
            "Date": ["", _date_str_from_rows(pm_min_i, dt_e, dt_s), _date_str_from_rows(pm_max_i, dt_e, dt_s)],
        }
    )

    stats_pmax_ok = pd.DataFrame(
        {
            "Indicateur": ["Moyenne (kW)", "Min (kW)", "Max (kW)"],
            "Valeur": [
                round(float(pmax.mean(skipna=True)) if isinstance(pmax, pd.Series) else 0.0, 3),
                round(float(pmax.min(skipna=True)) if isinstance(pmax, pd.Series) else 0.0, 3),
                round(float(pmax.max(skipna=True)) if isinstance(pmax, pd.Series) else 0.0, 3),
            ],
            "Date": ["", _date_str_from_rows(px_min_i, dt_e, dt_s), _date_str_from_rows(px_max_i, dt_e, dt_s)],
        }
    )

    stats_soc_ok = pd.DataFrame(
        {
            "Indicateur": ["SOC début moyen (%)", "SOC fin moyen (%)"],
            "Valeur": [
                round(float(soc_s.mean(skipna=True)) if isinstance(soc_s, pd.Series) else 0.0, 2),
                round(float(soc_e.mean(skipna=True)) if isinstance(soc_e, pd.Series) else 0.0, 2),
            ],
            "Date": ["", ""],
        }
    )

    stats_durations_ok = pd.DataFrame(
        {
            "Indicateur": ["Moyenne (min)", "Min (min)", "Max (min)"],
            "Valeur": [
                round(float(dur_min.mean(skipna=True)) if isinstance(dur_min, pd.Series) else 0.0, 1),
                round(float(dur_min.min(skipna=True)) if isinstance(dur_min, pd.Series) else 0.0, 1),
                round(float(dur_min.max(skipna=True)) if isinstance(dur_min, pd.Series) else 0.0, 1),
            ],
            "Date": ["", _date_str_from_rows(d_min_i, dt_e, dt_s), _date_str_from_rows(d_max_i, dt_e, dt_s)],
        }
    )

    return {
        "stats_energy_ok": stats_energy_ok,
        "stats_pmean_ok": stats_pmean_ok,
        "stats_pmax_ok": stats_pmax_ok,
        "stats_soc_ok": stats_soc_ok,
        "stats_durations_ok": stats_durations_ok,
    }


def build_evi_combo_tables(df: pd.DataFrame) -> dict:
    site_col = "Site" if "Site" in df.columns else "Name Project"
    soc_col = "State of charge(0:good, 1:error)"
    code_col = "EVI Error Code"
    step_col = "EVI Status during error"
    pdc_col = "PDC" if "PDC" in df.columns else None
    start = "Datetime start"

    soc = pd.to_numeric(df.get(soc_col, 0), errors="coerce").fillna(0).astype(int)
    code = pd.to_numeric(df.get(code_col, 0), errors="coerce").fillna(0).astype(int)
    step = pd.to_numeric(df.get(step_col, 0), errors="coerce").fillna(0).astype(int)

    fail = df.loc[soc.eq(1)].copy()
    fail["EVI_Code"] = code.loc[fail.index]
    fail["EVI_Step"] = step.loc[fail.index]
    mask_combo = (fail["EVI_Code"].ne(0)) | (fail["EVI_Step"].ne(0))

    def _map_step_to_moment_int(s: int) -> str:
        if 1 <= s <= 2:
            return "Init"
        if 4 <= s <= 6:
            return "Lock Connector"
        if s == 7:
            return "CableCheck"
        if s == 8:
            return "Charge"
        if s > 8:
            return "Fin de charge"
        return "Unknown"

    evi_long = fail.loc[
        mask_combo,
        [site_col, start, "EVI_Code", "EVI_Step"] + ([pdc_col] if pdc_col else []),
    ].copy()
    evi_long.rename(columns={site_col: "Site"}, inplace=True)
    evi_long["Datetime start"] = pd.to_datetime(evi_long["Datetime start"], errors="coerce")
    evi_long["step_num"] = pd.to_numeric(evi_long["EVI_Step"], errors="coerce").fillna(-1).astype(int)
    evi_long["code_num"] = pd.to_numeric(evi_long["EVI_Code"], errors="coerce").fillna(-1).astype(int)
    evi_long["moment"] = evi_long["step_num"].map(_map_step_to_moment_int)
    by_site = (
        evi_long.groupby(["Site", "EVI_Code", "EVI_Step"], as_index=False)
        .size()
        .rename(columns={"size": "Occurrences"})
    )
    by_site["%_site"] = (
        by_site["Occurrences"]
        / by_site.groupby("Site")["Occurrences"].transform("sum")
        * 100
    ).round(2)

    if pdc_col:
        by_site_pdc = (
            evi_long.groupby(["Site", "PDC", "EVI_Code", "EVI_Step"], as_index=False)
            .size()
            .rename(columns={"size": "Occurrences"})
        )
        by_site_pdc["%_site_pdc"] = (
            by_site_pdc["Occurrences"]
            / by_site_pdc.groupby(["Site", "PDC"])["Occurrences"].transform("sum")
            * 100
        ).round(2)
    else:
        by_site_pdc = pd.DataFrame(
            columns=["Site", "PDC", "EVI_Code", "EVI_Step", "Occurrences", "%_site_pdc"]
        )

    return {
        "evi_combo_long": evi_long.sort_values(["Site", "Datetime start"]),
        "evi_combo_by_site": by_site.sort_values(["Site", "Occurrences"], ascending=[True, False]),
        "evi_combo_by_site_pdc": by_site_pdc.sort_values(
            ["Site", "PDC", "Occurrences"], ascending=[True, True, False]
        ),
    }


def build_durations_daily(df: pd.DataFrame) -> dict:
    site_col = "Site" if "Site" in df.columns else ("Name Project" if "Name Project" in df.columns else "Site")
    if site_col not in df.columns:
        df[site_col] = "Unknown"

    dt_s = pd.to_datetime(df.get("Datetime start"), errors="coerce")
    dt_e = pd.to_datetime(df.get("Datetime end"), errors="coerce")
    dur_min = (dt_e - dt_s).dt.total_seconds().div(60)
    dur_min = dur_min.mask(dur_min < 0, 0).fillna(0)

    ok_mask = df.get("is_ok", False).astype(bool)
    ok = df[ok_mask].copy()
    ok["_day"] = pd.to_datetime(ok["Datetime start"], errors="coerce").dt.floor("D")
    ok["dur_min"] = dur_min.loc[ok.index].fillna(0)

    dur_site_daily = (
        ok.groupby([site_col, "_day"], dropna=False)["dur_min"].sum().reset_index().rename(columns={site_col: "Site", "_day": "day"})
    )

    if "PDC" in ok.columns:
        dur_pdc_daily = (
            ok.groupby([site_col, "PDC", "_day"], dropna=False)["dur_min"].sum().reset_index().rename(columns={site_col: "Site", "_day": "day"})
        )
    else:
        dur_pdc_daily = pd.DataFrame(columns=["Site", "PDC", "day", "dur_min"])

    return {
        "durations_site_daily": dur_site_daily.sort_values(["Site", "day"]),
        "durations_pdc_daily": dur_pdc_daily.sort_values(["Site", "PDC", "day"]),
    }


def build_tables(df: pd.DataFrame) -> dict:
    total = len(df)
    ok = int(df["is_ok"].sum())
    nok = total - ok
    global_df = pd.DataFrame(
        [
            {
                "total": total,
                "ok": ok,
                "nok": nok,
                "%_reussite": round(ok / total * 100, 2) if total else 0.0,
                "%_echec": round(nok / total * 100, 2) if total else 0.0,
            }
        ]
    )

    err = df[~df["is_ok"]].copy()

    by_moment = (
        err.groupby("moment").size().reset_index(name="Nb").sort_values("Nb", ascending=False)
    )

    by_type = (
        err[err["type_erreur"] != ""]
        .groupby("type_erreur")
        .size()
        .reset_index(name="Nb")
        .sort_values("Nb", ascending=False)
    )

    by_site = (
        df.groupby(SITE_COL)
        .agg(Total_Charges=("is_ok", "count"), Charges_OK=("is_ok", "sum"))
        .reset_index()
    )
    by_site["Charges_NOK"] = by_site["Total_Charges"] - by_site["Charges_OK"]
    by_site["% Réussite"] = (
        by_site["Charges_OK"] / by_site["Total_Charges"] * 100
    ).round(2)

    by_site_moment = err.groupby([SITE_COL, "moment"]).size().reset_index(name="Nb")

    by_pdc = (
        df.groupby([SITE_COL, PDC_COL])
        .agg(Total_Charges=("is_ok", "count"), Charges_OK=("is_ok", "sum"))
        .reset_index()
    )
    by_pdc["Charges_NOK"] = by_pdc["Total_Charges"] - by_pdc["Charges_OK"]
    by_pdc["% Réussite"] = (
        by_pdc["Charges_OK"] / by_pdc["Total_Charges"] * 100
    ).round(2)

    return {
        "global": global_df,
        "by_moment": by_moment,
        "by_type": by_type,
        "by_site": by_site,
        "by_site_moment": by_site_moment,
        "by_pdc": by_pdc,
    }


def _norm_mac_full(x: str) -> str:
    if pd.isna(x):
        return ""
    s = str(x).strip().lower()
    if s.startswith("0x"):
        s = s[2:]
    for ch in [":", "-", " "]:
        s = s.replace(ch, "")
    s = "".join(ch for ch in s if ch in "0123456789abcdef")
    return "" if (s == "" or all(ch == "0" for ch in s)) else s


def _norm_hex_frag(x: str) -> str:
    if pd.isna(x):
        return ""
    s = str(x).strip().lower()
    if s.startswith("0x"):
        s = s[2:]
    s = "".join(ch for ch in s if ch in "0123456789abcdef")
    if s == "" or all(ch == "0" for ch in s):
        return ""
    return s


def _compose_full_mac(
    row,
    c1_candidates=("mac_adress_1", "mac_address_1", "ac_adress_", "mac1"),
    c2_candidates=("mac_adress_2", "mac_address_2", "mac2"),
    c_single=("mac", "mac_address", "mac_adress"),
):
    def _get_first(colnames):
        for c in colnames:
            if c in row and pd.notna(row[c]):
                return row[c]
        return ""

    m1 = _norm_hex_frag(_get_first(c1_candidates))
    m2 = _norm_hex_frag(_get_first(c2_candidates))
    if m1 and m2:
        return f"{m1}{m2}"
    return _norm_mac_full(_get_first(c_single))


def _fmt_mac(x: str) -> str:
    if pd.isna(x):
        return ""
    s = str(x).strip().lower()
    if s.startswith("0x"):
        s = s[2:]
    for ch in (":", "-", " "):
        s = s.replace(ch, "")
    s = "".join(ch for ch in s if ch in "0123456789abcdef")
    if s == "":
        return ""
    if set(s) == {"0"}:
        return "00"
    if len(s) % 2 == 1:
        s = "0" + s
    pairs = [s[i : i + 2].upper() for i in range(0, len(s), 2)]
    return ":".join(pairs)


def _load_mac_vehicle_mapping(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=["mac", "MAC Address", "Vehicle"])
    df = pd.read_excel(path)
    cols = {str(c).strip().lower(): c for c in df.columns}
    mac_col = next(
        (cols[k] for k in cols if k in ("mac id", "mac", "mac_id", "macid")),
        None,
    )
    veh_col = next(
        (cols[k] for k in cols if k in ("vehicle", "vehicule", "modèle", "modele", "model")),
        None,
    )

    if not mac_col or not veh_col:
        return pd.DataFrame(columns=["mac", "MAC Address", "Vehicle"])

    out = df[[mac_col, veh_col]].copy()
    out.columns = ["mac", "Vehicle"]
    out["mac"] = (
        out["mac"]
        .astype(str)
        .str.lower()
        .str.replace("0x", "", regex=False)
        .str.replace("[:\\- ]", "", regex=True)
        .map(lambda s: "".join(ch for ch in s if ch in "0123456789abcdef"))
    )

    out = out[out["mac"] != ""].drop_duplicates(subset=["mac"]).reset_index(drop=True)
    out["MAC Address"] = out["mac"].map(_fmt_mac)
    out["Vehicle"] = out["Vehicle"].astype(str).fillna("Unknown")
    return out


def build_charges_mac(df: pd.DataFrame) -> pd.DataFrame:
    map_df = _load_mac_vehicle_mapping(KPIS_MAC)
    work = df.copy()
    work["mac"] = work.apply(_compose_full_mac, axis=1)
    if "mac_adress_1" in work.columns:
        work["mac_adress_1"] = work["mac_adress_1"].map(_norm_hex_frag)
    if "mac_adress_2" in work.columns:
        work["mac_adress_2"] = work["mac_adress_2"].map(_norm_hex_frag)
    work = work[work["mac"] != ""].copy()
    work["MAC Address"] = work["mac"].map(_fmt_mac)
    if not map_df.empty:
        m = map_df.copy()
        m["mac"] = (
            m["mac"].astype(str)
            .str.lower()
            .str.replace("0x", "", regex=False)
            .str.replace("[:\\- ]", "", regex=True)
        )
        exact = m[["mac", "Vehicle"]].drop_duplicates(subset=["mac"])
        work = work.merge(exact, on="mac", how="left", suffixes=("", "_from_exact"))
        m["prefix6"] = m["mac"].str[:6]
        p6 = m[["prefix6", "Vehicle"]].drop_duplicates(subset=["prefix6"])
        work["prefix6"] = work["mac"].str[:6]
        work = work.merge(p6, on="prefix6", how="left", suffixes=("", "_from_p6"))
        m["prefix4"] = m["mac"].str[:4]
        p4 = m[["prefix4", "Vehicle"]].drop_duplicates(subset=["prefix4"])
        work["prefix4"] = work["mac"].str[:4]
        work = work.merge(p4, on="prefix4", how="left", suffixes=("", "_from_p4"))
        work["Vehicle"] = (
            work["Vehicle"].fillna(work.pop("Vehicle_from_p6")).fillna(work.pop("Vehicle_from_p4"))
        )
        for tmp in ("prefix6", "prefix4"):
            if tmp in work.columns:
                work.drop(columns=[tmp], inplace=True)
    else:
        work["Vehicle"] = ""
    id_col = next((c for c in ["ID", "Id", "session_id", "Session ID"] if c in work.columns), None)
    if id_col is None:
        raise ValueError("Aucune colonne ID trouvée dans le DataFrame (charge.csv devrait contenir 'id').")
    elif id_col != "ID":
        work.rename(columns={id_col: "ID"}, inplace=True)
    site_col = "Site" if "Site" in work.columns else ("Name Project" if "Name Project" in work.columns else "Site")
    if site_col not in work.columns:
        work[site_col] = "Unknown"
    keep = [
        "ID",
        site_col,
        "Datetime start",
        "is_ok",
        "SOC Start",
        "SOC End",
        "mac_adress_1",
        "mac_adress_2",
        "mac",
        "MAC Address",
        "Vehicle",
    ]
    keep = [c for c in keep if c in work.columns]
    out = (
        work[keep]
        .rename(columns={site_col: "Site"})
        .sort_values(
            ["Site", "Datetime start", "ID"] if "Datetime start" in keep else ["Site", "ID"],
            na_position="last",
        )
        .reset_index(drop=True)
    )
    return out


def resolve_session_id(df: pd.DataFrame) -> str:
    if "id" not in df.columns:
        raise ValueError("charge.csv doit contenir la colonne 'id'.")
    df.rename(columns={"id": "ID"}, inplace=True)
    df["ID"] = df["ID"].astype(str).str.strip()
    return "ID"


def build_suspicious_under_1kwh(df: pd.DataFrame) -> pd.DataFrame:
    site_col = "Site" if "Site" in df.columns else ("Name Project" if "Name Project" in df.columns else None)
    need = [
        "ID",
        site_col,
        "PDC" if "PDC" in df.columns else None,
        "Datetime start",
        "Datetime end",
        "Energy (Kwh)" if "Energy (Kwh)" in df.columns else None,
        "SOC Start" if "SOC Start" in df.columns else None,
        "SOC End" if "SOC End" in df.columns else None,
        "is_ok" if "is_ok" in df.columns else None,
    ]
    need = [c for c in need if c]
    out = df[need].copy()

    if site_col and site_col != "Site":
        out.rename(columns={site_col: "Site"}, inplace=True)

    out["ID"] = out["ID"].astype(str).str.strip()
    out["Datetime start"] = pd.to_datetime(out.get("Datetime start"), errors="coerce")
    out["Datetime end"] = pd.to_datetime(out.get("Datetime end"), errors="coerce")
    if "Energy (Kwh)" in out.columns:
        out["Energy (Kwh)"] = pd.to_numeric(out["Energy (Kwh)"], errors="coerce")

    mask_ok = out["is_ok"] if "is_ok" in out.columns else False
    mask_e = out["Energy (Kwh)"].lt(1) if "Energy (Kwh)" in out.columns else False
    out = out[mask_ok & mask_e].copy()

    for c in ("Datetime start", "Datetime end"):
        if c in out.columns:
            out[c] = pd.to_datetime(out[c], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")

    keep = [
        "ID",
        "Site",
        "PDC",
        "Datetime start",
        "Datetime end",
        "Energy (Kwh)",
        "SOC Start",
        "SOC End",
    ]
    keep = [c for c in keep if c in out.columns]
    return (
        out[keep]
        .sort_values(["Site", "Datetime start", "ID"], na_position="last")
        .reset_index(drop=True)
    )


def build_multi_attempts_hour(df: pd.DataFrame) -> pd.DataFrame:
    site_col = "Site" if "Site" in df.columns else ("Name Project" if "Name Project" in df.columns else None)
    required_cols = ["Datetime start", "ID", "MAC Address"]
    if site_col is None or not all(col in df.columns for col in required_cols):
        return pd.DataFrame(
            columns=[
                "Site",
                "Heure",
                "Date_heure",
                "MAC Address",
                "tentatives",
                "PDC(s)",
                "1ère tentative",
                "Dernière tentative",
                "ID(s)",
                "ID_ref",
                "SOC start min",
                "SOC start max",
                "SOC end min",
                "SOC end max",
            ]
        )

    work = df.copy()
    work["MAC Address"] = work["MAC Address"].astype(str).str.strip().str.lower()
    work["MAC Address"] = work["MAC Address"].replace(["", "none", "nan", "nat"], np.nan)
    work = work.dropna(subset=["MAC Address"])
    work = work[work["MAC Address"].str.contains(r"[0-9a-f]{4,}", regex=True, na=False)]
    if work.empty:
        return pd.DataFrame(
            columns=[
                "Site",
                "Heure",
                "Date_heure",
                "MAC Address",
                "tentatives",
                "PDC(s)",
                "1ère tentative",
                "Dernière tentative",
                "ID(s)",
                "ID_ref",
            ]
        )
    work["Datetime start"] = pd.to_datetime(work["Datetime start"], errors="coerce")
    work = work.dropna(subset=["Datetime start"])
    work["Date_heure"] = work["Datetime start"].dt.floor("h")
    grp_keys = [site_col, "Date_heure", "MAC Address"]
    agg_dict = {
        "tentatives": ("PDC", "count"),
        "PDC(s)": ("PDC", lambda s: ", ".join(sorted({str(x) for x in s.dropna().astype(str)}))),
        "1ère tentative": ("Datetime start", "min"),
        "Dernière tentative": ("Datetime start", "max"),
    }
    if "SOC Start" in work.columns:
        agg_dict["SOC start min"] = ("SOC Start", "min")
        agg_dict["SOC start max"] = ("SOC Start", "max")
    if "SOC End" in work.columns:
        agg_dict["SOC end min"] = ("SOC End", "min")
        agg_dict["SOC end max"] = ("SOC End", "max")

    agg = work.groupby(grp_keys).agg(**agg_dict).reset_index()
    sorted_work = work.sort_values(["Date_heure", "Datetime start"])

    ids_list = (
        sorted_work.groupby(grp_keys)["ID"].apply(lambda s: ", ".join([str(x).strip() for x in s.astype(str)])).reset_index(name="ID(s)")
    )

    last_id = (
        sorted_work.groupby(grp_keys)["ID"].last().reset_index(name="ID_ref")
    )
    merge_keys = [site_col, "Date_heure", "MAC Address"]
    out = agg.merge(ids_list, on=merge_keys, how="left").merge(last_id, on=merge_keys, how="left")

    out = out.rename(columns={site_col: "Site"})
    out = out[out["tentatives"] >= 2].copy()
    if out.empty:
        return out
    out["Heure"] = out["Date_heure"].dt.strftime("%Y-%m-%d %H:00")
    base_cols = [
        "Site",
        "Heure",
        "Date_heure",
        "MAC Address",
        "tentatives",
        "PDC(s)",
        "1ère tentative",
        "Dernière tentative",
        "ID(s)",
        "ID_ref",
    ]
    soc_cols = [c for c in ["SOC start min", "SOC start max", "SOC end min", "SOC end max"] if c in out.columns]
    out = (
        out[base_cols + soc_cols]
        .sort_values(["Date_heure", "Site", "tentatives"], ascending=[True, True, False])
        .reset_index(drop=True)
    )
    return out


def build_charges_time_stats(df: pd.DataFrame) -> dict:
    dt = pd.to_datetime(df.get("Datetime start"), errors="coerce")
    ok = df.get("is_ok", False).astype(bool)

    site_col = "Site" if "Site" in df.columns else ("Name Project" if "Name Project" in df.columns else None)
    site = df.get(site_col, "Unknown")
    pdc = df["PDC"].astype(str) if "PDC" in df.columns else pd.Series(["—"] * len(df), index=df.index)

    base = pd.DataFrame(
        {
            "Site": site,
            "PDC": pdc,
            "dt": dt,
            "Status": np.where(ok, "OK", "NOK"),
        }
    ).dropna(subset=["dt"]).copy()

    base["month"] = base["dt"].dt.to_period("M").astype(str)
    base["day"] = base["dt"].dt.strftime("%Y-%m-%d")

    m_all = base.groupby(["month", "Status"], as_index=False).size().rename(columns={"size": "Nb"})
    d_all = base.groupby(["day", "Status"], as_index=False).size().rename(columns={"size": "Nb"})
    m_site = base.groupby(["Site", "month", "Status"], as_index=False).size().rename(columns={"size": "Nb"})
    d_site = base.groupby(["Site", "day", "Status"], as_index=False).size().rename(columns={"size": "Nb"})
    d_site_pdc = (
        base.groupby(["Site", "PDC", "day", "Status"], as_index=False)
        .size()
        .rename(columns={"size": "Nb"})
        .sort_values(["Site", "day", "Nb"], ascending=[True, True, False])
    )

    return {
        "charges_monthly": m_all.sort_values("month"),
        "charges_daily": d_all.sort_values("day"),
        "charges_monthly_by_site": m_site.sort_values(["Site", "month"]),
        "charges_daily_by_site": d_site.sort_values(["Site", "day"]),
        "charges_daily_by_site_pdc": d_site_pdc,
    }


def fetch_charge_data() -> pd.DataFrame:
    query = """
        SELECT *
        FROM charge_info
        WHERE start_time >= '2025-02-06' AND Tri_charge = 1
        ORDER BY start_time ASC
    """
    df = pd.read_sql(query, engine)

    rename_map = {
        "start_time": "Datetime start",
        "end_time": "Datetime end",
        "start_time_utc": "Datetime start utc",
        "end_time_utc": "Datetime end utc",
        "borne_id": "PDC",
        "energy": "Energy (Kwh)",
        "soc_debut": "SOC Start",
        "soc_fin": "SOC End",
        "duration": "Duration",
        "Etat": "State of charge(0:good, 1:error)",
        "mean_power": "Mean Power (Kw)",
        "max_power": "Max Power (Kw)",
        "status_upstream": "Status Upstream",
        "status_downstream": "Status Downstream",
        "upstream_ic": "Upstream Code IC",
        "upstream_pc": "Upstream Code PC",
        "EVI_error": "EVI Status",
        "EVi_status_at_error": "EVI Status during error",
        "Evi_error_code": "EVI Error Code",
        "downstream_ic": "Downstream Code IC",
        "downstream_pc": "Downstream Code PC",
        "project_num": "Id Project",
        "project_name": "Name Project",
        "mac": "MAC Address",
    }

    df.rename(columns=rename_map, inplace=True)
    return df


def save_to_indicator(table_dict: dict):
    metadata = MetaData()
    for name, df in table_dict.items():
        if not isinstance(df, pd.DataFrame) or df.empty:
            print(f"⚠️ Table ignorée (vide) : {name}")
            continue

        table_name = f"kpi_{name.lower()}"
        schema_name = "indicator"

        try:
            table = Table(table_name, metadata, autoload_with=engine, schema=schema_name)
        except Exception as e:
            print(f"❌ Table non trouvée ou erreur chargement : {table_name} → {e}")
            continue

        df_cleaned = df.where(pd.notna(df), None)

        with engine.begin() as conn:
            try:
                stmt = insert(table).prefix_with("IGNORE")
                conn.execute(stmt, df_cleaned.to_dict(orient="records"))
                print(
                    f"✅ Table insérée (BATCH INSERT IGNORE) : {schema_name}.{table_name} ({len(df)} lignes)"
                )
            except Exception as e:
                print(f"❌ Erreur insertion batch pour {table_name} → {e}")


def main():
    df = fetch_charge_data()
    resolve_session_id(df)
    df = classify_errors(df)
    if "Site" not in df.columns and "Name Project" in df.columns:
        df["Site"] = df["Name Project"]
    df["Site"] = df["Site"].astype(str).str.strip().replace({"": "Unknown"})
    df["moment_avancee"] = df["moment"].map(
        {
            "Init": "Avant charge",
            "Lock Connector": "Avant charge",
            "CableCheck": "Avant charge",
            "Charge": "Charge",
            "Fin de charge": "Fin de charge",
        }
    ).fillna("Unknown")

    tables = build_tables(df)
    stats = build_stats_global_tables(df)
    evi = build_evi_combo_tables(df)

    mac_lookup = pd.read_sql("SELECT * FROM indicator.mac_lookup", con=engine)
    mac_lookup.columns = [c.lower().strip() for c in mac_lookup.columns]
    mac_lookup = mac_lookup.rename(columns={"mac address": "mac", "vehicle": "Vehicle"})

    global _load_mac_vehicle_mapping
    original_mac_loader = _load_mac_vehicle_mapping

    def _patched_mac_loader(_):
        return mac_lookup

    _load_mac_vehicle_mapping = _patched_mac_loader

    try:
        charges_mac = build_charges_mac(df)
    finally:
        _load_mac_vehicle_mapping = original_mac_loader

    durations = build_durations_daily(df)
    time_stats = build_charges_time_stats(df)
    multi_attempts = build_multi_attempts_hour(df)
    suspicious_under_1kwh = build_suspicious_under_1kwh(df)

    df["month"] = pd.to_datetime(df["Datetime start"], errors="coerce").dt.to_period("M").astype(str)
    success_rate = (
        df.groupby(["Site", "month"])
        .agg(
            Total_Charges=("is_ok", "count"),
            Charges_OK=("is_ok", "sum"),
        )
        .reset_index()
    )
    success_rate["Charges_NOK"] = success_rate["Total_Charges"] - success_rate["Charges_OK"]
    success_rate["Success_Rate(%)"] = (
        (success_rate["Charges_OK"] / success_rate["Total_Charges"]) * 100
    ).round(2)

    if not charges_mac.empty and not multi_attempts.empty:
        cm_min = charges_mac[["ID", "MAC Address", "Vehicle"]].drop_duplicates("ID", keep="last")

        multi_attempts = multi_attempts.merge(
            cm_min, left_on="ID_ref", right_on="ID", how="left"
        ).drop(columns="ID", errors="ignore")
        multi_attempts["MAC Address"] = multi_attempts["MAC Address_y"].fillna("").map(_fmt_mac)
        multi_attempts["Vehicle"] = multi_attempts["Vehicle"].fillna("Unknown")
        multi_attempts["MAC"] = multi_attempts["MAC Address"]
        multi_attempts = multi_attempts.drop(
            columns=["MAC Address_x", "MAC Address_y"], errors="ignore"
        )
    else:
        multi_attempts["MAC Address"] = ""
        multi_attempts["Vehicle"] = "Unknown"
        multi_attempts["MAC"] = ""

    if not charges_mac.empty and not suspicious_under_1kwh.empty:
        cm_min = charges_mac[["ID", "MAC Address", "Vehicle"]].drop_duplicates("ID", keep="last")
        suspicious_under_1kwh = suspicious_under_1kwh.merge(cm_min, on="ID", how="left")
        suspicious_under_1kwh["MAC Address"] = (
            suspicious_under_1kwh["MAC Address"].fillna("").map(_fmt_mac)
        )
        suspicious_under_1kwh["Vehicle"] = suspicious_under_1kwh["Vehicle"].fillna("Unknown")
    else:
        suspicious_under_1kwh["MAC Address"] = ""
        suspicious_under_1kwh["Vehicle"] = "Unknown"

    sessions_cols = [
        "Site",
        "Name Project",
        "PDC",
        "Datetime start",
        "Datetime end",
        "State of charge(0:good, 1:error)",
        "ID",
        "is_ok",
        "type_erreur",
        "moment",
        "moment_avancee",
        "Energy (Kwh)",
        "Mean Power (Kw)",
        "Max Power (Kw)",
        "SOC Start",
        "SOC End",
        "EVI Error Code",
        "Downstream Code PC",
        "EVI Status during error",
        "MAC Address",
        "charge_900V",
    ]
    sessions_cols = [c for c in sessions_cols if c in df.columns]
    sessions = df[sessions_cols].copy()

    all_tables = {
        **tables,
        **stats,
        "evi_combo_long": evi["evi_combo_long"],
        "evi_combo_by_site": evi["evi_combo_by_site"],
        "evi_combo_by_site_pdc": evi["evi_combo_by_site_pdc"],
        "charges_mac": charges_mac,
        "multi_attempts_hour": multi_attempts,
        "suspicious_under_1kwh": suspicious_under_1kwh,
        "durations_site_daily": durations["durations_site_daily"],
        "durations_pdc_daily": durations["durations_pdc_daily"],
        "charges_monthly": time_stats["charges_monthly"],
        "charges_daily": time_stats["charges_daily"],
        "charges_monthly_by_site": time_stats["charges_monthly_by_site"],
        "charges_daily_by_site": time_stats["charges_daily_by_site"],
        "charges_daily_by_site_pdc": time_stats["charges_daily_by_site_pdc"],
        "sessions": sessions,
        "success_rate_monthly_by_site": success_rate,
    }
    save_to_indicator(all_tables)


if __name__ == "__main__":
    main()