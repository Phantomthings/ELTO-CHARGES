import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import create_engine

from tabs.context import get_context

TAB_CODE = """
st.markdown("### 📊 Vue d'ensemble des alertes et performances")

# Indicateurs pour transactions suspectes et tentatives multiples
col_kpi1, col_kpi2 = st.columns(2)

with col_kpi1:
    suspicious = tables.get("suspicious_under_1kwh", pd.DataFrame())
    nb_suspicious = 0
    if not suspicious.empty:
        df_s_temp = suspicious.copy()
        if "Datetime start" in df_s_temp.columns:
            ds = pd.to_datetime(df_s_temp["Datetime start"], errors="coerce")
            mask = ds.ge(pd.Timestamp(d1)) & ds.lt(pd.Timestamp(d2) + pd.Timedelta(days=1))
            df_s_temp = df_s_temp[mask]
        if site_sel and "Site" in df_s_temp.columns:
            df_s_temp = df_s_temp[df_s_temp["Site"].isin(site_sel)]
        nb_suspicious = len(df_s_temp)

    st.markdown("### ⚠️ Transactions suspectes")
    color = "#dc3545" if nb_suspicious > 5 else "#ffc107" if nb_suspicious > 0 else "#28a745"
    st.markdown(f'''
<div style='padding: 20px; background: {color}; border-radius: 10px; text-align: center;'>
    <h1 style='color: white; margin: 0; font-size: 2.5em;'>{nb_suspicious}</h1>
    <p style='color: white; margin: 10px 0 0 0; font-size: 1em;'>Transactions <1 kWh</p>
</div>
''', unsafe_allow_html=True)

with col_kpi2:
    multi_attempts = tables.get("multi_attempts_hour", pd.DataFrame())
    nb_multi_attempts = 0
    if not multi_attempts.empty:
        dfm_temp = multi_attempts.copy()
        if "Date_heure" in dfm_temp.columns:
            dfm_temp["Date_heure"] = pd.to_datetime(dfm_temp["Date_heure"], errors="coerce")
            d1_ts = pd.Timestamp(d1)
            d2_ts = pd.Timestamp(d2) + pd.Timedelta(days=1)
            mask = dfm_temp["Date_heure"].between(d1_ts, d2_ts)
            dfm_temp = dfm_temp[mask]
        if site_sel and "Site" in dfm_temp.columns:
            dfm_temp = dfm_temp[dfm_temp["Site"].isin(site_sel)]
        nb_multi_attempts = len(dfm_temp)

    st.markdown("### ⚠️ Analyse tentatives multiples")
    color = "#dc3545" if nb_multi_attempts > 5 else "#ffc107" if nb_multi_attempts > 0 else "#28a745"
    st.markdown(f'''
<div style='padding: 20px; background: {color}; border-radius: 10px; text-align: center;'>
    <h1 style='color: white; margin: 0; font-size: 2.5em;'>{nb_multi_attempts}</h1>
    <p style='color: white; margin: 10px 0 0 0; font-size: 1em;'>Utilisateurs multiples tentatives</p>
</div>
''', unsafe_allow_html=True)

st.markdown("---")

try:
    engine = create_engine("mysql+pymysql://AdminNidec:u6Ehe987XBSXxa4@141.94.31.144:3306/indicator")

    query_alertes = \"\"\"
        SELECT
            Site,
            PDC,
            type_erreur,
            detection,
            occurrences_12h,
            moment,
            evi_code,
            downstream_code_pc
        FROM kpi_alertes
        ORDER BY detection DESC
        LIMIT 50
    \"\"\"

    df_alertes = pd.read_sql(query_alertes, con=engine)

    # Requête pour les défauts en cours (date_fin IS NULL)
    query_defauts_actifs = \"\"\"
        SELECT
            site,
            date_debut,
            defaut,
            eqp
        FROM kpi_defauts_log
        WHERE date_fin IS NULL
        ORDER BY date_debut DESC
    \"\"\"

    df_defauts_actifs = pd.read_sql(query_defauts_actifs, con=engine)
    engine.dispose()

    if not df_alertes.empty:
        df_alertes["detection"] = pd.to_datetime(df_alertes["detection"], errors="coerce")

        start_dt = pd.to_datetime(d1)
        end_dt = pd.to_datetime(d2) + pd.Timedelta(days=1)
        df_alertes = df_alertes[df_alertes["detection"].between(start_dt, end_dt)]

        if site_sel:
            df_alertes = df_alertes[df_alertes["Site"].isin(site_sel)]

    nb_alertes_actives = len(df_alertes) if not df_alertes.empty else 0

    col_alert1, col_alert2 = st.columns(2)

    with col_alert1:
        st.markdown("### Alertes Actives")
        if nb_alertes_actives > 0:
            alert_color = "#dc3545" if nb_alertes_actives > 10 else "#ffc107"
            st.markdown(f'''
<div style='padding: 30px; background: {alert_color}; border-radius: 10px; text-align: center;'>
    <h1 style='color: white; margin: 0; font-size: 3em;'>{nb_alertes_actives}</h1>
    <p style='color: white; margin: 10px 0 0 0; font-size: 1.2em;'>Alertes détectées</p>
</div>
''', unsafe_allow_html=True)
        else:
            st.markdown('''
<div style='padding: 30px; background: #28a745; border-radius: 10px; text-align: center;'>
    <h1 style='color: white; margin: 0; font-size: 3em;'>0</h1>
    <p style='color: white; margin: 10px 0 0 0; font-size: 1.2em;'>Aucune alerte</p>
</div>
''', unsafe_allow_html=True)

    with col_alert2:
        st.markdown("### Top 5 Sites en Alerte")
        if not df_alertes.empty:
            top_sites_alertes = df_alertes.groupby("Site").size().sort_values(ascending=True).head(5)

            fig_sites = go.Figure(go.Bar(
                x=top_sites_alertes.values,
                y=top_sites_alertes.index,
                orientation='h',
                marker=dict(
                    color=top_sites_alertes.values,
                    colorscale='Reds',
                    showscale=False
                ),
                text=top_sites_alertes.values,
                textposition='outside'
            ))

            fig_sites.update_layout(
                height=300,
                margin=dict(l=0, r=0, t=10, b=0),
                xaxis_title="Nombre d'alertes",
                yaxis_title="",
                showlegend=False
            )

            st.plotly_chart(fig_sites, use_container_width=True)
        else:
            st.info("Aucun site en alerte")

    st.markdown("---")

    # Section Défauts Actifs
    if not df_defauts_actifs.empty:
        df_defauts_actifs["date_debut"] = pd.to_datetime(df_defauts_actifs["date_debut"], errors="coerce")

        # Filtrer par site si nécessaire
        if site_sel:
            df_defauts_actifs = df_defauts_actifs[df_defauts_actifs["site"].isin(site_sel)]

    nb_defauts_actifs = len(df_defauts_actifs) if not df_defauts_actifs.empty else 0

    col_defaut1, col_defaut2 = st.columns(2)

    with col_defaut1:
        st.markdown("### 🔧 Défauts Actifs")
        if nb_defauts_actifs > 0:
            defaut_color = "#dc3545" if nb_defauts_actifs > 5 else "#ffc107"
            st.markdown(f'''
<div style='padding: 30px; background: {defaut_color}; border-radius: 10px; text-align: center;'>
    <h1 style='color: white; margin: 0; font-size: 3em;'>{nb_defauts_actifs}</h1>
    <p style='color: white; margin: 10px 0 0 0; font-size: 1.2em;'>Défauts en cours</p>
</div>
''', unsafe_allow_html=True)
        else:
            st.markdown('''
<div style='padding: 30px; background: #28a745; border-radius: 10px; text-align: center;'>
    <h1 style='color: white; margin: 0; font-size: 3em;'>0</h1>
    <p style='color: white; margin: 10px 0 0 0; font-size: 1.2em;'>Aucun défaut actif</p>
</div>
''', unsafe_allow_html=True)

    with col_defaut2:
        st.markdown("### Top 5 Sites avec Défauts")
        if not df_defauts_actifs.empty:
            top_sites_defauts = df_defauts_actifs.groupby("site").size().sort_values(ascending=True).head(5)

            fig_sites_defauts = go.Figure(go.Bar(
                x=top_sites_defauts.values,
                y=top_sites_defauts.index,
                orientation='h',
                marker=dict(
                    color=top_sites_defauts.values,
                    colorscale='Reds',
                    showscale=False
                ),
                text=top_sites_defauts.values,
                textposition='outside'
            ))

            fig_sites_defauts.update_layout(
                height=300,
                margin=dict(l=0, r=0, t=10, b=0),
                xaxis_title="Nombre de défauts",
                yaxis_title="",
                showlegend=False
            )

            st.plotly_chart(fig_sites_defauts, use_container_width=True)
        else:
            st.info("Aucun site avec défaut actif")

    st.markdown("---")

    # Tableau des défauts actifs
    if not df_defauts_actifs.empty:
        st.markdown("### 🔧 Derniers Défauts Actifs")

        df_defauts_display = df_defauts_actifs.head(15).copy()

        # Calculer "Depuis" (nombre de jours depuis date_debut)
        now = pd.Timestamp.now()
        df_defauts_display["Depuis (jours)"] = (now - df_defauts_display["date_debut"]).dt.days

        df_defauts_display = df_defauts_display.rename(columns={
            "site": "Site",
            "date_debut": "Date début",
            "defaut": "Défaut",
            "eqp": "Equipement"
        })

        display_cols = ["Site", "Date début", "Depuis (jours)", "Défaut", "Equipement"]

        st.dataframe(
            df_defauts_display[display_cols],
            use_container_width=True,
            hide_index=True,
            height=400
        )

    st.markdown("---")

    if not df_alertes.empty:
        st.markdown("### Dernières Alertes Critiques")

        df_display = df_alertes.head(10).copy()

        # Supprimer les lignes où toutes les colonnes importantes sont vides/null
        df_display = df_display.dropna(how='all', subset=['Site', 'PDC', 'type_erreur', 'detection'])

        df_display = df_display.rename(columns={
            "type_erreur": "Type",
            "detection": "Détection",
            "occurrences_12h": "Occurrences 12h",
            "moment": "Moment",
            "evi_code": "EVI",
            "downstream_code_pc": "DS Code"
        })

        display_cols = ["Site", "PDC", "Type", "Détection", "Occurrences 12h", "Moment"]
        if "EVI" in df_display.columns:
            display_cols.append("EVI")
        if "DS Code" in df_display.columns:
            display_cols.append("DS Code")

        # Ne garder que les colonnes qui existent et ont des données
        display_cols = [col for col in display_cols if col in df_display.columns]

        st.dataframe(
            df_display[display_cols],
            use_container_width=True,
            hide_index=True,
            height=400
        )

except Exception as e:
    st.error(f"Erreur de connexion: {str(e)}")
    nb_alertes_actives = 0

st.markdown("---")

by_site_kpi = (
    sess_kpi.groupby(SITE_COL, as_index=False)
            .agg(total=("is_ok_filt", "count"),
                ok=("is_ok_filt", "sum"))
)

if not by_site_kpi.empty:
    by_site_kpi["nok"] = by_site_kpi["total"] - by_site_kpi["ok"]
    by_site_kpi["taux_ok"] = (by_site_kpi["ok"] / by_site_kpi["total"] * 100).round(1)

    # Créer deux DataFrames : un trié par taux de réussite, un par échecs
    by_site_success = by_site_kpi.sort_values("taux_ok", ascending=True).head(10)
    by_site_fails = by_site_kpi.sort_values("nok", ascending=True).head(10)

    col_chart1, col_chart2 = st.columns(2)

    with col_chart1:
        st.markdown("### Top 10 Sites - Taux de Réussite")
        fig_success = go.Figure(go.Bar(
            x=by_site_success["taux_ok"],
            y=by_site_success[SITE_COL],
            orientation='h',
            marker=dict(
                color=by_site_success["taux_ok"],
                colorscale='RdYlGn',
                showscale=False,
                cmin=0,
                cmax=100
            ),
            text=by_site_success["taux_ok"].apply(lambda x: f"{x:.1f}%"),
            textposition='outside'
        ))

        fig_success.update_layout(
            height=400,
            margin=dict(l=0, r=0, t=10, b=0),
            xaxis_title="Taux de réussite (%)",
            yaxis_title="",
            xaxis=dict(range=[0, 105])
        )

        st.plotly_chart(fig_success, use_container_width=True)

    with col_chart2:
        st.markdown("### Top 10 Sites - Nombre d'Échecs")
        fig_fails = go.Figure(go.Bar(
            x=by_site_fails["nok"],
            y=by_site_fails[SITE_COL],
            orientation='h',
            marker=dict(
                color=by_site_fails["nok"],
                colorscale='Reds',
                showscale=False
            ),
            text=by_site_fails["nok"],
            textposition='outside'
        ))

        fig_fails.update_layout(
            height=400,
            margin=dict(l=0, r=0, t=10, b=0),
            xaxis_title="Nombre d'échecs",
            yaxis_title=""
        )

        st.plotly_chart(fig_fails, use_container_width=True)
"""

def render():
    ctx = get_context()
    globals_dict = {
        "np": np,
        "pd": pd,
        "px": px,
        "go": go,
        "st": st,
        "create_engine": create_engine
    }
    local_vars = dict(ctx.__dict__)
    local_vars.setdefault('plot', getattr(ctx, 'plot', None))
    local_vars.setdefault('hide_zero_labels', getattr(ctx, 'hide_zero_labels', None))
    local_vars.setdefault('with_charge_link', getattr(ctx, 'with_charge_link', None))
    local_vars.setdefault('evi_counts_pivot', getattr(ctx, 'evi_counts_pivot', None))
    local_vars = {k: v for k, v in local_vars.items() if v is not None}
    exec(TAB_CODE, globals_dict, local_vars)
