import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import create_engine

from tabs.context import get_context

TAB_CODE = """
st.markdown("### 📊 Vue d'ensemble des alertes et performances")

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
            st.markdown(f\"\"\"
            <div style='padding: 30px; background: {alert_color}; border-radius: 10px; text-align: center;'>
                <h1 style='color: white; margin: 0; font-size: 3em;'>{nb_alertes_actives}</h1>
                <p style='color: white; margin: 10px 0 0 0; font-size: 1.2em;'>Alertes détectées</p>
            </div>
            \"\"\", unsafe_allow_html=True)
        else:
            st.markdown(\"\"\"
            <div style='padding: 30px; background: #28a745; border-radius: 10px; text-align: center;'>
                <h1 style='color: white; margin: 0; font-size: 3em;'>0</h1>
                <p style='color: white; margin: 10px 0 0 0; font-size: 1.2em;'>Aucune alerte</p>
            </div>
            \"\"\", unsafe_allow_html=True)

    with col_alert2:
        st.markdown("### Top 5 Sites en Alerte")
        if not df_alertes.empty:
            top_sites_alertes = df_alertes.groupby("Site").size().sort_values(ascending=False).head(5)

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

    if not df_alertes.empty:
        st.markdown("### Dernières Alertes Critiques")

        df_display = df_alertes.head(10).copy()
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
    by_site_kpi = by_site_kpi.sort_values("nok", ascending=False).head(10)

    col_chart1, col_chart2 = st.columns(2)

    with col_chart1:
        st.markdown("### Top 10 Sites - Taux de Réussite")
        fig_success = go.Figure(go.Bar(
            x=by_site_kpi["taux_ok"],
            y=by_site_kpi[SITE_COL],
            orientation='h',
            marker=dict(
                color=by_site_kpi["taux_ok"],
                colorscale='RdYlGn',
                showscale=False,
                cmin=0,
                cmax=100
            ),
            text=by_site_kpi["taux_ok"].apply(lambda x: f"{x:.1f}%"),
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
            x=by_site_kpi["nok"],
            y=by_site_kpi[SITE_COL],
            orientation='h',
            marker=dict(
                color=by_site_kpi["nok"],
                colorscale='Reds',
                showscale=False
            ),
            text=by_site_kpi["nok"],
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
