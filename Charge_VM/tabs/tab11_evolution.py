import pandas as pd
import plotly.express as px
import streamlit as st

from tabs.context import get_context

TAB_CODE = """
st.subheader("Évolution mensuelle du taux de réussite global (Erreurs de fin de charge exclus)")

if evo_df.empty:
    st.info("Aucune donnée disponible dans `indicator.kpi_evo`.")
else:
    df = evo_df.copy()

    # Identifier les colonnes de mois et de taux
    mois_col = None
    for candidate in ["mois", "Mois", "month"]:
        if candidate in df.columns:
            mois_col = candidate
            break

    taux_col = None
    for candidate in ["tr", "taux_reussite", "taux", "success_rate"]:
        if candidate in df.columns:
            taux_col = candidate
            break

    if mois_col is None or taux_col is None:
        st.error("Colonnes `mois` et `taux de réussite` manquantes dans la table.")
    else:
        df[mois_col] = pd.to_datetime(df[mois_col], errors="coerce")
        df = df.dropna(subset=[mois_col])
        df = df.sort_values(mois_col)
        df["mois_affiche"] = df[mois_col].dt.strftime("%Y-%m")

        df["taux_val"] = pd.to_numeric(df[taux_col], errors="coerce")
        df = df.dropna(subset=["taux_val"])  # Retirer les taux non numériques

        if df.empty:
            st.info("Aucune donnée exploitable après nettoyage.")
        else:
            # Conversion en pourcentage si nécessaire
            if df["taux_val"].between(0, 1).all():
                df["taux_pct"] = df["taux_val"] * 100
            else:
                df["taux_pct"] = df["taux_val"]

            # Graphique en barres
            fig = px.bar(
                df,
                x="mois_affiche",
                y="taux_pct",
                text="taux_pct",
            )
            fig.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
            fig.update_layout(
                xaxis_title="Mois",
                yaxis_title="Taux de réussite (%)",
                hovermode="x unified",
                uniformtext_minsize=8,
                uniformtext_mode="hide",
                bargap=0.3,
            )

            st.plotly_chart(fig, use_container_width=True)

            # Tableau sans index ni colonne Site
            display_cols = ["mois_affiche", "taux_pct"]
            st.dataframe(
                df[display_cols]
                .rename(columns={
                    "mois_affiche": "Mois",
                    "taux_pct": "Taux de réussite (%)",
                })
                .reset_index(drop=True),
                use_container_width=True,
                hide_index=True,
            )
"""


def _get_kpi_evo_table(ctx):
    tables = getattr(ctx, "tables", {}) or {}
    if isinstance(tables, dict):
        for key in ("evo", "kpi_evo"):
            if key in tables:
                return tables[key]
    return pd.DataFrame()


def render():
    ctx = get_context()
    evo_df = _get_kpi_evo_table(ctx)
    exec(
        TAB_CODE,
        {"pd": pd, "px": px, "st": st},
        {"evo_df": evo_df},
    )
