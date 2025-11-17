import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import create_engine

from tabs.context import get_context

TAB_CODE = """
st.markdown("### 📋 Historique des Défauts")

try:
    engine = create_engine("mysql+pymysql://AdminNidec:u6Ehe987XBSXxa4@141.94.31.144:3306/indicator")

    # Requête pour récupérer tous les défauts (historique complet)
    query_defauts = \"\"\"
        SELECT
            site,
            date_debut,
            date_fin,
            defaut,
            eqp
        FROM kpi_defauts_log
        ORDER BY date_debut DESC
    \"\"\"

    df_defauts = pd.read_sql(query_defauts, con=engine)
    engine.dispose()

    if not df_defauts.empty:
        df_defauts["date_debut"] = pd.to_datetime(df_defauts["date_debut"], errors="coerce")
        df_defauts["date_fin"] = pd.to_datetime(df_defauts["date_fin"], errors="coerce")

        # Calculer la durée du défaut
        now = pd.Timestamp.now()
        df_defauts["Durée (jours)"] = ((df_defauts["date_fin"].fillna(now)) - df_defauts["date_debut"]).dt.days

        # Statut du défaut
        df_defauts["Statut"] = df_defauts["date_fin"].apply(lambda x: "En cours" if pd.isna(x) else "Résolu")

        # Obtenir la liste unique des sites
        sites_disponibles = sorted(df_defauts["site"].dropna().unique().tolist())

        # Filtre par site
        st.markdown("### 🔍 Filtres")
        col_filtre1, col_filtre2 = st.columns([2, 2])

        with col_filtre1:
            sites_selectionnes = st.multiselect(
                "Sélectionner un ou plusieurs sites",
                options=sites_disponibles,
                default=sites_disponibles,
                key="defauts_site_filter"
            )

        with col_filtre2:
            statut_selectionne = st.multiselect(
                "Statut du défaut",
                options=["En cours", "Résolu"],
                default=["En cours", "Résolu"],
                key="defauts_statut_filter"
            )

        # Appliquer les filtres
        df_filtree = df_defauts.copy()

        if sites_selectionnes:
            df_filtree = df_filtree[df_filtree["site"].isin(sites_selectionnes)]

        if statut_selectionne:
            df_filtree = df_filtree[df_filtree["Statut"].isin(statut_selectionne)]

        st.markdown("---")

        # KPIs
        col_kpi1, col_kpi2, col_kpi3, col_kpi4 = st.columns(4)

        nb_total = len(df_filtree)
        nb_en_cours = len(df_filtree[df_filtree["Statut"] == "En cours"])
        nb_resolus = len(df_filtree[df_filtree["Statut"] == "Résolu"])
        duree_moyenne = df_filtree["Durée (jours)"].mean() if not df_filtree.empty else 0

        with col_kpi1:
            st.metric("Total Défauts", nb_total)

        with col_kpi2:
            st.metric("En cours", nb_en_cours)

        with col_kpi3:
            st.metric("Résolus", nb_resolus)

        with col_kpi4:
            st.metric("Durée moyenne (jours)", f"{duree_moyenne:.1f}")

        st.markdown("---")

        # Graphiques
        col_chart1, col_chart2 = st.columns(2)

        with col_chart1:
            st.markdown("### Défauts par Site")
            defauts_par_site = df_filtree.groupby("site").size().sort_values(ascending=True).tail(10)

            fig_sites = go.Figure(go.Bar(
                x=defauts_par_site.values,
                y=defauts_par_site.index,
                orientation='h',
                marker=dict(
                    color=defauts_par_site.values,
                    colorscale='Reds',
                    showscale=False
                ),
                text=defauts_par_site.values,
                textposition='outside'
            ))

            fig_sites.update_layout(
                height=400,
                margin=dict(l=0, r=0, t=10, b=0),
                xaxis_title="Nombre de défauts",
                yaxis_title="",
                showlegend=False
            )

            st.plotly_chart(fig_sites, use_container_width=True)

        with col_chart2:
            st.markdown("### Répartition par Statut")
            defauts_par_statut = df_filtree.groupby("Statut").size()

            fig_statut = go.Figure(go.Pie(
                labels=defauts_par_statut.index,
                values=defauts_par_statut.values,
                marker=dict(colors=['#dc3545', '#28a745']),
                textinfo='label+percent+value'
            ))

            fig_statut.update_layout(
                height=400,
                margin=dict(l=0, r=0, t=10, b=0),
                showlegend=True
            )

            st.plotly_chart(fig_statut, use_container_width=True)

        st.markdown("---")

        # Tableau complet
        st.markdown("### 📊 Tableau des Défauts")

        df_display = df_filtree.copy()

        df_display = df_display.rename(columns={
            "site": "Site",
            "date_debut": "Date début",
            "date_fin": "Date fin",
            "defaut": "Défaut",
            "eqp": "Equipement"
        })

        # Formatter les dates pour l'affichage
        df_display["Date début"] = df_display["Date début"].dt.strftime("%Y-%m-%d %H:%M")
        df_display["Date fin"] = df_display["Date fin"].apply(
            lambda x: x.strftime("%Y-%m-%d %H:%M") if pd.notna(x) else "En cours"
        )

        display_cols = ["Site", "Date début", "Date fin", "Durée (jours)", "Statut", "Défaut", "Equipement"]

        # Options de tri
        col_tri1, col_tri2 = st.columns([2, 2])

        with col_tri1:
            tri_colonne = st.selectbox(
                "Trier par",
                options=display_cols,
                index=1,  # Par défaut, tri par "Date début"
                key="defauts_tri_colonne"
            )

        with col_tri2:
            tri_ordre = st.radio(
                "Ordre",
                options=["Décroissant", "Croissant"],
                index=0,
                horizontal=True,
                key="defauts_tri_ordre"
            )

        # Appliquer le tri
        ascending = True if tri_ordre == "Croissant" else False

        # Attention: on doit utiliser les colonnes originales pour le tri si ce sont des dates
        if tri_colonne in ["Date début", "Date fin"]:
            # Créer une colonne de tri temporaire pour les dates
            if tri_colonne == "Date début":
                df_display_sorted = df_display.sort_values(
                    by="Date début",
                    ascending=ascending,
                    key=lambda x: pd.to_datetime(x, format="%Y-%m-%d %H:%M", errors="coerce")
                )
            elif tri_colonne == "Date fin":
                df_display_sorted = df_display.sort_values(
                    by="Date fin",
                    ascending=ascending,
                    key=lambda x: pd.to_datetime(x.replace("En cours", pd.NaT), format="%Y-%m-%d %H:%M", errors="coerce")
                )
        else:
            df_display_sorted = df_display.sort_values(by=tri_colonne, ascending=ascending)

        st.dataframe(
            df_display_sorted[display_cols],
            use_container_width=True,
            hide_index=True,
            height=600
        )

        # Stats supplémentaires
        st.markdown("---")
        st.markdown("### 📈 Statistiques")

        col_stat1, col_stat2, col_stat3 = st.columns(3)

        with col_stat1:
            st.markdown("#### Top 5 Equipements")
            top_equipements = df_filtree["eqp"].value_counts().head(5)
            st.dataframe(
                top_equipements.reset_index().rename(columns={"index": "Equipement", "eqp": "Nombre"}),
                use_container_width=True,
                hide_index=True
            )

        with col_stat2:
            st.markdown("#### Top 5 Défauts")
            top_defauts = df_filtree["defaut"].value_counts().head(5)
            st.dataframe(
                top_defauts.reset_index().rename(columns={"index": "Défaut", "defaut": "Nombre"}),
                use_container_width=True,
                hide_index=True
            )

        with col_stat3:
            st.markdown("#### Durée des défauts")
            duree_min = df_filtree["Durée (jours)"].min()
            duree_max = df_filtree["Durée (jours)"].max()
            duree_mediane = df_filtree["Durée (jours)"].median()

            st.metric("Min (jours)", f"{duree_min:.0f}")
            st.metric("Médiane (jours)", f"{duree_mediane:.1f}")
            st.metric("Max (jours)", f"{duree_max:.0f}")

    else:
        st.info("Aucun défaut trouvé dans l'historique.")

except Exception as e:
    st.error(f"Erreur lors de la récupération des défauts: {str(e)}")
"""

def render():
    ctx = get_context()
    globals_dict = {
        "pd": pd,
        "px": px,
        "go": go,
        "st": st,
        "create_engine": create_engine
    }
    local_vars = dict(ctx.__dict__)
    local_vars = {k: v for k, v in local_vars.items() if v is not None}
    exec(TAB_CODE, globals_dict, local_vars)
