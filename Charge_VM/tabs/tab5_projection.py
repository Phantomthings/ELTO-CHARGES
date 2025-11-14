import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from tabs.context import get_context

TAB_CODE = """
def _map_moment(val: int) -> str:
    try:
        val = int(val)
    except:
        return "Unknown"
    if val == 0:
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
    return "Unknown"

st.subheader("Projection pivot — Moments (ligne 1) × Codes (ligne 2)")

# Vérifier que sess_kpi existe
if sess_kpi is None or sess_kpi.empty:
    st.info("Aucune donnée de session disponible.")
else:
    from analyses.kpi_cal import EVI_MOMENT, EVI_CODE, DS_PC
    
    # Filtrer sur les erreurs uniquement
    err = sess_kpi[~sess_kpi["is_ok_filt"]].copy()
    
    if err.empty:
        st.info("Aucune erreur à afficher sur ce périmètre.")
    else:
        # Préparer les colonnes numériques
        evi_step = pd.to_numeric(err[EVI_MOMENT], errors="coerce").fillna(0).astype(int)
        evi_code = pd.to_numeric(err[EVI_CODE], errors="coerce").fillna(0).astype(int)
        ds_pc = pd.to_numeric(err[DS_PC], errors="coerce").fillna(0).astype(int)
        
        # Séparer EVI et Downstream
        sub_evi = err.loc[(ds_pc.eq(8192)) | ((ds_pc.eq(0)) & (evi_code.ne(0)))].copy()
        sub_evi["step_num"] = evi_step.loc[sub_evi.index]
        sub_evi["code_num"] = evi_code.loc[sub_evi.index]
        sub_evi["moment"] = sub_evi["step_num"].map(_map_moment)
        sub_evi["Site"] = err[SITE_COL].loc[sub_evi.index]
        
        sub_ds = err.loc[ds_pc.ne(0) & ds_pc.ne(8192)].copy()
        sub_ds["step_num"] = evi_step.loc[sub_ds.index]
        sub_ds["code_num"] = ds_pc.loc[sub_ds.index]
        sub_ds["moment"] = sub_ds["step_num"].map(_map_moment)
        sub_ds["Site"] = err[SITE_COL].loc[sub_ds.index]
        
        # Combiner EVI et Downstream
        evi_long = pd.concat([sub_evi, sub_ds], ignore_index=True)
        
        # Filtre moments si disponible
        if "moment_sel" in st.session_state and st.session_state.moment_sel:
            evi_long = evi_long[evi_long["moment"].isin(st.session_state.moment_sel)]
        
        site_options = [] if evi_long.empty else sorted(evi_long["Site"].dropna().unique())
        
        if not site_options:
            st.info("Aucune combinaison sur ce périmètre (après filtres).")
        else:
            default_sites = st.session_state.get("tab5_projection_sites", [])
            if not isinstance(default_sites, list):
                default_sites = [default_sites] if default_sites else []
            default_sites = [s for s in default_sites if s in site_options]
            if not default_sites and site_options:
                default_sites = [site_options[0]]
            
            # Préparer la grille complète moments/codes
            all_steps = sorted(evi_long["step_num"].dropna().unique().tolist()) if not evi_long.empty else []
            all_codes = sorted(evi_long["code_num"].dropna().unique().tolist()) if not evi_long.empty else []
            
            column_template = None
            if all_steps and all_codes:
                column_template = pd.MultiIndex.from_product(
                    [all_steps, all_codes],
                    names=["Moments (ligne 1)", "Codes (ligne 2)"]
                )
            
            selected_sites = st.multiselect(
                "Sites (projection) - Maximum 2 sites",
                options=site_options,
                default=default_sites,
                key="tab5_projection_sites",
                help="Sélectionnez jusqu'à 2 sites pour l'analyse de projection.",
            )
            
            # Validation : limiter à 2 sites maximum
            if len(selected_sites) > 2:
                st.error("⚠️ Vous ne pouvez sélectionner que 2 sites maximum.")
                selected_sites = selected_sites[:2]
            
            if not selected_sites:
                st.info("Veuillez sélectionner au moins un site.")
            else:
                for site in selected_sites:
                    st.markdown(f"### 📍 {site}")
                    
                    # Taux de réussite du site
                    site_sessions = sess[sess[SITE_COL] == site] if SITE_COL in sess.columns else pd.DataFrame()
                    if not site_sessions.empty:
                        total_site = len(site_sessions)
                        ok_site = int(site_sessions["is_ok"].sum()) if "is_ok" in site_sessions.columns else 0
                        taux_site = round(ok_site / total_site * 100, 1) if total_site else 0.0
                        st.markdown(
                            f"**Taux de réussite du site : {taux_site:.1f}%** "
                            f"({ok_site}/{total_site} charges réussies)"
                        )
                    else:
                        st.markdown("**Taux de réussite du site :** N/A")
                    
                    hide_zero = st.checkbox("Masquer colonnes vides (0)", key=f"hide_zeros_{site}")
                    
                    evi_site = evi_long[evi_long["Site"] == site].copy()
                    
                    if "PDC" in evi_site.columns:
                        # Avec PDC
                        g_pdc = evi_site.groupby(["PDC", "step_num", "code_num"]).size().rename("Nb").reset_index()
                        g_tot = evi_site.groupby(["step_num", "code_num"]).size().rename("Nb").reset_index()
                        g_tot["PDC"] = "__TOTAL__"
                        full = pd.concat([g_tot, g_pdc], ignore_index=True)
                        
                        pv = full.pivot_table(
                            index="PDC",
                            columns=["step_num", "code_num"],
                            values="Nb",
                            fill_value=0,
                            aggfunc="sum",
                        )
                        
                        if column_template is not None:
                            pv = pv.reindex(columns=column_template, fill_value=0)
                        
                        pdcs = sorted(pv.index.tolist(), key=str)
                        if "__TOTAL__" in pdcs:
                            pdcs.remove("__TOTAL__")
                            pdcs = ["__TOTAL__"] + pdcs
                        pv = pv.reindex(pdcs)
                        
                        df_disp = pv.reset_index()
                        df_disp["Site / PDC"] = np.where(
                            df_disp["PDC"].eq("__TOTAL__"),
                            f"{site} (TOTAL)",
                            "   " + df_disp["PDC"].astype(str)
                        )
                        df_disp = df_disp.drop(columns=["PDC"])
                    else:
                        # Sans PDC
                        g_site = evi_site.groupby(["step_num", "code_num"]).size().rename("Nb").reset_index()
                        pv = g_site.pivot_table(
                            index=pd.Index([site], name="Site"),
                            columns=["step_num", "code_num"],
                            values="Nb",
                            fill_value=0,
                            aggfunc="sum",
                        )
                        
                        if column_template is not None:
                            pv = pv.reindex(columns=column_template, fill_value=0)
                        
                        df_disp = pv.reset_index()
                        df_disp["Site / PDC"] = f"{site} (TOTAL)"
                    
                    # Réorganisation colonnes
                    cols = df_disp.columns
                    disp_col = ("Site / PDC", "") if isinstance(cols, pd.MultiIndex) and ("Site / PDC", "") in cols else "Site / PDC"
                    if isinstance(cols, pd.MultiIndex):
                        value_cols = [c for c in cols if c != disp_col]
                        df_disp = df_disp.loc[:, [disp_col] + value_cols]
                    else:
                        value_cols = [c for c in cols if c != "Site / PDC"]
                        df_disp = df_disp[["Site / PDC"] + value_cols]
                    
                    # Calcul total par ligne
                    _total_col = ("∑", "Total") if (len(value_cols) and isinstance(value_cols[0], tuple)) else "∑ Total"
                    _total_pct_col = ("∑", "%") if isinstance(_total_col, tuple) else "∑ %"
                    _numeric_all = df_disp[value_cols].apply(pd.to_numeric, errors="coerce").fillna(0)
                    df_disp[_total_col] = _numeric_all.sum(axis=1)
                    
                    # Masquage colonnes vides
                    if hide_zero:
                        col_sums = _numeric_all.sum(axis=0)
                        value_cols = [c for c in value_cols if col_sums[c] > 0]
                    
                    # Calcul du pourcentage (sans ligne TOTAL GÉNÉRAL)
                    total_general_value = int(_numeric_all.sum().sum())
                    
                    if total_general_value:
                        df_disp[_total_pct_col] = (df_disp[_total_col] / total_general_value * 100).round(1)
                    else:
                        df_disp[_total_pct_col] = 0.0
                    
                    final_cols = [disp_col] + value_cols + [_total_col, _total_pct_col]
                    
                    def _cell_color(v):
                        try:
                            x = float(v)
                        except:
                            return ""
                        if x == 0: return "background-color: #ffffff;"
                        elif x <= 2: return "background-color: #E8F1FB;"
                        elif x <= 6: return "background-color: #CFE3F7;"
                        elif x <= 15: return "background-color: #A9CFF2;"
                        elif x <= 25: return "background-color: #7DB5EA;"
                        elif x <= 50: return "background-color: #4F97D9; color: white;"
                        elif x <= 100: return "background-color: #2F6FB7; color: white;"
                        else: return "background-color: #1F4F8F; color: white;"
                    
                    styled = (
                        df_disp[final_cols]
                        .style
                        .applymap(_cell_color, subset=value_cols)
                        .format(precision=0, na_rep="")
                        .format({_total_pct_col: "{:.1f}%"})
                        .set_table_styles([
                            {"selector": "th.col_heading.level0", "props": [("text-align", "center")]},
                            {"selector": "th.col_heading.level1", "props": [("text-align", "center")]},
                        ])
                    )
                    st.dataframe(styled, use_container_width=True, hide_index=True)
    
    st.markdown(\"\"\"
    **Légende (occurrences)**  
    <span style="display:inline-block;width:14px;height:14px;background:#ffffff;border:1px solid #ddd;"></span> 0  
    <span style="display:inline-block;width:14px;height:14px;background:#E8F1FB;"></span> 0–2  
    <span style="display:inline-block;width:14px;height:14px;background:#CFE3F7;"></span> 2–6  
    <span style="display:inline-block;width:14px;height:14px;background:#A9CFF2;"></span> 6–15  
    <span style="display:inline-block;width:14px;height:14px;background:#7DB5EA;"></span> 15–25  
    <span style="display:inline-block;width:14px;height:14px;background:#4F97D9;"></span> 25–50  
    <span style="display:inline-block;width:14px;height:14px;background:#2F6FB7;"></span> 50–100  
    <span style="display:inline-block;width:14px;height:14px;background:#1F4F8F;"></span> >100
    <style>
    .stDataFrame table thead th:first-child {min-width: 220px !important;}
    </style>
    \"\"\", unsafe_allow_html=True)
"""

def render():
    ctx = get_context()
    globals_dict = {"np": np, "pd": pd, "px": px, "go": go, "st": st}
    local_vars = dict(ctx.__dict__)
    local_vars.setdefault('plot', getattr(ctx, 'plot', None))
    local_vars.setdefault('hide_zero_labels', getattr(ctx, 'hide_zero_labels', None))
    local_vars.setdefault('with_charge_link', getattr(ctx, 'with_charge_link', None))
    local_vars.setdefault('evi_counts_pivot', getattr(ctx, 'evi_counts_pivot', None))
    local_vars = {k: v for k, v in local_vars.items() if v is not None}
    exec(TAB_CODE, globals_dict, local_vars)