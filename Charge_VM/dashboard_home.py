import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import create_engine
import datetime

def render_dashboard(context):
    sess_kpi = context.sess_kpi
    total = context.total
    ok = context.ok
    nok = context.nok
    taux_reussite = context.taux_reussite
    taux_echec = context.taux_echec

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.markdown(f"""
        <div style='padding: 20px; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); border-radius: 10px; text-align: center;'>
            <h3 style='color: white; margin: 0;'>{total}</h3>
            <p style='color: white; margin: 5px 0 0 0;'>Total Sessions</p>
        </div>
        """, unsafe_allow_html=True)

    with col2:
        st.markdown(f"""
        <div style='padding: 20px; background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%); border-radius: 10px; text-align: center;'>
            <h3 style='color: white; margin: 0;'>{taux_reussite:.1f}%</h3>
            <p style='color: white; margin: 5px 0 0 0;'>Taux de Réussite</p>
        </div>
        """, unsafe_allow_html=True)

    with col3:
        st.markdown(f"""
        <div style='padding: 20px; background: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%); border-radius: 10px; text-align: center;'>
            <h3 style='color: white; margin: 0;'>{ok}</h3>
            <p style='color: white; margin: 5px 0 0 0;'>Sessions Réussies</p>
        </div>
        """, unsafe_allow_html=True)

    with col4:
        st.markdown(f"""
        <div style='padding: 20px; background: linear-gradient(135deg, #fa709a 0%, #fee140 100%); border-radius: 10px; text-align: center;'>
            <h3 style='color: white; margin: 0;'>{nok}</h3>
            <p style='color: white; margin: 5px 0 0 0;'>Sessions en Échec</p>
        </div>
        """, unsafe_allow_html=True)
