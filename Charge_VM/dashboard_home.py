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
