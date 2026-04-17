from __future__ import annotations
import os
import streamlit as st

_HERE = os.path.dirname(os.path.abspath(__file__))

st.set_page_config(
    page_title="KPI Confidence – Berliner Verkehrsdetektion",
    page_icon="📊",
    layout="wide",
)

# -------------------
# TITLE
# -------------------

st.markdown(
"""
<h1 style='font-size:40px;font-weight:800'>
📊 KPI Confidence – Berliner Verkehrsdetektion
</h1>
<p style='color:#9CA3AF;font-size:18px'>
Analyse von Verkehrs-KPIs, Datenqualität und Sensorzuverlässigkeit
</p>
""",
unsafe_allow_html=True
)

st.divider()

# -------------------
# HERO IMAGE (Berlin Street)
# -------------------
col1, col2 = st.columns([1,2])
with col1:
    st.markdown("### 🔎 KPI Explorer")
    st.caption("Analyse von Traffic KPIs und Confidence Scores")


    st.markdown("### 📉 Stability & Confidence")
    st.caption(" Anomalien und Sensorstabilität")

with col2:
    st.image(os.path.join(_HERE, "images", "hamburg.jpg"), caption="Berlin Traffic")


st.divider()

# -------------------
# NAVIGATION
# -------------------

col1, col2, col3 = st.columns(3)

