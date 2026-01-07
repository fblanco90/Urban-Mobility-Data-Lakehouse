import streamlit as st
import os
import streamlit.components.v1 as components
from pathlib import Path

# --- CONFIGURATION ---
BASE_DIR = Path(__file__).parents[1] / "airflow" / "include" / "results"
AIRFLOW_URL = "http://localhost:8080/dags" 

st.set_page_config(
    page_title="Mobility Lakehouse Analytics",
    page_icon="🚗",
    layout="wide"
)

# --- HELPER FUNCTIONS ---
def load_html(file_path):
    if os.path.exists(file_path):
        with open(file_path, 'r', encoding='utf-8') as f:
            return f.read()
    return None

def render_html(html_content, height=700):
    if html_content:
        components.html(html_content, height=height, scrolling=True)
    else:
        st.error("Visualization file not found. Please run the Airflow DAG first.")

def render_metadata(bq_folder):
    """
    Looks for the generated Markdown report in the BQ folder 
    and displays it in an expander.
    """
    folder_path = BASE_DIR / bq_folder
    if folder_path.exists():
        # Find any .md files in the folder
        md_files = list(folder_path.glob("*.md"))
        if md_files:
            # We take the first one (usually the only one)
            with open(md_files[0], "r", encoding="utf-8") as f:
                content = f.read()
                with st.expander("📋 View Execution Metadata (Dates, Polygon, etc.)"):
                    st.markdown(content)
        else:
            st.caption("No execution metadata file found in this folder.")

# --- SIDEBAR NAVIGATION ---
with st.sidebar:
    st.header("Mobility Lakehouse 💿", 
              help="This dashboard visualizes Gold Layer outputs processed via Airflow & DuckDB.")
    
    st.markdown("---")
    
    selection = st.radio(
        "Select Business Question",
        ["BQ1: Mobility Patterns", "BQ2: Infrastructure Gaps", "BQ3: Functional Classification"]
    )

    st.markdown("---")
    st.link_button("⚙️ Open Airflow", AIRFLOW_URL, use_container_width=True)

# --- MAIN CONTENT ---

# --- BQ1: TYPICAL MOBILITY PATTERNS ---
if selection == "BQ1: Mobility Patterns":
    st.header("🏆 BQ1: Typical Daily Mobility Patterns")
    st.info("Goal: Identify typical hourly profiles and OD flows using K-Means clustering.")
    
    # Render Metadata from Airflow Report
    render_metadata("bq1")

    tab1, tab2 = st.tabs(["📊 Hourly Profiles", "🗺️ Interactive OD Matrix"])

    with tab1:
        st.subheader("Clustered Mobility Profiles")
        img_path = BASE_DIR / "bq1" / "mobility_report.png"
        if img_path.exists():
            st.image(str(img_path), caption="Mean hourly trips per cluster identified via K-Means.")
        else:
            st.warning("PNG report not found. Please run the Airflow DAG first.")

    with tab2:
        st.subheader("Origin-Destination Heatmap")
        st.write("Use the dropdown inside the map to switch between clusters.")
        html_content = load_html(BASE_DIR / "bq1" / "interactive_heatmap.html")
        render_html(html_content, height=850)


# --- BQ2: INFRASTRUCTURE GAPS ---
elif selection == "BQ2: Infrastructure Gaps":
    st.header("⚖️ BQ2: Infrastructure Gaps & Mobility Potential")
    st.info("Goal: Identify areas where current mobility flows do not match potential based on Gravity Models.")
    
    # Render Metadata from Airflow Report
    render_metadata("bq2")

    tab1, tab2 = st.tabs(["📍 Service Level Ranking", "🌉 Mobility Gaps (Arc Map)"])

    with tab1:
        st.subheader("Zone Service Level")
        st.markdown("""
        - **Red:** Service level below potential (Gap).
        - **Green:** Service level meets/exceeds potential.
        - **Size:** Economic importance ($Population \cdot Rent$).
        """)
        html_content = load_html(BASE_DIR / "bq2" / "ranking_service_map.html")
        render_html(html_content)

    with tab2:
        st.subheader("Inter-urban Mobility Gaps")
        st.markdown("- **Arc Color:** Mismatch ratio (Red = Under-served flow).")
        html_content = load_html(BASE_DIR / "bq2" / "mobility_gaps_map.html")
        render_html(html_content)


# --- BQ3: FUNCTIONAL CLASSIFICATION ---
elif selection == "BQ3: Functional Classification":
    st.header("🏗️ BQ3: Functional Zone Classification")
    st.info("Goal: Classify zones based on net mobility flows (Importers vs Exporters) and retention.")
    
    # Render Metadata from Airflow Report
    render_metadata("bq3")

    st.subheader("Functional Landscape Map")
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Hubs", "🔵 Blue", "Activity Importers")
    col2.metric("Bedroom", "🟠 Orange", "Activity Exporters")
    col3.metric("Self-Sustaining", "🟢 Green", "High Retention")
    col4.metric("Balanced", "⚪ Grey", "Mixed Use")

    st.markdown("---")
    
    html_content = load_html(BASE_DIR / "bq3" / "functional_classification_map.html")
    if html_content:
        st.write("The black bubbles represent **Total Activity** (Inflow + Outflow + Internal).")
        render_html(html_content, height=800)
    else:
        st.error("BQ3 map not found. Ensure the BQ3 Airflow DAG has finished.")

# --- FOOTER ---
st.markdown("---")
st.caption("Data source: MITMA / INE Mobility Project | Engine: DuckDB + Streamlit")
