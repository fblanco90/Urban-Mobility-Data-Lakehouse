from airflow.decorators import dag, task
from airflow.models.param import Param
from pendulum import datetime
from utils_db import get_connection
import logging

# --- CONFIGURATION ---
DEFAULT_POLYGON = "POLYGON((715000 4365000, 735000 4365000, 735000 4385000, 715000 4385000, 715000 4365000))"

@dag(
    dag_id="bq3_functional_classification",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    params={
        "start_date": Param("20230101", type="string", description="YYYY-MM-DD"),
        "end_date": Param("20231231", type="string", description="YYYY-MM-DD"),
        "polygon_wkt": Param(DEFAULT_POLYGON, type="string", title="Spatial Filter (WKT)", description="Paste your WKT Polygon here.")
    },
    tags=['mobility', 'gold', 'visualization', 'reporting']
)
def bq3_functional_classification_dag():

    @task
    def classify_zone_functions(**context):
        """
        Task: Functional Classification of Zones
        Filtra por polígono y clasifica según flujos de entrada/salida.
        """
        params = context['params']
        input_wkt = params['polygon_wkt']
        
        logging.info(f"--- 🏷️ Filtrando por polígono: {input_wkt[:50]}... ---")

        with get_connection() as con:
            # La consulta utiliza un INNER JOIN con las zonas filtradas por el polígono
            # para reducir el volumen de datos de movilidad procesados.
            con.execute(f"""
                CREATE OR REPLACE TABLE lakehouse.gold.zone_functional_classification AS
                WITH 
                -- 1. Identificar zonas dentro del polígono
                selected_zones AS (
                    SELECT zone_id, zone_name
                    FROM lakehouse.silver.dim_zones
                    WHERE ST_Intersects(polygon, ST_GeomFromText('{input_wkt}'))
                ),

                -- 2. Filtrar flujos de movilidad solo para esas zonas
                flat_flows AS (
                    SELECT f.origin_zone_id as zone_id, f.trips as outflow, 0 as inflow, 0 as internal
                    FROM lakehouse.silver.fact_mobility f
                    INNER JOIN selected_zones sz ON f.origin_zone_id = sz.zone_id
                    WHERE f.origin_zone_id != f.destination_zone_id
                    
                    UNION ALL
                    
                    SELECT f.destination_zone_id as zone_id, 0 as outflow, f.trips as inflow, 0 as internal
                    FROM lakehouse.silver.fact_mobility f
                    INNER JOIN selected_zones sz ON f.destination_zone_id = sz.zone_id
                    WHERE f.origin_zone_id != f.destination_zone_id
                    
                    UNION ALL
                    
                    SELECT f.origin_zone_id as zone_id, 0 as outflow, 0 as inflow, f.trips as internal
                    FROM lakehouse.silver.fact_mobility f
                    INNER JOIN selected_zones sz ON f.origin_zone_id = sz.zone_id
                    WHERE f.origin_zone_id = f.destination_zone_id
                ),

                -- 3. Agregación de estadísticas por zona
                zone_flow_stats AS (
                    SELECT 
                        zone_id,
                        SUM(internal) as internal_trips,
                        SUM(outflow) as outflow,
                        SUM(inflow) as inflow
                    FROM flat_flows
                    GROUP BY 1
                )

                -- 4. Cálculo de métricas y etiquetas finales
                SELECT 
                    m.zone_id,
                    sz.zone_name,
                    m.internal_trips,
                    m.outflow,
                    m.inflow,
                    
                    -- Net Flow Ratio: (In - Out) / (In + Out)
                    CASE 
                        WHEN (m.inflow + m.outflow) = 0 THEN 0 
                        ELSE (1.0 * m.inflow - m.outflow) / (m.inflow + m.outflow) 
                    END AS net_flow_ratio,

                    -- Retention Rate: Internal / (Out + Internal)
                    CASE 
                        WHEN (m.outflow + m.internal_trips) = 0 THEN 0
                        ELSE (1.0 * m.internal_trips) / (m.outflow + m.internal_trips)
                    END AS retention_rate, 

                    -- Clasificación Final basada en los ratios calculados
                    CASE 
                        WHEN ((1.0 * m.internal_trips) / NULLIF(m.outflow + m.internal_trips, 0)) > 0.6 
                            THEN 'Self-Sustaining Cell'
                        WHEN ((1.0 * m.inflow - m.outflow) / NULLIF(m.inflow + m.outflow, 0)) > 0.15 
                            THEN 'Activity Hub (Importer)'
                        WHEN ((1.0 * m.inflow - m.outflow) / NULLIF(m.inflow + m.outflow, 0)) < -0.15 
                            THEN 'Bedroom Community (Exporter)'
                        ELSE 'Balanced / Transit Zone'
                    END AS functional_label
                        
                FROM zone_flow_stats m
                JOIN selected_zones sz ON m.zone_id = sz.zone_id
                WHERE (m.internal_trips + m.inflow + m.outflow) > 0;
            """)

        logging.info("✅ Gold: Tabla de clasificación funcional actualizada correctamente.")

    # --- DAG FLOW ---
    classify_zone_functions()

bq3_functional_classification_dag()