import os
import logging
import pandas as pd
from airflow.decorators import dag, task
from pendulum import datetime
from utils_db import get_connection

@dag(
    dag_id="utils_export_mermaid_to_file",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=['utility', 'metadata'],
)
def export_mermaid_dag():

    @task
    def generate_and_save_mermaid():
        # 1. Get Connection
        con = get_connection()
        
        try:
            # 2. Extract Metadata
            query = """
            SELECT 
                table_schema, 
                table_name, 
                column_name, 
                data_type
            FROM information_schema.columns 
            WHERE table_catalog = 'lakehouse' 
              AND table_schema IN ('bronze', 'silver', 'gold')
            ORDER BY table_schema, table_name, ordinal_position;
            """
            df = con.execute(query).df()
            
            if df.empty:
                logging.error("No tables found.")
                return

            # 3. Build Mermaid Syntax
            mermaid = ["erDiagram"]
            tables = df.groupby(['table_schema', 'table_name'])
            
            for (schema, name), columns in tables:
                table_id = f"{schema}_{name}"
                mermaid.append(f"    {table_id} {{")
                for _, col in columns.iterrows():
                    dtype = col['data_type'].replace(" ", "_").lower()
                    cname = col['column_name']
                    mermaid.append(f"        {dtype} {cname}")
                mermaid.append("    }")

            # 4. Inferred Relationships
            mermaid.append("\n    %% Relationships")
            for (schema, name), columns in tables:
                table_id = f"{schema}_{name}"
                cols = columns['column_name'].tolist()
                if table_id != "silver_dim_zones":
                    if "zone_id" in cols:
                        mermaid.append(f"    silver_dim_zones ||--o{{ {table_id} : \"zone_id\"")
                    if "origin_zone_id" in cols or "org_zone_id" in cols:
                        mermaid.append(f"    silver_dim_zones ||--o{{ {table_id} : \"origin\"")
                    if "destination_zone_id" in cols or "dest_zone_id" in cols:
                        mermaid.append(f"    silver_dim_zones ||--o{{ {table_id} : \"destination\"")
                if "cluster_id" in cols and table_id != "gold_dim_cluster_assignments":
                    mermaid.append(f"    gold_dim_cluster_assignments ||--o{{ {table_id} : \"cluster_id\"")

            final_code = "\n".join(mermaid)

            # 5. Save to Local File
            # In Astro/Docker, /usr/local/airflow is the project root.
            # Saving to 'include/' makes it easy to find on your host machine.
            output_path = "/usr/local/airflow/include/lakehouse_schema.mermaid"
            
            # Ensure directory exists (if not standard include)
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            with open(output_path, "w") as f:
                f.write(final_code)
            
            logging.info(f"✅ Mermaid code saved to: {output_path}")
            print(f"FILE CREATED: {output_path}")

        finally:
            con.close()

    generate_and_save_mermaid()

export_mermaid_dag()