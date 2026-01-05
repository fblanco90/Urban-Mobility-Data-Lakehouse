# @task
#     def audit_dimensions():
#         logging.info("🕵️ Starting Data Quality Audit for Dimensions.")

#         # Helper to insert into log
#         def log_metric(table, metric, value, notes=''):
#             safe_val = value if value is not None else 0.0
#             query = f"""
#                 INSERT INTO lakehouse.silver.data_quality_log 
#                 VALUES (CURRENT_TIMESTAMP, '{table}', '{metric}', {safe_val}, '{notes}')
#             """
#             con.execute(query)
#             logging.info(f"   -> Audited {table}: {metric} = {safe_val}")

#         with get_connection() as con:
#             # 1. Zone Checks
#             missing_ine = con.execute("SELECT COUNT(*) FROM lakehouse.silver.dim_zones WHERE ine_code IS NULL").fetchone()[0]
#             log_metric('dim_zones', 'zones_missing_ine_code', missing_ine)

#             missing_geo = con.execute("SELECT COUNT(*) FROM lakehouse.silver.dim_zones WHERE polygon IS NULL").fetchone()[0]
#             log_metric('dim_zones', 'zones_missing_geo_coords', missing_geo)
            
#             zone_count = con.execute("SELECT COUNT(*) FROM lakehouse.silver.dim_zones").fetchone()[0]
#             log_metric('dim_zones', 'total_zones', zone_count)

#             # 2. Population Checks
#             pop_sum = con.execute("SELECT SUM(population) FROM lakehouse.silver.metric_population").fetchone()[0]
#             log_metric('metric_population', 'total_population_sum', pop_sum, 'Spain Total')

#             avg_rent = con.execute("SELECT AVG(income_per_capita) FROM lakehouse.silver.metric_ine_rent").fetchone()[0]
#             log_metric('metric_ine_rent', 'avg_income_per_capita', avg_rent, 'National Avg')
            
#             rent_coverage = con.execute("""
#                 SELECT (SELECT COUNT(DISTINCT zone_id) FROM lakehouse.silver.metric_ine_rent) * 100.0 / NULLIF((SELECT COUNT(*) FROM lakehouse.silver.dim_zones), 0)
#             """).fetchone()[0]
#             log_metric('metric_ine_rent', 'income_data_coverage_pct', rent_coverage)
        
#         logging.info("🕵️ Dimensions audited.")



# @task
#     def audit_batch_results(**context):
#         logging.info("🕵️ Starting Batch Quality Audit...")
        
#         # 1. Get Range from Params
#         conf = context['dag_run'].conf or {}
#         params = context['params']
#         start_str = conf.get('start_date') or params['start_date']
#         end_str = conf.get('end_date') or params['end_date']
        
#         logging.info(f"Auditing range: {start_str} to {end_str}")

#         with get_connection() as con:
#             # 2. Calculate Aggregates for the whole batch
#             # We query the Silver table for the date range
#             stats = con.execute(f"""
#                 SELECT 
#                     COUNT(*) as total_rows,
#                     SUM(trips) as total_trips,
#                     COUNT(DISTINCT partition_date) as days_loaded,
#                     -- Check for NULL zones (Data Quality Indicator)
#                     COUNT(*) FILTER (WHERE origin_zone_id IS NULL OR destination_zone_id IS NULL) as bad_rows
#                 FROM lakehouse.silver.fact_mobility
#                 WHERE partition_date BETWEEN strptime('{start_str}', '%Y%m%d') AND strptime('{end_str}', '%Y%m%d')
#             """).fetchone()
            
#             total_rows = stats[0] or 0
#             total_trips = stats[1] or 0.0
#             days_loaded = stats[2] or 0
#             bad_rows = stats[3] or 0
            
#             # 3. Log to Data Quality Table
#             batch_note = f"Batch: {start_str}-{end_str}"
            
#             # Helper to insert
#             def log_dq(metric, val):
#                 con.execute(f"INSERT INTO lakehouse.silver.data_quality_log VALUES (CURRENT_TIMESTAMP, 'fact_mobility_batch', '{metric}', {val or 0}, '{batch_note}')")

#             log_dq('batch_total_rows', total_rows)
#             log_dq('batch_total_trips', total_trips)
#             log_dq('batch_days_loaded', days_loaded)
            
#             # Calculate % of bad rows
#             bad_pct = (bad_rows / total_rows * 100.0) if total_rows > 0 else 0.0
#             log_dq('batch_bad_rows_pct', bad_pct)

#         logging.info(f"📊 Audit Complete. Loaded {days_loaded} days. Trips: {total_trips:,.0f}. Bad Rows: {bad_pct:.2f}%")
