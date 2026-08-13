# Databricks notebook source
# Cell 1 — Configure storage access
storage_account_name = "stlogisticsplatform"
storage_account_key = "your_account_key"  # Load from Key Vault

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key
)

silver_shipments_path = f"abfss://silver@{storage_account_name}.dfs.core.windows.net/shipments/"
silver_flights_path = f"abfss://silver@{storage_account_name}.dfs.core.windows.net/flights/"
gold_path = f"abfss://gold@{storage_account_name}.dfs.core.windows.net/"

print("Storage access configured")
print(f"Silver shipments: {silver_shipments_path}")
print(f"Silver flights: {silver_flights_path}")
print(f"Gold path: {gold_path}")

# Cell 2 — Load Silver data
df_shipments = spark.read.format("delta").load(silver_shipments_path)
df_flights = spark.read.format("delta").load(silver_flights_path)

print(f"Shipments loaded: {df_shipments.count()}")
print(f"Flights loaded: {df_flights.count()}")

# Cell 3 — Gold KPI 1: On-Time Delivery Rate by Carrier
from pyspark.sql.functions import count, sum, round, col

df_otd_by_carrier = df_shipments \
    .groupBy("carrier") \
    .agg(
        count("shipment_id").alias("total_shipments"),
        sum(col("is_delayed").cast("int")).alias("delayed_shipments"),
        round(
            (count("shipment_id") - sum(col("is_delayed").cast("int"))) * 100.0 
            / count("shipment_id"), 2
        ).alias("on_time_rate_pct"),
        round(sum("delay_days") / count("shipment_id"), 2).alias("avg_delay_days"),
        round(sum("cost_usd"), 2).alias("total_cost_usd"),
        round(sum("cost_usd") / count("shipment_id"), 2).alias("avg_cost_per_shipment")
    ) \
    .orderBy("on_time_rate_pct", ascending=False)

print("=== On-Time Delivery Rate by Carrier ===")
df_otd_by_carrier.show()

# Cell 4 — Gold KPI 2: Delay Analysis by Route
df_delay_by_route = df_shipments \
    .groupBy("origin", "destination") \
    .agg(
        count("shipment_id").alias("total_shipments"),
        round(sum(col("is_delayed").cast("int")) * 100.0 / count("shipment_id"), 2).alias("delay_rate_pct"),
        round(sum("delay_days") / count("shipment_id"), 2).alias("avg_delay_days"),
        round(sum("cost_usd") / count("shipment_id"), 2).alias("avg_cost_usd")
    ) \
    .orderBy("delay_rate_pct", ascending=False)

print("=== Delay Analysis by Route ===")
df_delay_by_route.show()

# Cell 5 — Gold KPI 3: Flight Status Summary by Country
from pyspark.sql.functions import count, round, col

df_flight_summary = df_flights \
    .groupBy("origin_country", "flight_status") \
    .agg(
        count("icao24").alias("total_flights"),
        round(sum("velocity_kmh") / count("icao24"), 2).alias("avg_speed_kmh"),
        round(sum("altitude_ft") / count("icao24"), 2).alias("avg_altitude_ft")
    ) \
    .orderBy("total_flights", ascending=False)

print("=== Flight Status Summary by Country ===")
df_flight_summary.show(20)

# Cell 6 — Write Gold tables to ADLS
# KPI 1 — OTD by carrier
df_otd_by_carrier.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{gold_path}otd_by_carrier/")

# KPI 2 — Delay by route
df_delay_by_route.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{gold_path}delay_by_route/")

# KPI 3 — Flight summary
df_flight_summary.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{gold_path}flight_summary/")

print("All Gold tables written successfully")
print(f"- gold/otd_by_carrier/")
print(f"- gold/delay_by_route/")
print(f"- gold/flight_summary/")