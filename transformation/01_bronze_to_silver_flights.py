# Cell 1 — Configure storage access
storage_account_name = "stlogisticsplatform"
storage_account_key = "your_account_key"  # Load from Key Vault

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key
)

bronze_path = f"abfss://bronze@{storage_account_name}.dfs.core.windows.net/tracking/api/"
silver_path = f"abfss://silver@{storage_account_name}.dfs.core.windows.net/flights/"

print("Storage access configured")
print(f"Bronze path: {bronze_path}")
print(f"Silver path: {silver_path}")


# Cell 2 — Read raw flight JSON from Bronze
file_path = "abfss://bronze@stlogisticsplatform.dfs.core.windows.net/tracking/api/2026/08/12/10-25-50.json"

# Read full file with 3MB limit
raw_content = dbutils.fs.head(file_path, 3000000)

import json
data = json.loads(raw_content)

states = data.get("states", [])
timestamp = data.get("time", 0)

print(f"Snapshot timestamp: {timestamp}")
print(f"Total aircraft: {len(states)}")
print(f"Sample record: {states[0]}")


# Cell 3 — Convert to DataFrame with proper column names
from pyspark.sql import Row
from datetime import datetime, timezone

# Map array positions to meaningful column names
def map_state_to_row(state):
    return Row(
        icao24=state[0],
        callsign=str(state[1]).strip() if state[1] else None,
        origin_country=state[2],
        time_position=state[3],
        last_contact=state[4],
        longitude=float(state[5]) if state[5] else None,
        latitude=float(state[6]) if state[6] else None,
        baro_altitude=float(state[7]) if state[7] else None,
        on_ground=state[8],
        velocity=float(state[9]) if state[9] else None,
        true_track=float(state[10]) if state[10] else None,
        vertical_rate=float(state[11]) if state[11] else None,
        geo_altitude=float(state[13]) if state[13] else None,
        squawk=state[14],
        snapshot_timestamp=timestamp
    )

rows = [map_state_to_row(s) for s in states]
df = spark.createDataFrame(rows)

print(f"DataFrame created with {df.count()} rows and {len(df.columns)} columns")
df.show(5)


# Cell 4 — Clean and validate data (Bronze → Silver)
from pyspark.sql.functions import col, from_unixtime, when, round

df_silver = df \
    .filter(col("icao24").isNotNull()) \
    .filter(col("latitude").isNotNull()) \
    .filter(col("longitude").isNotNull()) \
    .withColumn("last_contact_utc", from_unixtime(col("last_contact"))) \
    .withColumn("snapshot_utc", from_unixtime(col("snapshot_timestamp"))) \
    .withColumn("velocity_kmh", round(col("velocity") * 3.6, 2)) \
    .withColumn("altitude_ft", round(col("baro_altitude") * 3.28084, 2)) \
    .withColumn("flight_status", 
        when(col("on_ground") == True, "Landed")
        .when(col("velocity") < 50, "Holding")
        .otherwise("In Flight")
    ) \
    .drop("time_position", "snapshot_timestamp", "baro_altitude", "velocity")

print(f"Silver records: {df_silver.count()}")
print(f"Removed {8148 - df_silver.count()} invalid records")
df_silver.show(5)


# Cell 5 — Write Silver data to ADLS
df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .save(silver_path)

print(f"Successfully written {df_silver.count()} records to Silver layer")
print(f"Location: {silver_path}")


# Cell 6 — Verify Silver layer
df_verify = spark.read.format("delta").load(silver_path)
print(f"Records in Silver: {df_verify.count()}")
print(f"Columns: {df_verify.columns}")
df_verify.show(3)