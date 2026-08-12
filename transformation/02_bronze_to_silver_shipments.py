# Cell 1 — Configure storage access
storage_account_name = "stlogisticsplatform"
storage_account_key = "your_account_key"  # Load from Key Vault

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key
)

bronze_path = f"abfss://bronze@{storage_account_name}.dfs.core.windows.net/shipments/batch/"
silver_path = f"abfss://silver@{storage_account_name}.dfs.core.windows.net/shipments/"

print("Storage access configured")
print(f"Bronze path: {bronze_path}")
print(f"Silver path: {silver_path}")


# Cell 2 — Read raw shipments CSV from Bronze
file_path = "abfss://bronze@stlogisticsplatform.dfs.core.windows.net/shipments/batch/2026/08/12/shipments.csv"

df_raw = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(file_path)

print(f"Total records loaded: {df_raw.count()}")
print(f"Columns: {df_raw.columns}")
df_raw.printSchema()
df_raw.show(5)


# Cell 3 — Clean and validate shipments (Bronze → Silver)
from pyspark.sql.functions import col, when, datediff, lit

df_silver = df_raw \
    .filter(col("shipment_id").isNotNull()) \
    .filter(col("carrier").isNotNull()) \
    .filter(col("planned_delivery").isNotNull()) \
    .filter(col("actual_delivery").isNotNull()) \
    .withColumn("transit_days",
        datediff(col("actual_delivery"), col("planned_delivery"))
    ) \
    .withColumn("is_delayed",
        when(col("actual_delivery") > col("planned_delivery"), True)
        .otherwise(False)
    ) \
    .withColumn("delay_days",
        when(col("actual_delivery") > col("planned_delivery"),
            datediff(col("actual_delivery"), col("planned_delivery")))
        .otherwise(0)
    ) \
    .withColumn("cost_category",
        when(col("cost_usd") < 500, "Low")
        .when(col("cost_usd") < 2000, "Medium")
        .otherwise("High")
    )

print(f"Silver records: {df_silver.count()}")
print(f"Delayed shipments: {df_silver.filter(col('is_delayed') == True).count()}")
print(f"On-time shipments: {df_silver.filter(col('is_delayed') == False).count()}")
df_silver.show(5)


# Cell 4 — Write to Silver layer
df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .save(silver_path)

print(f"Successfully written {df_silver.count()} records to Silver layer")
print(f"Location: {silver_path}")