from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, avg, count, round,
    to_timestamp, from_unixtime, sum as spark_sum
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    FloatType, IntegerType, LongType
)

# ── Sesión Spark ──────────────────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("CalidadAire_Streaming") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# ── Esquema del mensaje Kafka ─────────────────────────────────────────────────
schema = StructType([
    StructField("estacion",     StringType()),
    StructField("departamento", StringType()),
    StructField("variable",     StringType()),
    StructField("promedio",     FloatType()),
    StructField("excedencias",  IntegerType()),
    StructField("año",          IntegerType()),
    StructField("timestamp",    LongType())
])

# ── Leer desde Kafka ──────────────────────────────────────────────────────────
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "calidad_aire_stream") \
    .load()

# ── Parsear JSON ──────────────────────────────────────────────────────────────
parsed_df = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# Convertir timestamp Unix a timestamp Spark
parsed_df = parsed_df.withColumn(
    "event_time",
    to_timestamp(from_unixtime(col("timestamp")))
)

# ── Análisis 1: Promedio de contaminación por departamento y variable ─────────
stats_depto = parsed_df \
    .groupBy(
        window(col("event_time"), "1 minute"),
        "departamento",
        "variable"
    ) \
    .agg(
        round(avg("promedio"), 2).alias("avg_contaminacion"),
        count("*").alias("num_registros")
    )

# ── Análisis 2: Total de excedencias por departamento ────────────────────────
excedencias_depto = parsed_df \
    .groupBy(
        window(col("event_time"), "1 minute"),
        "departamento"
    ) \
    .agg(
        spark_sum("excedencias").alias("total_excedencias"),
        count("*").alias("num_registros")
    )

# ── Escribir resultados en consola ────────────────────────────────────────────
query1 = stats_depto \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .queryName("ContaminacionPorDepartamento") \
    .start()

query2 = excedencias_depto \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .queryName("ExcedenciasPorDepartamento") \
    .start()

query1.awaitTermination()
