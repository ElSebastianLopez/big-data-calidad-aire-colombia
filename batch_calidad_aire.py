from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, max, min, round, regexp_replace
from pyspark.sql.types import FloatType, IntegerType
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

# ── 1. Sesión Spark ───────────────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("CalidadAireColombia_Batch") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# ── 2. Cargar dataset ─────────────────────────────────────────────────────────
df = spark.read.csv(
    "calidad_aire.csv",
    header=True,
    inferSchema=True,
    sep=","
)
print("=== ESQUEMA ===")
df.printSchema()
print(f"Total registros: {df.count()}")

# ── 3. Limpieza ───────────────────────────────────────────────────────────────
df = df.withColumn("Promedio", regexp_replace(col("Promedio"), ",", "").cast(FloatType())) \
       .withColumn("Año", regexp_replace(col("Año"), ",", "").cast(IntegerType())) \
       .withColumn("Mediana", regexp_replace(col("Mediana"), ",", "").cast(FloatType()))

df_clean = df.filter(
    col("Promedio").isNotNull() &
    col("Año").isNotNull() &
    col("Nombre del Departamento").isNotNull() &
    col("Variable").isNotNull()
)
print(f"Registros tras limpieza: {df_clean.count()}")

# ── 4. EDA ────────────────────────────────────────────────────────────────────
print("\n=== VARIABLES MONITOREADAS ===")
df_clean.groupBy("Variable").count() \
    .orderBy("count", ascending=False).show()

print("\n=== PM2.5 POR DEPARTAMENTO ===")
pm25 = df_clean.filter(col("Variable") == "PM2.5") \
    .groupBy("Nombre del Departamento") \
    .agg(
        round(avg("Promedio"), 2).alias("Promedio_PM25"),
        count("*").alias("Mediciones"),
        round(max("Promedio"), 2).alias("Maximo")
    ).orderBy("Promedio_PM25", ascending=False)
pm25.show(20)

print("\n=== EVOLUCIÓN ANUAL PM2.5 ===")
pm25_anual = df_clean.filter(col("Variable") == "PM2.5") \
    .groupBy("Año") \
    .agg(round(avg("Promedio"), 2).alias("Promedio_PM25")) \
    .orderBy("Año")
pm25_anual.show()

print("\n=== TOP 10 ESTACIONES MÁS CONTAMINADAS ===")
df_clean.filter(col("Variable") == "PM2.5") \
    .groupBy("Estación", "Nombre del Departamento") \
    .agg(round(avg("Promedio"), 2).alias("Promedio_PM25")) \
    .orderBy("Promedio_PM25", ascending=False) \
    .show(10)

# ── 5. Visualizaciones ────────────────────────────────────────────────────────

# Gráfica 1: Top 10 departamentos con mayor PM2.5
pm25_pd = pm25.limit(10).toPandas()
plt.figure(figsize=(12, 6))
plt.bar(pm25_pd["Nombre del Departamento"], pm25_pd["Promedio_PM25"], color="tomato")
plt.title("Top 10 Departamentos con Mayor Concentración de PM2.5")
plt.xlabel("Departamento")
plt.ylabel("Promedio PM2.5 (μg/m³)")
plt.xticks(rotation=45, ha="right")
plt.tight_layout()
plt.savefig("grafica1_pm25_departamentos.png", dpi=150)
plt.close()
print("✓ grafica1_pm25_departamentos.png")

# Gráfica 2: Evolución anual PM2.5
pm25_anual_pd = pm25_anual.toPandas()
plt.figure(figsize=(10, 5))
plt.plot(pm25_anual_pd["Año"], pm25_anual_pd["Promedio_PM25"],
         marker="o", color="steelblue", linewidth=2)
plt.title("Evolución Anual del Promedio de PM2.5 en Colombia")
plt.xlabel("Año")
plt.ylabel("Promedio PM2.5 (μg/m³)")
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig("grafica2_evolucion_anual.png", dpi=150)
plt.close()
print("✓ grafica2_evolucion_anual.png")

# Gráfica 3: Variables más monitoreadas
vars_pd = df_clean.groupBy("Variable").count() \
    .orderBy("count", ascending=False).limit(10).toPandas()
plt.figure(figsize=(10, 5))
plt.bar(vars_pd["Variable"], vars_pd["count"], color="mediumseagreen")
plt.title("Variables Ambientales más Monitoreadas en Colombia")
plt.xlabel("Variable")
plt.ylabel("Número de Registros")
plt.xticks(rotation=45, ha="right")
plt.tight_layout()
plt.savefig("grafica3_variables.png", dpi=150)
plt.close()
print("✓ grafica3_variables.png")

print("\n=== BATCH COMPLETADO ===")
spark.stop()
