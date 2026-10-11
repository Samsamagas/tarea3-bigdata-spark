from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, round, max, min, stddev, col, when

# Crear la sesión de Spark
spark = SparkSession.builder \
    .appName("Tarea3_AnalisisBatch") \
    .master("spark://bigdata:7077") \
    .getOrCreate()

# ===== CARGA DEL DATASET =====
df = spark.read.csv(
    "hdfs://localhost:9000/tarea_bigdata/combustibles/precios_combustibles.csv",
    header=True,
    inferSchema=True
)

print("===== ESQUEMA ORIGINAL =====")
df.printSchema()
print("Total de registros originales:", df.count())

# ===== LIMPIEZA DE DATOS =====
df_limpio = df.dropDuplicates()
df_limpio = df_limpio.dropna(subset=["precio", "producto", "departamentonombre"])
df_limpio = df_limpio.filter(col("precio") > 0)

print("===== DESPUES DE LIMPIEZA =====")
print("Total de registros despues de limpieza:", df_limpio.count())

# ===== TRANSFORMACION: Clasificar precios en categorias =====
df_transformado = df_limpio.withColumn(
    "categoria_precio",
    when(col("precio") < 7000, "Bajo")
    .when((col("precio") >= 7000) & (col("precio") < 10000), "Medio")
    .otherwise("Alto")
)

print("===== MUESTRA DE DATOS TRANSFORMADOS =====")
df_transformado.select("producto", "departamentonombre", "precio", "categoria_precio").show(10)

# ===== ANALISIS EXPLORATORIO DE DATOS (EDA) =====
print("===== ESTADISTICAS GENERALES DEL PRECIO =====")
df_transformado.select(
    round(avg("precio"), 2).alias("precio_promedio"),
    min("precio").alias("precio_minimo"),
    max("precio").alias("precio_maximo"),
    round(stddev("precio"), 2).alias("desviacion_estandar")
).show()

print("===== CONTEO POR CATEGORIA DE PRECIO =====")
df_transformado.groupBy("categoria_precio").count().orderBy("count", ascending=False).show()

print("===== PRECIO PROMEDIO POR TIPO DE PRODUCTO =====")
df_transformado.groupBy("producto") \
  .agg(round(avg("precio"), 2).alias("precio_promedio"), count("*").alias("registros")) \
  .orderBy("precio_promedio", ascending=False) \
  .show(20, truncate=False)

print("===== PRECIO PROMEDIO POR DEPARTAMENTO (TOP 10) =====")
df_transformado.groupBy("departamentonombre") \
  .agg(round(avg("precio"), 2).alias("precio_promedio")) \
  .orderBy("precio_promedio", ascending=False) \
  .show(10, truncate=False)

spark.stop()
