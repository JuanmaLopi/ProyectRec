from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Crear la sesión de Spark
spark = SparkSession.builder.appName("analisis_negocios").getOrCreate()

# Leer los datos desde el archivo CSV
df = spark.read.csv("/home/vagrant/labSpark/analisis_emprenred/dataset", header=True, inferSchema=True)

# Mostrar el DataFrame
df.show()

# Calcula los 5 campos de negocio más comunes y su número
campos_negocio_mas_comunes = df.groupBy("campoNegocio").count().orderBy(col("count").desc()).limit(5)

print("Los 5 campos de negocio más comunes son:")
campos_negocio_mas_comunes.show(truncate=False)

# Calcula los 5 cargos más comunes y su número
cargos_mas_comunes = df.groupBy("cargoExperto").count().orderBy(col("count").desc()).limit(5)

print("Los 5 cargos más comunes son:")
cargos_mas_comunes.show(truncate=False)

# Calcula los 5 nombres de las empresas que más se le solicitan vacantes
cargos_mas_comunes = df.groupBy("nombreNegocio").count().orderBy(col("count").desc()).limit(5)

print("Los 5 negocios a los que cuales se le solicitan más vacantes son:")
cargos_mas_comunes.show(truncate=False)
