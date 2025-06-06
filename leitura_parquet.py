from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Define manualmente o schema dos dados
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("nome", StringType(), True),
    StructField("valor", DoubleType(), True),
    StructField("data_cadastro", StringType(), True)
])

# Caminho base com expressão regex para filtrar os arquivos parquet desejados
file_path = "dbfs:/mnt/lake/bronze/sap/co/"

# Leitura dos arquivos Parquet usando filtro por padrão de nome (regex com pathGlobFilter)
df = spark.read \
    .schema(schema) \
    .option("pathGlobFilter", "*_dados.parquet") \
    .parquet(file_path)

# Exibe o schema resultante (deve bater com o schema manual definido)
df.printSchema()

# Mostra as primeiras 5 linhas dos dados
df.show(5)
