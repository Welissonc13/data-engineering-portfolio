from pyspark.sql.functions import explode, col

# Caminho do arquivo JSON com estrutura aninhada (nested)
file_path = "dbfs:/FileStore/sample_data/exemplo_nested.json"

# Leitura do arquivo JSON com inferência de schema
df = spark.read.option("multiline", True).json(file_path)

# Exibe o schema para entender a estrutura dos dados aninhados
df.printSchema()

# Exemplo de estrutura esperada:
# {
#   "id": 1,
#   "nome": "João",
#   "enderecos": [
#     {"tipo": "residencial", "cidade": "São Paulo"},
#     {"tipo": "comercial", "cidade": "Campinas"}
#   ]
# }

# Explode o array de endereços (enderecos) para transformar cada item em uma linha
df_exploded = df.withColumn("endereco", explode(col("enderecos")))

# Seleciona campos do nível raiz e campos internos do array exploded
df_result = df_exploded.select(
    col("id"),
    col("nome"),
    col("endereco.tipo").alias("tipo_endereco"),
    col("endereco.cidade").alias("cidade_endereco")
)

# Exibe o resultado com os campos extraídos
df_result.printSchema()
df_result.show(5, truncate=False)
