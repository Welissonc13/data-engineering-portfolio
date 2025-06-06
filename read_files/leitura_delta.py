from delta.tables import DeltaTable

# ========================================================
# 1. Leitura via spark.sql() – usando SQL direto no metastore
# ========================================================

# Lê a tabela Delta registrada no catálogo/metastore usando SQL
df_sql = spark.sql("SELECT * FROM curated.sales_data")

# Exibe o schema e os primeiros registros
df_sql.printSchema()
df_sql.show(5)

# ========================================================
# 2. Leitura via spark.table() – atalho para tabelas do metastore
# ========================================================

# Leitura da mesma tabela Delta registrada, mas usando spark.table()
df_table = spark.table("curated.sales_data")

# Exibe o schema e os primeiros registros
df_table.printSchema()
df_table.show(5)

# ========================================================
# 3. Leitura via DeltaTable.forPath().toDF() – acesso por caminho físico
# ========================================================

# Caminho físico da tabela Delta no storage (não precisa estar registrada)
delta_path = "dbfs:/mnt/lake/curated/sales_data"

# Cria um objeto DeltaTable e converte para DataFrame
delta_table = DeltaTable.forPath(spark, delta_path)
df_path = delta_table.toDF()

# Exibe o schema e os primeiros registros
df_path.printSchema()
df_path.show(5)
