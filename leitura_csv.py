# Caminho do arquivo CSV no DBFS
file_path = "dbfs:/FileStore/sample_data/exemplo.csv"

# Leitura do arquivo CSV com opções adicionais:
# - header: define se a primeira linha contém os nomes das colunas
# - sep: define o caractere separador (exemplo: ',' ou ';')
# - encoding: define a codificação do arquivo (exemplo: 'UTF-8', 'ISO-8859-1')
df = spark.read.options(
    header=True,
    sep=";",               # Altere para "," se necessário
    encoding="UTF-8"       # Ou "ISO-8859-1" para arquivos com acentuação em Windows
).csv(file_path)

# Exibe o schema do DataFrame
df.printSchema()

# Mostra as primeiras 5 linhas
df.show(5)
