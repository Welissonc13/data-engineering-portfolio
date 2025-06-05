import pandas as pd
import numpy as np
import hashlib

# Gerar intervalo de datas
data_inicial = '1990-01-01'
data_final = '2050-12-31'
datas = pd.date_range(start=data_inicial, end=data_final, freq='D')

# Criar DataFrame base
df = pd.DataFrame({'data_timestamp': datas})

# Colunas básicas
df['data_id'] = df['data_timestamp'].dt.strftime('%Y%m%d').astype(int)
df['data'] = df['data_timestamp'].dt.strftime('%d/%m/%Y')
df['ano'] = df['data_timestamp'].dt.year
df['mes'] = df['data_timestamp'].dt.month
df['dia'] = df['data_timestamp'].dt.day
df['dia_semana'] = df['data_timestamp'].dt.weekday + 1  # 1=Segunda, 7=Domingo
df['data_inicio_semana'] = (df['data_timestamp'] - pd.to_timedelta(df['dia_semana'] - 1, unit='D')).dt.strftime('%d/%m/%Y') # Colunas de início e fim da semana (segunda a domingo)
df['data_fim_semana'] = (df['data_timestamp'] + pd.to_timedelta(7 - df['dia_semana'], unit='D')).dt.strftime('%d/%m/%Y') # Colunas de início e fim da semana (segunda a domingo)
df['nome_dia_semana'] = df['data_timestamp'].dt.day_name(locale='pt_BR')
df['nome_mes'] = df['data_timestamp'].dt.month_name(locale='pt_BR')
df['dia_ano'] = df['data_timestamp'].dt.dayofyear
df['semana_ano'] = df['data_timestamp'].dt.isocalendar().week
df['trimestre'] = df['data_timestamp'].dt.quarter
df['semestre'] = np.where(df['mes'] <= 6, 1, 2)
df['ano_mes'] = df['data_timestamp'].dt.to_period('M').astype(str)
df['semana_iso'] = df['data_timestamp'].dt.isocalendar().week
df['ano_iso'] = df['data_timestamp'].dt.isocalendar().year

# Colunas booleanas e auxiliares
df['fim_de_semana'] = df['dia_semana'] >= 6
df['inicio_mes'] = df['data_timestamp'].dt.is_month_start
df['fim_mes'] = df['data_timestamp'].dt.is_month_end

# Abreviações
df['mes_abrev'] = df['data_timestamp'].dt.strftime('%b')
df['dia_semana_abrev'] = df['data_timestamp'].dt.strftime('%a')

# Datas formatadas
df['data_extenso'] = df['data_timestamp'].dt.strftime('%d de %B de %Y')
df['mes_ano'] = df['data_timestamp'].dt.strftime('%b/%Y')
df['ano_trimestre'] = df['ano'].astype(str) + '-T' + df['trimestre'].astype(str)

# Hash da data
def gerar_hash(valor):
    return hashlib.md5(str(valor).encode()).hexdigest()

df['data_hash'] = df['data_timestamp'].astype(str).apply(gerar_hash)

# Reordenar colunas (opcional)
colunas_ordenadas = [
    'data_id', 'data_timestamp', 'data', 'data_extenso', 'data_hash',
    'ano', 'ano_iso', 'mes', 'mes_abrev', 'nome_mes', 'semestre', 'trimestre', 'ano_trimestre',
    'dia', 'dia_ano', 'dia_semana', 'dia_semana_abrev', 'nome_dia_semana',
    'semana_ano', 'semana_iso', 'ano_mes', 'mes_ano',
    'inicio_mes', 'fim_mes', 'fim_de_semana', 'data_inicio_semana', 'data_fim_semana'
]
df = df[colunas_ordenadas]

# Salvar como Parquet
caminho_saida = r'C:\Users\User\Desktop\gestao\ults\dim_tabela_calendario.parquet'
df.to_parquet(caminho_saida, index=False, engine='pyarrow')

#Salva como excel
df.to_excel(r'C:\Users\User\Desktop\gestao\excel_convertido\dim_tabela_calendario.xlsx')



print(f"Tabela dimensão de datas salva com sucesso em: {caminho_saida}")
