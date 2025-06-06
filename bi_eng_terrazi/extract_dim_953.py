import pandas as pd
import os
import duckdb
import time

#cria nome
dia = time.strftime("%d")
mes = time.strftime("%m")
ano = time.strftime("%Y")

basepath_input = r'C:\Users\User\Desktop\gestao\excel_convertido\953'
nome_relatorio = f'_{dia}_{mes}_{ano}_conv.xlsx'

# diretórios
input_path = f'{basepath_input}{nome_relatorio}'
dim_empresa_output_path = r'C:\Users\User\Desktop\gestao\Dim\DIM_EMPRESAS.parquet'
dim_obras_output_path = r'C:\Users\User\Desktop\gestao\Dim\DIM_OBRAS.parquet'
dim_fornecedores_output_path = r'C:\Users\User\Desktop\gestao\Dim\DIM_FORNECEDORES.parquet'

try:
    # Lê o excel
    df_data = pd.read_excel(input_path, thousands=None, decimal=',', header=0)

    if not df_data.empty:
        # Remove espaços das pontas e muda por "_" no meio
        df_data.columns = df_data.columns.str.strip().str.replace(' ', '_')

        # Registra o DataFrame como uma tabela no DuckDB
        con = duckdb.connect()
        con.register("input_df", df_data)

        # Cria DIM_EMPRESAS e salva como parquet
        try:
            dim_empresas = con.execute("""
                SELECT DISTINCT Empresa, Empresa_Des AS Codigo_empresa 
                FROM input_df 
                ORDER BY 1
            """).df()
            dim_empresas.to_parquet(dim_empresa_output_path, index=False)
            print("DIM_EMPRESAS CRIADO COM SUCESSO!")
        except:
            print("ERRO AO TENTAR CRIAR DIM_EMPRESAS!")

        # Cria DIM_FORNECEDORES e salva como parquet
        try:
            dim_fornecedores = con.execute("""
                SELECT DISTINCT CodForn_Des AS Codigo_Forn, Fornecedor, "CPF/CNPJ" 
                FROM input_df 
                ORDER BY 1
            """).df()
            dim_fornecedores.to_parquet(dim_fornecedores_output_path, index=False)
            print("DIM_FORNECEDORES CRIADO COM SUCESSO!")
        except:
            print('ERRO AO TENTAR CRIAR DIM_FORNECEDORES!')

        # Cria DIM_OBRAS e salva como parquet
        try:
            dim_obras = con.execute("""
                SELECT DISTINCT Obra, Obra_Des AS Codigo_Obra 
                FROM input_df 
                ORDER BY 1
            """).df()
            dim_obras.to_parquet(dim_obras_output_path, index=False)
            print("DIM_OBRAS CRIADO COM SUCESSO!")
        except:
            print('ERRO AO TENTAR CRIAR DIM_OBRAS!')

    else:
        print("Nenhuma tabela encontrada no arquivo HTML.")

except Exception as e:
    print(f"Erro ao processar o arquivo: {e}")
