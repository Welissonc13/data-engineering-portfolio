import pandas as pd
import time

#Cria data, diretório base e nome do arquivo
dia_hoje = time.strftime("%d")
mes_hoje = time.strftime("%m")
ano_hoje = time.strftime("%Y")

#Lista de relatórios à converter

relatorios = [295,740,771,862,953]

for relatorio in relatorios:
        
    nome = f"_{dia_hoje}_{mes_hoje}_{ano_hoje}"
    dirbase_input = r"C:\Users\User\Desktop\gestao\raw\\"
    dirbase_output = r"C:\Users\User\Desktop\gestao\excel_convertido\\"


    #Diretórios
    input_df =  f"{dirbase_input}{relatorio}{nome}.xls"
    output_df =  f"{dirbase_output}{relatorio}{nome}_conv.xlsx"

    df_list = pd.read_html(input_df, thousands=None, decimal=',', header=1)

        
    if df_list:
            df = df_list[0]

    try:
            df.to_excel(output_df, index=False)
            print(f"Conversão do relatório {relatorio} completa!")

    except Exception as e:
            print(f"Não foi possível realizar a conversão do relatório {relatorio}. Erro:\n{e}")