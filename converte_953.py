import pandas as pd
import time

#Cria data, diretório base e nome do arquivo
dia_hoje = time.strftime("%d")
mes_hoje = time.strftime("%m")
ano_hoje = time.strftime("%Y")

nome = f"_{dia_hoje}_{mes_hoje}_{ano_hoje}"
dirbase_input = r"C:\Users\User\Desktop\gestao\raw\953"
dirbase_output = r"C:\Users\User\Desktop\gestao\excel_convertido\953"


#Diretórios
input_df =  f"{dirbase_input}{nome}.xls"
output_df =  f"{dirbase_output}{nome}_conv.xlsx"

df_list = pd.read_html(input_df, thousands=None, decimal=',', header=1)

    
if df_list:
        df = df_list[0]

try:
        df.to_excel(output_df, index=False)
        print("Conversão do relatório 953 completa!")

except:
        print("Não foi possível realizar a conversão do relatório 953.")