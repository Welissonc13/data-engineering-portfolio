import pandas as pd
import duckdb
import os
import time

#Criando Diretórios

inputpath_953base = r'C:\Users\User\Desktop\gestao\excel_convertido\953_'
inputpath_295base = r'C:\Users\User\Desktop\gestao\excel_convertido\295_'
nome_dia = f'{time.strftime('%d')}_{time.strftime('%m')}_{time.strftime('%Y')}_conv.xlsx'

path_953 = f'{inputpath_953base}{nome_dia}'
path_295 = f'{inputpath_295base}{nome_dia}'
output_path = r'C:\Users\User\Desktop\gestao\Fact\FACT_PAGAMENTOS.parquet'


 # Lê 953
try:
    df_953= pd.read_excel(path_953, thousands=None, decimal=',', header=0)
except Exception as e:
    print(f"Erro ao processar o arquivo 953: {e}")  
    

 # Lê 295
try:
    df_295 = pd.read_excel(path_295, thousands=None, decimal=',', header=0)
except Exception as e:
    print(f"Erro ao processar o arquivo 295: {e}")
    
    

# Registra osDataFrames como uma tabela no DuckDB
con = duckdb.connect()
con.register("Plan953", df_953)
con.register("Plan295", df_295)

try:
    FACT_PAGAMENTOS = con.execute("""
                SELECT 
                    x.Empresa_Des as Codigo_Empresa,
                    x.Obra_Des as Codigo_Obra,
                    x.NumProc_Des as Processo,
                    x.NumParc_Des as Numero_Parcela,
                    x.CodForn_Des as Codigo_Fornecedor,
                    x.DataVencimento,
                    x.DataProrrogacao,
                    x.DtPgto_Des as Data_Pagamento,
                    y.DtGeracao as Data_Criacao,
                    DATE_DIFF('day',  STRPTIME(y.DtGeracao, '%d/%m/%Y'),STRPTIME(y.DataPg, '%d/%m/%Y')) AS Prazo_Lancamento, 
                    x.StatusParc_Des as Status_Pagamento,
                    CASE
                        WHEN x.NumContrato != 0 THEN 'Sim'
                        ELSE 'Não'
                    END AS E_Medicao,
                    x.NumContrato as Numero_Contrato,
                    x.ItemProc_Des as Codigo_Item,
                    x.DescItemProc as Item,
                    x.InsumoPL as CodItem_Plan_Alocado,
                    x.Insumo as Item_Plan_Alocado,
                    x.plmes_des as Mes_Alocado,
                    x.SubTotal, 
                    x.Acrescimo,
                    x.Desconto,	
                    x.TotalReal,
                    y.DocFiscal_Des as Numero_NF
                FROM Plan953 x
                LEFT JOIN Plan295 y
                    ON x.NumProc_Des = y.Processo and x.Obra_Des = y.Obra_Des and x.NumParc_Des = y.Parcela and x.ItemProc_Des = y.ItemProc_Des and x.TotalReal = y."Pagar&Pago" and x.ItemPl_Des = y.Item_SiAp
                """).df()
    
    FACT_PAGAMENTOS.to_parquet(output_path, index=False)
    FACT_PAGAMENTOS.to_excel(r'C:\Users\User\Desktop\gestao\excel_convertido\FACT_PAGAMENTOS.xlsx', index=False)
    
    print("FACT_PAGAMENTOS CRIADO COM SUCESSO!")
    
except Exception as e:
    print(f"ERRO AO TENTAR CRIAR FACT_PAGAMENTOS! \n{e}")


