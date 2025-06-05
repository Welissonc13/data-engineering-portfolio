import pandas as pd
import duckdb
import time



#diretórios
basepath_740 = r"C:\Users\User\Desktop\gestao\excel_convertido\740_"
basepath_771 = r"C:\Users\User\Desktop\gestao\excel_convertido\771_"
basepath_862 = r"C:\Users\User\Desktop\gestao\excel_convertido\862_"
nome_dia = f'{time.strftime('%d')}_{time.strftime('%m')}_{time.strftime('%Y')}_conv.xlsx'

inputpath_740 = f'{basepath_740}{nome_dia}'
inputpath_771 = f'{basepath_771}{nome_dia}'
inputpath_862 = f'{basepath_862}{nome_dia}'
output_path = r'C:\Users\User\Desktop\gestao\Fact\FACT_CONTRACTS.parquet'
output_pathexcel = r'C:\Users\User\Desktop\gestao\excel_convertido\FACT_CONTRACTS.xlsx'


#abre excel das planilhas 740, 771 e 862
try:
    df_740 = pd.read_excel(inputpath_740, header=0)
except Exception as e:
    print(f"Erro ao tentar abrir o arquivo 740: {e} ")

try:
    df_771 = pd.read_excel(inputpath_771, header=0)
except Exception as e:
    print(f"Erro ao tentar abrir o arquivo 771: {e} ")

try:
    df_862 = pd.read_excel(inputpath_862, header=0)
except Exception as e:
    print(f"Erro ao tentar abrir o arquivo 862: {e} ")
    
    
#Conecta duckdb e cria tabelas
con = duckdb.connect()
duckdb.register("Plan740", df_740)
duckdb.register("Plan771", df_771)
duckdb.register("Plan862", df_862)

#Cria Tabela FACT_CONTRACTS
FACT_CONTRACTS = duckdb.execute('''
    SELECT 
        x.Empresa_Cont as Cod_Empresa,
        x.Obra_Cont as Cod_Obra,
        x.Cod_Cont as Cod_Contrato,
        x.Contrato,
        x.CodPes_Cont as Cod_Fornecedor,
        x.Fornecedor as Fornecedor,
        x.DtInicio_Cont,
        x.DtFim_Cont,
        x.Situacao,
        x.Status,
        y.Total_Itens as Valor_Total_CT,
        y.ValorMedido as Valor_Total_Medido,
        x.Item_Itens,
        x.Serv_Itens as Cod_Itens,
        x.Descr_Itens,
        x.Qtde_Itens as Qtde_do_Item,
        x.Preco_Itens as Preco_do_Item,
        x.ValorServico Valor_Total_Servico,
        z.PrecoUnit_Item,
        z.QtdeTotMedida,
        z.QtdeEmMedicao,
        z.TotMedAPagar,
        z.TotMedidoPago,
        z.QtdeTotalAMedir
    FROM Plan740 x
    LEFT JOIN Plan771 y ON x.Obra_Cont = y.Obra_Cont AND x.Cod_Cont = y.Cod_Cont
    LEFT JOIN
    (SELECT
        Item_Itens,
        SPLIT_PART(Obra,' - ', 1) as Obra_Cont, 
        regexp_extract(Contrato, '^(\\d+)', 1) AS Cod_Cont,
        SPLIT_PART(ItemContrato,' - ', 1) as Serv_Itens,
        PrecoUnit_Item,
        QtdeTotMedida,
        QtdeEmMedicao,
        TotMedAPagar,
        TotMedidoPago,
        QtdeTotalAMedir
    from Plan862) z
    ON x.Obra_Cont = z.Obra_Cont AND x.Cod_Cont = z.Cod_Cont AND x.Item_Itens = z.Item_Itens
    ''').df()

#Salva fact como excel e parquet

try:
    FACT_CONTRACTS.to_excel(output_pathexcel, index=False)
    FACT_CONTRACTS.to_parquet(output_path, index=False)
    print("FACT_CONTRACTS CRIADA COM SUCESSO!")

except Exception as e:
    print(f"Erro ao tentar salvar a tabela FACT_CONTRACT:\n{e}")
