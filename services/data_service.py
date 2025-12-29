import os
import pandas as pd
import geopandas as gpd
from unidecode import unidecode

# 1. Configuração de Caminhos
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data')

MAPA_MESES = {
    1: 'JAN', 2: 'FEV', 3: 'MAR', 4: 'ABR', 
    5: 'MAI', 6: 'JUN', 7: 'JUL', 8: 'AGO', 
    9: 'SET', 10: 'OUT', 11: 'NOV', 12: 'DEZ'
}

def carregar_dados_por_ano_mes(ano, mes):
    # --- VALIDAÇÃO ---
    try:
        ano = int(ano)
        mes = int(mes)
    except ValueError:
        raise ValueError("Ano e Mês devem ser números válidos.")

    coluna_mes = MAPA_MESES.get(mes)
    if not coluna_mes:
        raise ValueError("Mês inválido (use 1 a 12).")

    # --- DEFINIÇÃO DOS ARQUIVOS ---
    nome_csv = f"violencia_domestica_pe_{ano}.csv"
    csv_path = os.path.join(DATA_DIR, 'csv', nome_csv)

    # AQUI ESTA A MUDANÇA: Caminho entrando na subpasta
    # data/shapefile/Pe_municipios_2024/SEU_ARQUIVO.shp
    
    # ⚠️ ⚠️ ⚠️ ATENÇÃO: SUBSTITUA O NOME ABAIXO PELO NOME REAL DO ARQUIVO .SHP ⚠️ ⚠️ ⚠️
    # Pode ser 'PE_Municipios_2023.shp', '26MUE250GC_SIR.shp', etc.
    nome_arquivo_shp_real = 'PE_Municipios_2024.shp' 
    
    shp_path = os.path.join(DATA_DIR, 'shapefile', 'Pe_municipios_2024', nome_arquivo_shp_real)

    # --- DEBUG (Para garantir) ---
    if not os.path.exists(shp_path):
        print(f"❌ ERRO: Não achei o arquivo em: {shp_path}")
        print("Confira se o nome 'PE_Municipios_2024.shp' está correto dentro da pasta.")
        raise FileNotFoundError(f"Shapefile não encontrado.")

    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV do ano {ano} não encontrado.")

    # --- PROCESSAMENTO (Igual ao anterior) ---
    df = pd.read_csv(csv_path)
    df['MUNICIPIO_NORM'] = df['MUNICIPIO'].astype(str).str.strip().apply(lambda x: unidecode(x).upper())
    
    if coluna_mes not in df.columns:
        df['VALOR_FILTRADO'] = 0
    else:
        df['VALOR_FILTRADO'] = df[coluna_mes].fillna(0).astype(int)

    df_selecionado = df[['MUNICIPIO_NORM', 'MUNICIPIO', 'TOTAL', 'VALOR_FILTRADO']].copy()

    gdf = gpd.read_file(shp_path)
    
    # Ajuste aqui se a coluna do nome no shapefile não for 'NM_MUN'
    # Às vezes o IBGE usa 'NM_MUNICIP' ou 'NM_MUN_2022'
    coluna_nome_shapefile = 'NM_MUN' 
    
    if coluna_nome_shapefile not in gdf.columns:
         # Tenta achar a coluna de nome automaticamente se não for NM_MUN
         for col in gdf.columns:
             if 'NM_' in col or 'NOME' in col:
                 coluna_nome_shapefile = col
                 break
    
    gdf['NM_MUN_NORM'] = gdf[coluna_nome_shapefile].apply(lambda x: unidecode(x).upper())

    gdf_final = gdf.merge(df_selecionado, left_on='NM_MUN_NORM', right_on='MUNICIPIO_NORM', how='left')

    gdf_final['VALOR_FILTRADO'] = gdf_final['VALOR_FILTRADO'].fillna(0).astype(int)
    gdf_final['TOTAL'] = gdf_final['TOTAL'].fillna(0).astype(int)
    
    # Renomeia para o front-end
    gdf_final = gdf_final.rename(columns={'VALOR_FILTRADO': 'valor', 'TOTAL': 'total_ano', coluna_nome_shapefile: 'municipio'})

    return gdf_final.to_json()