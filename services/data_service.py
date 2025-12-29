import os
import pandas as pd
import geopandas as gpd
from unidecode import unidecode

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data')

MAPA_MESES = {
    1: 'JAN', 2: 'FEV', 3: 'MAR', 4: 'ABR', 
    5: 'MAI', 6: 'JUN', 7: 'JUL', 8: 'AGO', 
    9: 'SET', 10: 'OUT', 11: 'NOV', 12: 'DEZ'
}

def normalizar_texto(texto):
    if not isinstance(texto, str): return ""
    return unidecode(texto).upper().strip()

def processar_csv(caminho_csv, coluna_mes):
    if not os.path.exists(caminho_csv): return None
    try:
        df = pd.read_csv(caminho_csv)
        col_municipio = df.columns[0] 
        for col in df.columns:
            if 'MUNICIPIO' in col.upper():
                col_municipio = col
                break
        df['MUNICIPIO_NORM'] = df[col_municipio].apply(normalizar_texto)

        if coluna_mes is None:
            colunas_existentes = [c for c in MAPA_MESES.values() if c in df.columns]
            valor_mes = df[colunas_existentes].sum(axis=1).fillna(0).astype(int)
        else:
            if coluna_mes not in df.columns: valor_mes = 0
            else: valor_mes = df[coluna_mes].fillna(0).astype(int)

        df_temp = pd.DataFrame({'MUNICIPIO_NORM': df['MUNICIPIO_NORM'], 'VALOR_MES': valor_mes})
        return df_temp.groupby('MUNICIPIO_NORM', as_index=False).sum()
    except Exception as e:
        print(f"Erro CSV {caminho_csv}: {e}")
        return None

def carregar_dados_por_ano_mes(ano, mes):
    try:
        ano = int(ano)
        mes = int(mes)
    except ValueError: raise ValueError("Erro data")
    
    if mes == 0: coluna_mes = None
    else: coluna_mes = MAPA_MESES.get(mes)

    # --- CAMINHOS ---
    path_violencia = os.path.join(DATA_DIR, 'csv', f"violencia_domestica_pe_{ano}.csv")
    path_estupro = os.path.join(DATA_DIR, 'csv', f"casos_estupro_pe_{ano}.csv")
    path_populacao = os.path.join(DATA_DIR, 'csv', "populacao_pe.csv")

    nome_shp = 'PE_Municipios_2024.shp' 
    path_shp = os.path.join(DATA_DIR, 'shapefile', 'Pe_municipios_2024', nome_shp)
    
    if not os.path.exists(path_shp):
        # Tenta fallback na raiz da pasta shapefile
        path_shp_alt = os.path.join(DATA_DIR, 'shapefile', nome_shp)
        if os.path.exists(path_shp_alt):
            path_shp = path_shp_alt
        else:
            # Se ainda não achar, erro
            raise FileNotFoundError(f"Shapefile não encontrado em {path_shp}")

    # 1. Carrega Shapefile
    gdf = gpd.read_file(path_shp)
    col_nome_shp = 'NM_MUN'
    for col in gdf.columns:
        if col in ['NM_MUNICIP', 'NM_MUN_2022', 'NOME']: 
            col_nome_shp = col; break
    gdf['NM_MUN_NORM'] = gdf[col_nome_shp].apply(normalizar_texto)

    # 2. Carrega CSVs Dados
    df_violencia = processar_csv(path_violencia, coluna_mes)
    df_estupro = processar_csv(path_estupro, coluna_mes)

    # 3. Carrega População
    df_pop = None
    if os.path.exists(path_populacao):
        df_pop_raw = pd.read_csv(path_populacao)
        df_pop = pd.DataFrame()
        df_pop['MUNICIPIO_NORM'] = df_pop_raw.iloc[:,0].apply(normalizar_texto)
        df_pop['populacao'] = df_pop_raw.iloc[:,1].fillna(1).astype(int)

    # 4. Merges
    
    # Merge Violencia
    if df_violencia is not None:
        gdf = gdf.merge(df_violencia, left_on='NM_MUN_NORM', right_on='MUNICIPIO_NORM', how='left')
        gdf.rename(columns={'VALOR_MES': 'violencia'}, inplace=True)
    else: gdf['violencia'] = 0

    # Merge Estupro
    if df_estupro is not None:
        gdf = gdf.merge(df_estupro, left_on='NM_MUN_NORM', right_on='MUNICIPIO_NORM', how='left')
        gdf.rename(columns={'VALOR_MES': 'estupro'}, inplace=True)
    else: gdf['estupro'] = 0

    # Merge População (CORRIGIDO AQUI)
    if df_pop is not None:
        # Usa left_on e right_on porque os nomes das colunas são diferentes
        gdf = gdf.merge(df_pop, left_on='NM_MUN_NORM', right_on='MUNICIPIO_NORM', how='left')
    else:
        gdf['populacao'] = 1 

    # Limpeza
    gdf['violencia'] = gdf['violencia'].fillna(0).astype(int)
    gdf['estupro'] = gdf['estupro'].fillna(0).astype(int)
    gdf['populacao'] = gdf['populacao'].fillna(1).astype(int)
    
    gdf['valor'] = gdf['violencia'] 
    gdf['municipio'] = gdf[col_nome_shp]

    if gdf.crs != "EPSG:4326": gdf = gdf.to_crs(epsg=4326)

    return gdf.to_json()