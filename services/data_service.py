import os
import pandas as pd
import geopandas as gpd
from utils.normalizer_str import normalizar_texto
from utils.extract_date import enriquecer_datas
from utils.col_numeric import tratar_metricas_vitimas
from utils.read_archive import ler_arquivo_bruto
from utils.pattern_municipios import padronizar_municipios
from utils.read_archive import carregar_arquivo_generico

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data')

# Nomes dos arquivos (certifique-se que estão na pasta /data/)
ARQUIVO_VIOLENCIA = 'MICRODADOS_DE_VIOLÊNCIA_DOMÉSTICA_JAN_2015_A_NOV_2025.xlsx'
ARQUIVO_ESTUPRO   = 'MICRODADOS_ESTUPRO_JAN_2015_A_NOV_2025.xlsx'

# Cache em memória (para não ler o Excel a cada clique)
_CACHE_VIOLENCIA = None
_CACHE_ESTUPRO = None


def carregar_arquivo_processado(nome_arquivo):
    df = ler_arquivo_bruto(nome_arquivo)
    
    if df is None:
        return None

    # 2. Pipeline de Transformação
    try:
        df = padronizar_municipios(df)
        df = enriquecer_datas(df)
        df = tratar_metricas_vitimas(df)
        
        return df
        
    except Exception as e:
        print(f"Erro no processamento lógico de {nome_arquivo}: {e}")
        return None
    
def obter_dados_cacheados():
    """Gerencia o carregamento único (Singleton) dos dados."""
    global _CACHE_VIOLENCIA, _CACHE_ESTUPRO

    if _CACHE_VIOLENCIA is None:
        _CACHE_VIOLENCIA = carregar_arquivo_generico(ARQUIVO_VIOLENCIA)
    
    if _CACHE_ESTUPRO is None:
        _CACHE_ESTUPRO = carregar_arquivo_generico(ARQUIVO_ESTUPRO)

    return _CACHE_VIOLENCIA, _CACHE_ESTUPRO

def filtrar_e_agrupar(df, ano, mes):
    """Filtra o DataFrame por data e soma as vítimas por cidade."""
    if df is None: return None

    # Filtra Ano
    df_f = df[df['ANO_FATO'] == int(ano)].copy()
    
    # Filtra Mês (se for > 0)
    if int(mes) > 0:
        df_f = df_f[df_f['MES_FATO'] == int(mes)]
        
    # Agrupa
    return df_f.groupby('MUNICIPIO_NORM')['TOTAL_VITIMAS'].sum().reset_index()

def carregar_dados_por_ano_mes(ano, mes):
    # 1. Garante que os dados estão na memória
    df_viol, df_est = obter_dados_cacheados()

    # 2. Processa Violência
    dados_violencia = filtrar_e_agrupar(df_viol, ano, mes)
    
    # 3. Processa Estupro
    dados_estupro = filtrar_e_agrupar(df_est, ano, mes)

    # 4. Carrega Mapa
    nome_shp = 'PE_Municipios_2024.shp' 
    path_shp = os.path.join(DATA_DIR, 'shapefile', 'Pe_municipios_2024', nome_shp)
    if not os.path.exists(path_shp):
        # Tenta caminho alternativo
        path_shp = os.path.join(DATA_DIR, 'shapefile', nome_shp)

    gdf = gpd.read_file(path_shp)
    
    # Normaliza nome do mapa
    col_nome_mapa = 'NM_MUN' # Padrão IBGE recente
    for col in gdf.columns:
        if col in ['NM_MUNICIP', 'NM_MUN_2022', 'NOME']: 
            col_nome_mapa = col; break
    gdf['NM_MUN_NORM'] = gdf[col_nome_mapa].apply(normalizar_texto)

    # 5. Merges (Juntar tudo no mapa)
    
    # Merge Violência
    if dados_violencia is not None:
        gdf = gdf.merge(dados_violencia, left_on='NM_MUN_NORM', right_on='MUNICIPIO_NORM', how='left')
        gdf.rename(columns={'TOTAL_VITIMAS': 'violencia'}, inplace=True)
    else:
        gdf['violencia'] = 0

    # Merge Estupro
    if dados_estupro is not None:
        gdf = gdf.merge(dados_estupro, left_on='NM_MUN_NORM', right_on='MUNICIPIO_NORM', how='left')
        gdf.rename(columns={'TOTAL_VITIMAS': 'estupro'}, inplace=True)
    else:
        gdf['estupro'] = 0
        
    # Merge População (Opcional)
    path_pop = os.path.join(DATA_DIR, 'csv', 'populacao_pe.csv')
    if os.path.exists(path_pop):
        try:
            df_pop = pd.read_csv(path_pop)
            df_pop['MUNICIPIO_NORM_POP'] = df_pop.iloc[:,0].apply(normalizar_texto)
            df_pop['pop_val'] = df_pop.iloc[:,1].fillna(1).astype(int)
            gdf = gdf.merge(df_pop, left_on='NM_MUN_NORM', right_on='MUNICIPIO_NORM_POP', how='left')
            gdf['populacao'] = gdf['pop_val']
        except:
            gdf['populacao'] = 1
    else:
        gdf['populacao'] = 1

    # 6. Limpeza Final
    gdf['violencia'] = gdf['violencia'].fillna(0).astype(int)
    gdf['estupro'] = gdf['estupro'].fillna(0).astype(int)
    gdf['populacao'] = gdf['populacao'].fillna(1).astype(int)
    
    # Define o valor padrão para o mapa colorir
    gdf['valor'] = gdf['violencia'] 
    gdf['municipio'] = gdf[col_nome_mapa]

    if gdf.crs != "EPSG:4326": gdf = gdf.to_crs(epsg=4326)

    return gdf.to_json()

def get_dados_municipio(nome_municipio, ano, mes):
    """
    Busca dados específicos de uma cidade em um período, 
    sem precisar gerar o mapa inteiro.
    """
    # 1. Garante cache
    df_viol, df_est = obter_dados_cacheados()
    
    nome_norm = normalizar_texto(nome_municipio)
    ano = int(ano)
    mes = int(mes)

    def processar_df(df_fonte):
        if df_fonte is None: return 0
        
        # Filtra Ano
        dff = df_fonte[df_fonte['ANO_FATO'] == ano]
        
        # Filtra Mês (Se for 0, pega o ano todo)
        if mes > 0:
            dff = dff[dff['MES_FATO'] == mes]
            
        # Filtra Cidade
        dff = dff[dff['MUNICIPIO_NORM'] == nome_norm]
        
        return int(dff['TOTAL_VITIMAS'].sum())

    # 2. Obtém totais
    total_violencia = processar_df(df_viol)
    total_estupro = processar_df(df_est)

    # 3. Obtém População (para cálculo de taxa)
    populacao = 1 # Evitar divisão por zero
    path_pop = os.path.join(DATA_DIR, 'csv', 'populacao_pe.csv')
    if os.path.exists(path_pop):
        try:
            df_pop = pd.read_csv(path_pop)
            # Assume col 0 cidade, col 1 pop
            row = df_pop[df_pop.iloc[:,0].apply(normalizar_texto) == nome_norm]
            if not row.empty:
                populacao = int(row.iloc[0,1])
        except:
            pass

    # 4. Calcula Taxas (por 100k habitantes)
    # Se for mês específico, a taxa geralmente é anualizada ou mensal. 
    # Aqui faremos a taxa simples do período.
    taxa_violencia = (total_violencia / populacao) * 100000
    taxa_estupro = (total_estupro / populacao) * 100000

    return {
        "municipio": nome_municipio, # Retorna o nome original ou formatado
        "ano": ano,
        "mes": mes,
        "populacao": populacao,
        "violencia": total_violencia,
        "estupro": total_estupro,
        "taxa_violencia": round(taxa_violencia, 2),
        "taxa_estupro": round(taxa_estupro, 2)
    }