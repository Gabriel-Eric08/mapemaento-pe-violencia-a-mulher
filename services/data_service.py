import os
import pandas as pd
import geopandas as gpd
from utils.normalizer_str import normalizar_texto
from utils.extract_date import enriquecer_datas
from utils.col_numeric import tratar_metricas_vitimas
from utils.pattern_municipios import padronizar_municipios
# Apenas importamos ler_arquivo_bruto, pois carregar_arquivo_generico não existe lá
from utils.read_archive import ler_arquivo_bruto

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data')

# Nomes dos arquivos (certifique-se que estão na pasta /data/)
ARQUIVO_VIOLENCIA = 'MICRODADOS_DE_VIOLÊNCIA_DOMÉSTICA_JAN_2015_A_NOV_2025.xlsx'
ARQUIVO_ESTUPRO   = 'MICRODADOS_ESTUPRO_JAN_2015_A_NOV_2025.xlsx'

# Cache em memória
_CACHE_VIOLENCIA = None
_CACHE_ESTUPRO = None

def carregar_arquivo_processado(nome_arquivo):
    # CORREÇÃO: Passamos DATA_DIR e o nome do arquivo, pois sua função pede 2 argumentos
    df = ler_arquivo_bruto(DATA_DIR, nome_arquivo)
    
    if df is None:
        return None

    # Pipeline de Transformação
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

    # CORREÇÃO: Usamos a função local 'carregar_arquivo_processado'
    if _CACHE_VIOLENCIA is None:
        print("Carregando Cache Violência...")
        _CACHE_VIOLENCIA = carregar_arquivo_processado(ARQUIVO_VIOLENCIA)
    
    if _CACHE_ESTUPRO is None:
        print("Carregando Cache Estupro...")
        _CACHE_ESTUPRO = carregar_arquivo_processado(ARQUIVO_ESTUPRO)

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

def calcular_detalhes(df_filtrado, nome_coluna_tipo='NATUREZA'):
    """
    Agrupa por tipo, soma vitimas e calcula porcentagem.
    """
    if df_filtrado is None or df_filtrado.empty:
        return []

    # Verifica se a coluna existe
    if nome_coluna_tipo not in df_filtrado.columns:
        if 'DESCRICAO' in df_filtrado.columns:
            nome_coluna_tipo = 'DESCRICAO'
        else:
            return [] 

    agrupado = df_filtrado.groupby(nome_coluna_tipo)['TOTAL_VITIMAS'].sum().reset_index()
    total_geral = agrupado['TOTAL_VITIMAS'].sum()

    if total_geral == 0:
        return []

    resultados = []
    for _, row in agrupado.iterrows():
        qtd = int(row['TOTAL_VITIMAS'])
        pct = (qtd / total_geral) * 100
        resultados.append({
            "tipo": row[nome_coluna_tipo],
            "qtd": qtd,
            "pct": round(pct, 1)
        })

    resultados.sort(key=lambda x: x['qtd'], reverse=True)
    return resultados

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
        path_shp = os.path.join(DATA_DIR, 'shapefile', nome_shp)

    # Verifica se o shapefile existe antes de tentar ler
    if not os.path.exists(path_shp):
        print(f"ERRO CRÍTICO: Shapefile não encontrado em {path_shp}")
        return "{}"

    gdf = gpd.read_file(path_shp)
    
    col_nome_mapa = 'NM_MUN' 
    for col in gdf.columns:
        if col in ['NM_MUNICIP', 'NM_MUN_2022', 'NOME']: 
            col_nome_mapa = col; break
    gdf['NM_MUN_NORM'] = gdf[col_nome_mapa].apply(normalizar_texto)

    # 5. Merges
    if dados_violencia is not None:
        gdf = gdf.merge(dados_violencia, left_on='NM_MUN_NORM', right_on='MUNICIPIO_NORM', how='left')
        gdf.rename(columns={'TOTAL_VITIMAS': 'violencia'}, inplace=True)
    else:
        gdf['violencia'] = 0

    if dados_estupro is not None:
        gdf = gdf.merge(dados_estupro, left_on='NM_MUN_NORM', right_on='MUNICIPIO_NORM', how='left')
        gdf.rename(columns={'TOTAL_VITIMAS': 'estupro'}, inplace=True)
    else:
        gdf['estupro'] = 0
        
    # Merge População
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
    
    gdf['valor'] = gdf['violencia'] 
    gdf['municipio'] = gdf[col_nome_mapa]

    if gdf.crs != "EPSG:4326": gdf = gdf.to_crs(epsg=4326)

    return gdf.to_json()

def get_dados_municipio(nome_municipio, ano, mes):
    df_viol, df_est = obter_dados_cacheados()
    
    nome_norm = normalizar_texto(nome_municipio)
    ano = int(ano)
    mes = int(mes)

    def filtrar_basico(df_fonte):
        if df_fonte is None: return pd.DataFrame()
        dff = df_fonte[df_fonte['ANO_FATO'] == ano]
        if mes > 0:
            dff = dff[dff['MES_FATO'] == mes]
        dff = dff[dff['MUNICIPIO_NORM'] == nome_norm]
        return dff

    dff_viol = filtrar_basico(df_viol)
    dff_est = filtrar_basico(df_est)

    total_violencia = int(dff_viol['TOTAL_VITIMAS'].sum()) if not dff_viol.empty else 0
    total_estupro = int(dff_est['TOTAL_VITIMAS'].sum()) if not dff_est.empty else 0

    detalhes_violencia = calcular_detalhes(dff_viol, nome_coluna_tipo='NATUREZA')
    detalhes_estupro = calcular_detalhes(dff_est, nome_coluna_tipo='NATUREZA')

    populacao = 1
    path_pop = os.path.join(DATA_DIR, 'csv', 'populacao_pe.csv')
    if os.path.exists(path_pop):
        try:
            df_pop = pd.read_csv(path_pop)
            row = df_pop[df_pop.iloc[:,0].apply(normalizar_texto) == nome_norm]
            if not row.empty:
                populacao = int(row.iloc[0,1])
        except:
            pass

    taxa_violencia = (total_violencia / populacao) * 100000
    taxa_estupro = (total_estupro / populacao) * 100000

    return {
        "municipio": nome_municipio,
        "ano": ano,
        "mes": mes,
        "populacao": populacao,
        "violencia": {
            "total": total_violencia,
            "taxa": round(taxa_violencia, 2),
            "detalhes": detalhes_violencia
        },
        "estupro": {
            "total": total_estupro,
            "taxa": round(taxa_estupro, 2),
            "detalhes": detalhes_estupro
        }
    }