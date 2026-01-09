from utils.normalizer_str import normalizar_texto 
import pandas as pd

def padronizar_municipios(df):
    colunas_possiveis = ['MUNICÍPIO DO FATO', 'MUNICIPIO DO FATO', 'MUNICÍPIO', 'MUNICIPIO']
    
    col_alvo = None
    
    # 1. Tenta achar o nome exato
    for col in colunas_possiveis:
        if col in df.columns:
            col_alvo = col
            break
            
    # 2. Se não achou, busca parcial (fallback)
    if not col_alvo:
        for col in df.columns:
            if 'MUNICÍPIO' in col.upper() or 'MUNICIPIO' in col.upper():
                col_alvo = col
                break
    
    if col_alvo:
        df['MUNICIPIO_NORM'] = df[col_alvo].apply(normalizar_texto)
    else:
        print("AVISO: Coluna de município não encontrada.")
        df['MUNICIPIO_NORM'] = "DESCONHECIDO"
        
    return df