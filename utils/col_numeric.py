import pandas as pd
def tratar_metricas_vitimas(df):
    col_vitimas = 'TOTAL DE VÍTIMAS'
    
    if col_vitimas in df.columns:
        df['TOTAL_VITIMAS'] = pd.to_numeric(df[col_vitimas], errors='coerce').fillna(1).astype(int)
    else:
        df['TOTAL_VITIMAS'] = 1  
    return df