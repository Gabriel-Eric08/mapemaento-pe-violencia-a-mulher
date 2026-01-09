import pandas as pd

def enriquecer_datas(df):
    col_data = 'DATA DO FATO'

    if col_data in df.columns:
        df['DATA0OBJ'] = pd.to_datetime(df[col_data], errors='coerce', dayfirst=True)
        df['ANO_FATO'] = df['DATA0OBJ'].dt.year.fillna(0).astype(int)
        df['MES_FATO'] = df['DATA0OBJ'].dt.month.fillna(0).astype(int)
    else:
        df['ANO_FATO'] = 0
        df['MES_FATO'] = 0
    return df