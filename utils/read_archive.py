import os
import pandas as pd

def ler_arquivo_bruto(data_dir, nome_arquivo):
    caminho = os.path.join(data_dir, nome_arquivo)
    
    if not os.path.exists(caminho):
        caminho_csv = caminho.replace('.xlsx', '.csv')
        if os.path.exists(caminho_csv):
            caminho = caminho_csv
        else:
            print(f"ERRO: Arquivo não encontrado: {nome_arquivo}")
            return None

    print(f"--- Carregando bruto: {os.path.basename(caminho)} ---")
    
    try:
        if caminho.endswith('.csv'):
            # Dica: Adicione sep=';' ou encoding='latin1' aqui se necessário no futuro
            return pd.read_csv(caminho)
        else:
            return pd.read_excel(caminho)
    except Exception as e:
        print(f"Erro de I/O em {nome_arquivo}: {e}")
        return None