from unidecode import unidecode

def normalizar_texto(texto):
    """Padroniza nomes de cidades (sem acento, maiúsculo)."""
    if not isinstance(texto, str): return ""
    return unidecode(texto).upper().strip()