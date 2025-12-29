from flask import Blueprint, Response, jsonify, request
from services.data_service import carregar_dados_por_ano_mes

# Criamos o "Blueprint". Pense nele como um "grupo de rotas".
# O nome 'map_bp' será usado para registrar lá no main.py
map_bp = Blueprint('map_bp', __name__)

@map_bp.route('/api/mapa/<int:ano>/<int:mes>', methods=['GET'])
def get_mapa_filtrado(ano, mes):
    """
    Rota que retorna o GeoJSON filtrado.
    Exemplo de uso no navegador/React: 
    GET http://localhost:5000/api/mapa/2025/1
    """
    try:
        # Chama a função inteligente do service
        geojson_data = carregar_dados_por_ano_mes(ano, mes)
        
        # Retorna os dados com o cabeçalho correto de JSON
        return Response(geojson_data, mimetype='application/json')

    except FileNotFoundError as e:
        # Retorna erro 404 se não achar o arquivo do ano pedido
        return jsonify({'erro': str(e)}), 404

    except ValueError as e:
        # Retorna erro 400 se o usuário mandar mês 13 ou texto no lugar de número
        return jsonify({'erro': str(e)}), 400

    except Exception as e:
        # Erro genérico de servidor
        print(f"Erro no servidor: {e}")
        return jsonify({'erro': 'Erro interno ao processar mapa.'}), 500