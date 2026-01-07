from flask import Blueprint, Response, jsonify, request
from services.data_service import carregar_dados_por_ano_mes

map_bp = Blueprint('map_bp', __name__)

@map_bp.route('/api/mapa/<int:ano>/<int:mes>', methods=['GET'])
def get_mapa_filtrado(ano, mes):
    try:
        # A mágica acontece aqui dentro
        geojson_data = carregar_dados_por_ano_mes(ano, mes)
        
        return Response(geojson_data, mimetype='application/json')

    except FileNotFoundError as e:
        return jsonify({'erro': str(e)}), 404

    except ValueError as e:
        return jsonify({'erro': str(e)}), 400

    except Exception as e:
        print(f"Erro no servidor: {e}")
        return jsonify({'erro': 'Erro interno ao processar mapa.'}), 500
    
@map_bp.route('/api/comparar', methods=['POST'])
def comparar_municipios():
    try:
        data = request.json
        # Espera receber: { "cenarioA": {municipio, ano, mes}, "cenarioB": {...} }
        
        req_a = data.get('cenarioA')
        req_b = data.get('cenarioB')

        # Importe a função nova aqui ou no topo
        from services.data_service import get_dados_municipio

        res_a = get_dados_municipio(req_a['municipio'], req_a['ano'], req_a['mes'])
        res_b = get_dados_municipio(req_b['municipio'], req_b['ano'], req_b['mes'])

        return jsonify({
            "cenarioA": res_a,
            "cenarioB": res_b
        })

    except Exception as e:
        print(f"Erro na comparação: {e}")
        return jsonify({"erro": str(e)}), 500