from flask import Flask
from flask_cors import CORS
# Importamos o Blueprint que acabamos de criar
from routes.map import map_bp 

app = Flask(__name__)

# Habilita CORS (para o React conseguir acessar)
CORS(app)

# Registra o grupo de rotas.
# Agora a aplicação conhece a URL /api/mapa/...
app.register_blueprint(map_bp)

@app.route('/')
def health_check():
    return "API de Violência Doméstica - PE Online!", 200

if __name__ == '__main__':
    # debug=True é ótimo para desenvolvimento, mostra erros detalhados
    app.run(host='0.0.0.0', port=5015, debug=True)