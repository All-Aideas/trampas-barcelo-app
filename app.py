"""Web App para mostrar mapa de casos en Vicente López.
"""
from flask import Flask, render_template
import folium
from utils.util import download_objects_from_s3, predict_casos, get_casos_por_centro

app = Flask(__name__)

@app.route('/')
def index():
    """ Página principal de la aplicación.
    """
    return render_template('index.html')


@app.route('/mapa')
def mapa():
    """ Mostrar el mapa de Vicente López.
    Latitud: -34.5106, Longitud: -58.4964
    """
    mapa = folium.Map(
        location=[-34.5106, -58.4964],
        zoom_start=13,
    )
    
    marcador_casos(mapa)
    return mapa._repr_html_()


@app.route('/detalle-casos')
def detalle_casos():
    """ Mostrar detalle de los casos.
    """
    return render_template('detalle-casos.html')


def descargar_informacion():
    model_name = "tmp"
    prefix_bucket = ""
    download_objects_from_s3(model_name, prefix_bucket)


def marcador_casos(mapa):
    """ Mostrar los centros y cantidad de casos detectados.
    """
    descargar_informacion()
    get_casos_por_centro(mapa)


if __name__ == "__main__":
    app.run(host="0.0.0.0", load_dotenv=True)
