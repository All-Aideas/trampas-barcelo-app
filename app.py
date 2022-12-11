"""Web App para mostrar mapa de casos en Vicente López.
"""
from flask import Flask, render_template, request
import folium
from utils.util import descargar_informacion, get_casos_por_centro, get_casos_por_centro_from_s3, lista_casos, get_resumen_diario


app = Flask(__name__)


@app.route('/predict')
def predecir():
    """ Descarga las imágenes del repositorio, 
    obtiene el total de aedes, mosquitos y moscas encontradas, 
    y almacena en base de datos.
    """
    descargar_informacion()
    get_casos_por_centro_from_s3()
    return "OK"


@app.route('/resumen_diario')
def obtener_resumen_diario():
    """ Formato esperado: "2022-12-09"
    """
    fecha = request.args.get("fecha")
    get_resumen_diario(fecha)
    return "OK"


@app.route('/')
def index():
    """ Página principal de la aplicación.
    """
    marcador_casos()
    json_datos_resumen_diario, json_datos_resumen_diario_detalle = lista_casos(None, None)
    return render_template('index.html', 
            resumenes_diario_datos=json_datos_resumen_diario,
            resumenes_diario_detalle=json_datos_resumen_diario_detalle)


@app.route('/mapa')
def mostrar_mapa():
    return render_template('mapa.html')


@app.route('/detalle-casos')
def detalle_casos():
    """ Mostrar detalle de los casos.
    """
    fecha_formato = request.args.get("fecha_formato")
    print(f"Fecha: {fecha_formato}")
    centro = request.args.get("centro")
    print(f"Centro: {centro}")
    json_datos_resumen_diario, json_datos_resumen_diario_detalle = lista_casos(fecha_formato, centro)

    return render_template('detalle-casos.html', 
            resumenes_diario_datos=json_datos_resumen_diario,
            resumenes_diario_detalle=json_datos_resumen_diario_detalle)


def marcador_casos(fecha=None):
    """ Mostrar los centros y cantidad de casos detectados.
    """
    mapa = folium.Map(
        location=[-34.5106, -58.4964],
        zoom_start=13,
    )

    get_casos_por_centro(mapa, fecha)
    mapa.save('templates/mapa.html')


if __name__ == "__main__":
    app.run(host="0.0.0.0")
