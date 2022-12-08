"""Web App para mostrar mapa de casos en Vicente López.
"""
from flask import Flask, render_template, request
import folium
import json
from utils.util import descargar_informacion, get_casos_por_centro, get_casos_por_centro_from_s3
from database.connect import get_datos_resumen_diario, get_datos_prediccion

app = Flask(__name__)

@app.route('/')
def index():
    """ Página principal de la aplicación.
    """
    return render_template('index.html')


@app.route('/predict')
def predecir():
    descargar_informacion()
    get_casos_por_centro_from_s3()
    #return


@app.route('/mapa')
def mapa():
    """ Mostrar el mapa de Vicente López.
    Latitud: -34.5106, Longitud: -58.4964
    """
    mapa = folium.Map(
        location=[-34.5106, -58.4964],
        zoom_start=13,
    )
    
    marcador_casos(mapa, "2022-12-04")
    return mapa._repr_html_()


@app.route('/detalle-casos')
def detalle_casos():
    """ Mostrar detalle de los casos.
    """
    df_resumen_diario = get_datos_resumen_diario()
    print(df_resumen_diario)
    json_datos_resumen_diario = json.loads(df_resumen_diario.to_json(orient="records"))
    
    json_datos_resumen_diario_detalle = []
    fecha_formato = request.args.get("fecha_formato")
    print(f"Fecha: {fecha_formato}")
    centro = request.args.get("centro")
    print(f"Centro: {centro}")
    
    if fecha_formato is not None and centro is not None:
        df_datos_prediccion = get_datos_prediccion(centro=centro)
        print(df_datos_prediccion)

        #filtro = df_datos_prediccion["centro"]==centro
        #df_datos_prediccion = df_datos_prediccion.where(filtro).dropna()

        json_datos_resumen_diario_detalle = json.loads(df_datos_prediccion.to_json(orient="records"))
    
    return render_template('detalle-casos.html', 
            resumenes_diario_datos=json_datos_resumen_diario,
            resumenes_diario_detalle=json_datos_resumen_diario_detalle)


def marcador_casos(mapa, fecha):
    """ Mostrar los centros y cantidad de casos detectados.
    """
    get_casos_por_centro(mapa, fecha)


if __name__ == "__main__":
    app.run(host="0.0.0.0", load_dotenv=True)
