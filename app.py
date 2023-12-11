"""Web App para mostrar mapa de casos en Vicente López.
"""
from flask import Flask, render_template, request, jsonify
# from apscheduler.schedulers.background import BackgroundScheduler
import os
import folium
from utils.util import download_objects_from_s3, get_casos_por_centro, get_casos_por_centro_from_s3, lista_casos, get_resumen_diario, show_image

file_env = open(".env", "r")
file_config = open(os.path.join("static", "config.js"), "w")
for var_env in file_env:
    # Eliminar espacios en blanco al principio y al final de la línea
    var_env = var_env.strip()
    # Verificar si la línea no está en blanco
    if var_env:
        if var_env.startswith("FIREBASE") or var_env.startswith("AWS_S3_BUCKET"):
            print("export const " + var_env.replace("=", " = '") + "'")
            file_config.write("export const " + var_env.replace("=", " = '") + "'\n")
file_config.close()

app = Flask(__name__)


def predict_photos():
    """ Descarga las imágenes del repositorio, 
    obtiene el total de aedes, mosquitos y moscas encontradas, 
    y almacena en base de datos.
    """
    try:
        with app.app_context():
            full_path_file_download = download_objects_from_s3()
            if full_path_file_download is not None:
                get_casos_por_centro_from_s3(full_path_file_download)
                return jsonify({"status": "OK"}), 200
            return jsonify({"error": "Error durante descarga de objetos del bucket."}), 503
    except Exception as e:
        return jsonify({"error": str(e)}), 503


@app.route('/predict')
def predecir():
    return predict_photos()


@app.route('/resumen_diario')
def obtener_resumen_diario():
    """ Contabilizar aedes, mosquitos y moscas encontradas en todo un día.
    Consulta la metadata de las fotos por device_id y device_location.
    Almacena la suma de aedes, mosquitos y moscas en base de datos.
    Input:
        - fecha: Formato esperado YYYY-MM-DD. Ejemplo: "2022-12-09"
    """
    try:
        fecha = request.args.get("fecha")
        get_resumen_diario(fecha)
        return jsonify({"status": "OK"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 503


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
    centro = request.args.get("centro")
    print(f"Fecha: {fecha_formato}. Centro: {centro}")
    json_datos_resumen_diario, json_datos_resumen_diario_detalle = lista_casos(fecha_formato, centro)

    return render_template('detalle-casos.html', 
            resumenes_diario_datos=json_datos_resumen_diario,
            resumenes_diario_detalle=json_datos_resumen_diario_detalle)


@app.route('/imagen')
def mostrar_image():
    object_key = request.args.get("key")
    return show_image(object_key)

def marcador_casos(fecha=None):
    """ Mostrar los centros y cantidad de casos detectados.
    """
    mapa = folium.Map(
        location=[-34.5106, -58.4964],
        zoom_start=13,
    )

    get_casos_por_centro(mapa, fecha)
    mapa.save('templates/mapa.html')

# Ejecutar periodicamente
# scheduler = BackgroundScheduler()
# scheduler.add_job(predict_photos, trigger='interval', hours=0, minutes=5)
# scheduler.start()

# Iniciar aplicación
if __name__ == "__main__":
    app.run(host="0.0.0.0")
