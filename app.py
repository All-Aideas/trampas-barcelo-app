"""Web App para mostrar mapa de casos en Vicente López.
"""
import os
import folium
from flask import Flask, render_template, request, jsonify
from apscheduler.schedulers.background import BackgroundScheduler
from utils.date_format import get_datetime
from utils.util import PredictPhotosService, DashboardService, PhotosService, DeviceLocationService
from utils.config import SCHEDULER_HORAS, SCHEDULER_MINUTOS

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
app.config['TEMPLATES_AUTO_RELOAD'] = True

def predict_photos():
    """ Descarga las imágenes del repositorio, 
    obtiene el total de aedes, mosquitos y moscas encontradas, 
    y almacena en base de datos.
    """
    try:
        datetime_inicio = get_datetime()
        print(f"Inicia el proceso predict_photos: {datetime_inicio}")

        service = PredictPhotosService()
        list_new_objects = service.get_new_objects()
        
        if list_new_objects is not None:
            data_objects = service.process(list_new_objects)            
            service.resume(data_objects)
            
            key, message, code = "status", "OK", 200
        else:
            key, message, code = "error", "Error durante descarga de objetos del bucket.", 503
    except Exception as e:
        datetime_fin = get_datetime()
        print(f"Error durante el proceso predict_photos: {datetime_fin} {e}")
        key, message, code = "error", str(e), 503
    finally:
        datetime_fin = get_datetime()
        print(f"Finaliza el proceso predict_photos: {datetime_fin}")
        return jsonify({key: message}), code


@app.route('/predict')
def predecir():
    with app.app_context():
        response = predict_photos()
    return response


@app.route('/')
def index():
    """ Página principal de la aplicación.
    """
    dashboard = DashboardService()
    json_datos_resumen_diario = dashboard.get_resumenes()
    return render_template('index.html', 
            resumenes_diario_datos=json_datos_resumen_diario)


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
    
    dashboard = DashboardService()
    json_datos_resumen_diario_detalle = dashboard.get_detalle(foto_fecha=fecha_formato, device_location=centro)
    return jsonify(json_datos_resumen_diario_detalle)


@app.route('/imagen')
def mostrar_image():
    object_key = request.args.get("key")
    photos_service = PhotosService()
    return photos_service.get_image_base64(object_key=object_key)


@app.route('/locations', methods=['POST'])
def locations():
    """
    Descripción:
        Registrar un nuevo municipio.
    """
    try:
        device_location = request.json.get('device_location')
        direccion = request.json.get('direccion')
        latitud = request.json.get('latitud')
        localidad = request.json.get('localidad')
        longitud = request.json.get('longitud')
        nombre_centro = request.json.get('nombre_centro')

        devicelocationservice = DeviceLocationService()
        devicelocationservice.insert_location(device_location, direccion, latitud, localidad, longitud, nombre_centro)
        return jsonify({"status": "OK"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 503


# # Ejecutar periodicamente
# scheduler = BackgroundScheduler()
# scheduler.add_job(predecir, trigger='interval', hours=SCHEDULER_HORAS, minutes=SCHEDULER_MINUTOS)
# scheduler.start()

# Iniciar aplicación
if __name__ == "__main__":
    app.run(host="0.0.0.0")
