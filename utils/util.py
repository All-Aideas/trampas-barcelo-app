import os
import base64
import urllib3
import json
import folium
import io, base64
from PIL import Image
from database.connect import get_lista_centros, get_datos_resumen_diario, campos_json, insert_dato_prediccion, get_datos_prediccion, get_timestamp_from_date, get_timestamp_format, insert_resumen_diario
from utils.config import s3, BUCKET_NAME, API_URL_PREDICT


def download_objects_from_s3(model_name, prefix_bucket):
    if not os.path.exists(model_name):
        os.mkdir(model_name)
    lista_objetos = s3.list_objects(Bucket=BUCKET_NAME, Prefix=prefix_bucket)['Contents']
    for key in lista_objetos:
        print(f"Key: {key['Key']}")
        file_name = key['Key']
        file_name_downloaded = key['Key'].replace(prefix_bucket, "")
        print(file_name_downloaded)
        file_path = file_name_downloaded.split("/")
        file_path = [x for x in file_path if len(x) > 0]
        print(f"Archivos: {file_path}")
        if len(file_path) > 1:
            if not os.path.exists(f"{model_name}/{file_path[0]}"):
                os.mkdir(f"{model_name}/{file_path[0]}")
            # Es un archivo
            file_name_downloaded = f"{model_name}/{file_name_downloaded}"
            if not os.path.exists(f"{file_name_downloaded}"):
                print(f"Nombre de archivo descargado: {file_name_downloaded}")
                s3.download_file(BUCKET_NAME, file_name, file_name_downloaded)


def encode_img(centro, nombre_imagen):
    with open(os.path.join("tmp", centro, nombre_imagen), "rb") as image_file:
        encoded_string = base64.b64encode(image_file.read())
    return encoded_string.decode('utf-8')


def invoke_api(url, encoded_string):
    headers = {'Content-Type': 'application/json'}
    cadena = f"data:image/jpeg;base64,{encoded_string}"
    data = {"data": ["640", 0.45, 0.75, cadena]}
    body = json.dumps(data).encode('utf-8')
    http = urllib3.PoolManager()
    response = http.request("POST",
                            url,
                            body=body,
                            headers=headers)
    return response


def upload_imagen_s3(base64_str, centro, nombre_imagen):
    if not os.path.isdir(f"tmp_yolov5"):
        os.mkdir(f"tmp_yolov5")
    if not os.path.isdir(os.path.join("tmp_yolov5", centro)):
        os.mkdir(os.path.join("tmp_yolov5", centro))
    
    ruta_imagen_tmp = os.path.join("tmp_yolov5", centro, nombre_imagen)
    ruta_imagen_bucket = os.path.join("yolov5", centro, nombre_imagen)
    
    img = Image.open(io.BytesIO(base64.decodebytes(bytes(base64_str, "utf-8"))))
    img.save(ruta_imagen_tmp)

    s3.upload_file(ruta_imagen_tmp, BUCKET_NAME, ruta_imagen_bucket, ExtraArgs={'ACL': 'public-read'})
    print("Respuesta de AWS S3")
    url_imagen_yolov5 = get_url_imagen(ruta_imagen_bucket)
    print(url_imagen_yolov5)
    url_imagen_foto_original = get_url_imagen(ruta_imagen_bucket.replace("yolov5/", "foto_original/").replace("_yolov5.jpg", ".jpg"))
    print(url_imagen_foto_original)
    
    return nombre_imagen, url_imagen_foto_original, url_imagen_yolov5


def get_url_imagen(ruta_imagen_bucket):
    return "%s/%s/%s" % (s3.meta.endpoint_url, BUCKET_NAME, ruta_imagen_bucket)


def predict_casos(centro, nombre_imagen):
    """ Obtiene la cantidad de aedes, mosquitos y moscas detectadas por la inteligencia artificial y almacena la imagen.
    """
    encoded_string = encode_img(centro, nombre_imagen)
    response = invoke_api(API_URL_PREDICT, encoded_string)
    print('Resultado de API: {}'.format(response.status))
    response_data = json.loads(response.data.decode('utf-8'))["data"]
    
    response_data_imagen_yolo = response_data[0]
    response_data_imagen_yolo = response_data_imagen_yolo.split("data:image/png;base64,")[1]
    _, url_imagen_foto_original, url_imagen_yolov5 = upload_imagen_s3(response_data_imagen_yolo, centro, nombre_imagen.replace(".jpg","_yolov5.jpg"))
    
    response_data_mosquitos = response_data[1]["data"]
    print(response_data_mosquitos)
    aedes = int(response_data_mosquitos[0][0])
    mosquitos = int(response_data_mosquitos[1][0])
    moscas = int(response_data_mosquitos[2][0])
    foto_fecha = nombre_imagen.split("_")[2]
    return aedes, mosquitos, moscas, url_imagen_foto_original, url_imagen_yolov5, foto_fecha


def get_casos_por_centro(mapa, fecha=None):
    centros_prevencion = get_lista_centros()
    
    df_resumenes_diario = get_datos_resumen_diario(fecha)
    
    for centro in centros_prevencion:
        aedes_total, mosquitos_total, moscas_total = 0, 0, 0
        aedes, mosquitos, moscas = 0, 0, 0
        if not df_resumenes_diario.empty:
            filtro = df_resumenes_diario["centro"]==centro[0]
            df_resumen_diario = df_resumenes_diario.where(filtro).dropna()
            if not df_resumen_diario.empty:
                aedes = int(df_resumen_diario.iloc[0]["cantidad_aedes"])
                mosquitos = int(df_resumen_diario.iloc[0]["cantidad_mosquitos"])
                moscas = int(df_resumen_diario.iloc[0]["cantidad_moscas"])
            aedes_total += aedes
            mosquitos_total += mosquitos
            moscas_total += moscas

        icon_config = folium.Icon(color="green", icon="info-sign")
        if aedes_total > 0 or mosquitos_total > 0 or moscas_total > 0:
            icon_config = folium.Icon(color="red", icon="info-sign")

        texto_resumen = f"<div>Aedes: {aedes_total}</div><div>Mosquitos: {mosquitos_total}</div><div>Moscas: {moscas_total}</div>"
        folium.Marker(
            location=centro[3],
            popup=f"<div style='width: 120px'>\
                <b>{centro[1]}</b>\
                {texto_resumen}\
                </div>",
            tooltip=centro[1],
            icon=icon_config,
        ).add_to(mapa)


def get_casos_por_centro_from_s3():
    centros_prevencion = get_lista_centros()

    for centro in centros_prevencion:
        aedes_total, mosquitos_total, moscas_total = 0, 0, 0
        if os.path.exists(os.path.join("tmp", centro[0])):
            archivos_en_carpeta = os.listdir(os.path.join("tmp", centro[0]))
            for nombre_archivo in archivos_en_carpeta:
                aedes, mosquitos, moscas, url_imagen_foto_original, url_imagen_yolov5, foto_fecha = predict_casos(centro[0], nombre_archivo)
                aedes_total += aedes
                mosquitos_total += mosquitos
                moscas_total += moscas

                datos_json = campos_json(centro[0], aedes, mosquitos, moscas, url_imagen_foto_original, url_imagen_yolov5, foto_fecha)
                insert_dato_prediccion(centro[0], datos_json)


def descargar_informacion():
    model_name = "tmp"
    prefix_bucket = "foto_original"
    download_objects_from_s3(model_name, prefix_bucket)


def get_resumen_diario(fecha):
    insert_resumen_diario(fecha)


def marcador_casos(fecha):
    """ Mostrar los centros y cantidad de casos detectados.
    Los marcadores son almacenados en un HTML.
    """
    mapa = folium.Map(
        location=[-34.5106, -58.4964],
        zoom_start=13,
    )
    
    get_casos_por_centro(mapa, fecha)
    mapa.save('templates/mapa.html')


def lista_casos(fecha_formato=None, centro=None):
    """ Mostrar detalle de los casos.
    """
    if fecha_formato is not None:
        fecha_busqueda = get_timestamp_from_date(int(fecha_formato))
        fecha_busqueda = get_timestamp_format(fecha_busqueda, format="%Y-%m-%d")
        marcador_casos(fecha_busqueda)

    df_resumen_diario = get_datos_resumen_diario()
    json_datos_resumen_diario = json.loads(df_resumen_diario.to_json(orient="records"))
    
    json_datos_resumen_diario_detalle = []
    if fecha_formato is not None and centro is not None:
        df_datos_prediccion = get_datos_prediccion(fecha=fecha_formato, centro=centro)
        json_datos_resumen_diario_detalle = json.loads(df_datos_prediccion.to_json(orient="records"))

    return json_datos_resumen_diario, json_datos_resumen_diario_detalle
