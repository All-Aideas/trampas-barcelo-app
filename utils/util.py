import os
from os import environ
import boto3
import base64
import urllib3
import json
import folium
import io, base64
from PIL import Image
from database.connect import get_datos_resumen_diario, campos_json, insert_dato_prediccion


clave_acceso = environ.get("AWS_API_KEY")
clave_acceso_secreta = environ.get("AWS_SECRET_KEY")
BUCKET_NAME = environ.get("AWS_S3_BUCKET")
API_URL_PREDICT = environ.get("API_URL_PREDICT")

session = boto3.Session(
    aws_access_key_id=clave_acceso,
    aws_secret_access_key=clave_acceso_secreta,
)

s3 = session.client('s3')

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
    with open(f"tmp\{centro}\{nombre_imagen}", "rb") as image_file:
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
    if not os.path.isdir(f"tmp_yolov5\{centro}"):
        os.mkdir(f"tmp_yolov5\{centro}")
    
    ruta_imagen_tmp = f"tmp_yolov5\{centro}\{nombre_imagen}"
    ruta_imagen_bucket = f"yolov5/{centro}/{nombre_imagen}"
    
    img = Image.open(io.BytesIO(base64.decodebytes(bytes(base64_str, "utf-8"))))
    img.save(ruta_imagen_tmp)

    s3.upload_file(ruta_imagen_tmp, BUCKET_NAME, ruta_imagen_bucket, ExtraArgs={'ACL': 'public-read'})
    print("Respuesta de AWS S3")
    url_imagen_yolov5 = '%s/%s/%s' % (s3.meta.endpoint_url, BUCKET_NAME, ruta_imagen_bucket)
    print(url_imagen_yolov5)
    url_imagen_foto_original = '%s/%s/%s' % (s3.meta.endpoint_url, BUCKET_NAME, ruta_imagen_bucket.replace("yolov5/", "foto_original/").replace("_yolov5.jpg", ".jpg"))
    print(url_imagen_foto_original)
    
    return nombre_imagen, url_imagen_foto_original, url_imagen_yolov5


def predict_casos(centro, nombre_imagen):
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
    return aedes, mosquitos, moscas, url_imagen_foto_original, url_imagen_yolov5


def get_lista_centros():
    centros_prevencion = [
        ("MVL001", "Centro Universitario Vicente López", "Carlos Villate 4480, B1605EKT Munro, Provincia de Buenos Aires, Argentina", [-34.53156552888027, -58.519968291402265]),
        ("MVL002", "Hospital Municipal Dr. Bernardo Houssay", "Pres. Hipólito Yrigoyen 1757, Florida, Provincia de Buenos Aires, Argentina", [-34.5217910510323, -58.48992035822424]),
        ("MVL003", "Honorable Concejo Deliberante de Vicente López", "AAF, Av. Maipú 2502, B1636 Olivos, Provincia de Buenos Aires, Argentina", [-34.51214514705522, -58.49007573743368]),
        ("MVL004", "Campo de Deportes Municipal", "Pelliza, Olivos, Provincia de Buenos Aires, Argentina", [-34.512933493180974, -58.50444092807812])
    ]
    return centros_prevencion


def get_casos_por_centro(mapa, fecha=None):
    centros_prevencion = get_lista_centros()
    
    df_resumenes_diario = get_datos_resumen_diario(fecha)
    
    for centro in centros_prevencion:
        aedes_total, mosquitos_total, moscas_total = 0, 0, 0
        aedes, mosquitos, moscas = 0, 0, 0
        filtro = df_resumenes_diario["centro"]==centro[0]
        df_resumen_diario = df_resumenes_diario.where(filtro).dropna()
        if not df_resumen_diario.empty:
            aedes = int(df_resumen_diario.iloc[0]["cantidad_aedes"])
            mosquitos = int(df_resumen_diario.iloc[0]["cantidad_mosquitos"])
            moscas = int(df_resumen_diario.iloc[0]["cantidad_moscas"])
        aedes_total += aedes
        mosquitos_total += mosquitos
        moscas_total += moscas

        texto_resumen = f"<div>Aedes: {aedes_total}</div><div>Mosquito: {mosquitos_total}</div><div>Mosca: {moscas_total}</div>"
        folium.Marker(
            location=centro[3],
            popup=f"<div style='width: 120px'>\
                <b>{centro[1]}</b>\
                {texto_resumen}\
                </div>",
            tooltip=centro[1],
            icon=folium.Icon(color="red", icon="info-sign"),
        ).add_to(mapa)


def get_casos_por_centro_from_s3():
    centros_prevencion = get_lista_centros()

    for centro in centros_prevencion:
        aedes_total, mosquitos_total, moscas_total = 0, 0, 0
        if os.path.exists(f"tmp/{centro[0]}"):
            archivos_en_carpeta = os.listdir(f"tmp/{centro[0]}")
            for nombre_archivo in archivos_en_carpeta:
                aedes, mosquitos, moscas, url_imagen_foto_original, url_imagen_yolov5 = predict_casos(centro[0], nombre_archivo)
                aedes_total += aedes
                mosquitos_total += mosquitos
                moscas_total += moscas

                datos_json = campos_json(centro[0], aedes, mosquitos, moscas, url_imagen_foto_original, url_imagen_yolov5)
                insert_dato_prediccion(centro[0], datos_json)


def descargar_informacion():
    model_name = "tmp"
    prefix_bucket = "foto_original"
    download_objects_from_s3(model_name, prefix_bucket)
