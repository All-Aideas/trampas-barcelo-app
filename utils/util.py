import os
import base64
import urllib3
import json
import folium
import io, base64
from PIL import Image
from utils.date_format import get_str_format_from_date_str
from database.connect import get_lista_centros, get_datos_resumen_diario, campos_json, insert_dato_prediccion, get_datos_prediccion, get_timestamp_from_date, get_timestamp_format, insert_resumen_diario
from utils.config import s3, BUCKET_NAME, API_URL_PREDICT, PATH_TEMPORAL, AWS_BUCKET_RAW
import pandas as pd
from datetime import datetime


def download_objects_from_s3():
    """
    Descripción:
    Crear el directorio temporal donde estarán las fotos descargadas del bucket.
    Descarga solamente los archivos con extensión JPG y cuya nomenclatura sea igual al de la función is_valid_format().
    
    Input:
        - None.
    Output:
        - Lista: Ubicación de cada archivo descargado.
    """
    try:
        model_name = PATH_TEMPORAL
        prefix_bucket = AWS_BUCKET_RAW

        # Crea una carpeta temporal para almacenar las descargas
        if not os.path.exists(model_name):
            os.mkdir(model_name)
        carpeta_temporal = model_name
        print(f"carpeta_temporal: {carpeta_temporal}")
        
        objetos = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix_bucket)
        archivos_jpg = [objeto['Key'] for objeto in objetos.get('Contents', []) if objeto['Key'].endswith('.jpg')]
        print(archivos_jpg)
        files_downloaded = [file for file in [get_valid_files(archivo_valido, carpeta_temporal) for archivo_valido in archivos_jpg] if file is not None]
        return files_downloaded
    except Exception as e:
        print(f"Ocurrió un error durante la descarga de objetos del bucket. Detalle del error: {e}")
        return None


def get_valid_files(full_path:str, carpeta_temporal:str):
    try:
        full_path_split = full_path.split('/')
        nombre_archivo = full_path_split[-1]
        path = os.path.join(*full_path_split[1:-1])

        carpeta_destino = os.path.join(carpeta_temporal, path)
        flag, _ = is_valid_format(nombre_archivo)
        
        if flag:
            destino_archivo_local = os.path.join(carpeta_destino, nombre_archivo)
            os.makedirs(carpeta_destino, exist_ok=True)
            s3.download_file(BUCKET_NAME, full_path, destino_archivo_local)
            print(f"Ubicación de archivo descargado: {destino_archivo_local}")
            return destino_archivo_local
        return None
    except Exception as e:
        print(f"Ocurrió un error durante la descarga del objeto {full_path} del bucket. Detalle del error: {e}")
        return None


def encode_img(nombre_imagen):
    # with open(os.path.join("tmp", centro, nombre_imagen), "rb") as image_file:
    with open(nombre_imagen, "rb") as image_file:
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


def upload_imagen_s3(base64_str, full_path):
    try:
        print(f"File name to upload to S3: {full_path}")

        ruta_normalizada = os.path.normpath(full_path)
        partes_ruta = ruta_normalizada.split(os.path.sep)
        root_path = ["tmp_yolov5"] + partes_ruta[1:-1]
        # print(root_path)
        full_path_imagen_tmp = root_path + [partes_ruta[-1]]
        # print(full_path_imagen_tmp)
        root_path_bucket = ["yolov5"] + partes_ruta[1:]
        root_path_bucket = '/'.join(root_path_bucket)
        # print(root_path_bucket)
        
        ruta_directorio = os.path.join(*root_path)
        # print(f"ruta_directorio: {ruta_directorio}")
        full_path_imagen_tmp = os.path.join(*full_path_imagen_tmp)
        # print(f"ruta_directorio: {full_path_imagen_tmp}")

        # Crear el directorio si no existe
        if not os.path.exists(ruta_directorio):
            os.makedirs(ruta_directorio)
        
        img = Image.open(io.BytesIO(base64.decodebytes(bytes(base64_str, "utf-8"))))
        img.save(full_path_imagen_tmp)

        # s3.upload_file(full_path_imagen_tmp, BUCKET_NAME, root_path_bucket, ExtraArgs={'ACL': 'public-read'})
        print(f"La imagen se ha subido exitosamente a AWS S3 {root_path_bucket}")
        
        url_imagen_yolov5 = get_url_imagen(root_path_bucket)
        print(url_imagen_yolov5)
        url_imagen_foto_original = get_url_imagen(root_path_bucket.replace("yolov5/", "raw/").replace("_yolov5.jpg", ".jpg"))
        print(url_imagen_foto_original)
        
        return url_imagen_foto_original, url_imagen_yolov5
    except Exception as e:
        print(f"Ocurrió un error en la carga de la imagen procesada por YOLO en el bucket. Detalle del error: {e}")
        return None, None


def get_url_imagen(ruta_imagen_bucket):
    return "%s/%s/%s" % (s3.meta.endpoint_url, BUCKET_NAME, ruta_imagen_bucket)


def predict_casos(nombre_imagen):
    """ Obtiene la cantidad de aedes, mosquitos y moscas detectadas por la inteligencia artificial y almacena la imagen.
    """
    try:
        encoded_string = encode_img(nombre_imagen)
        response = invoke_api(API_URL_PREDICT, encoded_string)
        print('Resultado de API: {}'.format(response.status))
        response_data = json.loads(response.data.decode('utf-8'))["data"]
        # El primer elemento contiene la imagen.
        # El segundo elemento contiene la metadata.
        print(f'Resultado de API para la foto {nombre_imagen}: {response_data[1]["data"]}')
        
        response_data_imagen_yolo = response_data[0]
        response_data_imagen_yolo = response_data_imagen_yolo.split("data:image/png;base64,")[1]
        url_imagen_foto_original, url_imagen_yolov5 = upload_imagen_s3(response_data_imagen_yolo, nombre_imagen.replace(".jpg","_yolov5.jpg"))
        
        if not url_imagen_foto_original:
            return 0, 0, 0, None, None, None
        
        response_data_mosquitos = response_data[1]["data"]
        print(response_data_mosquitos)
        aedes = int(response_data_mosquitos[0][0])
        mosquitos = int(response_data_mosquitos[1][0])
        moscas = int(response_data_mosquitos[2][0])
        print(f"nombre_imagen: {nombre_imagen}")
        
        ruta_normalizada = os.path.normpath(nombre_imagen)
        partes_ruta = ruta_normalizada.split(os.path.sep)
        # root_path = ["tmp_yolov5"] + partes_ruta[1:-1]
        # full_path_split = full_path.split('/')
        nombre_archivo = partes_ruta[-1]
        # carpeta_destino = os.path.join(carpeta_temporal, path)
        flag, timestamp = is_valid_format(nombre_archivo)
        print(timestamp)
        foto_fecha = timestamp.strftime('%Y-%m-%d')

        # foto_fecha = timestamp#nombre_imagen.split("_")[2].replace(".jpg", "")
        return aedes, mosquitos, moscas, url_imagen_foto_original, url_imagen_yolov5, foto_fecha
    except Exception as e:
        print(f"Ocurrió un error en el proceso de invocar el API de YOLO. Detalle del error: {e}")
        return 0, 0, 0, None, None, None


def get_casos_por_centro(mapa, fecha=None):
    # fecha='2023-12-30'
    """
    Descripción:
    Mostrar la suma de cantidades de aedes, mosquitos y moscas por cada ubicación en un día específico.
    Si no se especifica el día de consulta, entonces retorna el resumen de la última fecha registrada.
    Si el valor de aedes es mayor a 0, entonces mostrará el punto en el mapa de color rojo.
    Si no existen fotos que hayan sido procesadas por la IA, entonces se mostrarán los puntos de color verde en el mapa y las cantidades serán 0.
    Ejemplo: Para el día 01/01/24 hubo 1 aedes, 1 mosquito y 1 mosca.
    Input:
        - mapa: Objeto mapa.
        - fecha: Formato YYYY-MM-DD.
    Output:
    """
    centros_prevencion = get_lista_centros()
    print(centros_prevencion)

    df_resumenes_diario = get_datos_resumen_diario(fecha)
    print(df_resumenes_diario)
    if df_resumenes_diario.empty:
        for centro in centros_prevencion.keys():
            centro_lat = centros_prevencion[centro][1]
            centro_lng = centros_prevencion[centro][2]
            centro_lat_lng = [centro_lat, centro_lng]
            centro_nombre = centros_prevencion[centro][0]
            set_market(mapa, lat_lng=centro_lat_lng, 
                    name=centro_nombre)
    else:
        # Obtener fecha o última fecha procesada
        ultima_fecha_procesada = df_resumenes_diario['fecha'].iloc[0] if fecha is None else fecha
        df_resumenes_diario = df_resumenes_diario[df_resumenes_diario['fecha'] == ultima_fecha_procesada]
        
        # Convertir el diccionario a DataFrame
        centro_df = pd.DataFrame.from_dict(centros_prevencion, orient='index',
                                        columns=['nombre_centro', 'latitud', 'longitud', 'direccion', 'ciudad']).reset_index()

        # Renombrar la columna 'index' a 'centro' para que coincida con el DataFrame original
        centro_df = centro_df.rename(columns={'index': 'centro'})

        # Realizar la unión (merge) por la columna 'centro'
        df_resultado = pd.merge(centro_df, df_resumenes_diario, on='centro', how='left')
        
        columnas_a_llenar_con_cero = ['cantidad_aedes', 'cantidad_mosquitos', 'cantidad_moscas']
        columnas_a_agrupar = ['centro', 'nombre_centro', 'latitud', 'longitud', 'fecha', 'ultima_foto']
        df_resultado = df_resultado[columnas_a_agrupar + columnas_a_llenar_con_cero]

        default_values = {'fecha': ultima_fecha_procesada, 'ultima_foto': '', 'cantidad_aedes': 0, 'cantidad_mosquitos': 0, 'cantidad_moscas': 0}
        df_resultado = df_resultado.fillna(default_values)
        # print(df_resultado.groupby(columnas_a_agrupar)[columnas_a_llenar_con_cero].sum())
        resultado_agrupado = df_resultado.groupby(columnas_a_agrupar)[columnas_a_llenar_con_cero].sum().reset_index()
        resultado_agrupado['lat_lng'] = resultado_agrupado.apply(lambda row: [row['latitud'], row['longitud']], axis=1)

        for _, row in resultado_agrupado.iterrows():
            centro_lat_lng = row['lat_lng']
            centro_nombre = row['nombre_centro']
            texto_resumen_imagen = ""

            # timestamp_value = None
            # if fecha is None:
                # centro_codigo = row['centro']
                # fecha_formato = row['fecha_formato']#df_resumen_diario.iloc[0]["fecha_formato"]
                # fecha_formato = get_str_format_from_date_str(fecha_formato, format_old="%d/%m/%Y", format_new="%Y-%m-%d")
            
            # url_ultima_foto = get_ultima_foto(timestamp_value=timestamp_value, centro_codigo=centro_codigo)
            url_ultima_foto = row['ultima_foto']
            print(url_ultima_foto)
            texto_resumen_imagen = f"<div>Última foto tomada el día {ultima_fecha_procesada}<img id='resumen_diario_ultima_foto_yolov5' class='img-fluid' src='{url_ultima_foto}' width='100%' /></div>"
            texto_resumen_no_imagen = f"<div>No hay fotos del día {ultima_fecha_procesada}.</div>"
            texto_resumen_imagen = texto_resumen_imagen if len(url_ultima_foto) > 0 else texto_resumen_no_imagen
            mostrar_descripcion = True if len(url_ultima_foto) > 0 else False

            aedes_total, mosquitos_total, moscas_total = row['cantidad_aedes'], row['cantidad_mosquitos'], row['cantidad_moscas']
            set_market(mapa, lat_lng=centro_lat_lng, 
                    name=centro_nombre, 
                    description=texto_resumen_imagen, 
                    show_description=mostrar_descripcion,
                    aedes_total=aedes_total, mosquitos_total=mosquitos_total, moscas_total=moscas_total)


def set_market(mapa, lat_lng:list, name:str, description:str="", show_description:bool=False, aedes_total:int=0, mosquitos_total:int=0, moscas_total:int=0):
    """
    Descripción:
    Asignar los puntos de ubicación en el mapa.
    """
    icon_config = folium.Icon(color="green", icon="info-sign")
    if aedes_total > 0:
        icon_config = folium.Icon(color="red", icon="info-sign")
    
    texto_resumen = f"<div>Aedes: {aedes_total}</div><div>Mosquitos: {mosquitos_total}</div><div>Moscas: {moscas_total}</div>{description}"
    popup_width = "360" if show_description else "180"
    folium.Marker(
        location=lat_lng,
        popup=f"<div style='width: {popup_width}px'>\
            <b>{name}</b>\
            {texto_resumen}\
            </div>",
        tooltip=name,
        icon=icon_config,
    ).add_to(mapa)


def get_casos_por_centro_from_s3(files_downloaded:list):
    """
    Descripción:
    Analizar cada una de las imágenes descargadas por la IA.
    """
    try:
        # centros_prevencion = get_lista_centros()
        # print(centros_prevencion)
        grupos_por_codigo = {}

        for ruta in files_downloaded:
            codigo = ruta.split('\\')[1]  # Obtener el código desde la ruta
            grupos_por_codigo.setdefault(codigo, []).append(ruta)

        # El resultado es un diccionario donde las claves son los códigos y los valores son listas de rutas
        print(grupos_por_codigo)

        for codigo in grupos_por_codigo.keys():
            archivos = grupos_por_codigo[codigo]
            for file_path in archivos:
                print(file_path)
                # print(type(file_path))
                aedes_total, mosquitos_total, moscas_total = 0, 0, 0
                # if os.path.exists(os.path.join("tmp", centro[0])):
                #     print()
                # archivos_en_carpeta = os.listdir(os.path.join("tmp", centro[0]))
                # for nombre_archivo in archivos_en_carpeta:
                aedes, mosquitos, moscas, url_imagen_foto_original, url_imagen_yolov5, foto_fecha = predict_casos(file_path)
                
                if foto_fecha:
                    aedes_total += aedes
                    mosquitos_total += mosquitos
                    moscas_total += moscas

                    datos_json = campos_json(codigo, aedes, mosquitos, moscas, url_imagen_foto_original, url_imagen_yolov5, foto_fecha)
                    insert_dato_prediccion(codigo, datos_json)
    except Exception as e:
        print(f"Ocurrió un error durante el proceso de análisis de las imágenes del bucket. Detalle del error: {e}")


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
        marcador_casos(fecha_formato)

    df_resumen_diario = get_datos_resumen_diario()
    json_datos_resumen_diario = json.loads(df_resumen_diario.to_json(orient="records"))
    
    json_datos_resumen_diario_detalle = []
    if fecha_formato is not None and centro is not None:
        df_datos_prediccion = get_datos_prediccion(fecha=fecha_formato, centro=centro)
        json_datos_resumen_diario_detalle = json.loads(df_datos_prediccion.to_json(orient="records"))

    return json_datos_resumen_diario, json_datos_resumen_diario_detalle


def get_ultima_foto(timestamp_value=None, centro_codigo=None):
    """ Obtiene la última foto procesada a partir de la fecha y el código del centro.
    Input:
    - timestamp_value: Fecha en timestamp.
    - centro_codigo: Código del centro.
    Output:
    - URL de la imagen procesada por la inteligencia artificial.
    """
    df_datos_prediccion = get_datos_prediccion(dato_prediccion=centro_codigo, fecha=timestamp_value, centro=centro_codigo)
    return df_datos_prediccion.iloc[0]["foto_yolov5"]


def is_valid_format(nombre_archivo):
    """Validar que el formato del archivo sea igual al indicado en la constante PATRON_FORMATO."""
    PATRON_FORMATO = '%Y-%m-%dT%H-%M-%S.jpg'

    try:
        timestamp = datetime.strptime(nombre_archivo, PATRON_FORMATO)
        print(f"El timestamp obtenido: {timestamp}")
        return True, timestamp
    except ValueError:
        print("El formato del nombre del archivo no es válido.")
        return False, None
