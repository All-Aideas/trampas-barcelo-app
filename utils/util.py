import os
import base64
import urllib3
import json
import folium
import io, base64
from PIL import Image
# from utils.date_format import get_str_format_from_date_str
from database.connect import get_lista_centros, get_datos_resumen_diario, campos_json, insert_dato_prediccion, get_datos_prediccion, get_timestamp_from_date, get_timestamp_format, insert_resumen_diario
from utils.config import s3, BUCKET_NAME, API_URL_PREDICT, PATH_TEMPORAL, AWS_BUCKET_RAW
import pandas as pd
from datetime import datetime


def download_objects_from_s3(reprocessing:bool=False):
    """
    Descripción:
    Crear el directorio temporal donde estarán las fotos descargadas del bucket.
    Descarga solamente los archivos con extensión JPG y cuya nomenclatura sea igual al de la función is_valid_format().
    Filtra las fotos que ya fueron procesadas por la IA anteriormente. Esto evita reprocesamiento.
    
    Input:
        - reprocessing:bool Si es True, entonces se procesa todas las fotos del bucket. 
                            Si es False, entonces se procesa las nuevas fotos del bucket.
    Output:
        - Lista: Ubicación de cada archivo descargado.
    """
    try:
        carpeta_temporal = PATH_TEMPORAL
        prefix_bucket = AWS_BUCKET_RAW

        # Crea una carpeta temporal para almacenar las descargas
        os.makedirs(carpeta_temporal, exist_ok=True)
        
        objetos = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix_bucket)
        archivos_jpg = [objeto['Key'] for objeto in objetos.get('Contents', []) if objeto['Key'].endswith('.jpg')]
        # print(f"Archivos del bucket: {archivos_jpg}")
        path_files_valid = [file for file in [get_valid_file(archivo_valido) for archivo_valido in archivos_jpg] if file]
        print(f"Archivos JPG con la nomenclatura esperada en el bucket: {path_files_valid}")
        
        if not reprocessing:
            df_fotos_procesadas = get_datos_prediccion()
            if df_fotos_procesadas:
                # print(df_fotos_procesadas[['path_foto_raw']])
                # print(df_fotos_procesadas.columns)
                df_fotos_procesadas = df_fotos_procesadas[['path_foto_raw']]
                path_files_will_be_processed = [elemento for elemento in path_files_valid if elemento not in df_fotos_procesadas['path_foto_raw'].unique()]
                print(f"Archivos JPG que serán procesados {path_files_will_be_processed}")
                path_files_valid = path_files_will_be_processed
        
        if path_files_valid:
            path_files = [download_object(path_file) for path_file in path_files_valid]
        
            # Filtrar los resultados
            lista_path_files = [lista for lista in path_files if lista[1]]
            return lista_path_files
        return []
    except Exception as e:
        print(f"Ocurrió un error durante la descarga de objetos del bucket. Detalle del error: {e}")
        return None


def get_valid_file(full_path:str):
    try:
        partes_ruta = os.path.normpath(full_path).split(os.path.sep)
        nombre_archivo = partes_ruta[-1]
        flag, _ = is_valid_format(nombre_archivo)
        
        if flag:
            print(f"Ubicación de archivo en el bucket: {full_path}")
            return full_path
        return None
    except Exception as e:
        print(f"Ocurrió un error durante la descarga del objeto {full_path} del bucket. Detalle del error: {e}")
        return None


def download_object(full_path_bucket:str):
    try:
        carpeta_temporal = PATH_TEMPORAL
        
        partes_ruta = os.path.normpath(full_path_bucket).split(os.path.sep)
        nombre_archivo = partes_ruta[-1]
        path_file_download = [carpeta_temporal] + partes_ruta[1:-1]
        path_file_download = os.path.join(*path_file_download)
        full_path_file_download = os.path.join(path_file_download, nombre_archivo)
        
        os.makedirs(path_file_download, exist_ok=True)
        s3.download_file(BUCKET_NAME, full_path_bucket, full_path_file_download)
        print(f"Descarga del archivo {full_path_bucket} en {full_path_file_download} de manera exitosa.")
        return full_path_bucket, full_path_file_download
    except Exception as e:
        print(f"Ocurrió un error durante la descarga del objeto {full_path_bucket} del bucket. Detalle del error: {e}")
        return None, None


def encode_img(nombre_imagen):
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
        print(f"File name {full_path} to upload to S3.")

        ruta_normalizada = os.path.normpath(full_path)
        partes_ruta = ruta_normalizada.split(os.path.sep)
        # root_path = ["static", "yolov5"] + partes_ruta[1:-1] # Carpeta donde se encontrarán los archivos procesados.
        root_path = ["yolov5"] + partes_ruta[1:-1] # Carpeta donde se encontrarán los archivos procesados.
        full_path_imagen_tmp = root_path + [partes_ruta[-1]]
        root_path_bucket = ["yolov5"] + partes_ruta[1:]
        root_path_bucket = '/'.join(root_path_bucket)
        ruta_directorio = os.path.join(*root_path)
        full_path_imagen_tmp = os.path.join(*full_path_imagen_tmp)
        
        # Crear el directorio si no existe
        os.makedirs(ruta_directorio, exist_ok=True)
        
        img = Image.open(io.BytesIO(base64.decodebytes(bytes(base64_str, "utf-8"))))
        img.save(full_path_imagen_tmp)

        s3.upload_file(full_path_imagen_tmp, BUCKET_NAME, root_path_bucket, ExtraArgs={'ACL': 'public-read'})
        # s3.upload_file(full_path_imagen_tmp, BUCKET_NAME, root_path_bucket)
        print(f"La imagen se ha subido exitosamente a AWS S3 {root_path_bucket}")
        
        return root_path_bucket
    except Exception as e:
        print(f"Ocurrió un error en la carga de la imagen procesada por YOLO en el bucket. Detalle del error: {e}")
        return None


def get_url_imagen(ruta_imagen_bucket):
    return "%s/%s/%s" % (s3.meta.endpoint_url, BUCKET_NAME, ruta_imagen_bucket)


def predict_casos(nombre_imagen):
    """ Obtiene la cantidad de aedes, mosquitos y moscas detectadas por la inteligencia artificial y almacena la imagen.
    """
    try:
        encoded_string = encode_img(nombre_imagen)
        response = invoke_api(API_URL_PREDICT, encoded_string)
        print('Resultado de API {} para la foto {}'.format(response.status, nombre_imagen))
        response_data = json.loads(response.data.decode('utf-8'))["data"]
        # El primer elemento contiene la imagen.
        # El segundo elemento contiene la metadata.
        response_metadata = response_data[1]["data"]
        print(f'Resultado de API para la foto {nombre_imagen}: {response_metadata}')
        
        response_data_imagen_yolo = response_data[0]
        response_data_imagen_yolo = response_data_imagen_yolo.split("data:image/png;base64,")[1]
        path_foto_yolo = upload_imagen_s3(response_data_imagen_yolo, nombre_imagen.replace(".jpg","_yolov5.jpg"))
        
        if not path_foto_yolo:
            return 0, 0, 0, None, None, None
        
        aedes = int(response_metadata[0][0])
        mosquitos = int(response_metadata[1][0])
        moscas = int(response_metadata[2][0])
        # print(f"nombre_imagen: {nombre_imagen}")
        
        ruta_normalizada = os.path.normpath(nombre_imagen)
        partes_ruta = ruta_normalizada.split(os.path.sep)
        nombre_archivo = partes_ruta[-1]
        
        flag, timestamp = is_valid_format(nombre_archivo)
        if flag:
            foto_fecha = timestamp.strftime('%Y-%m-%d')
            return aedes, mosquitos, moscas, path_foto_yolo, foto_fecha
        return 0, 0, 0, None, None
    except Exception as e:
        print(f"Ocurrió un error en el proceso de invocar el API de YOLO. Detalle del error: {e}")
        return 0, 0, 0, None, None


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
        - None.
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
        # Obtener última fecha procesada
        ultima_fecha_procesada = df_resumenes_diario['foto_fecha'].iloc[0] if fecha is None else fecha
        df_resumenes_diario = df_resumenes_diario[df_resumenes_diario['foto_fecha'] == ultima_fecha_procesada]
        
        # Convertir el diccionario a DataFrame
        centro_df = pd.DataFrame.from_dict(centros_prevencion, orient='index',
                                        columns=['nombre_centro', 'latitud', 'longitud', 'direccion', 'ciudad']).reset_index()

        # Renombrar la columna 'index' a 'centro' para que coincida con el DataFrame original
        centro_df = centro_df.rename(columns={'index': 'centro'})

        # Realizar la unión (merge) por la columna 'centro'
        df_resultado = pd.merge(centro_df, df_resumenes_diario, on='centro', how='left')
        
        columnas_a_llenar_con_cero = ['cantidad_aedes', 'cantidad_mosquitos', 'cantidad_moscas']
        columnas_a_agrupar = ['centro', 'nombre_centro', 'latitud', 'longitud', 'foto_fecha', 'ultima_foto']
        df_resultado = df_resultado[columnas_a_agrupar + columnas_a_llenar_con_cero]

        default_values = {'foto_fecha': ultima_fecha_procesada, 'ultima_foto': '', 'cantidad_aedes': 0, 'cantidad_mosquitos': 0, 'cantidad_moscas': 0}
        df_resultado = df_resultado.fillna(default_values)
        
        resultado_agrupado = df_resultado.groupby(columnas_a_agrupar)[columnas_a_llenar_con_cero].sum().reset_index()
        resultado_agrupado['lat_lng'] = resultado_agrupado.apply(lambda row: [row['latitud'], row['longitud']], axis=1)

        for _, row in resultado_agrupado.iterrows():
            centro_lat_lng = row['lat_lng']
            centro_nombre = row['nombre_centro']
            texto_resumen_imagen = ""

            # url_ultima_foto = 'static/' + row['ultima_foto'] # Visualizar foto en HTML
            url_ultima_foto = row['ultima_foto'] # Visualizar foto en HTML
            # print(url_ultima_foto)
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


def get_casos_por_centro_from_s3(full_path_file_download:list):
    """
    Descripción:
    Analizar cada una de las imágenes descargadas por la IA.
    """
    try:
        print(full_path_file_download)

        for full_path_bucket, full_path in full_path_file_download:
            print(f"ruta: {full_path}")
            partes_ruta = os.path.normpath(full_path).split(os.path.sep)
            print(f"\t{partes_ruta}")
            
            device_location = partes_ruta[1]  # Obtener el código desde la ruta
            device_id = partes_ruta[2] # Obtener el código del dispositivo

            aedes, mosquitos, moscas, path_foto_yolo, foto_fecha = predict_casos(full_path)
            if foto_fecha:
                path_foto_raw = full_path_bucket
                # print(f"path_foto_raw: {path_foto_raw}")
                url_imagen_yolov5 = get_url_imagen(path_foto_yolo)
                # print(f"url_imagen_yolov5: {url_imagen_yolov5}")
                url_imagen_foto_original = get_url_imagen(path_foto_yolo.replace("yolov5/", "raw/").replace("_yolov5.jpg", ".jpg"))
                # print(f"url_imagen_foto_original: {url_imagen_foto_original}")
                
                datos_json = campos_json(device_location, device_id, aedes, mosquitos, moscas, url_imagen_foto_original, url_imagen_yolov5, path_foto_raw, path_foto_yolo, foto_fecha)
                insert_dato_prediccion(device_location, datos_json)
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
