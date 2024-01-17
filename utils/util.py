import os
import base64
import urllib3
import json
import folium
import base64
from database.connect import PrediccionesFotoRepository, ConnectBucket, ConnectDataBase, get_timestamp_from_date, get_timestamp_format, LocationsRepository
from utils.config import API_URL_PREDICT, AWS_BUCKET_RAW
import pandas as pd
from datetime import datetime


class DeviceLocationService():

    def __init__(self):
        self.repository = LocationsRepository()

    def all_data(self):
        data = self.repository.all_data()
        resultado = {item['device_location']: item for item in data}
        return resultado

    def insert_location(self, device_location, direccion, latitud, localidad, longitud, nombre_centro):
        self.repository.add_location(device_location, direccion, latitud, localidad, longitud, nombre_centro)


connectdb = ConnectDataBase()
conncets3 = ConnectBucket()
prediccionesfoto_repository = PrediccionesFotoRepository()

class PhotosService():
    
    def get_image_base64(self, object_key):
        return conncets3.get_image_base64(object_key=object_key)



def predict_objects_from_s3(reprocessing:bool=False):
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
        archivos_jpg = []
        
        lista_centros = connectdb.get_lista_centros()
        lista_centros = list(lista_centros.keys())
        
        for location in lista_centros:
            prefix_bucket = f"{AWS_BUCKET_RAW}/{location}"

            pages = conncets3.get_objects(prefix_bucket=prefix_bucket)
        
            for page in pages:
                archivos_jpg.extend([objeto['Key'] for objeto in page.get('Contents', []) if objeto['Key'].lower().endswith('.jpg')])

        #print(f"Archivos JPG que se encuentran en el bucket: {archivos_jpg}")
        
        path_files_valid = [file for file in [get_valid_file(archivo_valido) for archivo_valido in archivos_jpg] if file]
        print(f"Archivos JPG con la nomenclatura esperada en el bucket: {path_files_valid}")
        
        if not reprocessing:
            df_fotos_procesadas = connectdb.get_datos_prediccion()
            if not df_fotos_procesadas.empty:
                df_fotos_procesadas = df_fotos_procesadas[['path_foto_raw']]
                path_files_will_be_processed = [elemento for elemento in path_files_valid if elemento not in df_fotos_procesadas['path_foto_raw'].unique()]
                print(f"Archivos JPG que serán procesados {path_files_will_be_processed}")
                path_files_valid = path_files_will_be_processed
        
        if path_files_valid:
            return path_files_valid
        return []
    except Exception as e:
        print(f"Ocurrió un error durante la descarga de objetos del bucket. Detalle del error: {e}")
        return None


def get_valid_file(full_path:str):
    try:
        partes_ruta = os.path.normpath(full_path).split(os.path.sep)
        device_location = partes_ruta[1]
        nombre_archivo = partes_ruta[-1]
        flag, _ = is_valid_format(nombre_archivo)
        
        if flag:
            # print(f"Ubicación de archivo en el bucket: {full_path}")
            return full_path
        return None
    except Exception as e:
        print(f"Ocurrió un error durante la validación del objeto {full_path} del bucket. Detalle del error: {e}")
        return None


def invoke_api(url, encoded_string):
    headers = {'Content-Type': 'application/json'}
    cadena = f"data:image/jpeg;base64,{encoded_string}"
    data = {"data": ["640", 0.45, 0.83, cadena]}
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
        root_path = ["yolov5"] + partes_ruta[1:-1] # Carpeta donde se encontrarán los archivos procesados.
        full_path_imagen_tmp = root_path + [partes_ruta[-1]]
        root_path_bucket = ["yolov5"] + partes_ruta[1:]
        root_path_bucket = '/'.join(root_path_bucket)
        full_path_imagen_tmp = os.path.join(*full_path_imagen_tmp)
        
        image_data = base64.decodebytes(bytes(base64_str, "utf-8"))

        conncets3.upload(image_data=image_data, root_path_bucket=root_path_bucket)
        
        return root_path_bucket
    except Exception as e:
        print(f"Ocurrió un error en la carga de la imagen procesada por YOLO en el bucket. Detalle del error: {e}")
        return None


def predict_casos(nombre_imagen, encoded_string):
    """ Obtiene la cantidad de aedes, mosquitos y moscas detectadas por la inteligencia artificial y almacena la imagen.
    """
    try:
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
        
        ruta_normalizada = os.path.normpath(nombre_imagen)
        partes_ruta = ruta_normalizada.split(os.path.sep)
        nombre_archivo = partes_ruta[-1]
        
        flag, timestamp = is_valid_format(nombre_archivo)
        if flag:
            foto_date = timestamp.strftime('%Y-%m-%d')
            foto_datetime = timestamp.strftime('%Y-%m-%d %H:%M:%S')
            return aedes, mosquitos, moscas, path_foto_yolo, foto_date, foto_datetime
        return 0, 0, 0, None, None, None
    except Exception as e:
        print(f"Ocurrió un error en el proceso de invocar el API de YOLO. Detalle del error: {e}")
        return 0, 0, 0, None, None, None


def get_casos_por_centro(mapa, fecha=None):
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
    #centros_prevencion = connectdb.get_lista_centros()
    devicelocationservice = DeviceLocationService()
    centros_prevencion = devicelocationservice.all_data()
    
    df_resumenes_diario = connectdb.get_datos_resumen_diario(fecha, centros=centros_prevencion)
    # print(df_resumenes_diario)
    if df_resumenes_diario.empty:
        for centro in centros_prevencion.keys():
            centro_lat = centros_prevencion[centro]["latitud"]#centros_prevencion[centro][1]
            centro_lng = centros_prevencion[centro]["longitud"]#centros_prevencion[centro][2]
            centro_lat_lng = [centro_lat, centro_lng]
            centro_nombre = centros_prevencion[centro]["nombre_centro"]#centros_prevencion[centro][0]
            set_market(mapa, lat_lng=centro_lat_lng, 
                    name=centro_nombre)
    else:
        # Obtener última fecha procesada
        ultima_fecha_procesada = df_resumenes_diario['foto_fecha'].iloc[0] if fecha is None else fecha
        df_resumenes_diario = df_resumenes_diario[df_resumenes_diario['foto_fecha'] == ultima_fecha_procesada]
        
        # Convertir el diccionario a DataFrame
        centro_df = pd.DataFrame.from_dict(centros_prevencion, orient='index',
                                        columns=['nombre_centro', 'latitud', 'longitud', 'direccion', 'localidad']).reset_index()

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
            image_base64 = ""

            url_ultima_foto = row['ultima_foto'] # Visualizar foto en HTML
            
            if url_ultima_foto:
                image_base64 = conncets3.get_image_base64(url_ultima_foto)
            texto_resumen_imagen = f"<div>Última foto tomada el día {ultima_fecha_procesada}<img id='resumen_diario_ultima_foto_yolov5' class='img-fluid' src='data:image/jpeg;base64,{image_base64}' width='100%' /></div>"

            texto_resumen_no_imagen = f"<div>No hay fotos del día {ultima_fecha_procesada}.</div>"
            texto_resumen_imagen = texto_resumen_imagen if len(image_base64) > 0 else texto_resumen_no_imagen
            mostrar_descripcion = True if len(image_base64) > 0 else False

            aedes_total, mosquitos_total, moscas_total = int(row['cantidad_aedes']), int(row['cantidad_mosquitos']), int(row['cantidad_moscas'])
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
        fechas_fotos_device_locations = set() # Lista de las fechas de las fotos que fueron procesadas correctamente.

        for full_path_bucket in full_path_file_download:
            image_base64 = conncets3.get_image_base64(full_path_bucket)
            aedes, mosquitos, moscas, path_foto_yolo, foto_fecha, foto_datetime = predict_casos(full_path_bucket, image_base64)

            partes_ruta = os.path.normpath(full_path_bucket).split(os.path.sep)
            
            device_location = partes_ruta[1]  # Obtener el código desde la ruta
            device_id = partes_ruta[2] # Obtener el código del dispositivo

            if foto_fecha:
                fechas_fotos_device_locations.add((foto_fecha, device_location))

                path_foto_raw = full_path_bucket
                
                url_imagen_yolov5 = conncets3.get_url_imagen(path_foto_yolo)
                
                url_imagen_foto_original = conncets3.get_url_imagen(path_foto_yolo.replace("yolov5/", "raw/").replace("_yolov5.jpg", ".jpg"))
                
                #datos_json = connectdb.campos_json(device_location, device_id, aedes, mosquitos, moscas, url_imagen_foto_original, url_imagen_yolov5, path_foto_raw, path_foto_yolo, foto_fecha, foto_datetime)
                #connectdb.insert_dato_prediccion(device_location, datos_json)
                prediccionesfoto_repository.add_prediction(device_location, device_id, aedes, mosquitos, moscas, url_imagen_foto_original, url_imagen_yolov5, path_foto_raw, path_foto_yolo, foto_fecha, foto_datetime)
        return fechas_fotos_device_locations
    except Exception as e:
        print(f"Ocurrió un error durante el proceso de análisis de las imágenes del bucket. Detalle del error: {e}")
        return fechas_fotos_device_locations


def contabilizar_resumen_diario(fecha, device_location):
    connectdb.insert_resumen_diario(fecha, device_location)


def marcador_casos(fecha=None):
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
    devicelocationservice = DeviceLocationService()
    locations = devicelocationservice.all_data()
    
    if fecha_formato is not None:
        marcador_casos(fecha_formato)

    df_resumen_diario = connectdb.get_datos_resumen_diario(centros=locations)

    json_datos_resumen_diario = json.loads(df_resumen_diario.to_json(orient="records"))
    
    json_datos_resumen_diario_detalle = []
    if fecha_formato is not None and centro is not None:
        df_datos_prediccion = connectdb.get_datos_prediccion(fecha=fecha_formato, centro=centro)
        json_datos_resumen_diario_detalle = json.loads(df_datos_prediccion.to_json(orient="records"))
    return json_datos_resumen_diario, json_datos_resumen_diario_detalle


def is_valid_format(nombre_archivo):
    """Validar que el formato del archivo sea igual al indicado en la constante PATRON_FORMATO."""
    PATRON_FORMATO = '%Y-%m-%dT%H-%M-%S.jpg'

    try:
        timestamp = datetime.strptime(nombre_archivo, PATRON_FORMATO)
        # print(f"El timestamp obtenido: {timestamp}")
        return True, timestamp
    except ValueError:
        print(f"El formato del nombre del archivo no es válido. El nombre del archivo es: {nombre_archivo}")
        return False, None
    except Exception as e:
        print(f"Error durante la validación del formato del archivo. {e}")
        return False, None
