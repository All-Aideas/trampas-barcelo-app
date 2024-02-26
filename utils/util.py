import os
import base64
import urllib3
import json
import folium
from database.connect import ResumenesDiarioRepository, PrediccionesFotoRepository, ConnectBucket, get_timestamp_from_date, get_timestamp_format, LocationsRepository
from utils.config import API_URL_PREDICT, AWS_BUCKET_RAW
import numpy as np
import pandas as pd
from datetime import datetime
import pytz
from utils.date_format import get_datetime, get_datetime_from_str, get_str_format_from_date_str

conncets3 = ConnectBucket()
prediccionesfoto_repository = PrediccionesFotoRepository()

class DeviceLocationService():

    def __init__(self):
        self.repository = LocationsRepository()
        self.data = None

    def all_data(self):
        resultado = self.repository.all_data()
        return resultado

    def insert_location(self, device_location, direccion, latitud, localidad, longitud, nombre_centro):
        self.repository.add_location(device_location, direccion, latitud, localidad, longitud, nombre_centro)

class PhotosService():
    
    def get_image_base64(self, object_key):
        return conncets3.get_image_base64(object_key=object_key)
    
    def upload_imagen_s3(self, base64_str, full_path):
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


def get_valid_file(full_path:str):
    """
    Ejemplo:
    full_path = raw/modernizacion/mosq-trampa_1/2022-11-29T11-11-11.jpg
    """
    try:
        partes_ruta = os.path.normpath(full_path).split(os.path.sep)
        device_location = partes_ruta[1]
        device_id = partes_ruta[2]
        nombre_archivo = partes_ruta[-1]
        flag, _ = is_valid_format(nombre_archivo)
        
        if flag:
            # print(f"Ubicación de archivo en el bucket: {full_path}")
            return device_location, f"{device_id}.{nombre_archivo}", full_path
        return None, None, None
    except Exception as e:
        print(f"Ocurrió un error durante la validación del objeto {full_path} del bucket. Detalle del error: {e}")
        return None, None, None


def invoke_api(image_size="640", nms_threshold=0.45, threshold=0.55, encoded_string=""):
    try:
        status, encoded_imagen, metadata = None, None, None

        cadena = f"data:image/jpeg;base64,{encoded_string}"
        data = {"data": [image_size, nms_threshold, threshold, cadena]}
        body = json.dumps(data).encode('utf-8')
        http = urllib3.PoolManager()
        response = http.request("POST",
                                API_URL_PREDICT,
                                body=body,
                                headers={'Content-Type': 'application/json'})
        status = response.status
        if status == 200:
            # El primer elemento contiene la imagen.
            # El segundo elemento contiene la metadata.
            encoded_imagen, _, metadata = json.loads(response.data.decode('utf-8'))["data"]
            metadata = metadata.get('detail', [])
            encoded_imagen = encoded_imagen.split("data:image/png;base64,")[1]
    except Exception as err:
        print(f"Error durante la invocación de la IA: {err}")
    finally:
        return status, encoded_imagen, metadata


def predict_casos(full_path_file):
    """ Obtiene la cantidad de aedes, mosquitos y moscas detectadas por la inteligencia artificial y almacena la imagen.
    """
    try:
        photos_service = PhotosService()

        encoded_string = conncets3.get_image_base64(full_path_file)
        status, encoded_imagen, metadata_detail = invoke_api(encoded_string=encoded_string)

        print(f'Resultado de API ({status}) para la foto {full_path_file}: {metadata_detail}')

        if status != 200:
            # Si hubo un error durante la invocación, entonces se almacena la misma foto.
            encoded_imagen = encoded_string
            aedes, mosquitos, moscas = 0, 0, 0
        
        # Almacenar la foto procesada en el bucket
        renamed_full_path_bucket = full_path_file.replace(".jpg","_yolov5.jpg")
        path_foto_yolo = photos_service.upload_imagen_s3(encoded_imagen, renamed_full_path_bucket)
        
        if not metadata_detail:
            return 0, 0, 0, path_foto_yolo
        else:
            # Recuperar la metadata
            aedes = sum(int(item.get('quantity', 0)) for item in metadata_detail if item.get('description') == 'Aedes')
            mosquitos = sum(int(item.get('quantity', 0)) for item in metadata_detail if item.get('description') == 'Mosquito')
            moscas = sum(int(item.get('quantity', 0)) for item in metadata_detail if item.get('description') == 'Mosca')
        
        return aedes, mosquitos, moscas, path_foto_yolo
    except Exception as e:
        print(f"Ocurrió un error en el proceso de invocar el API de YOLO. Detalle del error: {e}")
        return 0, 0, 0, None


def set_market(mapa, lat_lng:list, name:str, description:str="", show_description:bool=False, aedes_total:int=0, mosquitos_total:int=0, moscas_total:int=0, is_today=True):
    """
    Descripción:
    Asignar los puntos de ubicación en el mapa.
    """
    icon_config = folium.Icon(color="green", icon="info-sign")
    if aedes_total > 0:
        icon_config = folium.Icon(color="red", icon="info-sign")
    if not is_today:
        icon_config = folium.Icon(color="gray", icon="info-sign")
    
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


class DashboardService():
    def get_resumenes(self):
        """Mostrar el resumen de los casos detectados por día.
        """
        devicelocationservice = DeviceLocationService()
        df_locations = devicelocationservice.all_data()

        resumenesdiario_repository = ResumenesDiarioRepository()
        df_resumenesdiario = resumenesdiario_repository.data()

        df_merged = pd.merge(df_resumenesdiario, df_locations, on='device_location', how='right') \
                     .sort_values(by=['foto_fecha', 'nombre_centro'], ascending=[False, True])
        print(df_merged.head())
        mapa = folium.Map(
            location=[-34.5106, -58.4964],
            zoom_start=13,
        )

        if df_merged.empty:
            df_locations.apply(self.add_marker, axis=1, mapa=mapa)
        else:
            # Obtener última fecha procesada
            ultima_fecha_procesada = df_merged['foto_fecha'].iloc[0] # YYYY-MM-DD
            df_mapa_points = df_merged.groupby(['device_location'])\
                                        .agg(foto_fecha=('foto_fecha', 'first'),
                                            cantidad_aedes=('cantidad_aedes', 'first'), 
                                            cantidad_moscas=('cantidad_moscas', 'first'),
                                            cantidad_mosquitos=('cantidad_mosquitos', 'first'),
                                            path_foto_yolo=('path_foto_yolo', 'first'),
                                            latitud=('latitud', 'first'),
                                            localidad=('localidad', 'first'),
                                            longitud=('longitud', 'first'),
                                            nombre_centro=('nombre_centro', 'first'),
                                            )\
                                        .sort_values(by=["device_location"])\
                                        .reset_index() # Datos de la última fecha en que tomaron foto.

            # df_mapa_points = pd.merge(df_resumen_por_diario, df_locations, on='device_location', how='right') \
            #             .sort_values(by=['foto_fecha', 'nombre_centro'], ascending=[False, True])
            default_values = {'foto_fecha': ultima_fecha_procesada, 'path_foto_yolo': '', 'cantidad_aedes': 0, 'cantidad_mosquitos': 0, 'cantidad_moscas': 0}
            df_mapa_points = df_mapa_points.fillna(default_values)

            # Obtén la fecha actual del sistema
            fecha_actual = datetime.now(pytz.timezone('America/Argentina/Buenos_Aires'))

            df_mapa_points['diferencia_dias_foto'] = (fecha_actual - pd.to_datetime(df_mapa_points['foto_fecha'], utc=True)).dt.days

            # Agrega la columna is_today basada en la diferencia de días
            df_mapa_points['is_today'] = (df_mapa_points['diferencia_dias_foto'] < 2).astype(bool)

            # Elimina la columna temporal de diferencia de días si no es necesaria
            df_mapa_points.drop('diferencia_dias_foto', axis=1, inplace=True)

            # df_mapa_points['is_today'] = df_mapa_points['foto_fecha'].apply(lambda x: x == ultima_fecha_procesada).astype(bool)

            df_merged = df_merged.fillna(default_values)
            df_merged['fecha_formato'] = df_merged['foto_fecha'].apply(lambda col: get_str_format_from_date_str(col))
            df_merged.sort_values(by=["foto_fecha", "nombre_centro"], ascending=[False, True], inplace=True)
            
            df_mapa_points.apply(self.add_marker, axis=1, mapa=mapa)
        mapa.save('templates/mapa.html')

        json_datos_resumen_diario = json.loads(df_merged.to_json(orient="records"))
        return json_datos_resumen_diario
    
    def get_detalle(self, foto_fecha:str, device_location:str):
        """Mostrar el detalle por ubicación y por fecha.
        """
        devicelocationservice = DeviceLocationService()
        df_locations = devicelocationservice.all_data()

        resumenesdiario_repository = ResumenesDiarioRepository()
        df_resumen_diario = resumenesdiario_repository.get(device_location=device_location, foto_fecha=foto_fecha)
        list_device_id_timestamp = df_resumen_diario["list_device_id_timestamp"].iloc[0] # Lista de SortedKey para buscar el detalle.

        # # Recuperar todos los resumenes para mostrar puntos rojos en el mapa para un día en concreto.
        # # df_resumenesdiario = resumenesdiario_repository.data(foto_fecha=foto_fecha)
        # df_resumenesdiario = resumenesdiario_repository.data()
        # df_merged = pd.merge(df_resumenesdiario, df_locations, on='device_location', how='right') \
        #              .sort_values(by=['foto_fecha', 'nombre_centro'], ascending=[False, True])
        # df_mapa_points = df_merged.groupby(['device_location'])\
        #                                 .agg(foto_fecha=('foto_fecha', 'first'),
        #                                     cantidad_aedes=('cantidad_aedes', 'first'), 
        #                                     cantidad_moscas=('cantidad_moscas', 'first'),
        #                                     cantidad_mosquitos=('cantidad_mosquitos', 'first'),
        #                                     path_foto_yolo=('path_foto_yolo', 'first'),
        #                                     latitud=('latitud', 'first'),
        #                                     localidad=('localidad', 'first'),
        #                                     longitud=('longitud', 'first'),
        #                                     nombre_centro=('nombre_centro', 'first'),
        #                                     )\
        #                                 .sort_values(by=["device_location"])\
        #                                 .reset_index() # Datos de la última fecha en que tomaron foto.
        
        # # Puntos en el mapa
        # # df_mapa_points = pd.merge(df_resumenesdiario, df_locations, on='device_location', how='right')
        # default_values = {'foto_fecha': foto_fecha, 'path_foto_yolo': '', 'cantidad_aedes': 0, 'cantidad_mosquitos': 0, 'cantidad_moscas': 0}
        # df_mapa_points = df_mapa_points.fillna(default_values)

        # # Obtener última fecha procesada
        # # df_mapa_points = df_mapa_points[df_mapa_points['foto_fecha'] == foto_fecha]
        
        # # Mostrar en mapa
        # mapa = folium.Map(
        #     location=[-34.5106, -58.4964],
        #     zoom_start=13,
        # )
        # df_mapa_points.apply(self.add_marker, axis=1, mapa=mapa)
        # mapa.save('templates/mapa.html')
        
        data = []
        for device_id_timestamp in list_device_id_timestamp:
            data_prediccion = prediccionesfoto_repository.find(device_location=device_location, device_id_timestamp=device_id_timestamp)
            data.extend(data_prediccion)
        df_resultados_por_centro = pd.DataFrame(data)

        # Tabla detalle
        df_merged = pd.merge(df_resultados_por_centro, df_locations, on='device_location', how='inner')
        df_merged['fecha_formato'] = df_merged['foto_fecha'].apply(lambda col: get_str_format_from_date_str(col))
        df_merged["fecha"] = df_merged["foto_fecha"].apply(lambda col: col)
        df_merged["fecha_datetime"] = df_merged["foto_fecha"].apply(lambda col: get_datetime_from_str(col))
        df_merged.sort_values(by=["fecha_datetime", "device_location", "path_foto_yolo"], inplace=True)
        json_datos_resumen_diario_detalle = json.loads(df_merged.to_json(orient="records"))

        return json_datos_resumen_diario_detalle
    
    def add_marker(self, row, mapa):
        """
        Descripción:
        Asignar los puntos de ubicación en el mapa.
        """
        centro_lat, centro_lng = row["latitud"], row["longitud"]
        centro_lat_lng = [centro_lat, centro_lng]
        centro_nombre = row["nombre_centro"]
        texto_resumen_imagen = ""
        mostrar_descripcion = ""
        image_base64 = ""
        aedes_total, mosquitos_total, moscas_total = 0, 0, 0

        if type(row.get("foto_fecha")) == str:
            ultima_fecha_procesada = row["foto_fecha"]
            url_ultima_foto = row['path_foto_yolo'] # Visualizar foto en HTML
            is_today = row.get("is_today", False)
            
            if url_ultima_foto:
                image_base64 = conncets3.get_image_base64(url_ultima_foto)
            texto_resumen_imagen = f"<div>Última foto tomada el día {ultima_fecha_procesada}<img id='resumen_diario_ultima_foto_yolov5' class='img-fluid' src='data:image/jpeg;base64,{image_base64}' width='100%' /></div>"

            texto_resumen_no_imagen = f"<div>No hay fotos del día {ultima_fecha_procesada}.</div>"
            texto_resumen_imagen = texto_resumen_imagen if len(image_base64) > 0 else texto_resumen_no_imagen
            mostrar_descripcion = True if len(image_base64) > 0 else False

            aedes_total, mosquitos_total, moscas_total = int(row['cantidad_aedes']), int(row['cantidad_mosquitos']), int(row['cantidad_moscas'])
        set_market(mapa, lat_lng=centro_lat_lng, name=centro_nombre, description=texto_resumen_imagen, show_description=mostrar_descripcion, aedes_total=aedes_total, mosquitos_total=mosquitos_total, moscas_total=moscas_total, is_today=is_today)

class PredictPhotosService():

    def get_new_objects(self, reprocessing:bool=False):
        """
        Descripción:
        Recupera la lista de edificios registrados en base de datos.
        Crea una lista de objetos que se encuentran en cada carpeta del bucket.
        Filtra solamente los archivos con extensión JPG y cuya nomenclatura sea igual al de la función is_valid_format().
        Filtra las fotos que ya fueron procesadas anteriormente por la IA. Esto evita reprocesamiento.
        
        Input:
            - reprocessing:bool Si es True, entonces se procesa todas las fotos del bucket. 
                                Si es False, entonces se procesa las nuevas fotos del bucket.
        Output:
            - Lista:List[set] Cada elemento de la lista está compuesto por un set(str, str, str).
                Elementos: (device_location, device_id-timestamp, full_path)
        """
        try:
            last_datetime = prediccionesfoto_repository.get_last_datetime()
            print(f'Procesando objetos a partir de la fecha {last_datetime}')
            
            archivos_jpg = []
            archivos_metadata = []
            
            devicelocationservice = DeviceLocationService()
            df_locations = devicelocationservice.all_data()
            locations = df_locations["device_location"].tolist()

            for location in locations:
                prefix_bucket = f"{AWS_BUCKET_RAW}/{location}"

                pages = conncets3.get_objects(prefix_bucket=prefix_bucket)
            
                for page in pages:
                    archivos_metadata.extend([{'Key':objeto['Key'], 'LastModified':objeto['LastModified']} for objeto in page.get('Contents', []) if objeto['Key'].lower().endswith('.jpg')])

            if last_datetime is None:
                archivos_jpg = [elemento['Key'] for elemento in archivos_metadata]
            else:
                archivos_jpg = [elemento['Key'] for elemento in archivos_metadata if elemento['LastModified'] >= last_datetime]

            last_modified = max(archivos_metadata, key=lambda x: x['LastModified']).get('LastModified', None)
            prediccionesfoto_repository.add_last_datetime(date_time=last_modified)

            #print(f"Archivos JPG que se encuentran en el bucket: {archivos_jpg}")
            
            path_files_valid = [file for file in [get_valid_file(archivo_valido) for archivo_valido in archivos_jpg] if file]
            #print(f"Archivos JPG con la nomenclatura esperada en el bucket: {path_files_valid}")
            
            if not reprocessing:
                predicciones_result = prediccionesfoto_repository.all_data(items=path_files_valid)

                # Retirar los elementos que se encuentran en la base de datos
                new_elements = set(path_files_valid) - predicciones_result
                path_files_valid = list(new_elements)
                #print(f"Los archivos JPG que no se encuentran en la base de datos son: {path_files_valid}")
            
            if path_files_valid:
                return path_files_valid
            return []
        except Exception as e:
            print(f"Ocurrió un error durante la descarga de objetos del bucket. Detalle del error: {e}")
            return None


    def process(self, data_objects:list):
        """
        Descripción:
        Analizar cada una de las imágenes descargadas por la IA.

        Input:
            - Lista:List[set] Cada elemento de la lista está compuesto por un set(str, str, str).
                Elementos: (device_location, device_id-timestamp, full_path)
        Output:
            - Lista:set(str, str) Cada elemento del conjunto está compuesto por un set(str, str).
                Elementos: (foto_date, device_location)
        """
        try:
            fechas_fotos_device_locations = set() # Lista de las fechas de las fotos que fueron procesadas correctamente.

            for device_location, device_id_timestamp, full_path_bucket in data_objects:
                aedes, mosquitos, moscas, path_foto_yolo = predict_casos(full_path_bucket)

                if path_foto_yolo:
                    device_id, timestamp, ext = device_id_timestamp.split(".")
                    foto_date = get_str_format_from_date_str(date_str=timestamp, format_old='%Y-%m-%dT%H-%M-%S', format_new='%Y-%m-%d')
                    foto_datetime = get_str_format_from_date_str(date_str=timestamp, format_old='%Y-%m-%dT%H-%M-%S', format_new='%Y-%m-%d %H:%M:%S')
                    
                    prediccionesfoto_repository.add_prediction(device_location, device_id_timestamp, device_id, aedes, mosquitos, moscas, full_path_bucket, path_foto_yolo, foto_date, foto_datetime)

                    fechas_fotos_device_locations.add((device_location, device_id_timestamp, aedes, mosquitos, moscas, full_path_bucket, path_foto_yolo, foto_date, foto_datetime))
                
            data = list(fechas_fotos_device_locations)
        except Exception as e:
            print(f"Ocurrió un error durante el proceso de análisis de las imágenes del bucket. Detalle del error: {e}")
            data = []
        finally:
            columnas = ['device_location', 'device_id_timestamp', 'cantidad_aedes', 'cantidad_mosquitos', 'cantidad_moscas', 'full_path_bucket', 'path_foto_yolo', 'foto_fecha', 'foto_datetime']
            resultado = pd.DataFrame(data, columns=columnas)
            return resultado
    
    def new_resumen(self, fila):
        """
        Descripción:
        Consultar si existe un registro previo con la fecha y device_location.
        Si no existe un registro previo, entonces se agregará un nuevo registro.
        Si si existe un registro previo, entonces se evaluará el valor de path_foto_yolo para actualizar los valores
        con las cantidades de la última foto. De lo contario, no se hará ninguna acción.
        """
        resumenesdiario_repository = ResumenesDiarioRepository()
        df_resumenesdiario = resumenesdiario_repository.get(device_location=fila["device_location"], foto_fecha=fila["foto_fecha"])

        if df_resumenesdiario.empty:
            resumenesdiario_repository.add(device_location=fila["device_location"], 
                                            aedes=fila["cantidad_aedes"], 
                                            mosquitos=fila["cantidad_mosquitos"], 
                                            moscas=fila["cantidad_moscas"], 
                                            path_foto_yolo=fila["path_foto_yolo"], 
                                            foto_fecha=fila["foto_fecha"], 
                                            foto_datetime=fila["foto_datetime"],
                                            list_device_id_timestamp=fila["list_device_id_timestamp"])

        else:
            path_foto_yolo_previous = df_resumenesdiario['foto_datetime'].iloc[0] # '2022-11-29 11:11:11'
            path_foto_yolo_new = fila["foto_datetime"]
            list_device_id_timestamp = df_resumenesdiario['list_device_id_timestamp'].iloc[0]
            list_device_id_timestamp_new = list(set(list_device_id_timestamp + fila["list_device_id_timestamp"]))

            if path_foto_yolo_new >= path_foto_yolo_previous:
                resumenesdiario_repository.update(device_location=fila["device_location"], 
                                                foto_fecha=fila["foto_fecha"], 
                                                aedes=fila["cantidad_aedes"], 
                                                mosquitos=fila["cantidad_mosquitos"], 
                                                moscas=fila["cantidad_moscas"], 
                                                path_foto_yolo=fila["path_foto_yolo"],
                                                list_device_id_timestamp=list_device_id_timestamp_new)

    def resume(self, data_objects):
        """ 
        Descripción:
            Obtener la cantidad de aedes, mosquitos y moscas encontradas en la última foto de un día.
            Almacena la suma de aedes, mosquitos y moscas en base de datos.
        Input:
            - data_objects:DataFrame Objetos que fueron analizados por la IA.
        """
        df_resumen = data_objects.groupby(["foto_fecha", "device_location"])\
                                .agg(list_device_id_timestamp=('device_id_timestamp', list),
                                    cantidad_aedes=('cantidad_aedes', 'last'),
                                    cantidad_moscas=('cantidad_moscas', 'last'),
                                    cantidad_mosquitos=('cantidad_mosquitos', 'last'),
                                    path_foto_yolo=('path_foto_yolo', 'last'),
                                    foto_datetime=('foto_datetime', 'last'))\
                                .sort_values(by=["foto_fecha", "device_location"])\
                                .reset_index()
        if not df_resumen.empty:
            df_resumen.apply(self.new_resumen, axis=1) # Recorrer fila por fila
