import calendar
import time
from datetime import datetime, timezone, timedelta
import pandas as pd
import json
import base64
from decimal import Decimal
from utils.date_format import get_datetime_from_str, get_timestamp_from_datetime, get_str_format_from_date_str, get_str_date_tz_from_timestamp
from utils.config import s3, dynamodb, dynamodb_client, BUCKET_NAME
from boto3.dynamodb.conditions import Attr, Key


def get_timestamp():
    current_GMT = time.gmtime()
    time_stamp = calendar.timegm(current_GMT)
    return time_stamp


def get_timestamp_from_date(timestamp_value):
    date_value = datetime.fromtimestamp(timestamp_value).date()
    return get_timestamp_from_datetime(date_value)


def get_timestamp_format(timestamp_value, format="%d/%m/%Y"):
    date_value = datetime.fromtimestamp(timestamp_value)
    return date_value.strftime(format)


class ConnectBucket():
    def __init__(self, bucket_name=BUCKET_NAME):
        self.s3_resource = s3
        self.bucket_name = bucket_name
    
    def get_objects(self, prefix_bucket):
        paginator = self.s3_resource.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=self.bucket_name, Prefix=prefix_bucket, PaginationConfig={'PageSize': 1000})
        return pages
    
    def get_image_base64(self, object_key):
        try:
            image_data = self.s3_resource.get_object(Bucket=self.bucket_name, Key=object_key)['Body'].read()
            image_base64 = base64.b64encode(image_data).decode('utf-8')
            return image_base64
        except Exception as e:
            print(f"Error al recuperar imagen base64 de {object_key}: {e}")
            return ""
    
    def upload(self, image_data, root_path_bucket):
        self.s3_resource.put_object(Body=image_data, Bucket=self.bucket_name, Key=root_path_bucket)
        print(f"La imagen se ha subido exitosamente a AWS S3 {root_path_bucket}")
    
    # def get_url_imagen(self, ruta_imagen_bucket):
    #     return "%s/%s/%s" % (self.s3_resource.meta.endpoint_url, self.bucket_name, ruta_imagen_bucket)


class LocationsRepository():

    def __init__(self):
        self.dyn_resource = dynamodb
        self.table = self.dyn_resource.Table("ubicaciones_trampas")
    
    def all_data(self) -> pd.DataFrame:
        """Obtener los datos de los edificios que tienen trampas.
        """
        try:
            response = self.table.scan()
            data = response.get('Items', [])
        except Exception as e:
            print(f"Ocurrió un error durante la consulta de la lista de edificios en la base de datos. Detalle del error: {e}")
            data = []
        finally:
            columns = ["device_location", "latitud", "localidad", "longitud", "nombre_centro"]
            return pd.DataFrame(data, columns=columns)

    def add_location(self, device_location, direccion, latitud, localidad, longitud, nombre_centro):
        """ Registrar ubicaciones de las cámaras en base de datos.
        """
        timestamp = get_timestamp()
        try:
            data = {
                "device_location": device_location,
                "direccion": direccion, 
                "latitud": Decimal(str(latitud)), 
                "localidad": localidad, 
                "longitud": Decimal(str(longitud)), 
                "nombre_centro": nombre_centro,
                "fecha_registro": get_str_date_tz_from_timestamp(timestamp, format="%Y-%m-%d %H:%M:%S")
            }
            self.table.put_item(Item=data)
        except Exception as err:
            print(f"Ocurrió un error durante el registro de un edificio en la base de datos. Detalle del error: {err}")


class PrediccionesFotoRepository():
    
    def __init__(self):
        self.dyn_resource = dynamodb_client
        self.table_name = "predicciones_foto"
    
    def all_data(self, items:list):
        """Obtener los datos de los edificios que tienen trampas.
        Input:
            - Lista:List[set] Cada elemento de la lista está compuesto por un set(str, str, str).
                Elementos: (device_location, device_id-timestamp, full_path)
        """
        try:
            resultado = set()

            # Dividir la lista en bloques de hasta 100 elementos (límite de batch_get_item)
            for i in range(0, len(items), 100):
                items_batch = items[i:i+100]
                keys = [{'device_location': {'S': location}, 'device_id_timestamp': {'S': device_id_timestamp}} for location, device_id_timestamp, _ in items_batch]
                response = self.dyn_resource.batch_get_item(
                    RequestItems={
                        self.table_name: {
                            'Keys': keys,
                            'ProjectionExpression': 'device_location, device_id_timestamp, path_foto_raw'
                        }
                    }
                )
                resultado.update((item['device_location']['S'], item['device_id_timestamp']['S'], item['path_foto_raw']['S']) for item in response.get('Responses', {}).get(self.table_name, []))
        except Exception as e:
            print(f"Ocurrió un error durante la consulta de la tabla predicciones_foto en la base de datos. Detalle del error: {e}")
            resultado = set()
        finally:
            return resultado
    
    def find(self, device_location:str, device_id_timestamp:str) -> list:
        """
        Output:
        - Lista[dict] : Los campos de cada elemento del diccionario son:
            device_location, device_id, device_id_timestamp, path_foto_yolo, foto_fecha, foto_datetime, cantidad_aedes, cantidad_moscas, cantidad_mosquitos
        """
        try:
            data = []

            keys = [{'device_location': {'S': device_location}, 'device_id_timestamp': {'S': device_id_timestamp}}]
            response = self.dyn_resource.batch_get_item(
                RequestItems={
                    self.table_name: {
                        'Keys': keys,
                        'ProjectionExpression': 'device_location, device_id, device_id_timestamp, path_foto_yolo, foto_fecha, foto_datetime, cantidad_aedes, cantidad_moscas, cantidad_mosquitos'
                    }
                }
            )
            data = [{clave: valor['S'] if 'S' in valor else int(valor['N']) for clave, valor in diccionario.items()} for diccionario in response.get('Responses', {}).get(self.table_name, [])]
        except Exception as e:
            print(f"Ocurrió un error durante la consulta de la tabla predicciones_foto en la base de datos. Detalle del error: {e}")
            data = []
        finally:
            return data

    def add_prediction(self, device_location, device_id_timestamp, device_id, aedes, mosquitos, moscas, path_foto_raw, path_foto_yolo, foto_fecha, foto_datetime):
        """ Registrar objeto en base de datos.
        """
        try:
            timestamp = get_timestamp()
            data = {
                "path_foto_raw": {'S':path_foto_raw},
                "path_foto_yolo": {'S':path_foto_yolo},
                "device_location": {'S':device_location},#"centro": device_location,
                "device_id_timestamp": {'S':device_id_timestamp},#
                "device_id": {'S':device_id},
                "cantidad_aedes": {'N':str(aedes)},
                "cantidad_mosquitos": {'N':str(mosquitos)},
                "cantidad_moscas": {'N':str(moscas)},
                "foto_fecha": {'S':foto_fecha},
                "foto_datetime": {'S':foto_datetime},
                "timestamp_procesamiento": {'S':str(timestamp)}, # Fecha de procesamiento
                "fecha_procesamiento": {'S':get_str_date_tz_from_timestamp(timestamp, format="%Y-%m-%d %H:%M:%S")}
            }
            self.dyn_resource.put_item(TableName=self.table_name, Item=data)
        except Exception as err:
            print(f"Ocurrió un error durante el registro de la metadata de la foto en la base de datos. Detalle del error: {err}")

    def add_last_datetime(self, date_time:datetime):
        """ Registrar objeto en base de datos.
        """
        try:
            data = {
                "device_location": {'S':'last_datetime_scheduler'},
                "device_id_timestamp": {'S':'last_datetime_scheduler'},
                "fecha_procesamiento": {'S':str(date_time)},
            }
            self.dyn_resource.put_item(TableName=self.table_name, Item=data)
            print(f'Actualización exitosa de la última ejecución en la base de datos. La nueva fecha es: {date_time}')
        except Exception as err:
            print(f"Ocurrió un error durante el registro de los datos de la última ejecución en la base de datos. Detalle del error: {err}")

    def get_last_datetime(self) -> datetime:
        """
        """
        try:
            result = None
            keys = {
                'device_location': {'S': 'last_datetime_scheduler'},
                'device_id_timestamp': {'S': 'last_datetime_scheduler'}
            }

            response = self.dyn_resource.get_item(
                TableName=self.table_name,
                Key=keys,
                AttributesToGet=['device_location', 'device_id_timestamp', 'fecha_procesamiento']
            )
            
            # Obtener el valor de 'fecha_procesamiento' de la respuesta
            datetime_str = response.get('Item', {}).get('fecha_procesamiento', {}).get('S', None)
            
            if datetime_str:
                # Convertir la cadena a un objeto datetime
                datetime_value = datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S%z")
                result = datetime(year=datetime_value.year, month=datetime_value.month, day=datetime_value.day, hour=datetime_value.hour, minute=datetime_value.minute, second=datetime_value.second, tzinfo=timezone(timedelta(hours=0)))
        except Exception as e:
            print(f"Ocurrió un error durante la consulta de los datos de la última ejecución en la base de datos. Detalle del error: {e}")
            result = None
        finally:
            return result

class ResumenesDiarioRepository():
    
    def __init__(self):
        self.dyn_resource = dynamodb
        self.table = self.dyn_resource.Table("resumenes_diario")
    
    def data(self, foto_fecha=None) -> pd.DataFrame:
        try:
            lastEvaluatedKey = None
            data = [] # Result Array

            while True:
                if lastEvaluatedKey == None:
                    response = self.table.scan() # This only runs the first time - provide no ExclusiveStartKey initially
                else:
                    response = self.table.scan(
                    ExclusiveStartKey=lastEvaluatedKey # In subsequent calls, provide the ExclusiveStartKey
                )

                data.extend(response.get('Items', [])) # Appending to our resultset list
                
                # Set our lastEvlauatedKey to the value for next operation,
                # else, there's no more results and we can exit
                if 'LastEvaluatedKey' in response:
                    lastEvaluatedKey = response['LastEvaluatedKey']
                else:
                    break
        except Exception as err:
            print(f"Ocurrió un error durante la consulta a la tabla resumenes_diario en la base de datos. Detalle del error: {err}")
            data = []
        finally:
            columns = ["cantidad_aedes", "cantidad_moscas", "cantidad_mosquitos", "device_location", "foto_fecha", "path_foto_yolo"]
            return pd.DataFrame(data, columns=columns)

    def get(self, device_location:str, foto_fecha:str) -> pd.DataFrame:
        try:
            response = self.table.get_item(
                Key={
                    'device_location': device_location,
                    'foto_fecha': foto_fecha
                }
            )
            item = response.get('Item', [])
            if item: # No vacío
                item = [item]
        except Exception as err:
            print(f"Ocurrió un error durante la consulta del registro ({device_location} {foto_fecha}) a la tabla resumenes_diario en la base de datos. Detalle del error: {err}")
            item = []
        finally:
            columns = ["cantidad_aedes", "cantidad_moscas", "cantidad_mosquitos", "device_location", "foto_fecha", "foto_datetime", "path_foto_yolo", "list_device_id_timestamp"]
            return pd.DataFrame(item, columns=columns)

    def add(self, device_location, aedes, mosquitos, moscas, path_foto_yolo, foto_fecha, foto_datetime, list_device_id_timestamp):
        try:
            timestamp = get_timestamp()
            data = {
                'cantidad_aedes': aedes,
                'cantidad_moscas': moscas,
                'cantidad_mosquitos': mosquitos,
                'device_location': device_location,
                'foto_fecha': foto_fecha, # Ejemplo: "2024-01-22"
                'foto_datetime': foto_datetime,
                'path_foto_yolo': path_foto_yolo,
                'timestamp_procesamiento': timestamp, # Fecha de procesamiento
                'fecha_procesamiento': get_str_date_tz_from_timestamp(timestamp, format="%Y-%m-%d %H:%M:%S"),
                'list_device_id_timestamp': list_device_id_timestamp
            }
            
            self.table.put_item(Item=data)
            print(f"Registro exitoso en base de datos para la fecha {foto_fecha} y device_location {device_location}.")
        except Exception as err:
            print(f"Ocurrió un error durante el registro en la tabla resumenes_diario en la base de datos. Detalle del error: {err}")

    def update(self, device_location, foto_fecha, aedes, mosquitos, moscas, path_foto_yolo, list_device_id_timestamp):
        try:
            timestamp = get_timestamp()
            new_values = {
                'cantidad_aedes': aedes, 
                'cantidad_moscas': moscas, 
                'cantidad_mosquitos': mosquitos, 
                'path_foto_yolo': path_foto_yolo,
                'timestamp_procesamiento': timestamp, # Fecha de procesamiento
                'fecha_procesamiento': get_str_date_tz_from_timestamp(timestamp, format="%Y-%m-%d %H:%M:%S"),
                'list_device_id_timestamp': list_device_id_timestamp
            }

            # Construir la expresión de actualización
            expresion_actualizacion = "SET " + ", ".join([f"{campo} = :{campo}" for campo in new_values])

            # Valores de expresión de actualización
            valores_actualizacion = {f":{campo}": valor for campo, valor in new_values.items()}

            # Actualizar el elemento en la tabla
            self.table.update_item(
                Key={
                    'device_location': device_location,
                    'foto_fecha': foto_fecha
                },
                UpdateExpression=expresion_actualizacion,
                ExpressionAttributeValues=valores_actualizacion
            )
            print(f"Actualización exitosa en base de datos para la fecha {foto_fecha} y device_location {device_location}.")
        except Exception as err:
            print(f"Ocurrió un error durante la actualización en la tabla resumenes_diario en la base de datos. Detalle del error: {err}")
