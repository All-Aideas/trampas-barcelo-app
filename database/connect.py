import calendar
import time
from datetime import datetime
import pandas as pd
import json
from utils.date_format import get_datetime_from_str, get_timestamp_from_datetime, get_str_format_from_date_str, get_str_date_tz_from_timestamp
from utils.config import db, dynamodb


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


class ConnectDynamoDB():

    def __init__(self, dyn_resource, table_name):
        self.dyn_resource = dynamodb
        self.table = self.dyn_resource.Table(table_name)
    
    def get_locations(self):
        """Obtener los datos de los edificios que tienen trampas.
        """
        try:
            table = self.table
            response = table.scan()
            data = response.get('Items', [])
            print(data)
            
            # print(type(table))
            # resultado = table.get_item(
            #     Key={
            #         'device_location': 'biblioteca_froilan'
            #     }
            # )
            # item = resultado['Item']
            # print(item)
        except Exception as e:
            print(f"Ocurrió un error durante la consulta de la lista de edificios en la base de datos. Detalle del error: {e}")
            resultado = []
        finally:
            return resultado

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


class ConnectDataBase():

    def get_lista_centros(self):
        """Obtener los datos de los edificios que tienen trampas.
        """
        try:
            key_find = f"ubicaciones_trampas"
            resultado = db.child(key_find).get().val()
            resultado = dict(resultado)
        except Exception as e:
            print(f"Ocurrió un error durante la consulta de la lista de edificios en la base de datos. Detalle del error: {e}")
            resultado = {}
        finally:
            return resultado


    def campos_json(self, device_location, device_id, aedes, mosquitos, moscas, foto_original, foto_yolov5, path_foto_raw, path_foto_yolo, foto_fecha, foto_datetime):
        timestamp = get_timestamp()
        return {
            "foto_original": foto_original,
            "foto_yolov5": foto_yolov5,
            "path_foto_raw": path_foto_raw,
            "path_foto_yolo": path_foto_yolo,
            "centro": device_location,
            "device_id": device_id,
            "cantidad_aedes": aedes,
            "cantidad_mosquitos": mosquitos,
            "cantidad_moscas": moscas,
            "foto_fecha": foto_fecha,
            "foto_datetime": foto_datetime,
            "timestamp_procesamiento": timestamp, # Fecha de procesamiento
            "fecha_procesamiento": get_str_date_tz_from_timestamp(timestamp, format="%Y-%m-%d %H:%M:%S")
        }


    def insert_dato_prediccion(self, centro, datos_prediccion_foto):
        """ Registrar objeto en base de datos.
        """
        return db.child(f"predicciones_foto/{centro}").push(datos_prediccion_foto)


    def get_datos_prediccion(self, fecha=None, centro=None):
        """ Obtener los datos almacenados en base de datos.
        """
        try:
            df_resultados_por_centro = pd.DataFrame()
            key_find = "predicciones_foto"
            resultado = db.child(key_find).get().val()
            
            for resultados_por_foto in resultado.keys():
                resultado_por_foto = resultado[resultados_por_foto]
                for identificador_foto in resultado_por_foto.keys():
                    new_row = pd.DataFrame([resultado_por_foto[identificador_foto]])
                    df_resultados_por_centro = pd.concat([df_resultados_por_centro, new_row], ignore_index=True)

            df_resultados_por_centro["fecha"] = df_resultados_por_centro["foto_fecha"]\
                                                    .apply(lambda col: col)
            df_resultados_por_centro["fecha_datetime"] = df_resultados_por_centro["foto_fecha"]\
                                                    .apply(lambda col: get_datetime_from_str(col))
            df_resultados_por_centro["fecha_formato"] = df_resultados_por_centro["foto_fecha"]\
                                                    .apply(lambda col: get_str_format_from_date_str(col))
            
            centros = self.get_lista_centros()
            df_resultados_por_centro["centro_nombre"] = df_resultados_por_centro["centro"]\
                                                    .apply(lambda centro_codigo: centros.get(centro_codigo, {}).get("nombre_centro", ""))
            
            df_resultados_por_centro["path_foto_yolo"] = df_resultados_por_centro["path_foto_yolo"] # Visualizar foto en HTML
            df_resultados_por_centro["foto_yolov5"] = df_resultados_por_centro["path_foto_yolo"]

            if fecha is not None and centro is not None:
                filtro = df_resultados_por_centro["centro"]==centro
                df_resultados_por_centro = df_resultados_por_centro.where(filtro).dropna()
                filtro = df_resultados_por_centro["foto_fecha"]==fecha
                df_resultados_por_centro = df_resultados_por_centro.where(filtro).dropna()

            return df_resultados_por_centro.sort_values(by=["fecha_datetime", "centro", "path_foto_yolo"])
        except Exception as e:
            print(f"Ocurrió un error durante la consulta a la base de datos. Detalle del error: {e}")
            return pd.DataFrame()


    def insert_resumen_diario(self, fecha_insert:str=None, device_location:str=None):
        """ 
        Descripción:
            Contabilizar aedes, mosquitos y moscas encontradas en todo un día.
            Consulta la metadata de las fotos por device_id y device_location.
            Almacena la suma de aedes, mosquitos y moscas en base de datos.
        Input:
            - fecha_insert:str Formato esperado YYYY-MM-DD. Ejemplo: "2022-12-09"
            - device_location:str Ubicación de la cámara.
        """
        dict_resumen_diario = {}
        if fecha_insert:
            df_datos_prediccion = self.get_datos_prediccion(fecha=fecha_insert, centro=device_location)
            
            df_resumen_diario = df_datos_prediccion.groupby(["foto_fecha", "centro"])\
                                    .agg({'cantidad_aedes': 'sum', 
                                        'cantidad_moscas': 'sum', 
                                        'cantidad_mosquitos': 'sum', 
                                        'foto_yolov5': 'last',
                                        'path_foto_yolo': 'last',
                                        'timestamp_procesamiento': 'last'})\
                                    .sort_values(by=["foto_fecha", "centro"])\
                                    .reset_index()
            df_resumen_diario['ultima_foto'] = df_resumen_diario['foto_yolov5'] # S3 o relative path
            df_resumen_diario = df_resumen_diario.drop(["foto_yolov5"], axis=1)
            
            filtro = df_resumen_diario["foto_fecha"]==fecha_insert
            df_resumen_diario = df_resumen_diario.where(filtro).dropna()

            list_resumen_diario = json.loads(df_resumen_diario.to_json(orient="records"))
            
            for resumen_diario in list_resumen_diario:
                # Validar si crear un nuevo nodo o actualizar
                key_find = f"resumenes_diario/{fecha_insert}/{device_location}"
                resultado_fechas = db.child(key_find).get().val()
            
                if resultado_fechas is None:
                    db.child(key_find).set(resumen_diario)
                    print(f"Registro exitoso en base de datos para la fecha {fecha_insert} y device_location {device_location}.")

                if resultado_fechas:
                    db.child(key_find).update(resumen_diario)
                    print(f"Actualización exitosa en base de datos para la fecha {fecha_insert} y device_location {device_location}.")
                    
                dict_resumen_diario[fecha_insert,resumen_diario["centro"]] = resumen_diario
            return dict_resumen_diario
        return dict_resumen_diario


    def get_datos_resumen_diario(self, fecha_filtro=None, centros={}):
        """Obtener las cantidades de aedes, mosquitos y moscas detectadas por día.
        """
        df_resumen_diario = pd.DataFrame()
        try:
            key_find = f"resumenes_diario"
            if fecha_filtro:
                key_find = f"{key_find}/{fecha_filtro}"
                resultado = db.child(key_find).get().val()
                if not resultado:
                    return df_resumen_diario
                resumenes_diarios = [resultado[col] for col in resultado.keys()]
            else:
                resultado = db.child(key_find).get().val()
                resumenes_diarios = [item for sublist in [resultado[col].values() for col in resultado.keys()] for item in sublist]

            if resultado is not None:
                df_resumen_diario = pd.DataFrame(resumenes_diarios)
                df_resumen_diario = df_resumen_diario.sort_values(by=["foto_fecha", "centro"]).sort_values(by=["foto_fecha"], ascending=False)
                if fecha_filtro:
                    return df_resumen_diario
                else:
                    df_resumen_diario["fecha_formato"] = df_resumen_diario["foto_fecha"].apply(get_str_format_from_date_str)
                    #centros = self.get_lista_centros()
                    df_resumen_diario["centro_nombre"] = df_resumen_diario["centro"].apply(lambda centro_codigo: centros.get(centro_codigo, {}).get("nombre_centro", ""))
                    return df_resumen_diario
        except Exception as e:
            print(f"Ocurrió un error durante la consulta del resumen en la base de datos. Detalle del error: {e}")
        finally:
            return df_resumen_diario


    def insert_location(self, device_location, direccion, latitud, localidad, longitud, nombre_centro):
        """ Registrar ubicaciones de las cámaras en base de datos.
        """
        data = {
            "direccion": direccion, 
            "latitud": latitud, 
            "localidad": localidad, 
            "longitud": longitud, 
            "nombre_centro": nombre_centro
        }
        return db.child(f"ubicaciones_trampas/{device_location}").set(data)
