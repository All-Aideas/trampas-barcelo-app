import calendar
import time
from datetime import datetime
import pandas as pd
import json
from utils.date_format import get_datetime_from_str, get_timestamp_from_datetime, get_str_format_from_date_str, get_str_date_tz_from_timestamp
from utils.config import db, lista_centros_prevencion


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


def get_lista_centros():
    return lista_centros_prevencion


def get_nombre_del_centro(centro_codigo):
    centros = get_lista_centros()
    return centros.get(centro_codigo, [""])[0]


def campos_json(device_location, device_id, aedes, mosquitos, moscas, foto_original, foto_yolov5, path_foto_raw, path_foto_yolo, foto_fecha):
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
        "timestamp_procesamiento": timestamp, # Fecha de procesamiento
        "fecha_procesamiento": get_str_date_tz_from_timestamp(timestamp, format="%Y-%m-%d %H:%M:%S")
    }


def insert_dato_prediccion(centro, datos_prediccion_foto):
    """ Registrar objeto en base de datos.
    """
    return db.child(f"predicciones_foto/{centro}").push(datos_prediccion_foto)


def get_datos_prediccion(dato_prediccion="", fecha=None, centro=None):
    """ Obtener los datos almacenados en base de datos.
    """
    try:
        df_resultados_por_centro = pd.DataFrame()
        key_find = "predicciones_foto" if dato_prediccion == "" else f"predicciones_foto/{dato_prediccion}"
        resultado = db.child(key_find).get().val()
        
        if dato_prediccion != "":
            for resultados_por_foto in resultado.keys():
                resultado_por_foto = resultado[resultados_por_foto]
                df_resultados_por_centro = df_resultados_por_centro.append(resultado_por_foto, ignore_index=True)
        else:
            for resultados_por_foto in resultado.keys():
                resultado_por_foto = resultado[resultados_por_foto]
                for identificador_foto in resultado_por_foto.keys():
                    new_row = pd.DataFrame([resultado_por_foto[identificador_foto]])
                    df_resultados_por_centro = pd.concat([df_resultados_por_centro, new_row], ignore_index=True)
        # print(df_resultados_por_centro.columns)
        df_resultados_por_centro["fecha"] = df_resultados_por_centro["foto_fecha"]\
                                                .apply(lambda col: col)
        df_resultados_por_centro["fecha_datetime"] = df_resultados_por_centro["foto_fecha"]\
                                                .apply(lambda col: get_datetime_from_str(col))
        df_resultados_por_centro["fecha_formato"] = df_resultados_por_centro["foto_fecha"]\
                                                .apply(lambda col: get_str_format_from_date_str(col))
        df_resultados_por_centro["centro_nombre"] = df_resultados_por_centro["centro"]\
                                                .apply(lambda col: get_nombre_del_centro(col))
        
        # df_resultados_por_centro["path_foto_yolo"] = 'static/' + df_resultados_por_centro["path_foto_yolo"] # Visualizar foto en HTML
        df_resultados_por_centro["path_foto_yolo"] = df_resultados_por_centro["path_foto_yolo"] # Visualizar foto en HTML
        df_resultados_por_centro["foto_yolov5"] = df_resultados_por_centro["path_foto_yolo"]

        if fecha is not None and centro is not None:
            filtro = df_resultados_por_centro["centro"]==centro
            df_resultados_por_centro = df_resultados_por_centro.where(filtro).dropna()
            filtro = df_resultados_por_centro["foto_fecha"]==fecha
            df_resultados_por_centro = df_resultados_por_centro.where(filtro).dropna()

        return df_resultados_por_centro.sort_values(by=["fecha_datetime", "centro", "foto_original"])
    except Exception as e:
        print(f"Ocurrió un error durante la consulta a la base de datos. Detalle del error: {e}")
        return None


def insert_resumen_diario(fecha_insert:str=None):
    """ Insertar registros en tabla resumen diario.
    Parámetro:
    - fecha_insert: Formato: AAAA-MM-DD. Tipo: String.
    """
    dict_resumen_diario = {}
    if fecha_insert:
        df_datos_prediccion = get_datos_prediccion(dato_prediccion="")
        
        df_datos_prediccion = df_datos_prediccion.drop(["timestamp_procesamiento"], axis=1)
        df_resumen_diario = df_datos_prediccion.groupby(["foto_fecha", "centro"])\
                                .agg({'cantidad_aedes': 'sum', 
                                      'cantidad_moscas': 'sum', 
                                      'cantidad_mosquitos': 'sum', 
                                      'foto_yolov5': 'last',
                                      'path_foto_yolo': 'last'})\
                                .sort_values(by=["foto_fecha", "centro"])\
                                .reset_index()
        
        # df_resumen_diario['ultima_foto'] = df_resumen_diario['path_foto_yolo'] # S3 o relative path
        # df_resumen_diario = df_resumen_diario.drop(["path_foto_yolo"], axis=1)
        df_resumen_diario['ultima_foto'] = df_resumen_diario['foto_yolov5'] # S3 o relative path
        df_resumen_diario = df_resumen_diario.drop(["foto_yolov5"], axis=1)
        # print(df_resumen_diario)
        
        filtro = df_resumen_diario["foto_fecha"]==fecha_insert
        df_resumen_diario = df_resumen_diario.where(filtro).dropna()

        list_resumen_diario = json.loads(df_resumen_diario.to_json(orient="records"))
        for resumen_diario in list_resumen_diario:
            db.child(f"resumenes_diario/{fecha_insert}").push(resumen_diario)
            dict_resumen_diario[fecha_insert,resumen_diario["centro"]] = resumen_diario
        return dict_resumen_diario
    return dict_resumen_diario


def get_datos_resumen_diario(fecha_filtro=None):
    """Obtener las cantidades de aedes, mosquitos y moscas detectadas por día.
    """
    df_resumen_diario = pd.DataFrame()

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
            df_resumen_diario["centro_nombre"] = df_resumen_diario["centro"].apply(get_nombre_del_centro)
            return df_resumen_diario

    return df_resumen_diario
