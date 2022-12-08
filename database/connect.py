from os import environ
from firebase import firebase
import calendar
import time
from datetime import datetime
import pandas as pd
import json


FIREBASE_DATABASE = environ.get("FIREBASE_DATABASE")
FIREBASE_DATABASE = "https://all-aideas-default-rtdb.firebaseio.com/"

firebase = firebase.FirebaseApplication(FIREBASE_DATABASE, None)


def get_timestamp():
    current_GMT = time.gmtime()
    time_stamp = calendar.timegm(current_GMT)
    return time_stamp


def get_timestamp_from_datetime(date_value):
    datetime_value = datetime(year=date_value.year, month=date_value.month, day=date_value.day)
    return int(datetime_value.timestamp())


def get_timestamp_from_date(timestamp_value):
    date_value = datetime.fromtimestamp(timestamp_value).date()
    return get_timestamp_from_datetime(date_value)


def get_timestamp_format(timestamp_value, format="%d/%m/%Y"):
    date_value = datetime.fromtimestamp(timestamp_value)
    return date_value.strftime(format)


def campos_json(centro, aedes, mosquitos, moscas, foto_original, foto_yolov5):
    return {
        "foto_original": foto_original,
        "foto_yolov5": foto_yolov5,
        "centro": centro,
        "cantidad_aedes": aedes,
        "cantidad_mosquitos": mosquitos,
        "cantidad_moscas": moscas,
        "timestamp": get_timestamp()
    }


def insert_dato_prediccion(centro, datos_prediccion_foto):
    """ Registrar objeto en base de datos.
    """
    return firebase.post(f"/predicciones_foto/{centro}",datos_prediccion_foto)


def get_datos_prediccion(dato_prediccion="", centro=None):
    """ Obtener los datos almacenados en base de datos.
    """
    resultado = firebase.get("/predicciones_foto",dato_prediccion)
    df_resultados_por_centro = pd.DataFrame()
    for resultados_por_foto in resultado.keys():
        resultado_por_foto = resultado[resultados_por_foto]
        for identificador_foto in resultado_por_foto.keys():
            df_resultados_por_centro = df_resultados_por_centro.append(resultado_por_foto[identificador_foto], ignore_index=True)

    if centro is not None:
        filtro = df_resultados_por_centro["centro"]==centro
        df_resultados_por_centro = df_resultados_por_centro.where(filtro).dropna()

    df_resultados_por_centro["fecha"] = df_resultados_por_centro["timestamp"]\
                                            .apply(lambda col: get_timestamp_from_date(col))
    df_resultados_por_centro["fecha_formato"] = df_resultados_por_centro["timestamp"]\
                                            .apply(lambda col: get_timestamp_format(col))
    return df_resultados_por_centro.sort_values(by=["fecha", "centro"])#.sort_values(by=["fecha_formato"], ascending=False)


def insert_resumen_diario(fecha_insert=None):
    """ Insertar registros en tabla resumen diario.
    Parámetro:
    - fecha_insert: Formato: AAAAMMDD. Tipo: String.
    """
    df_datos_prediccion = get_datos_prediccion(dato_prediccion="")
    df_datos_prediccion = df_datos_prediccion.drop(["timestamp"], axis=1)
    df_resumen_diario = df_datos_prediccion.groupby(["fecha", "centro"]).sum()\
                            .sort_values(by=["fecha", "centro"])\
                            .reset_index()

    if fecha_insert is not None:
        fecha_formato = get_timestamp_from_datetime(datetime.strptime(fecha_insert, "%Y-%m-%d").date())
        filtro = df_resumen_diario["fecha"]==fecha_formato
        df_resumen_diario = df_resumen_diario.where(filtro).dropna()
    
    dict_resumen_diario = {}
    list_resumen_diario = json.loads(df_resumen_diario.to_json(orient="records"))
    for resumen_diario in list_resumen_diario:
        fecha = int(resumen_diario["fecha"])
        firebase.post(f"/resumenes_diario/{fecha}", resumen_diario)
        dict_resumen_diario[fecha,resumen_diario["centro"]] = resumen_diario
    return dict_resumen_diario


def get_datos_resumen_diario(fecha_filtro=None):
    """ Obtener las cantidades de aedes, mosquitos y moscas detectadas por día.
    """
    fecha_formato = ""
    df_resumen_diario = pd.DataFrame()
    if fecha_filtro is not None:
        fecha_formato = get_timestamp_from_datetime(datetime.strptime(fecha_filtro, "%Y-%m-%d").date())
        resultado = firebase.get("/resumenes_diario",fecha_formato)
        for identificador_dia in resultado.keys():
            df_resumen_diario = df_resumen_diario.append(resultado[identificador_dia], ignore_index=True)
        return df_resumen_diario.sort_values(by=["fecha", "centro"]).sort_values(by=["fecha"], ascending=False)
    
    resultado = firebase.get("/resumenes_diario",fecha_formato)
    for resultado_diario in resultado.keys():
        resultado_por_dia = resultado[resultado_diario]
        for identificador_dia in resultado_por_dia.keys():
            df_resumen_diario = df_resumen_diario.append(resultado_por_dia[identificador_dia], ignore_index=True)
    
    df_resumen_diario["fecha_formato"] = df_resumen_diario["fecha"]\
                                            .apply(lambda col: get_timestamp_format(col))
    return df_resumen_diario.sort_values(by=["fecha", "centro"]).sort_values(by=["fecha"], ascending=False)

#print(insert_dato_prediccion("MVL001", datos_prediccion_foto))
#print(insert_dato_prediccion("MVL001", datos_prediccion_foto_2))
#print(insert_dato_prediccion("MVL002", datos_prediccion_foto_3))
#print(insert_dato_prediccion("MVL001", datos_prediccion_foto_4))

#print(insert_resumen_diario(fecha_insert=None))
fecha_test = "2022-12-04"
#print(fecha_test)
#print(insert_resumen_diario(fecha_insert=fecha_test))

#print(get_datos_prediccion())
#print(get_datos_resumen_diario())
print(get_datos_resumen_diario(fecha_filtro=fecha_test))
