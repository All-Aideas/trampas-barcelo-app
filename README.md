
# Trampas Barceló Dashboard

Dashboard para visualizar las fotografías procesadas por el modelo de YOLOv8 y mostrar las cantidades de aedes, mosquitos y moscas por ubicación en un mapa.

Invoca el API del modelo reentrenado en YOLOv8 para detectar tres categorías: aedes, mosquitos y moscas.

Recupera las fotografías almacenadas en un bucket de AWS S3 a través de AWS Lambda. Luego, esas imágenes son procesadas por la IA y el resultado es almacenado en la base de datos NoSQL DynamoDB. Finalmente, el resultado es visualizado en el tablero a través de un mapa con las ubicaciones por cada edificio donde se encuentran las cámaras.


## Authors

- [@cesarriat](https://github.com/cesarriat)
- [@fcernafukuzaki](https://github.com/fcernafukuzaki)


## Installation

Instalar con pip

```bash
  pip install -r requirements.txt
```
    
## Environment Variables

Para ejecutar este proyecto, deberá agregar las siguientes variables de entorno a su archivo .env

`API_KEY`

`AWS_S3_BUCKET` Nombre del bucket en AWS.

`FIREBASE_USER` Variable de configuración de Firebase.

`FIREBASE_PASSWORD` Variable de configuración de Firebase.

`FIREBASE_PROJECTID` Variable de configuración de Firebase.

`FIREBASE_API_KEY` Variable de configuración de Firebase.

`FIREBASE_AUTHDOMAIN` Variable de configuración de Firebase.

`FIREBASE_DATABASE` Variable de configuración de Firebase.

`FIREBASE_STORAGEBUCKET` Variable de configuración de Firebase.

`FIREBASE_MESSAGINGSENDERID` Variable de configuración de Firebase.

`FIREBASE_API_ID` Variable de configuración de Firebase.

`FIREBASE_MEASUREMENTID` Variable de configuración de Firebase.

`API_URL_PREDICT` Endpoint del modelo desplegado en HuggingFace.

`SCHEDULER_HORAS` Cantidad de horas en que se volverá a ejecutar el proceso batch.

`SCHEDULER_MINUTOS` Cantidad de minutos en que se volverá a ejecutar el proceso batch.



## Deployment

Desplegar la aplicación

```bash
  cd trampas-barcelo-app
  python3 app.py
```


## Entrenamiento del modelo

[Link](https://github.com/Municipalidad-de-Vicente-Lopez/Trampa_Barcelo) al repositorio con el Notebook para el entrenamiento del modelo.


## License

[MIT](https://choosealicense.com/licenses/mit/)

