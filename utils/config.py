import os
from dotenv import load_dotenv
import pyrebase
import boto3
import pandas as pd

load_dotenv()

# Config Firebase
FIREBASE_USER = os.getenv("FIREBASE_USER")
FIREBASE_PASSWORD = os.getenv("FIREBASE_PASSWORD")
FIREBASE_API_KEY = os.getenv("FIREBASE_API_KEY")
FIREBASE_AUTHDOMAIN = os.getenv("FIREBASE_AUTHDOMAIN")
FIREBASE_DATABASE = os.getenv("FIREBASE_DATABASE")
FIREBASE_PROJECTID = os.getenv("FIREBASE_PROJECTID")
FIREBASE_STORAGEBUCKET = os.getenv("FIREBASE_STORAGEBUCKET")
FIREBASE_MESSAGINGSENDERID = os.getenv("FIREBASE_MESSAGINGSENDERID")
FIREBASE_API_ID = os.getenv("FIREBASE_API_ID")
FIREBASE_MEASUREMENTID = os.getenv("FIREBASE_MEASUREMENTID")


config = {
    "apiKey": FIREBASE_API_KEY,
    "authDomain": FIREBASE_AUTHDOMAIN,
    "databaseURL": FIREBASE_DATABASE,
    "projectId": FIREBASE_PROJECTID,
    "storageBucket": FIREBASE_STORAGEBUCKET,
    "messagingSenderId": FIREBASE_MESSAGINGSENDERID,
    "appId": FIREBASE_API_ID,
    "measurementId": FIREBASE_MEASUREMENTID
}


firebase = pyrebase.initialize_app(config)
auth = firebase.auth()
user = auth.sign_in_with_email_and_password(FIREBASE_USER, FIREBASE_PASSWORD)
db = firebase.database()

# Config AWS
AWS_API_KEY = os.getenv("AWS_API_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
BUCKET_NAME = os.getenv("AWS_S3_BUCKET")
API_URL_PREDICT = os.getenv("API_URL_PREDICT")
PATH_TEMPORAL = "tmp" # Carpeta donde se encontrarán temporalmente las fotos descargadas del bucket.
AWS_BUCKET_RAW = "raw"


session = boto3.Session(
    # aws_access_key_id=AWS_API_KEY,
    # aws_secret_access_key=AWS_SECRET_KEY,
)

s3 = session.client('s3')

# Config API
# lista_centros_prevencion = {
#     "MVL001": ["Oficinas Modernización", "Av. Maipú 2502, B1636AAR Olivos, Provincia de Buenos Aires, Argentina", [-34.51222612434279, -58.49020586908288]],
#     "MVL005": ["Centro Universitario Vicente López", "Carlos Villate 4480, B1605EKT Munro, Provincia de Buenos Aires, Argentina", [-34.53156552888027, -58.519968291402265]],
#     "MVL002": ["Hospital Municipal Dr. Bernardo Houssay", "Pres. Hipólito Yrigoyen 1757, Florida, Provincia de Buenos Aires, Argentina", [-34.5217910510323, -58.48992035822424]],
#     "MVL003": ["Honorable Concejo Deliberante de Vicente López", "AAF, Av. Maipú 2502, B1636 Olivos, Provincia de Buenos Aires, Argentina", [-34.51214514705522, -58.49007573743368]],
#     "MVL004": ["Campo de Deportes Municipal", "Pelliza, Olivos, Provincia de Buenos Aires, Argentina", [-34.512933493180974, -58.50444092807812]]
# }

df = pd.read_excel('UbicacionesTrampas.xlsx')
lista_centros_prevencion = df.set_index('Código')[['EDIFICIOS MUNICIPALES', 'Latitud', 'Longitud', 'Dirección', 'Localidad']].apply(tuple, axis=1).to_dict()
