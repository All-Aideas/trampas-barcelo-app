from os import environ
import pyrebase
import boto3


# Config Firebase
FIREBASE_USER = environ.get("FIREBASE_USER")
FIREBASE_PASSWORD = environ.get("FIREBASE_PASSWORD")
FIREBASE_API_KEY = environ.get("FIREBASE_API_KEY")
FIREBASE_AUTHDOMAIN = environ.get("FIREBASE_AUTHDOMAIN")
FIREBASE_DATABASE = environ.get("FIREBASE_DATABASE")
FIREBASE_PROJECTID = environ.get("FIREBASE_PROJECTID")
FIREBASE_STORAGEBUCKET = environ.get("FIREBASE_STORAGEBUCKET")
FIREBASE_MESSAGINGSENDERID = environ.get("FIREBASE_MESSAGINGSENDERID")
FIREBASE_API_ID = environ.get("FIREBASE_API_ID")
FIREBASE_MEASUREMENTID = environ.get("FIREBASE_MEASUREMENTID")


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
AWS_API_KEY = environ.get("AWS_API_KEY")
AWS_SECRET_KEY = environ.get("AWS_SECRET_KEY")
BUCKET_NAME = environ.get("AWS_S3_BUCKET")
API_URL_PREDICT = environ.get("API_URL_PREDICT")

session = boto3.Session(
    aws_access_key_id=AWS_API_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
)

s3 = session.client('s3')

# Config API
lista_centros_prevencion = [
    ("MVL001", "Centro Universitario Vicente López", "Carlos Villate 4480, B1605EKT Munro, Provincia de Buenos Aires, Argentina", [-34.53156552888027, -58.519968291402265]),
    ("MVL002", "Hospital Municipal Dr. Bernardo Houssay", "Pres. Hipólito Yrigoyen 1757, Florida, Provincia de Buenos Aires, Argentina", [-34.5217910510323, -58.48992035822424]),
    ("MVL003", "Honorable Concejo Deliberante de Vicente López", "AAF, Av. Maipú 2502, B1636 Olivos, Provincia de Buenos Aires, Argentina", [-34.51214514705522, -58.49007573743368]),
    ("MVL004", "Campo de Deportes Municipal", "Pelliza, Olivos, Provincia de Buenos Aires, Argentina", [-34.512933493180974, -58.50444092807812])
]