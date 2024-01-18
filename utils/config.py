import os
from dotenv import load_dotenv
import pyrebase
import boto3

load_dotenv()

# Scheduler
SCHEDULER_HORAS = int(os.getenv("SCHEDULER_HORAS"), 0)
SCHEDULER_MINUTOS = int(os.getenv("SCHEDULER_MINUTOS"), 0)


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
PATH_TEMPORAL = "raw" # Carpeta donde se encontrarán temporalmente las fotos descargadas del bucket.
AWS_BUCKET_RAW = "raw"


session = boto3.Session(
    # aws_access_key_id=AWS_API_KEY,
    # aws_secret_access_key=AWS_SECRET_KEY,
    region_name='us-east-1'
)

s3 = session.client('s3')
dynamodb = session.resource('dynamodb')
dynamodb_client = session.client('dynamodb')
