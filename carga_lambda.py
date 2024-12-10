import boto3
from botocare.client import config

BUCKET_NAME =  'electra-data-lake'
S3_FOLDER_PREFIX = 'archivos/cargados/'  # Carpeta destino en S3

S3 = boto3.resource('s3')
data = open()