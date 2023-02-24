# Define imports
try:
    import unzip_requirements
except ImportError:
    pass

import json
from io import BytesIO
import time
import os
from utils import  get_db2
import boto3
from urllib.parse import unquote_plus
import json
import os
from textractor import Textractor
from textractor.data.constants import TextractFeatures
from textractor.visualizers.entitylist import EntityList
from textractor.data.constants import TextractFeatures, Direction, DirectionalFinderType
import re
import os.path

import datetime
from PIL import Image
# def addData_all(role,country,state, lga, ward, pu, file, remark, type, lat, long, phone, email):
import urllib.request
import difflib
import uuid
from utils import preprcess_file
#Function to extract the features of an audio file

 
#The lambda_function that calls the other fucntions and do the prediction
def lambda_handler(event, context):
    s3_client = boto3.client('s3')
    
    bucket_name = event['Records'][0]['s3']['bcuket']['name']
    key = event['Records'][0]['s3']['object']['key']
    key = urllib.parse.unquote_plus(key, encoding = 'utf-8')
    tmpkey = key.replace('/', '')
    download_path = '/tmp/{}{}'.format(uuid.uuid4(), tmpkey)
    s3_client.download_file(bucket_name, key, download_path)

    response = preprcess_file(download_path)
    
    return {
        'statusCode': 200,
        'headers':{
            'Content-type':'application/json'
        },
        'body': response
    }
    