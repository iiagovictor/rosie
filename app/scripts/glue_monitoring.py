import sys
import boto3
import datetime
import pandas as pd
import json

session = boto3.Session()
client = session.client('glue')
BUCKET = 'rosie-raw-data'

s3_resource = boto3.resource('s3')
s3_resource.Bucket(BUCKET).download_file('src/config.json', 'tmp/config.json')
sys.path.append('tmp/')
with open('tmp/config.json', 'r') as f:
    config = json.load(f)
s3_resource.Bucket(BUCKET).download_file('src/rosie.py', 'tmp/rosie.py')
from rosie import Rosie

rosie = Rosie(config=config)
rosie.monitor_glue()