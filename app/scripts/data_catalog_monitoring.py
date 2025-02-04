import sys
import boto3
import json

session = boto3.Session()
glue_client = session.client('glue')

def get_account_id():
    sts_client = boto3.client('sts')
    account_id = sts_client.get_caller_identity()['Account']
    return account_id

ACCOUNT_ID = get_account_id()
BUCKET = f"itau-self-wkp-sa-east-1-{ACCOUNT_ID}"

s3_resource = boto3.resource('s3')
s3_resource.Bucket(BUCKET).download_file('ROSIE/src/config.json', 'tmp/config.json')
sys.path.append('tmp/')
with open('tmp/config.json', 'r') as f:
    config = json.load(f)
s3_resource.Bucket(BUCKET).download_file('ROSIE/src/rosie.py', 'tmp/rosie.py')
from rosie import Rosie

rosie = Rosie(config=config)
rosie.monitor_catalog()