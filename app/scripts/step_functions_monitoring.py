import sys
import boto3
import json
import os
import importlib.util

session = boto3.Session()
s3_resource = boto3.resource('s3')

def get_account_id():
    sts_client = boto3.client('sts')
    account_id = sts_client.get_caller_identity()['Account']
    return account_id

ACCOUNT_ID = get_account_id()
BUCKET = f"itau-self-wkp-us-east-1-{ACCOUNT_ID}"

if not os.path.exists('tmp'):
    os.makedirs('tmp')

try:
    s3_resource.Bucket(BUCKET).download_file('ROSIE/src/config.json', 'tmp/config.json')
except Exception as e:
    print(f"Error ao fazer o download do arquivo de configuração: {e}")

try:
    s3_resource.Bucket(BUCKET).download_file('ROSIE/src/rosie.py', 'tmp/rosie.py')
except Exception as e:
    print(f"Error ao fazer o download do arquivo de módulo Rosie: {e}")

sys.path.append('tmp/')

print("Arquivos no diretório tmp:")
for filename in os.listdir('tmp'):
    print(filename)

try:
    with open('tmp/config.json', 'r') as f:
        config = json.load(f)
except FileNotFoundError as e:
    print(f"Error ao abrir o arquivo de configuração: {e}")

spec = importlib.util.spec_from_file_location("rosie", "tmp/rosie.py")
rosie_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(rosie_module)

rosie = rosie_module.Rosie(config=config)
rosie.monitor_sfn()