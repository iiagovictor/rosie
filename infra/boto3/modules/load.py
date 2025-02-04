import boto3
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))
from config.constants import *

def scripts(
        bucket_name: str,
        region: str,
        AWS_ACCESS_KEY_ID: str,
        AWS_SECRET_ACCESS_KEY: str
    ):
    
    print(f"{YELLOW_START}{BOLD_START}\n⏳ Carregando scripts runtime e arquivo config no bucket '{bucket_name}' ...{END}")
    
    try:
        s3 = boto3.client('s3', region_name=region, aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
        s3.upload_file(Filename=f'{os.path.join(os.path.dirname(__file__), "../../../app/config.json")}', Bucket=bucket_name, Key='ROSIE/src/config.json')
        s3.upload_file(Filename=f'{os.path.join(os.path.dirname(__file__), "../../../app/rosie.py")}', Bucket=bucket_name, Key='ROSIE/src/rosie.py')

        scripts_path = os.path.join(os.path.dirname(__file__), "../../../app/scripts/")
        for script in os.listdir(scripts_path):
            if script.endswith(".py"):
                s3.upload_file(Filename=os.path.join(scripts_path, script), Bucket=bucket_name, Key=f'ROSIE/scripts/{script}')
        
        print(f"{GREEN_START}{BOLD_START}✅ Scripts carregados com sucesso!{END}")
    except Exception as e:
        print(f"{RED_START}{BOLD_START}>>> Erro ao carregar os scripts no bucket '{bucket_name}': {e}{END}")