import boto3
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))
from config.constants import *

def create_bucket(
        aws_account_id: str,
        bucket_name: str,
        region: str,
        AWS_ACCESS_KEY_ID: str,
        AWS_SECRET_ACCESS_KEY: str
    ):

    print(f"{YELLOW_START}{BOLD_START}\n⏳ Criando bucket '{bucket_name}' ...{END}")
    try:
        s3 = boto3.client('s3', region_name=region, aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
        response = s3.create_bucket(Bucket=bucket_name)
        print(f"{GREEN_START}{BOLD_START}✅ Bucket '{bucket_name}' criado na conta {aws_account_id}.{END}")
        return response
    except Exception as e:
        print(f"{RED_START}{BOLD_START}>>> Erro ao criar o bucket '{bucket_name}': {e}{END}")