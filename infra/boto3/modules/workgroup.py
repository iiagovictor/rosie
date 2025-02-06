import boto3
import json
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))
from config.constants import *

# Criar um workgroup da Rosie no Athena

def create(
        workgroup_name: str,
        s3_output_location: str,
        region: str,
        AWS_ACCESS_KEY_ID: str, 
        AWS_SECRET_ACCESS_KEY: str
    ):
    
    athena_client = boto3.client('athena', region_name=region, aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

    try:
        response = athena_client.get_work_group(WorkGroup=workgroup_name)
    except Exception as e:
        response = None

    if response:
        print(f"{BLUE_START}{BOLD_START}\n🔵 Workgroup {workgroup_name} já existe na conta!{END}")
        athena_client.delete_work_group(WorkGroup=workgroup_name)
        print(f"{YELLOW_START}{BOLD_START}⏳ Recriando workgroup {workgroup_name} ...{END}")
    else:
        print(f"{YELLOW_START}{BOLD_START}\n⏳ Criando workgroup {workgroup_name} ...{END}")

    try:
        response = athena_client.create_work_group(
            Name=workgroup_name,
            Configuration={
                'ResultConfiguration': {
                    'OutputLocation': f"{s3_output_location}"
                },
                'EnforceWorkGroupConfiguration': True,
                'PublishCloudWatchMetricsEnabled': True,
                'BytesScannedCutoffPerQuery': 100000000,
                'RequesterPaysEnabled': False,
                'EngineVersion': {
                    'SelectedEngineVersion': 'Athena engine version 3'
                }
            },
            Description=f"ROSIE - Workgroup {workgroup_name}",
            Tags={
                'rosie': 'workgroup'
            }
        )
        print(f"{GREEN_START}{BOLD_START}✅ Workgroup criado com sucesso!{END}")
        return response
    except Exception as e:
        print(f"{RED_START}{BOLD_START}>>> Erro ao criar o workgroup: {e}{END}")
        