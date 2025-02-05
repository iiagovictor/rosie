import boto3
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))
from config.constants import *

def create(
        glue_job_name: str,
        role_arn: str,
        script_location: str, 
        region: str,
        AWS_ACCESS_KEY_ID: str, 
        AWS_SECRET_ACCESS_KEY: str
    ):
    
    glue_client = boto3.client('glue', region_name=region, aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

    try:
        response = glue_client.get_job(JobName=glue_job_name)
    except Exception as e:
        response = None

    if response:
        print(f"{BLUE_START}{BOLD_START}\n🔵 Módulo de monitoramento para os recursos {glue_job_name.replace('_monitoring', '').replace('rosie-', '').replace('_', ' ').upper()} já existe na conta!{END}")
        glue_client.delete_job(JobName=glue_job_name)
        print(f"{YELLOW_START}{BOLD_START}⏳ Recriando módulo de monitoramento para os recursos {glue_job_name.replace('_monitoring', '').replace('rosie-', '').replace('_', ' ').upper()} ...{END}")
    else:
        print(f"{YELLOW_START}{BOLD_START}\n⏳ Criando módulo de monitoramento para os recursos {glue_job_name.replace('_monitoring', '').replace('rosie-', '').replace('_', ' ').upper()} ...{END}")

    try:
        response = glue_client.create_job(
            Name=glue_job_name,
            Description=f"ROSIE - Job de monitoramento para os recursos {glue_job_name.replace('_monitoring', '').replace('rosie-', '').replace('_', ' ')}",
            Role=role_arn,
            Command={
                'Name': 'pythonshell',
                'ScriptLocation': script_location,
                'PythonVersion': '3.9'
            },
            # Connections={
            #     'Connections': [
            #         'analytics-glue-connection-aza',
            #         'analytics-glue-connection-azc'
            #     ]
            # },
            MaxCapacity=0.0625,
            GlueVersion='5.0',
            Timeout=60,
            Tags={
                'rosie': 'monitoring'
            }
        )
        print(f"{GREEN_START}{BOLD_START}✅ Módulo de monitoramento criado com sucesso!{END}")
        return response
    except Exception as e:
        print(f"{RED_START}{BOLD_START}>>> Erro ao criar o módulo de monitoramento: {e}{END}")

def delete(
        glue_job_name: str,
        region: str,
        AWS_ACCESS_KEY_ID: str, 
        AWS_SECRET_ACCESS_KEY: str
    ):
    
    glue_client = boto3.client('glue', region_name=region, aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

    try:
        response = glue_client.get_job(JobName=glue_job_name)
    except Exception as e:
        response = None

    if response:
        print(f"{BLUE_START}{BOLD_START}\n🟡 Módulo de monitoramento para os recursos {glue_job_name.replace('_monitoring', '').replace('rosie-', '').replace('_', ' ').upper()} desativado!{END}")
        print(f"{YELLOW_START}{BOLD_START}⏳ Deletando módulo de monitoramento para os recursos {glue_job_name.replace('_monitoring', '').replace('rosie-', '').replace('_', ' ').upper()} ...{END}")
        glue_client.delete_job(JobName=glue_job_name)
        print(f"{GREEN_START}{BOLD_START}✅ Módulo de monitoramento deletado com sucesso!{END}")
    else:
        pass