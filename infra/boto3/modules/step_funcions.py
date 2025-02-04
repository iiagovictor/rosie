import boto3
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))
from config.constants import *

def create(
        sfn_name: str,
        role_arn: str,
        region: str,
        AWS_ACCESS_KEY_ID: str,
        AWS_SECRET_ACCESS_KEY: str,
        AWS_SESSION_TOKEN: str
    ):

    sfn_client = boto3.client('stepfunctions', region_name=region, aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY, aws_session_token=AWS_SESSION_TOKEN)

    try:
        response = sfn_client.describe_state_machine(stateMachineArn=sfn_name)
    except Exception as e:
        response = None

    if response:
        print(f"{BLUE_START}{BOLD_START}\n🟡 Módulo de monitoramento para os recursos {sfn_name.replace('rosie-', '').replace('_', ' ').upper()} já existe na conta!{END}")
        sfn_client.delete_state_machine(stateMachineArn=sfn_name)
        print(f"{YELLOW_START}{BOLD_START}⏳ Recriando módulo de monitoramento para os recursos {sfn_name.replace('rosie-', '').replace('_', ' ').upper()} ...{END}")

    else:
        print(f"{YELLOW_START}{BOLD_START}\n⏳ Criando módulo de monitoramento para os recursos {sfn_name.replace('rosie-', '').replace('_', ' ').upper()} ...{END}")

    try:
        response = sfn_client.create_state_machine(
            name=sfn_name,
            definition='{}',
            roleArn=role_arn,
            tags=[
                {
                    'key': 'rosie',
                    'value': 'monitoring'
                },
            ]
        )
        print(f"{GREEN_START}{BOLD_START}✅ Módulo de monitoramento criado com sucesso!{END}")
        return response
    except Exception as e:
        print(f"{RED_START}{BOLD_START}>>> Erro ao criar o módulo de monitoramento: {e}{END}")
