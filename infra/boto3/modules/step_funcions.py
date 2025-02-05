import boto3
import json
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))
from config.constants import *
import time

def create(
        config: dict,
        sfn_name: str,
        role_arn: str,
        region: str,
        AWS_ACCOUNT_ID: str,
        AWS_ACCESS_KEY_ID: str,
        AWS_SECRET_ACCESS_KEY: str,
    ):

    sfn_client = boto3.client('stepfunctions', region_name=region, aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

    MONITORING = config['ROSIE_INFOS']['INSTALLATION']['RUNTIME']['MONITORING']
    branchs = []
    for monitor in MONITORING:
        if config['ROSIE_INFOS']['INSTALLATION']['RUNTIME']['MONITORING'][monitor]['ENABLE_VALIDATION']:
            branchs.append({
                'StartAt': f'Rosie {monitor.replace("_", " ").title()}',
                'States': {
                    f'Rosie {monitor.replace("_", " ").title()}': {
                        'Type': 'Task',
                        'Resource': f'arn:aws:states:::glue:startJobRun',
                        'Arguments': {'JobName': f'rosie-{monitor.lower()}'},
                        'End': True
                    }
                }
            })

    definition = {'Comment': 'Orquestrador dos módulos Rosie',
                    'StartAt': 'Trigger Modulos de Monitoramento',
                    'States': {'Trigger Modulos de Monitoramento': {'Type': 'Parallel',
                    'Branches': branchs,
                    'End': True}},
                    'QueryLanguage': 'JSONata'}

    try:
        response = sfn_client.describe_state_machine(stateMachineArn=f"arn:aws:states:{region}:{AWS_ACCOUNT_ID}:stateMachine:{sfn_name}")
    except Exception as e:
        response = None

    if response:
        print(f"{BLUE_START}{BOLD_START}\n🔵 Orquestrador dos módulos da Rosie já existe na conta!{END}")
        print(f"{YELLOW_START}{BOLD_START}⏳ Recriando orquestrador dos módulos da Rosie ...{END}")
        sfn_client.update_state_machine(
            stateMachineArn=f"arn:aws:states:{region}:{AWS_ACCOUNT_ID}:stateMachine:{sfn_name}",
            definition=json.dumps(definition),
            roleArn=role_arn
        )
    else:
        print(f"{YELLOW_START}{BOLD_START}\n⏳ Criando orquestrador dos módulos da Rosie ...{END}")

    try:
        response = sfn_client.create_state_machine(
            name=sfn_name,
            definition=json.dumps(definition),
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
