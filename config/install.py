import inquirer
from constants import *
from utils import *
from datetime import datetime
import json

resources = [
    {
        "name": "GLUE",
        "lifecycle": [
            "UNIQUE",
            "TAG",
            "RESOURCE_NAME",
        ],
    },
    {
        "name": "STEP FUNCTIONS",
        "lifecycle": [
            "UNIQUE",
            "TAG",
            "RESOURCE_NAME",
        ],
    },
    {
        "name": "S3",
        "lifecycle": [
            "UNIQUE",
            "RESOURCE_NAME",
        ],
    },
    {
        "name": "DATA CATALOG",
        "lifecycle": [
            "UNIQUE",
            "TAG",
            "RESOURCE_NAME",
        ],
    }
];

date = datetime.now().strftime("%d/%m/%Y")

def install():
    # Inicialização do arquivo de configuração
    clear()
    print_header("ROSIE - Housekeeping Consumer Tool | Instalação em nova conta AWS")
    print(f"\n🤖 Olá, eu sou a Rosie e vou te auxiliar na jornada de instalação da ferramenta de Housekeeping Consumer Tool na sua conta AWS.\n")
    print(f"Caso tenha dúvida neste procedimento, acesse minha documentação em '{URL_DOCUMENTATION}'.\n")
    print("🔧 Vamos começar?")
    print_separator()
    
    # Input e validação da conta AWS
    validate_account = False
    while not validate_account:
        validate_account, ACCOUNT_ID, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN = get_aws_account_info()
    
    # Seleção da ferramenta de instalação
    clear()
    print_header("ROSIE - Housekeeping Consumer Tool | Instalação em nova conta AWS")
    print(f"\nQual ferramenta deseja utilizar para instalar o Housekeeping Consumer Tool?")
    questions = [
        inquirer.List('type_of_installation',
                    message="Selecione a ferramenta desejada",
                    choices=[
                        f"Boto3",
                        f"Terraform {YELLOW_START}(O terraform deve está instalado localmente){END}",
                    ],
                ),
    ]
    answers = inquirer.prompt(questions)
    type_of_installation = answers['type_of_installation']
    clear()

    # Módulos de monitoramento para o ciclo de vida dos recursos
    monitoring = {}
    active_modules = True
    while active_modules:
        for resource in resources:
            module = lifcycle_module(resource_name=resource['name'], lifecycle_allowed=resource['lifecycle'])
            clear()
            monitoring.update({
                    f"{resource['name'].replace(' ', '_').upper()}_MONITORING": module
                }
            )
            if module['ENABLE_VALIDATION']:
                active_modules = False
        if active_modules:
            print_separator()
            print(f"\n🚨 {RED_START}{BOLD_START}Nenhum módulo de monitoramento foi habilitado, por favor, habilite ao menos um módulo para prosseguir.{END}")
            print_separator()
            input(f"\nPressione {BLUE_START}ENTER{END} para continuar...")
            clear()

    # Validação dos recursos legados
    print_header("ROSIE - Housekeeping Consumer Tool | Instalação em nova conta AWS")
    legacy = check_legacy(date)

    # Gerar arquivo de parametrização
    with open(f"../app/config.json", "w") as file:
        data = {
            "ROSIE_INFOS": {
                "INSTALLATION": {
                    "TYPE_OF_INSTALLATION": type_of_installation,
                    "AWS_ACCOUNT": {
                        "AWS_ACCOUNT_ID": ACCOUNT_ID,
                        "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID,
                        "AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY,
                        "AWS_SESSION_TOKEN": AWS_SESSION_TOKEN,
                    },
                    "LEGACY": legacy,
                    "RUNTIME": {
                        "ROLE_ARN": "",
                        "DATABASE_NAME": "",
                        "TABLE_NAME": "",
                    "MONITORING": monitoring,
                    }
                }
            }
        }
        json.dump(data, file, indent=4)

if __name__ == "__main__":
    install()
