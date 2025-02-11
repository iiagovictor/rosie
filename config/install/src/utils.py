import sys
import os
from getpass import getpass
import inquirer
import time
import boto3
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from config.constants import *

def clear():
    os.system('cls' if os.name == 'nt' else 'clear')

def print_header(message: str):
    print(f"{BOLD_START}{'='*140}")
    print(f"\n{message}")
    print(f"\n{'='*140}{END}")

def print_separator():
    print(f"\n{BOLD_START}{'='*140}{END}")

def validate_bool_input(message: str) -> bool:
    r = input(f"{message}")
    while r.lower() not in ['y', 'n']:
        print(f"{BOLD_START}{RED_START}>>> Opção inválida. Por favor, entre com 'y' para sim, ou 'n' para não.{END}")
        r = input(f"{message}")
    return r.lower() == 'y'

def validate_number_input(message: str, min: int, operator: str) -> int:
    while True:
        r = input(f"{message}")
        if r.isnumeric() and ((operator == 'MAIOR' and int(r) > min) or (operator == 'MENOR' and int(r) < min)):
            return int(r)
        print(f"{BOLD_START}{RED_START}>>> Valor inválido. Por favor, entre com um número {'maior' if operator == 'MAIOR' else 'menor'} que {min}.{END}\n")

def validate_list_input(message: str, sep: str) -> list:
    while True:
        r = input(f"{message}")
        values = r.split(sep)
        values = [value.upper() for value in values]
        if len(values) > 1 and all(value.strip() for value in values):
            return values
        print(f"{BOLD_START}{RED_START}>>> Valor inválido. Por favor, entre com pelo menos dois (2) valores separados por '{sep}'.{END}\n")

def validate_input(message: str, type_validate: str) -> str:
    while True:
        r = getpass(f"{message}") if type_validate == 'password' else input(f"{message}")
        if r:
            return r
        print(f"{BOLD_START}{RED_START}>>> Valor inválido. Valor não pode ser vazio.{END}\n")

def validate_values_input(message: str, values: list) -> str:
    while True:
        r = input(f"{message}")
        if r in values:
            return r
        print(f"{BOLD_START}{RED_START}>>> Valor inválido. Por favor, entre com um dos valores válidos: {values}{END}")

def get_lifecycle_details(lifecycle: str, resource_name: str) -> dict:
    if lifecycle == 'UNIQUE':
        return get_lifecycle_details_unique(resource_name)
    elif lifecycle == 'TAG':
        return get_lifecycle_details_tag(resource_name)
    elif lifecycle == 'RESOURCE_NAME':
        return get_lifecycle_details_resource_name(resource_name)
    else:
        raise ValueError(f"{BOLD_START}{RED_START}>>> Ciclo de vida inválido. O ciclo de vida deve ser 'UNIQUE', 'TAG' ou 'RESOURCE_NAME'.{END}")

def get_lifecycle_details_unique(resource_name: str) -> dict:
    retention_days = validate_number_input(f"\nQual o tempo de retenção {YELLOW_START}(em dias){END} que deseja configurar para o recurso {resource_name}? ", 6, 'MAIOR')
    retention_alert_coming_days = validate_number_input(f"Qual o tempo de antecedência {YELLOW_START}(em dias){END} que deseja ser alertado sobre a deleção dos recursos do {resource_name}?\nO valor deve ser menor que {retention_days} dias(s): ", retention_days, 'MENOR')
    return {
        "TYPE_OF_MANAGEMENT": "UNIQUE",
        "RETENTION_DAYS": retention_days,
        "DELETION_ALERT_COMING_DAYS": retention_alert_coming_days
    }

def get_lifecycle_details_tag(resource_name: str) -> dict:
    tag_name = validate_input(f"\nInforme o nome da TAG que será usada para o gerenciamento do ciclo de vida, e que todo recurso {resource_name} deverá possuir {YELLOW_START}(KEY){END}: ", 'string')
    tag_values = validate_list_input(f"\nInforme os valores {YELLOW_START}(VALUE){END} possíveis para a TAG {YELLOW_START}'{tag_name}'{END}, separados por vírgula {YELLOW_START}(deverá ser informado no mínimo duas classes){END}: ", ',')
    lifecycle_allowed_values = [get_tag_value_details(value, resource_name) for value in tag_values]
    irregular_value = get_quarantine(resource_name=resource_name, values=tag_values)
    return {
        "TYPE_OF_MANAGEMENT": "TAG",
        "DETAILS": {
            "TAG_NAME": tag_name,
            "ALLOWED_VALUES": lifecycle_allowed_values,
            "IRREGULAR_FORMAT": irregular_value
        }
    }

def get_tag_value_details(value: str, resource_name: str) -> dict:
    print_separator()
    lifecycle_retention = validate_bool_input(f"\nDeseja configurar um tempo de retenção para o valor da TAG {YELLOW_START}'{value}'{END} {YELLOW_START}(y/n){END}? ")
    if lifecycle_retention:
        retention_days = validate_number_input(f"Qual o tempo de retenção {YELLOW_START}(em dias){END} que deseja configurar para o valor da TAG {YELLOW_START}'{value}'{END} do recurso {resource_name}? ", 6, 'MAIOR')
        retention_alert_coming_days = validate_number_input(f"Qual o tempo de antecedência {YELLOW_START}(em dias){END} que deseja ser alertado sobre a deleção dos recursos do {resource_name} com a TAG {YELLOW_START}'{value}'{END}?\nO valor deve ser menor que {retention_days} dias(s): ", retention_days, 'MENOR')
        return {
            "VALUE": value,
            "RETENTION": True,
            "RETENTION_DAYS": retention_days,
            "DELETION_ALERT_COMING_DAYS": retention_alert_coming_days,
            "CHECK_IDLE": False,
            "IDLE_DAYS": None
        }
    else:
        return get_idle_check_details(value, resource_name)

def get_idle_check_details(value: str, resource_name: str) -> dict:
    check_idle = validate_bool_input(f"Deseja verificar se os recursos {resource_name} com a TAG {YELLOW_START}'{value}'{END} estão ociosos para deletá-los? {YELLOW_START}(y/n){END}: ")
    if check_idle:
        idle_days = validate_number_input(f"Qual o tempo de inatividade {YELLOW_START}(em dias){END} que deseja configurar para os recursos {resource_name} com a TAG {YELLOW_START}'{value}'{END} possam ser deletados? ", 6, 'MAIOR')
        retention_alert_coming_days = validate_number_input(f"Quando dias antes deseja definir para que recursos ociosos do {resource_name} com a TAG {YELLOW_START}'{value}'{END} sejam alertados sobre a deleção? {YELLOW_START}(em dias){END}\nO valor deve ser menor que {idle_days} dia(s): ", idle_days, 'MENOR')
        return {
            "VALUE": value,
            "RETENTION": False,
            "RETENTION_DAYS": None,
            "DELETION_ALERT_COMING_DAYS": retention_alert_coming_days,
            "CHECK_IDLE": check_idle,
            "IDLE_DAYS": idle_days
        }
    else:
        return {
            "VALUE": value,
            "RETENTION": False,
            "RETENTION_DAYS": None,
            "DELETION_ALERT_COMING_DAYS": None,
            "CHECK_IDLE": check_idle,
            "IDLE_DAYS": None
        }

def get_lifecycle_details_resource_name(resource_name: str) -> dict:
    sep = validate_values_input(f"\nInforme o separador que deseja usar para separar o afixo do nome do recurso {resource_name} {YELLOW_START}('_' ou '-'){END}: ", ['_', '-'])
    affix = get_affix_type()
    values = validate_list_input(f"\nInforme os valores possíveis para para as classes dos recursos, separados por vírgula {YELLOW_START}(deverá ser informado no mínimo duas classes){END}: ", ",")
    lifecycle_allowed_values = [get_tag_value_details(value, resource_name) for value in values]
    irregular_format = get_quarantine(resource_name=resource_name, values=values)
    return {
        "TYPE_OF_MANAGEMENT": "RESOURCE_NAME",
        "DETAILS": {
            "SEPARATOR": sep,
            "AFFIX": affix,
            "ALLOWED_VALUES": lifecycle_allowed_values,
            "IRREGULAR_FORMAT": irregular_format
        }
    }

def get_affix_type() -> str:
    questions = [
        inquirer.List('affix',
            message="Informe o tipo de afixo que deseja utilizar para identificar as classes dos recursos: (PREFIX, SUFFIX, INFIX)",
            choices=[
                f"PREFIX {YELLOW_START}(Ex: 'RUN_resource_name'){END}",
                f"SUFFIX {YELLOW_START}(Ex: 'resource_name_RUN'){END}",
                f"INFIX {YELLOW_START}(Ex: 'resource_RUN_racf'){END}"
            ]
        )
    ]
    answers = inquirer.prompt(questions)
    return answers['affix'].split(' ')[0]

def lifecycle_module(resource_name: str, lifecycle_allowed: list) -> dict:
    if not lifecycle_allowed:
        raise ValueError(f"A lista de tipos de gerenciamento de ciclo de vida não pode ser vazia.")
    print_header(f"🤖 Módulo de Monitoramento do {resource_name}")
    module = validate_bool_input(f"\nDeseja habilitar o módulo de monitoramento do ciclo de vida dos recursos {resource_name}? {YELLOW_START}(y/n){END}: ")
    if module:
        print(f"\n{GREEN_START}{BOLD_START}Você ativou o módulo de monitoramento do {resource_name}.{END}")
        print_separator()
        questions = [
            inquirer.List('lifecycle',
                message=f"{BLUE_START}Selecione o tipo de gerenciamento de ciclo de vida deseja aplicar para os recursos do {resource_name}? Escolha uma opção{END}",
                choices=lifecycle_allowed
            )
        ]
        answers = inquirer.prompt(questions)
        lifecycle = answers['lifecycle']
        print_separator()
        lifecycle_details = get_lifecycle_details(lifecycle, resource_name)
        return {
            "ENABLE_VALIDATION": module,
            "LIFECYCLE": lifecycle_details
        }
    else:
        print(f"\n{RED_START}{BOLD_START}Você desativou o módulo de monitoramento do {resource_name}.{END}")
        print_separator()
        print(f"\n{BOLD_START}⏳ Carregando próximo módulo...{END}")
        time.sleep(1.5)
        return {
            "ENABLE_VALIDATION": module,
            "LIFECYCLE": None
        }

def get_quarantine(resource_name: str, values: list) -> dict:
    print_separator()
    quarantine = validate_bool_input(f"\nCaso o recurso {resource_name} não tenha definido as classes permitidas {YELLOW_START}{values}{END}, deseja colocá-lo em quarentena por um tempo?\nCaso não, o recurso será deletado {BLUE_START}imediatamente{END}.\n{YELLOW_START}(y/n){END}: ")
    if quarantine:
        quarantine_days = validate_number_input(f"Qual o tempo de quarentena {YELLOW_START}(em dias){END} que deseja configurar para o recurso {resource_name} que não possua as classes permitidas {YELLOW_START}{values}{END}?\n(depois deste prazo, o recurso será deletado {BLUE_START}imediatamente{END}): ", 6, 'MAIOR')
        return {
            "QUARANTINE": quarantine,
            "QUARANTINE_DAYS": quarantine_days
        }
    else:
        return {
            "QUARANTINE": quarantine,
            "QUARANTINE_DAYS": None
        }

def check_legacy(date: str) -> dict:
    legacy = validate_bool_input(f"\n[ATENÇÃO] As padronizações definidas para o gerenciamento dos recursos pela Rosie, devem ser aplicadas para recursos já criados na conta AWS até a data de hoje {BLUE_START}({date}){END}?\n{YELLOW_START}(y/n){END}: ")
    if legacy:
        adequacy_term = validate_number_input(f"\nInforme o prazo {YELLOW_START}(em dias){END} para que os recursos na conta AWS seja adequado de acordo com a padronização definida {YELLOW_START}(mínimo 90 dias){END}: ", 89, 'MAIOR')
        return {
            "ENABLED": legacy,
            "ADEQUACY_TERM": adequacy_term,
            "DATE_START": date
        }
    else:
        return {
            "ENABLED": legacy,
            "ADEQUACY_TERM": None,
            "DATE_START": None
        }

def get_aws_account_info():
    ACCOUNT_ID = validate_input(f"\n{BLUE_START}1. Informe o ID da sua conta AWS:{END} ", "string")
    AWS_ACCESS_KEY_ID = validate_input(f"{BLUE_START}2. Informe a chave de acesso da sua conta AWS:{END} ", "string")
    AWS_SECRET_ACCESS_KEY = validate_input(f"{BLUE_START}3. Informe a chave secreta da sua conta AWS:{END} ", "string")
    AWS_SESSION_TOKEN = validate_input(f"{BLUE_START}4. Informe o token de sessão da sua conta AWS:{END} ", "password")
    print(f"{BLUE_START}5. Informe a região da sua conta AWS:{END}")
    AWS_REGION = inquirer.prompt([
        inquirer.List('region',
            message=f"Selecione a região desejada",
            choices=[
                f"sa-east-1",
                f"us-east-1"
            ]
        )
    ])['region']

    print(f"\n🔍 Vamos validar as informações fornecidas...\n")

    validate_account = validate_aws_account(AWS_REGION, ACCOUNT_ID, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN)

    if validate_account:
        os.environ['AWS_ACCESS_KEY_ID'] = AWS_ACCESS_KEY_ID
        os.environ['AWS_SECRET_ACCESS_KEY'] = AWS_SECRET_ACCESS_KEY
        os.environ['AWS_SESSION_TOKEN'] = AWS_SESSION_TOKEN
        os.environ['AWS_DEFAULT_REGION'] = AWS_REGION

    return validate_account, ACCOUNT_ID, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN, AWS_REGION

def validate_aws_account(AWS_REGION: str, ACCOUNT_ID: str, AWS_ACCESS_KEY_ID: str, AWS_SECRET_ACCESS_KEY: str, AWS_SESSION_TOKEN: str) -> bool:
    print(f"{YELLOW_START}🔍 Validando informações...{END}")

    try:
        sts = boto3.client('sts', region_name=AWS_REGION, aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
        account_id = sts.get_caller_identity().get('Account')
        if account_id != ACCOUNT_ID:
            print(f"\n{RED_START}❌ ID da conta AWS informado não corresponde ao ID da conta autenticada.{END}")
            return False
        print(f"\n{GREEN_START}✅ Conta AWS validada com sucesso.{END}")
        time.sleep(1.5)
        return True
    except Exception as e:
        print(f"\n{RED_START}❌ Erro ao validar ID da conta AWS:{END} {e}")
        return False