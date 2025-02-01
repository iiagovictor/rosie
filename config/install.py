import inquirer
from constants import *
from utils import *
from datetime import datetime
import json

class RosieInstaller:
    def __init__(self):
        self.resources = [
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
        ]
        self.date = datetime.now().strftime("%d/%m/%Y")
        self.monitoring = {}
        self.type_of_installation = None
        self.account_info = {}

    def install(self):
        self.clear()
        self.print_header("ROSIE - Housekeeping Consumer Tool | Instalação em nova conta AWS")
        print(f"\n🤖 Olá, eu sou a Rosie e vou te auxiliar na jornada de instalação da ferramenta de Housekeeping Consumer Tool na sua conta AWS.\n")
        print(f"Caso tenha dúvida neste procedimento, acesse minha documentação em '{URL_DOCUMENTATION}'.\n")
        print("🔧 Vamos começar?")
        self.print_separator()
        
        self.get_aws_account_info()
        self.select_installation_tool()
        self.configure_monitoring_modules()
        legacy = self.check_legacy(self.date)
        self.generate_config_file(legacy)

    def get_aws_account_info(self):
        validate_account = False
        while not validate_account:
            validate_account, ACCOUNT_ID, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN = get_aws_account_info()
            self.account_info = {
                "AWS_ACCOUNT_ID": ACCOUNT_ID,
                "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID,
                "AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY,
                "AWS_SESSION_TOKEN": AWS_SESSION_TOKEN,
            }

    def select_installation_tool(self):
        self.clear()
        self.print_header("ROSIE - Housekeeping Consumer Tool | Instalação em nova conta AWS")
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
        self.type_of_installation = answers['type_of_installation']
        self.clear()

    def configure_monitoring_modules(self):
        active_modules = True
        while active_modules:
            for resource in self.resources:
                module = lifecycle_module(resource_name=resource['name'], lifecycle_allowed=resource['lifecycle'])
                self.clear()
                self.monitoring.update({
                        f"{resource['name'].replace(' ', '_').upper()}_MONITORING": module
                    }
                )
                if module['ENABLE_VALIDATION']:
                    active_modules = False
            if active_modules:
                self.clear()
                self.print_separator()
                print(f"\n🚨 {RED_START}{BOLD_START}Nenhum módulo de monitoramento foi habilitado, por favor, habilite ao menos um módulo para prosseguir.{END}")
                self.print_separator()
                input(f"\nPressione {BLUE_START}ENTER{END} para continuar...")
                self.clear()

    def generate_config_file(self, legacy):
        with open(f"../app/config.json", "w") as file:
            data = {
                "ROSIE_INFOS": {
                    "INSTALLATION": {
                        "TYPE_OF_INSTALLATION": self.type_of_installation,
                        "AWS_ACCOUNT": self.account_info,
                        "LEGACY": legacy,
                        "RUNTIME": {
                            "ROLE_ARN": "",
                            "DATABASE_NAME": "",
                            "TABLE_NAME": "",
                            "MONITORING": self.monitoring,
                        }
                    }
                }
            }
            json.dump(data, file, indent=4)

    def clear(self):
        clear()

    def print_header(self, text):
        print_header(text)

    def print_separator(self):
        print_separator()

    def check_legacy(self, date):
        return check_legacy(date)

if __name__ == "__main__":
    rosie = RosieInstaller()
    rosie.install()