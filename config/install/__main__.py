import inquirer
from ..constants import *
from config.install.src.utils import *
from datetime import datetime
import json
from croniter import croniter, CroniterBadCronError, CroniterBadDateError

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
                    "RESOURCE_NAME",
                ],
            }
        ]
        self.date = datetime.now().strftime("%Y-%m-%d")
        self.monitoring = {}
        self.type_of_installation = None
        self.account_info = {}
        self.enable_backup = False
        self.backup_retention = None
        self.enable_rosie_cleaner = False

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
        self.configure_rosie_cleaner()
        if self.enable_rosie_cleaner:
            self.configure_backup()
        legacy = self.check_legacy(self.date)
        self.cron_expression = self.get_trigger_info()
        self.generate_config_file(legacy)

    def get_aws_account_info(self):
        validate_account = False
        while not validate_account:
            validate_account, ACCOUNT_ID, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN, AWS_REGION = get_aws_account_info()
            self.account_info = {
                "AWS_REGION": AWS_REGION,
                "AWS_ACCOUNT_ID": ACCOUNT_ID,
                "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID,
                "AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY,
                "AWS_SESSION_TOKEN": AWS_SESSION_TOKEN,
            }
    
    def get_trigger_info(self):
        cron_expression = validate_input("\nInforme a expressão CRON que deseja definir para disparar a execução da ROSIE: ", "string")
        while True:
            try:
                croniter(cron_expression)
                break
            except CroniterBadCronError:
                print(f"{RED_START}{BOLD_START}>>> Expressão CRON inválida, por favor, digite uma expressão válida.{END}\n")
                cron_expression = validate_input("Informe a expressão CRON que deseja definir para disparar a execução da ROSIE: ", "string")
            except CroniterBadDateError:
                print(f"{RED_START}{BOLD_START}>>> Data inválida, por favor, digite uma data válida.{END}\n")
                cron_expression = validate_input("Informe a expressão CRON que deseja definir para disparar a execução da ROSIE: ", "string")
        
        return cron_expression

    def select_installation_tool(self):
        self.clear()
        self.print_header("ROSIE - Housekeeping Consumer Tool | Instalação em nova conta AWS")
        print(f"\nQual ferramenta deseja utilizar para instalar o Housekeeping Consumer Tool?")
        questions = [
            inquirer.List('type_of_installation',
                        message=f"Selecione a ferramenta desejada {YELLOW_START}(O terraform deve está instalado localmente){END}",
                        choices=[
                            f"Boto3",
                            f"Terraform",
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
    
    def configure_backup(self):

        self.enable_backup = validate_bool_input(
            f"\nDeseja habilitar o backup dos recursos deletados pela ROSIE? {YELLOW_START}(y/n){END} "
            )

        if self.enable_backup:
            self.backup_retention = validate_number_input(
                f"\nQual o tempo de BACKUP {YELLOW_START}(em dias){END} que deseja configurar? ", 6, 'MAIOR'
                )

    def configure_rosie_cleaner(self):

        self.enable_rosie_cleaner = validate_bool_input(
            f"Deseja habilitar o ROSIE CLEANER para limpeza dos recursos monitorados pela ROSIE? {YELLOW_START}(y/n){END} "
            )

    def generate_config_file(self, legacy):
        config_path = os.path.join(os.path.dirname(__file__), "../../app/config.json")
        with open(config_path, "w") as file:
            data = {
                "ROSIE_INFOS": {
                    "INSTALLATION": {
                        "TYPE_OF_INSTALLATION": self.type_of_installation,
                        "AWS_ACCOUNT": self.account_info,
                        "LEGACY": legacy,
                        "RUNTIME": {
                            "ROLE_ARN": f"arn:aws:iam::{self.account_info['AWS_ACCOUNT_ID']}:role/rosie",
                            "DATABASE_NAME": "workspace_db",
                            "TABLE_NAME": "rosie-control_table",
                            "CRON_EXPRESSION": self.cron_expression,
                            "BUCKET_NAME": f"itau-self-wkp-{self.account_info['AWS_REGION']}-{self.account_info['AWS_ACCOUNT_ID']}",
                            "ENABLE_ROSIE_CLEANER": self.enable_rosie_cleaner,
                            "BACKUP": {
                                "ENABLE_BACKUP": self.enable_backup,
                                "BACKUP_RETENTION": self.backup_retention,
                            },
                            "MONITORING": self.monitoring,
                        }
                    }
                }
            }
            json.dump(data, file, indent=4)
        self.clear()
        print(f"\n{GREEN_START}{BOLD_START}✅ Configuração gerada com sucesso!{END}")
        print(f"\nA instalação da ROSIE começará em instantes, aguarde...\n")

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