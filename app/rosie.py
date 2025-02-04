import datetime
import boto3
import pandas as pd
from uuid import uuid4
import sys

class RosieLifecycleManager:
    def __init__(self,
                 config: dict, 
                 date_status: str,
                 ):
        
        self.config = config
        self.date_status = date_status
        self.region = config['ROSIE_INFOS']['INSTALLATION']['AWS_ACCOUNT']['AWS_REGION']
        self.account_id = config['ROSIE_INFOS']['INSTALLATION']['AWS_ACCOUNT']['AWS_ACCOUNT_ID']

    def get_lifecycle(self, monitoring: str):
        return self.config['ROSIE_INFOS']['INSTALLATION']['RUNTIME']['MONITORING'][monitoring]['LIFECYCLE']

    def calculate_days(self, date_status: str, target_date: str):
        return (datetime.datetime.strptime(date_status, '%Y-%m-%d') - datetime.datetime.strptime(target_date, '%Y-%m-%d')).days

    def verify_lifecycle(
            self,
            monitoring: str,
            client: boto3.client,
            resource_name: str,
            creation_date: str,
            last_execution_date: str,
            created_in: int = None,
            execution_in: int = None,
    ):
        lifecycle = self.get_lifecycle(monitoring)

        if resource_name in [
            'rosie-step_functions_monitoring',
            'rosie-glue_monitoring',
            'rosie-data_catalog_monitoring',
            'rosie-s3_monitoring',
            'rosie-orquestrador',
            ]:
            return 'ROSIE', 'ignore', 'IGNORE - Recurso de monitoramento da ROSIE.', None, 'ROSIE'


        if created_in is None:
            created_in = self.calculate_days(self.date_status, creation_date)
        if execution_in is None:
            execution_in = self.calculate_days(self.date_status, last_execution_date)

        if self.config['ROSIE_INFOS']['INSTALLATION']['LEGACY']['ENABLED']:
            adequacy_term = self.config['ROSIE_INFOS']['INSTALLATION']['LEGACY']['ADEQUACY_TERM']
            date_start = self.config['ROSIE_INFOS']['INSTALLATION']['LEGACY']['DATE_START']
            start_in = self.calculate_days(self.date_status, date_start)

            if start_in <= adequacy_term:
                return 'N/A', 'legacy', f'LEGACY - Recurso dentro do prazo de adequacao de {adequacy_term} dia(s).', adequacy_term, lifecycle['TYPE_OF_MANAGEMENT']

        if lifecycle['TYPE_OF_MANAGEMENT'] == 'UNIQUE':
            return self.handle_unique_management(lifecycle, created_in)

        if lifecycle['TYPE_OF_MANAGEMENT'] == 'RESOURCE_NAME':
            return self.handle_resource_name_management(lifecycle, resource_name, created_in, execution_in)
        
        if lifecycle['TYPE_OF_MANAGEMENT'] == 'TAG':
            return self.handle_tag_management(lifecycle, monitoring, client, resource_name, created_in, execution_in)

        return 'N/A', 'unknown', 'TYPE_OF_MANAGEMENT não cadastrado', None, lifecycle['TYPE_OF_MANAGEMENT']

    def handle_unique_management(self, lifecycle: dict, created_in: int):
        if created_in > (lifecycle['RETENTION_DAYS'] - lifecycle['DELETION_ALERT_COMING_DAYS']) and created_in <= lifecycle['RETENTION_DAYS']:
            return 'deletion_coming', f'DELETE COMING - Tempo limite de retencao expirara em {abs(lifecycle["RETENTION_DAYS"] - created_in) + 1} dia(s).', lifecycle['RETENTION_DAYS'], lifecycle['TYPE_OF_MANAGEMENT']
        elif created_in > lifecycle['RETENTION_DAYS']:
            return 'UNIQUE', 'delete', 'DELETE - Tempo limite de retencao expirou.', lifecycle['RETENTION_DAYS'], lifecycle['TYPE_OF_MANAGEMENT']
        else:
            return 'UNIQUE', 'keep', 'KEEP - Recursos dentro do tempo limite de retencao.', lifecycle['RETENTION_DAYS'], lifecycle['TYPE_OF_MANAGEMENT']

    def handle_resource_name_management(self, lifecycle: dict, resource_name: str, created_in: int, execution_in: int):
        values = [value['VALUE'].upper() for value in lifecycle['DETAILS']['ALLOWED_VALUES']]
        type_of_management = lifecycle['TYPE_OF_MANAGEMENT']
        separator = lifecycle['DETAILS']['SEPARATOR']
        affix = lifecycle['DETAILS']['AFFIX']

        classification = self.classify_resource(resource_name, separator, affix, values)
        if classification not in values:
            return self.handle_irregular_format(lifecycle, created_in)

        retention_info = next(value for value in lifecycle['DETAILS']['ALLOWED_VALUES'] if value['VALUE'].upper() == classification)
        return self.handle_retention(retention_info, created_in, execution_in, classification, type_of_management)
    
    def handle_tag_management(self, lifecycle: dict, monitoring: str, client: boto3.client, resource_name: str, created_in: int, execution_in: int):
        tag_name = lifecycle['DETAILS']['TAG_NAME']
        type_of_management = lifecycle['TYPE_OF_MANAGEMENT']
        values = [value['VALUE'].upper() for value in lifecycle['DETAILS']['ALLOWED_VALUES']]

        if monitoring == 'GLUE_MONITORING':
            tags = client.get_tags(ResourceArn=f"arn:aws:glue:{self.region}:{self.account_id}:job/{resource_name}")['Tags']
            classification = tags[tag_name].upper() if tags.get(tag_name) else 'N/A'
        elif monitoring == 'STEP_FUNCTIONS_MONITORING':
            tags = client.list_tags_for_resource(resourceArn=f"arn:aws:states:{self.region}:{self.account_id}:stateMachine:{resource_name}")['tags']
            classification = tags[tag_name].upper() if tags.get(tag_name) else 'N/A'
        else:
            raise Exception(f'Monitoramento via TAG não foi cadastrado para o monitoramento {monitoring}.')

        if classification not in values:
            return self.handle_irregular_format(lifecycle, created_in)
        
        retention_info = next(value for value in lifecycle['DETAILS']['ALLOWED_VALUES'] if value['VALUE'].upper() == classification)
        return self.handle_retention(retention_info, created_in, execution_in, classification, type_of_management)

    def classify_resource(self, resource_name, separator, affix, values):
        parts = resource_name.upper().split(separator)
        if affix == 'PREFIX':
            return parts[0]
        elif affix == 'SUFFIX':
            return parts[-1]
        elif affix == 'INFIX':
            for part in parts[1:-1]:
                if part in values:
                    return part
        return 'N/A'

    def handle_irregular_format(self, lifecycle, created_in):
        if lifecycle['DETAILS']['IRREGULAR_FORMAT']['QUARANTINE']:
            if created_in <= lifecycle['DETAILS']['IRREGULAR_FORMAT']['QUARANTINE_DAYS']:
                return 'N/A', 'quarantine', f'QUARANTINE - Recurso nao possui uma classificacao valida, e sera mantido por {lifecycle["DETAILS"]["IRREGULAR_FORMAT"]["QUARANTINE_DAYS"]} dia(s) para que seja adequado.', lifecycle['DETAILS']['IRREGULAR_FORMAT']['QUARANTINE_DAYS'], lifecycle['TYPE_OF_MANAGEMENT']
            else:
                return 'N/A', 'delete', 'DELETE - Recurso deletado por nao possuir uma classificacao valida.', lifecycle['DETAILS']['IRREGULAR_FORMAT']['QUARANTINE_DAYS'], lifecycle['TYPE_OF_MANAGEMENT']
        else:
            return 'N/A', 'delete', 'DELETE - Recurso deletado por nao possuir uma classificacao valida.', None, lifecycle['TYPE_OF_MANAGEMENT']

    def handle_retention(self, retention_info, created_in, execution_in, classification, type_of_management):
        retention_days = retention_info['RETENTION_DAYS']
        deletion_alert_coming_days = retention_info['DELETION_ALERT_COMING_DAYS']
        check_idle = retention_info['CHECK_IDLE']
        idle_days = retention_info['IDLE_DAYS']
        classification = classification.upper()

        if retention_info['RETENTION']:
            if created_in > (retention_days - deletion_alert_coming_days) and created_in <= retention_days:
                return classification, 'deletion_coming', f'DELETE COMING - Tempo limite de retencao expirara em {abs(retention_days - created_in) + 1} dia(s).', retention_days, type_of_management
            elif created_in > retention_days:
                return classification, 'delete', 'DELETE - Tempo limite de retencao expirou.', retention_days, type_of_management
            else:
                return classification, 'keep', 'KEEP - Recursos dentro do tempo limite de retencao.', retention_days, type_of_management
        else:
            if check_idle:
                if execution_in > (idle_days - deletion_alert_coming_days) and execution_in <= idle_days:
                    return classification, 'deletion_coming', f'DELETE COMING - Recurso ocioso por {execution_in} dia(s), e sera deletado em {abs(idle_days - execution_in) + 1} dia(s).', None, type_of_management
                elif execution_in > idle_days:
                    return classification, 'delete', f'DELETE - Recurso ocioso por {execution_in} dia(s). Tempo limite de {idle_days} dia(s) de ociosidade expirou.', None, type_of_management
                else:
                    return classification, 'keep', 'KEEP - Recurso ativo e sem ociosidade.', None, type_of_management
            else:
                return classification, 'keep', 'KEEP - Recurso ativo. Não há verificação de ociosidade.', None, type_of_management

class RosieTableMonitor:

    def __init__(self, config: dict, date_status: str):
        self.config = config
        self.date_status = date_status
        self.ano = self.date_status.split('-')[0]
        self.mes = self.date_status.split('-')[1]
        self.dia = self.date_status.split('-')[2]
        self.database = config['ROSIE_INFOS']['INSTALLATION']['RUNTIME']['DATABASE_NAME']
        self.table = config['ROSIE_INFOS']['INSTALLATION']['RUNTIME']['TABLE_NAME']
        self.bucket = config['ROSIE_INFOS']['INSTALLATION']['RUNTIME']['BUCKET_NAME']

    def save_result(self, verify_list: list, service: str):
        uuid = str(uuid4())

        if len(verify_list) > 0:
            df = pd.DataFrame(verify_list, columns=verify_list[0].keys())
            s3_url = f's3://{self.bucket}/ROSIE/{self.table}/ano_dt_safra={self.ano}/mes_dt_safra={self.mes}/dia_dt_safra={self.dia}/tipo={service}/{self.date_status}-{uuid}.parquet'
            df.to_parquet(s3_url)
            print(f'Dados salvos com sucesso em {s3_url}')
        else:
            print('Nenhum dado para salvar')

    def create_partition(self, glue_client: boto3.client, service: str):
        table_data = self.get_current_schema(glue_client=glue_client)
        partition_list = self.generate_partition(table_data, service)

        try:
            response = glue_client.batch_create_partition(
                DatabaseName=self.database,
                TableName=self.table,
                PartitionInputList=partition_list
            )
            print(f"Partição criada com sucesso para a tabela {self.table} no banco de dados {self.database}") 
        except Exception as e:
            print(f"Erro ao criar partição para a tabela {self.table} no banco de dados {self.database}: {e}")
            sys.exit(1)

    def get_current_schema(self, glue_client: boto3.client) -> dict:
        try:
            response = glue_client.get_table(
                DatabaseName=self.database,
                Name=self.table
            )
        except Exception as e:
            print(f"Erro ao buscar tabela {self.table} no banco de dados {self.database}: {e}")
            sys.exit(1)
        
        table_data = {}
        table_data['input_format'] = response['Table']['StorageDescriptor']['InputFormat']
        table_data['output_format'] = response['Table']['StorageDescriptor']['OutputFormat']
        table_data['location'] = response['Table']['StorageDescriptor']['Location']
        table_data['serde_info'] = response['Table']['StorageDescriptor']['SerdeInfo']
        table_data['partition_keys'] = response['Table']['PartitionKeys']

        return table_data
    
    def generate_partition(self, table_data: dict, service: str) -> list:
        partition_list = []
        part_location = f'{table_data["location"]}/ano_dt_safra={self.ano}/mes_dt_safra={self.mes}/dia_dt_safra={self.dia}/tipo={service}'
        input_dict = {
            'Values': [
                self.ano,
                self.mes,
                self.dia,
                service
            ],
            'StorageDescriptor': {
                'InputFormat': table_data['input_format'],
                'OutputFormat': table_data['output_format'],
                'Location': part_location,
                'SerdeInfo': table_data['serde_info']
            }
        }
        partition_list.append(input_dict.copy())
        return partition_list
    
class Rosie:
    def __init__(self, config):
        self.session = boto3.Session()
        self.config = config
        self.date_status = str(datetime.datetime.now().strftime('%Y-%m-%d'))
        self.lifecycle_manager = RosieLifecycleManager(config, self.date_status)
        self.table_monitor = RosieTableMonitor(config, self.date_status)

    def monitor_glue(
            self
        ):
        
        client = self.session.client('glue')
        service = 'GLUE'
        service = service.upper()

        verify = []
        next_token = None

        while True:
            if next_token:
                response = client.get_jobs(
                    nextToken=next_token
                    )
            else:
                response = client.get_jobs()

            for job in response['Jobs']:
                job_name = job['Name']
                connections = job['Connections']['Connections'] if job.get('Connections') else []

                creation_date = str(job['CreatedOn'].strftime('%Y-%m-%d'))
                created_in = (datetime.datetime.strptime(self.date_status, '%Y-%m-%d') - datetime.datetime.strptime(creation_date, '%Y-%m-%d')).days
                worker_type = job.get('WorkerType', 'N/A')
                number_of_workers = job.get('NumberOfWorkers', 'N/A')
                glue_version = job.get('GlueVersion', 'N/A')
                resource_type = f"{worker_type} - {number_of_workers} DPUS - {glue_version}"

                total_executions = 0
                next_token_jr = None
                last_execution = None

                while True:
                    if next_token_jr:
                        job_runs = client.get_job_runs(
                            JobName=job_name,
                            NextToken=next_token_jr
                            )
                    else:
                        job_runs = client.get_job_runs(
                            JobName=job_name
                            )
                    
                    total_executions += len(job_runs['JobRuns'])

                    if job_runs['JobRuns'] and last_execution is None:
                        last_execution = job_runs['JobRuns'][0]

                    next_token_jr = job_runs.get('NextToken')
                    if not next_token_jr:
                        break
                
                if last_execution:
                    last_execution = job_runs['JobRuns'][0]
                    last_execution_date = str(last_execution['StartedOn'].strftime('%Y-%m-%d'))
                    execution_in = (datetime.datetime.strptime(self.date_status, '%Y-%m-%d') - datetime.datetime.strptime(last_execution_date, '%Y-%m-%d')).days
                
                else:
                    last_execution_date = creation_date
                    execution_in = (datetime.datetime.strptime(self.date_status, '%Y-%m-%d') - datetime.datetime.strptime(creation_date, '%Y-%m-%d')).days

                resource_class, status, reason, retention_days, type_of_management = self.lifecycle_manager.verify_lifecycle(
                    monitoring=f'{service}_MONITORING',
                    client=client,
                    resource_name=job_name,
                    creation_date=creation_date,
                    last_execution_date=last_execution_date
                )

                verify_item = {
                    'nome_recurso': job_name,
                    'tipo_gerenciamento': type_of_management,
                    'classe_recurso': resource_class,
                    'servico': service,
                    'status': status,
                    'motivo': reason,
                    'dt_criacao': creation_date,
                    'dias_criacao': created_in,
                    'dt_ultima_atualizacao': last_execution_date,
                    'dias_ultima_atualizacao': execution_in,
                    'qtd_execucoes': total_executions,
                    'tipo_recurso': resource_type,
                    'dt_status': self.date_status,
                }

                verify.append(verify_item)

            next_token = response.get('NextToken')
            if not next_token:
                break

        self.table_monitor.save_result(verify_list=verify, service=service)
        self.table_monitor.create_partition(glue_client=client, service=service)

    def monitor_sfn(
            self
        ):
        
        client = self.session.client('stepfunctions')
        glue_client = self.session.client('glue')
        service = 'STEP_FUNCTIONS'
        service = service.upper()

        verify = []
        next_token = None

        while True:
            if next_token:
                response = client.list_state_machines(
                    nextToken=next_token
                    )
            else:
                response = client.list_state_machines()

            for state_machine in response['stateMachines']:
                state_machine_name = state_machine['name']
                state_machine_arn = state_machine['stateMachineArn']

                creation_date = str(state_machine['creationDate'].strftime('%Y-%m-%d'))
                created_in = (datetime.datetime.strptime(self.date_status, '%Y-%m-%d') - datetime.datetime.strptime(creation_date, '%Y-%m-%d')).days
                resource_type = state_machine['type']

                total_executions = 0
                next_token_sm = None
                last_execution = None

                while True:
                    if next_token_sm:
                        executions = client.list_executions(
                            stateMachineArn=state_machine_arn,
                            nextToken=next_token_sm
                            )
                    else:
                        executions = client.list_executions(
                            stateMachineArn=state_machine_arn
                            )
                    
                    total_executions += len(executions['executions'])

                    if executions['executions'] and last_execution is None:
                        last_execution = executions['executions'][0]

                    next_token_sm = executions.get('nextToken')
                    if not next_token_sm:
                        break

                if last_execution:
                    last_execution = executions['executions'][0]
                    last_execution_date = str(last_execution['startDate'].strftime('%Y-%m-%d'))
                    execution_in = (datetime.datetime.strptime(self.date_status, '%Y-%m-%d') - datetime.datetime.strptime(last_execution_date, '%Y-%m-%d')).days

                else:
                    last_execution_date = creation_date
                    execution_in = (datetime.datetime.strptime(self.date_status, '%Y-%m-%d') - datetime.datetime.strptime(creation_date, '%Y-%m-%d')).days

                resource_class, status, reason, retention_days, type_of_management = self.lifecycle_manager.verify_lifecycle(
                    monitoring=f'{service}_MONITORING',
                    client=client,
                    resource_name=state_machine_name,
                    creation_date=creation_date,
                    last_execution_date=last_execution_date
                )

                print(f"Tipo: {type(retention_days)}")

                verify_item = {
                    'nome_recurso': state_machine_name,
                    'tipo_gerenciamento': type_of_management,
                    'classe_recurso': resource_class,
                    'servico': service,
                    'status': status,
                    'motivo': reason,
                    'dt_criacao': creation_date,
                    'dias_criacao': created_in,
                    'dt_ultima_atualizacao': last_execution_date,
                    'dias_ultima_atualizacao': execution_in,
                    'qtd_execucoes': total_executions,
                    'tipo_recurso': resource_type,
                    'dt_status': self.date_status,
                }

                verify.append(verify_item)

            next_token = response.get('nextToken')
            if not next_token:
                break

        self.table_monitor.save_result(verify_list=verify, service=service)
        self.table_monitor.create_partition(glue_client=glue_client, service=service)