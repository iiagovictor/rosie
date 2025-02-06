import datetime
import boto3
import pandas as pd
from uuid import uuid4
import sys
import time
import json
import pickle

rosie_resources = [
            '',
            'rosie-step_functions_monitoring',
            'rosie-glue_monitoring',
            'rosie-data_catalog_monitoring',
            'rosie-s3_monitoring',
            'rosie-orquestrador',
            'ROSIE',
            'rosie-control_table',
            'rosie-cleaner_monitoring'
            ]

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
    
    def verify_legacy(
            self,
            status: str,
            reason: str,

    ):
        if self.config['ROSIE_INFOS']['INSTALLATION']['LEGACY']['ENABLED']:
            adequacy_term = self.config['ROSIE_INFOS']['INSTALLATION']['LEGACY']['ADEQUACY_TERM']
            date_start = self.config['ROSIE_INFOS']['INSTALLATION']['LEGACY']['DATE_START']
            start_in = self.calculate_days(self.date_status, date_start)
            
            if status == 'ignore':
                return status, reason
            
            if start_in <= adequacy_term:
                
                if status == 'quarantine':
                    return f'delete (legacy)', f'Recurso nao possui uma classificacao valida. | Recurso dentro do prazo de adequacao de {adequacy_term} dia(s).'
                
                return f'{status} (legacy)', f'{reason} | Recurso dentro do prazo de adequacao de {adequacy_term} dia(s).'
            
            else:
                return status, reason
        
        else:
            return status, reason

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

        if resource_name in rosie_resources:
            return 'ROSIE', 'ignore', 'IGNORE - Recurso de monitoramento da ROSIE.', None, 'ROSIE'


        if created_in is None:
            created_in = self.calculate_days(self.date_status, creation_date)
        if execution_in is None:
            execution_in = self.calculate_days(self.date_status, last_execution_date)

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
            tags_dict = {}
            for tag in tags:
                tags_dict[tag['key']] = tag['value']
            classification = tags_dict[tag_name].upper() if tags_dict.get(tag_name) else 'N/A'
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
        self.session = boto3.Session()
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

    def create_partition(self, service: str):
        table_data = self.get_current_schema()
        partition_list = self.generate_partition(table_data, service)

        try:
            response = self.session.client('glue').batch_create_partition(
                DatabaseName=self.database,
                TableName=self.table,
                PartitionInputList=partition_list
            )
            print(f"Partição criada com sucesso para a tabela {self.table} no banco de dados {self.database}") 
        except Exception as e:
            print(f"Erro ao criar partição para a tabela {self.table} no banco de dados {self.database}: {e}")
            sys.exit(1)

    def get_current_schema(self) -> dict:
        try:
            response = self.session.client('glue').get_table(
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
    
class RosieUtils:
    def __init__(self):
        pass

    def get_size_s3(self, client: boto3.client, bucket: str, folder: str):
        tmp_size_list = []

        paginator = client.get_paginator('list_objects_v2')
        response_iterator = paginator.paginate(
            Bucket=bucket,
            Prefix=folder
        )

        for response in response_iterator:
            if "Contents" in response:
                for file in response['Contents']:
                    tmp_size_list.append(file['Size'])
                    print(f"Found file: {file['Key']} with size: {file['Size']}")

            if "CommonPrefixes" in response:
                for r in response['CommonPrefixes']:
                    sub_response_iterator = paginator.paginate(
                        Bucket=bucket,
                        Prefix=r['Prefix']
                    )
                    for sub_response in sub_response_iterator:
                        files = sub_response.get('Contents')
                        if files:
                            for file in files:
                                tmp_size_list.append(file['Size'])
                                print(f"Found file in sub-prefix: {file['Key']} with size: {file['Size']}")

        total_bytes = sum(tmp_size_list)
        total_size = total_bytes * 0.000001
        total_size = "%.2f" % total_size

        return f"{total_size}mb"
    
    def creation_date_s3(self, client: boto3.client, bucket: str, folder: str):

        tmp_created_in = []

        response = client.list_objects_v2(
            Bucket=bucket,
            Prefix=folder,
            Delimiter='/'
        )

        if "Contents" in response:
            for r in response['Contents']:
                tmp_created_in.append(r['LastModified'].strftime('%Y-%m-%d'))

        if "CommonPrefixes" in response:
            for r in response['CommonPrefixes']:
                response = client.list_objects_v2(
                    Bucket=bucket,
                    Prefix=r['Prefix']
                )
                files = response.get('Contents')
                if files:
                    for file in files:
                        tmp_created_in.append(file['LastModified'].strftime('%Y-%m-%d'))

        return min(tmp_created_in), max(tmp_created_in)
    
class Rosie:
    def __init__(self, config):
        self.session = boto3.Session()
        self.config = config
        self.date_status = str(datetime.datetime.now().strftime('%Y-%m-%d'))
        self.lifecycle_manager = RosieLifecycleManager(config, self.date_status)
        self.table_monitor = RosieTableMonitor(config, self.date_status)
        self.rosie_utils = RosieUtils()

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

                status, reason = self.lifecycle_manager.verify_legacy(status, reason)

                print(f"Tipo: {resource_type}")

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
        self.table_monitor.create_partition(service=service)

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

                status, reason = self.lifecycle_manager.verify_legacy(status, reason)

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
        self.table_monitor.create_partition(service=service)

    def monitor_s3(
            self,
            buckets: list = [{'bucket': 'itau-self-wkp-us-east-1-197045787308', 'prefixes': ['', 'dados/'], 'objectNotDelete': ['dados/']}]
        ):
        
        client = self.session.client('s3')
        service = 'S3'
        service = service.upper()

        verify = []
        
        for bucket in buckets:
            for prefix in bucket['prefixes']:
                temp_list = [{'folder': []}, {'file': []}]
                response = client.list_objects(
                    Bucket=bucket['bucket'],
                    Prefix=prefix,
                    Delimiter='/'
                )
                if response.get('CommonPrefixes') is not None:
                    for item in response.get('CommonPrefixes'):
                        if len(prefix.split('/')) > 2:
                            pref = item['Prefix']
                        else:
                            pref = item.get('Prefix')
                        
                        if not pref in bucket['objectNotDelete'] and not pref in bucket['prefixes']:
                            if pref not in temp_list[0]['folder']:
                                temp_list[0]['folder'].append(pref)
                            else:
                                print(f'Pasta {pref} já foi adicionada')
                                continue
                
                if 'Contents' in response:
                    for item in response['Contents']:
                        if len(prefix.split('/')) > 2:
                            pref = item['Key']
                        else:
                            pref = item.get('Key')
                    
                        if not pref in bucket['objectNotDelete'] and not pref in bucket['prefixes']:
                            if pref not in temp_list[1]['file']:
                                temp_list[1]['file'].append(pref)
                            else:
                                print(f'Arquivo {pref} já foi adicionado')
                                continue
                
                for item in temp_list:
                    typeOfObject = list(item.keys())[0]
                    objects = list(item.values())[0]

                    for obj in objects:
                        if '/' in obj:
                            if obj.split('/')[-1:] == ['']:
                                sub_name = obj.split('/')[-2]
                            else:
                                sub_name = obj.split('/')[-1]
                        else:
                            sub_name = obj

                        creation_date, last_put_data_object = self.rosie_utils.creation_date_s3(
                            client=client, 
                            bucket=bucket['bucket'], 
                            folder=obj
                            )
                        created_in = (datetime.datetime.strptime(self.date_status, '%Y-%m-%d') - datetime.datetime.strptime(creation_date, '%Y-%m-%d')).days
                        updated_in = (datetime.datetime.strptime(self.date_status, '%Y-%m-%d') - datetime.datetime.strptime(last_put_data_object, '%Y-%m-%d')).days
                        size = self.rosie_utils.get_size_s3(client=client, bucket=bucket['bucket'], folder=obj)

                        if typeOfObject == 'folder' and (prefix in bucket['prefixes'] and prefix != ''):
                            resource_class, status, reason, retention_days, type_of_management = self.lifecycle_manager.verify_lifecycle(
                                monitoring=f'{service}_MONITORING',
                                client=client,
                                resource_name=sub_name,
                                creation_date=creation_date,
                                last_execution_date=creation_date
                            )

                        elif sub_name == 'ROSIE':
                            resource_class = 'ROSIE'
                            status = 'ignore'
                            reason = 'IGNORE - Recurso de monitoramento da ROSIE.'
                            retention_days = None
                            type_of_management = 'ROSIE'
                        else:
                            resource_class = 'N/A'
                            status = 'delete'
                            reason = 'DELETE - Objeto fora do diretório de monitoramento.'
                            retention_days = None
                            type_of_management = self.config['ROSIE_INFOS']['INSTALLATION']['RUNTIME']['MONITORING'][f'{service}_MONITORING']['LIFECYCLE']['TYPE_OF_MANAGEMENT']

                        status, reason = self.lifecycle_manager.verify_legacy(status, reason)

                        verify_item = {
                            'nome_recurso': f's3://{bucket["bucket"]}/{obj}',
                            'tipo_gerenciamento': type_of_management,
                            'classe_recurso': resource_class,
                            'servico': service,
                            'status': status,
                            'motivo': reason,
                            'dt_criacao': creation_date,
                            'dias_criacao': created_in,
                            'dt_ultima_atualizacao': last_put_data_object,
                            'dias_ultima_atualizacao': updated_in,
                            'tamanho': size,
                            'dt_status': self.date_status
                        }

                        verify.append(verify_item)

        self.table_monitor.save_result(verify_list=verify, service=service)
        self.table_monitor.create_partition(service=service)

    def monitor_data_catalog(
            self,
            databases: list = ['workspace_db'],
            buckets: list = [{'bucket': 'itau-self-wkp-us-east-1-197045787308', 'prefixes': ['', 'dados/'], 'objectNotDelete': ['dados/']}]
        ):
        
        s3_client = self.session.client('s3')
        glue_client = self.session.client('glue')
        service = 'DATA_CATALOG'
        verify = []
        next_token = None

        for database in databases:
            while True:
                if next_token:
                    response = glue_client.get_tables(
                        DatabaseName=database,
                        NextToken=next_token
                        )
                else:
                    response = glue_client.get_tables(
                        DatabaseName=database
                        )
                
                for table in response['TableList']:
                    path_location = table['StorageDescriptor']['Location'] if 'StorageDescriptor' in table and 'Location' in table['StorageDescriptor'] else ''
                    table_name = table['Name']

                    creation_date = str(table['CreateTime'].strftime('%Y-%m-%d'))
                    created_in = (datetime.datetime.strptime(self.date_status, '%Y-%m-%d') - datetime.datetime.strptime(creation_date, '%Y-%m-%d')).days
                    last_updated = str(table['UpdateTime'].strftime('%Y-%m-%d'))
                    updated_in = (datetime.datetime.strptime(self.date_status, '%Y-%m-%d') - datetime.datetime.strptime(last_updated, '%Y-%m-%d')).days

                    resource_class, status, reason, retention_days, type_of_management = self.lifecycle_manager.verify_lifecycle(
                        monitoring=f'{service}_MONITORING',
                        client=glue_client,
                        resource_name=table_name,
                        creation_date=creation_date,
                        last_execution_date=last_updated
                    )

                    status, reason = self.lifecycle_manager.verify_legacy(status, reason)

                    bucket_name = path_location.split('/')[2]
                    obj = '/'.join(path_location.split('/')[3:])
                    size = self.rosie_utils.get_size_s3(client=s3_client, bucket=bucket_name, folder=obj)

                    verify_item = {
                        'nome_recurso': path_location,
                        'database': database,
                        'tabela': table_name,
                        'tipo_gerenciamento': type_of_management,
                        'classe_recurso': resource_class,
                        'servico': service,
                        'status': status,
                        'motivo': reason,
                        'dt_criacao': creation_date,
                        'dias_criacao': created_in,
                        'dt_ultima_atualizacao': last_updated,
                        'dias_ultima_atualizacao': updated_in,
                        'tamanho': size,
                        'dt_status': self.date_status,
                    }

                    verify.append(verify_item)

                next_token = response.get('NextToken')
                if not next_token:
                    break

        self.table_monitor.save_result(verify_list=verify, service=service)
        self.table_monitor.create_partition(service=service)

class RosieCleaner:
    """
    Classe para limpeza de recursos monitorados pela ROSIE.

    Attributes:
        config (dict): Input do arquivo de configuração.
    """
    def __init__(self,
                 config: dict,
                 ):
        
        self.session = boto3.Session()
        self.date_status = str(datetime.datetime.now().strftime('%Y-%m-%d'))
        self.config = config
        self.table_monitor = RosieTableMonitor(config, self.date_status)

    def get_list(
            self,
            services: list,
            query_execution_id: str = None
            ):
        """
        Método para buscar a lista de recursos monitorados pela ROSIE,
        com status de DELETE.

        Args:
            services (str): Nome do recurso.
            query_execution_id (str): ID da execução da query.
        return:
            pd.DataFrame: Dataframe com a lista de recursos para deletar.
        """

        services = ', '.join([f"'{item}'" for item in services])
        workgroup = self.config['ROSIE_INFOS']['INSTALLATION']['RUNTIME']['WORKGROUP_ATHENA']
        database = self.config['ROSIE_INFOS']['INSTALLATION']['RUNTIME']['DATABASE_NAME']
        tabela = self.config['ROSIE_INFOS']['INSTALLATION']['RUNTIME']['TABLE_NAME']
        s3_output = self.config['ROSIE_INFOS']['INSTALLATION']['RUNTIME']['S3_OUTPUT']

        ano = self.date_status.split('-')[0]
        mes = self.date_status.split('-')[1]
        dia = self.date_status.split('-')[2]

        query = f"""
            SELECT * 
            FROM {database}."{tabela}"
            WHERE ano_dt_safra = '{ano}'
            AND mes_dt_safra = '{mes}'
            AND dia_dt_safra = '{dia}'
            AND status = 'delete'
            AND tipo IN ({services})"""
        
        if not query_execution_id:
            response = self.session.client('athena').start_query_execution(
                QueryString=query,
                QueryExecutionContext={
                    'Database': database
                },
                ResultConfiguration={
                    'OutputLocation': s3_output
                },
                WorkGroup=workgroup
            )

            query_execution_id = response['QueryExecutionId']
        
        status = 'RUNNING'
        
        while status == 'RUNNING':
            time.sleep(5)
            response = self.session.client('athena').get_query_execution(
                QueryExecutionId=query_execution_id
            )
            status = response['QueryExecution']['Status']['State']

        if status != 'SUCCEEDED':
            raise Exception(f"Erro ao executar a query: {query}")
        
        result_s3_path = response['QueryExecution']['ResultConfiguration']['OutputLocation']
        result = self.session.client('s3').get_object(
            Bucket=result_s3_path.split('/')[2],
            Key='/'.join(result_s3_path.split('/')[3:])
        )
        result = pd.read_csv(result['Body'])

        return result
        

    def clean(
            self,
            services: list,
    ):
        """
        Método para deletar os recursos monitorados pela ROSIE.

        Args:
            services (str): Nomes dos recurso.
        """

        glue_list = []
        sfn_list = []
        s3_list = []
        data_catalog_list = []
        unmapped_list = []
        BUCKET = self.config['ROSIE_INFOS']['INSTALLATION']['RUNTIME']['BUCKET_NAME']
        AWS_REGION = self.config['ROSIE_INFOS']['INSTALLATION']['AWS_ACCOUNT']['AWS_REGION']
        AWS_ACCOUNT_ID = self.config['ROSIE_INFOS']['INSTALLATION']['AWS_ACCOUNT']['AWS_ACCOUNT_ID']

        bucketr = self.session.resource('s3').Bucket(BUCKET)

        resource_list = self.get_list(services=services)
        resource_list = resource_list.drop(columns=['ano_dt_safra', 'mes_dt_safra', 'dia_dt_safra', 'tipo'])

        print("Total de recursos para deletar:", len(resource_list))

        for index, row in resource_list.iterrows():
            resource = row['nome_recurso']
            
            if row['servico'] == 'GLUE':
                print(f"\nDeletando recurso GLUE")
                if resource in rosie_resources:
                    continue
                else:
                    try:
                        print(f"Realizando backup dos metadados do recurso '{resource}'")
                        response = self.session.client('glue').get_job(
                            JobName=resource
                        )
                        tags = self.session.client('glue').get_tags(
                            ResourceArn=f"arn:aws:glue:{AWS_REGION}:{AWS_ACCOUNT_ID}:job/{resource}"
                        )
                        metadata = response['Job']
                        metadata['Tags'] = tags['Tags']

                        self.session.client('s3').put_object(
                            Bucket=f'{BUCKET}',
                            Key=f"ROSIE/backup/GLUE/metadata/{metadata['Name']}.pkl",
                            Body=pickle.dumps(metadata)
                        )
                        print(f"Backup dos metadados do recurso '{resource}' realizado com sucesso")

                        print(f"Deletando recurso '{resource}'")
                        self.session.client('glue').delete_job(
                            JobName=resource
                        )
                        print(f"Recurso '{resource}' deletado com sucesso")
                        glue_list.append(resource)

                        resource_list.at[index, 'dt_delete'] = self.date_status
                        resource_list.at[index, 'status'] = 'deleted - backup'

                    except Exception as e:
                        print(f"Erro ao fazer backup dos metadados do recurso e deletar o recurso '{resource}': {e}")

                        resource_list.at[index, 'status'] = 'error'
                                            
                    try:
                        print(f"Realizando backup do script '{resource}'")
                        script_location = response['Job']['Command']['ScriptLocation']
                        self.session.client('s3').copy_object(
                            Bucket=f'{BUCKET}',
                            CopySource={
                                'Bucket': script_location.split('/')[2],
                                'Key': '/'.join(script_location.split('/')[3:])
                            },
                            Key=f"ROSIE/backup/GLUE/scripts/{response['Job']['Name']}.{script_location.split('.')[-1]}"
                        )
                        print(f"Backup do script '{resource}' realizado com sucesso")

                        print(f"Deletando script '{resource}'")
                        self.session.client('s3').delete_object(
                            Bucket=script_location.split('/')[2],
                            Key='/'.join(script_location.split('/')[3:])
                        )
                        print(f"Script '{resource}' deletado com sucesso")

                    except Exception as e:
                        print(f"Erro ao fazer backup do script e deletar o script '{resource}': {e}")

            elif row['servico'] == 'STEP_FUNCTIONS':
                print(f"\nDeletando recurso STEP FUNCTIONS")
                if resource in rosie_resources:
                    continue
                else:
                    try:
                        print(f"Realizando backup dos metadados do recurso '{resource}'")
                        response = self.session.client('stepfunctions').describe_state_machine(
                            stateMachineArn=f"arn:aws:states:{AWS_REGION}:{AWS_ACCOUNT_ID}:stateMachine:{resource}"
                        )
                        response.pop('ResponseMetadata', None)
                        definition = response['definition']
                        response.pop('definition', None)
                        self.session.client('s3').put_object(
                            Bucket=f'{BUCKET}',
                            Key=f"ROSIE/backup/STEP_FUNCTIONS/metadata/{resource}.pkl",
                            Body=pickle.dumps(response)
                        )
                        self.session.client('s3').put_object(
                            Bucket=f'{BUCKET}',
                            Key=f"ROSIE/backup/STEP_FUNCTIONS/definition/{resource}.json",
                            Body=json.dumps(definition)
                        )
                        print(f"Backup dos metadados do recurso '{resource}' realizado com sucesso")

                        print(f"Deletando recurso '{resource}'")
                        self.session.client('stepfunctions').delete_state_machine(
                            stateMachineArn=f"arn:aws:states:{AWS_REGION}:{AWS_ACCOUNT_ID}:stateMachine:{resource}"
                        )
                        print(f"Recurso '{resource}' deletado com sucesso")
                        sfn_list.append(resource)

                        resource_list.at[index, 'dt_delete'] = self.date_status
                        resource_list.at[index, 'status'] = 'deleted - backup'
                    except Exception as e:
                        print(f"Erro ao realizar backup dos metadados e deletar o recurso '{resource}': {e}")

                        resource_list.at[index, 'status'] = 'error'

            elif row['servico'] == 'S3':
                print(f"\nDeletando recurso S3")
                if resource in rosie_resources:
                    continue
                else:
                    try:
                        print(f"Realizando backup dos dados do recurso '{resource}'")
                        bucket_name = resource.split('/')[2]
                        prefix = '/'.join(resource.split('/')[3:])
                        
                        bucket = self.session.resource('s3').Bucket(bucket_name)
                        
                        objects = bucket.objects.filter(Prefix=prefix)
                        
                        for obj in objects:
                            copy_source = {
                                'Bucket': bucket_name,
                                'Key': obj.key
                            }
                            destination_key = f"ROSIE/backup/S3/{obj.key}"
                            self.session.client('s3').copy_object(
                                Bucket=BUCKET,
                                CopySource=copy_source,
                                Key=destination_key
                            )
                        
                        print(f"Backup dos dados do recurso '{resource}' realizado com sucesso")

                        print(f"Deletando recurso '{resource}'")
                        bucketr.objects.filter(Prefix='/'.join(resource.split('/')[3:])).delete()
                        print(f"Recurso '{resource}' deletado com sucesso")
                        s3_list.append(f's3://{BUCKET}/{resource}')

                        resource_list.at[index, 'dt_delete'] = self.date_status
                        resource_list.at[index, 'status'] = 'deleted - backup'
                    except Exception as e:
                        print(f"Erro ao realizar backup dos dados e deletar o recurso '{resource}': {e}")

                        resource_list.at[index, 'status'] = 'error'

            elif row['servico'] == 'DATA_CATALOG':
                print(f"\nDeletando recurso DATA CATALOG")

                database = row['database']
                table = row['tabela']
                bucket = row['nome_recurso'].split('/')[2]
                s3_path = '/'.join(row['nome_recurso'].split('/')[3:])

                if table in rosie_resources:
                    continue
                else:
                    try:
                        print(f"Realizando backup dos metadados do recurso '{database}.{table}'")
                        response = self.session.client('glue').get_table(
                            DatabaseName=database,
                            Name=table
                        )
                        response.pop('ResponseMetadata', None)
                        self.session.client('s3').put_object(
                            Bucket=f'{BUCKET}',
                            Key=f"ROSIE/backup/DATA_CATALOG/metadata/{database}.{table}.pkl",
                            Body=pickle.dumps(response)
                        )
                        print(f"Backup dos metadados do recurso '{database}.{table}' realizado com sucesso")

                        print(f"Deletando recurso '{database}.{table}'")
                        self.session.client('glue').delete_table(
                            DatabaseName=database,
                            Name=table
                        )
                        print(f"Recurso '{database}.{table}' deletado com sucesso")

                        resource_list.at[index, 'dt_delete'] = self.date_status
                        resource_list.at[index, 'status'] = 'deleted - backup'
                    except Exception as e:
                        print(f"Erro ao realizar backup dos metadados e deletar o recurso '{database}.{table}': {e}")
                        resource_list.at[index, 'status'] = 'error'
                    
                    try:
                        print(f"Realizando backup dos dados no {s3_path}, respectivo a tabela lógica '{database}.{table}'")
                        if s3_path not in ['', 'dados/']:
                            print(f"Deletando dados no {s3_path}, respectivo a tabela lógica '{database}.{table}'")
                            
                            objects = self.session.resource('s3').Bucket(bucket).objects.filter(Prefix=s3_path)

                            for obj in objects:
                                copy_source = {
                                    'Bucket': bucket,
                                    'Key': obj.key
                                }
                                destination_key = f"ROSIE/backup/DATA_CATALOG/data/{obj.key}"
                                self.session.client('s3').copy_object(
                                    Bucket=BUCKET,
                                    CopySource=copy_source,
                                    Key=destination_key
                                )
                            self.session.resource('s3').Bucket(bucket).objects.filter(Prefix=s3_path).delete()
                            print(f"Dados deletados com sucesso!")
                    except Exception as e:
                        print(f"Erro ao deletar os dados no {s3_path}, respectivo a tabela lógica '{database}.{table}': {e}")                    

                    data_catalog_list.append(f'{database}.{table}')
            else:
                resource = row['nome_recurso']
                unmapped_list.append(row['nome_recurso'])

        print("\nLista de recursos GLUE deletados: ", glue_list)
        print("Total de recursos GLUE deletados: ", len(glue_list))
        print("Lista de recursos STEP FUNCTIONS deletados: ", sfn_list)
        print("Total de recursos STEP FUNCTIONS deletados: ", len(sfn_list))
        print("Lista de recursos S3 deletados: ", s3_list)
        print("Total de recursos S3 deletados: ", len(s3_list))
        print("Lista de recursos DATA CATALOG deletados: ", data_catalog_list)
        print("Total de recursos DATA CATALOG deletados: ", len(data_catalog_list))
        print("Lista de recursos não mapeados: ", unmapped_list)
        print("Total de recursos não mapeados: ", len(unmapped_list))
        
        resource_list = resource_list.astype(str)
        resource_list = resource_list.applymap(lambda x: None if x in ['None', 'nan'] else x)
        
        def safe_int_conversion(x):
            if x is None:
                return None
            return int(float(x))
        
        resource_list['dias_criacao'] = resource_list['dias_criacao'].apply(lambda x: x.split('.')[0] if x is not None else x)
        resource_list['dias_criacao'] = resource_list['dias_criacao'].apply(safe_int_conversion)
        resource_list['dias_ultima_atualizacao'] = resource_list['dias_ultima_atualizacao'].apply(lambda x: x.split('.')[0] if x is not None else x)
        resource_list['dias_ultima_atualizacao'] = resource_list['dias_ultima_atualizacao'].apply(safe_int_conversion)
        resource_list['qtd_execucoes'] = resource_list['qtd_execucoes'].apply(lambda x: x.split('.')[0] if x is not None else x)
        resource_list['qtd_execucoes'] = resource_list['qtd_execucoes'].apply(safe_int_conversion)
        
        resource_list['dias_criacao'] = resource_list['dias_criacao'].astype('Int64')
        resource_list['dias_ultima_atualizacao'] = resource_list['dias_ultima_atualizacao'].astype('Int64')
        resource_list['qtd_execucoes'] = resource_list['qtd_execucoes'].astype('Int64')

        self.table_monitor.save_result(verify_list=resource_list.to_dict('records'), service='CLEANER')
        self.table_monitor.create_partition(service='CLEANER')