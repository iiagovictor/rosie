import datetime
import boto3

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

        if created_in is None:
            created_in = self.calculate_days(self.date_status, creation_date)
        if execution_in is None:
            execution_in = self.calculate_days(self.date_status, last_execution_date)

        if self.config['ROSIE_INFOS']['INSTALLATION']['LEGACY']['ENABLED']:
            adequacy_term = self.config['ROSIE_INFOS']['INSTALLATION']['LEGACY']['ADEQUACY_TERM']
            date_start = self.config['ROSIE_INFOS']['INSTALLATION']['LEGACY']['DATE_START']
            start_in = self.calculate_days(self.date_status, date_start)

            if start_in <= adequacy_term:
                return 'legacy', f'LEGACY - Recurso dentro do prazo de adequacao de {adequacy_term} dia(s).', adequacy_term

        if lifecycle['TYPE_OF_MANAGEMENT'] == 'UNIQUE':
            return self.handle_unique_management(lifecycle, created_in)

        if lifecycle['TYPE_OF_MANAGEMENT'] == 'RESOURCE_NAME':
            return self.handle_resource_name_management(lifecycle, resource_name, created_in, execution_in)
        
        if lifecycle['TYPE_OF_MANAGEMENT'] == 'TAG':
            return self.handle_tag_management(lifecycle, monitoring, client, resource_name, created_in, execution_in)

        return 'unknown', 'TYPE_OF_MANAGEMENT não cadastrado', None

    def handle_unique_management(self, lifecycle: dict, created_in: int):
        if created_in > (lifecycle['RETENTION_DAYS'] - lifecycle['DELETION_ALERT_COMING_DAYS']) and created_in <= lifecycle['RETENTION_DAYS']:
            return 'deletion_coming', f'DELETE COMING - Tempo limite de retencao expirara em {abs(lifecycle["RETENTION_DAYS"] - created_in) + 1} dia(s).', lifecycle['RETENTION_DAYS']
        elif created_in > lifecycle['RETENTION_DAYS']:
            return 'delete', 'DELETE - Tempo limite de retencao expirou.', lifecycle['RETENTION_DAYS']
        else:
            return 'keep', 'KEEP - Recursos dentro do tempo limite de retencao.', lifecycle['RETENTION_DAYS']

    def handle_resource_name_management(self, lifecycle: dict, resource_name: str, created_in: int, execution_in: int):
        values = [value['VALUE'].upper() for value in lifecycle['DETAILS']['ALLOWED_VALUES']]
        separator = lifecycle['DETAILS']['SEPARATOR']
        affix = lifecycle['DETAILS']['AFFIX']

        classification = self.classify_resource(resource_name, separator, affix, values)
        if classification not in values:
            return self.handle_irregular_format(lifecycle, created_in)

        retention_info = next(value for value in lifecycle['DETAILS']['ALLOWED_VALUES'] if value['VALUE'].upper() == classification)
        return self.handle_retention(retention_info, created_in, execution_in)
    
    def handle_tag_management(self, lifecycle: dict, monitoring: str, client: boto3.client, resource_name: str, created_in: int, execution_in: int):
        tag_name = lifecycle['DETAILS']['TAG_NAME']
        values = [value['VALUE'].upper() for value in lifecycle['DETAILS']['ALLOWED_VALUES']]

        if monitoring == 'GLUE_MONITORING':
            tags = client.get_tags(ResourceArn=f"arn:aws:glue:{self.region}:{self.account_id}:job/{resource_name}")['Tags']
            classification = tags[tag_name].upper() if tags.get(tag_name) else 'N/A'

        if classification not in values:
            return self.handle_irregular_format(lifecycle, created_in)
        
        retention_info = next(value for value in lifecycle['DETAILS']['ALLOWED_VALUES'] if value['VALUE'].upper() == classification)
        return self.handle_retention(retention_info, created_in, execution_in)

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
                return 'quarantine', f'QUARANTINE - Recurso nao possui uma classificacao valida, e sera mantido por {lifecycle["DETAILS"]["IRREGULAR_FORMAT"]["QUARANTINE_DAYS"]} dia(s) para que seja adequado.', lifecycle['DETAILS']['IRREGULAR_FORMAT']['QUARANTINE_DAYS']
            else:
                return 'delete', 'DELETE - Recurso deletado por nao possuir uma classificacao valida.', lifecycle['DETAILS']['IRREGULAR_FORMAT']['QUARANTINE_DAYS']
        else:
            return 'delete', 'DELETE - Recurso deletado por nao possuir uma classificacao valida.', None

    def handle_retention(self, retention_info, created_in, execution_in):
        retention_days = retention_info['RETENTION_DAYS']
        deletion_alert_coming_days = retention_info['DELETION_ALERT_COMING_DAYS']
        check_idle = retention_info['CHECK_IDLE']
        idle_days = retention_info['IDLE_DAYS']

        if retention_info['RETENTION']:
            if created_in > (retention_days - deletion_alert_coming_days) and created_in <= retention_days:
                return 'deletion_coming', f'DELETE COMING - Tempo limite de retencao expirara em {abs(retention_days - created_in) + 1} dia(s).', retention_days
            elif created_in > retention_days:
                return 'delete', 'DELETE - Tempo limite de retencao expirou.', retention_days
            else:
                return 'keep', 'KEEP - Recursos dentro do tempo limite de retencao.', retention_days
        else:
            if check_idle:
                if execution_in > (idle_days - deletion_alert_coming_days) and execution_in <= idle_days:
                    return 'deletion_coming', f'DELETE COMING - Recurso ocioso por {execution_in} dia(s), e sera deletado em {abs(idle_days - execution_in) + 1} dia(s).', None
                elif execution_in > idle_days:
                    return 'delete', f'DELETE - Recurso ocioso por {execution_in} dia(s). Tempo limite de {idle_days} dia(s) de ociosidade expirou.', None
                else:
                    return 'keep', 'KEEP - Recurso ativo e sem ociosidade.', None
            else:
                return 'keep', 'KEEP - Recurso ativo. Não há verificação de ociosidade.', None

class Rosie:
    def __init__(self, session, config):
        self.session = boto3.Session()
        self.config = config
        self.date_status = str(datetime.datetime.now().strftime('%Y-%m-%d'))
        self.lifecycle_manager = RosieLifecycleManager(config, self.date_status)

    def monitor_glue(
            self
        ):
        
        client = self.session.client('glue')

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
                status = 'keep'
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

                glue_monitoring = self.config['ROSIE_INFOS']['INSTALLATION']['RUNTIME']['MONITORING']['GLUE_MONITORING']
                lifecycle = glue_monitoring['LIFECYCLE']
                


                self.lifecycle_manager.verify_lifecycle(
                    monitoring='GLUE_MONITORING',
                    client=client,
                    resource_name=job_name,
                    creation_date=creation_date,
                    last_execution_date=last_execution_date,
                    created_in=created_in,
                    execution_in=execution_in
                )

                
                
