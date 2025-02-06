import json
from modules import s3, glue, load, table, step_funcions
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))
from config.constants import *

def extract_config():
    with open(os.path.join(os.path.dirname(__file__), "../../app/config.json"), 'r') as f:
        return json.load(f)

if __name__ == "__main__":
    config = extract_config()

    AWS_ACCOUNT_ID =  config['ROSIE_INFOS']['INSTALLATION']['AWS_ACCOUNT']['AWS_ACCOUNT_ID']
    AWS_REGION = config['ROSIE_INFOS']['INSTALLATION']['AWS_ACCOUNT']['AWS_REGION']
    AWS_ACCESS_KEY_ID = config['ROSIE_INFOS']['INSTALLATION']['AWS_ACCOUNT']['AWS_ACCESS_KEY_ID']
    AWS_SECRET_ACCESS_KEY = config['ROSIE_INFOS']['INSTALLATION']['AWS_ACCOUNT']['AWS_SECRET_ACCESS_KEY']
    DATABASE = config['ROSIE_INFOS']['INSTALLATION']['RUNTIME']['DATABASE_NAME']
    TABLE = config['ROSIE_INFOS']['INSTALLATION']['RUNTIME']['TABLE_NAME']
    CRON_EXPRESSION = config['ROSIE_INFOS']['INSTALLATION']['RUNTIME']['CRON_EXPRESSION']
    ROLE_ARN = config['ROSIE_INFOS']['INSTALLATION']['RUNTIME']['ROLE_ARN']
    MONITORING = config['ROSIE_INFOS']['INSTALLATION']['RUNTIME']['MONITORING']
    BUCKET = config['ROSIE_INFOS']['INSTALLATION']['RUNTIME']['BUCKET_NAME']
    
    # s3.create_bucket(
    #     aws_account_id=AWS_ACCOUNT_ID,
    #     bucket_name=BUCKET,
    #     region=AWS_REGION,
    #     AWS_ACCESS_KEY_ID=AWS_ACCESS_KEY_ID,
    #     AWS_SECRET_ACCESS_KEY=AWS_SECRET_ACCESS_KEY
    # )

    load.scripts(
        bucket_name=BUCKET,
        region=AWS_REGION,
        AWS_ACCESS_KEY_ID=AWS_ACCESS_KEY_ID,
        AWS_SECRET_ACCESS_KEY=AWS_SECRET_ACCESS_KEY)

    for monitor in MONITORING:
        if config['ROSIE_INFOS']['INSTALLATION']['RUNTIME']['MONITORING'][monitor]['ENABLE_VALIDATION']:
            glue.create(
                glue_job_name=f"rosie-{monitor.lower()}",
                role_arn=ROLE_ARN,
                script_location=f"s3://{BUCKET}/ROSIE/scripts/{monitor.lower()}.py",
                region=AWS_REGION,
                AWS_ACCESS_KEY_ID=AWS_ACCESS_KEY_ID,
                AWS_SECRET_ACCESS_KEY=AWS_SECRET_ACCESS_KEY
            )
        else:
            glue.delete(
                glue_job_name=f"rosie-{monitor.lower()}",
                region=AWS_REGION,
                AWS_ACCESS_KEY_ID=AWS_ACCESS_KEY_ID,
                AWS_SECRET_ACCESS_KEY=AWS_SECRET_ACCESS_KEY
            )
    
    if config['ROSIE_INFOS']['INSTALLATION']['RUNTIME']['ENABLE_ROSIE_CLEANER']:
        glue.create(
            glue_job_name="rosie-cleaner_monitoring",
            role_arn=ROLE_ARN,
            script_location=f"s3://{BUCKET}/ROSIE/scripts/rosie_cleaner_monitoring.py",
            region=AWS_REGION,
            AWS_ACCESS_KEY_ID=AWS_ACCESS_KEY_ID,
            AWS_SECRET_ACCESS_KEY=AWS_SECRET_ACCESS_KEY
        )
    else:
        glue.delete(
            glue_job_name="rosie-cleaner_monitoring",
            region=AWS_REGION,
            AWS_ACCESS_KEY_ID=AWS_ACCESS_KEY_ID,
            AWS_SECRET_ACCESS_KEY=AWS_SECRET_ACCESS_KEY
        )

    table.create(
        bucket=BUCKET,
        table_name=TABLE,
        database_name=DATABASE,
        region=AWS_REGION,
        AWS_ACCESS_KEY_ID=AWS_ACCESS_KEY_ID,
        AWS_SECRET_ACCESS_KEY=AWS_SECRET_ACCESS_KEY
    )

    step_funcions.create(
        config=config,
        sfn_name=f"rosie-orquestrador",
        role_arn=ROLE_ARN,
        region=AWS_REGION,
        AWS_ACCOUNT_ID=AWS_ACCOUNT_ID,
        AWS_ACCESS_KEY_ID=AWS_ACCESS_KEY_ID,
        AWS_SECRET_ACCESS_KEY=AWS_SECRET_ACCESS_KEY
    )

    print(f"\n\n{GREEN_START}{BOLD_START}✅ Instalação da ROSIE finalizada com sucesso!{END}")