import json
from modules import s3, glue, load, table
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
    REGION = "us-east-1"
    AWS_ACCESS_KEY_ID = config['ROSIE_INFOS']['INSTALLATION']['AWS_ACCOUNT']['AWS_ACCESS_KEY_ID']
    AWS_SECRET_ACCESS_KEY = config['ROSIE_INFOS']['INSTALLATION']['AWS_ACCOUNT']['AWS_SECRET_ACCESS_KEY']
    DATABASE = config['ROSIE_INFOS']['INSTALLATION']['RUNTIME']['DATABASE_NAME']
    TABLE = config['ROSIE_INFOS']['INSTALLATION']['RUNTIME']['TABLE_NAME']
    CRON_EXPRESSION = config['ROSIE_INFOS']['INSTALLATION']['RUNTIME']['CRON_EXPRESSION']
    ROLE_ARN = config['ROSIE_INFOS']['INSTALLATION']['RUNTIME']['ROLE_ARN']
    MONITORING = config['ROSIE_INFOS']['INSTALLATION']['RUNTIME']['MONITORING']
    
    s3.create_bucket(
        aws_account_id=AWS_ACCOUNT_ID,
        bucket_name=f"aws-rosie-{AWS_ACCOUNT_ID}-{REGION}",
        region=REGION,
        AWS_ACCESS_KEY_ID=AWS_ACCESS_KEY_ID,
        AWS_SECRET_ACCESS_KEY=AWS_SECRET_ACCESS_KEY
    )

    load.scripts(
        bucket_name=f"aws-rosie-{AWS_ACCOUNT_ID}-{REGION}",
        region=REGION,
        AWS_ACCESS_KEY_ID=AWS_ACCESS_KEY_ID,
        AWS_SECRET_ACCESS_KEY=AWS_SECRET_ACCESS_KEY)

    for monitor in MONITORING:
        if config['ROSIE_INFOS']['INSTALLATION']['RUNTIME']['MONITORING'][monitor]['ENABLE_VALIDATION']:
            glue.create(
                glue_job_name=f"rosie-{monitor.lower()}",
                role_arn=ROLE_ARN,
                script_location=f"s3://aws-rosie-{AWS_ACCOUNT_ID}-{REGION}/scripts/{monitor.lower()}.py",
                region=REGION,
                AWS_ACCESS_KEY_ID=AWS_ACCESS_KEY_ID,
                AWS_SECRET_ACCESS_KEY=AWS_SECRET_ACCESS_KEY
            )

    table.create(
        table_name=TABLE,
        database_name=DATABASE,
        region=REGION,
        AWS_ACCOUNT_ID=AWS_ACCOUNT_ID,
        REGION=REGION,
        AWS_ACCESS_KEY_ID=AWS_ACCESS_KEY_ID,
        AWS_SECRET_ACCESS_KEY=AWS_SECRET_ACCESS_KEY
    )