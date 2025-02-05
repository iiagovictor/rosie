import boto3
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))
from config.constants import *

def create(
        eventbridge_name: str,
        region: str,
        AWS_ACCESS_KEY_ID: str,
        AWS_SECRET_ACCESS_KEY: str
    ):
    
    eventbridge_client = boto3.client('eventbridge', region_name=region, aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

    try:
        response = eventbridge_client.describe_rule(Name=eventbridge_name)
    except Exception as e:
        response = None

    if response:
        print(f"{BLUE_START}{BOLD_START}\n🟡 Trigger para Rosie