import json
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../")))
from config.constants import *
import subprocess

def extract_config():
    with open(os.path.join(os.path.dirname(__file__), "../app/config.json"), 'r') as f:
        return json.load(f)

if __name__ == "__main__":
    config = extract_config()
    type_of_installation = config['ROSIE_INFOS']['INSTALLATION']['TYPE_OF_INSTALLATION']
    if type_of_installation == 'Boto3':
        subprocess.call(['python3', 'infra/boto3'])
    elif type_of_installation == 'Terraform':
        subprocess.call(['python3', 'infra/terraform'])