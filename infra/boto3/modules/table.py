import boto3
import json
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))
from config.constants import *

with open(os.path.join(os.path.dirname(__file__), "../../../app/table/schema.json"), 'r') as f:
    schema = json.load(f)

with open(os.path.join(os.path.dirname(__file__), "../../../app/table/partitions.json"), 'r') as f:
    partitions = json.load(f)

def create(
        bucket: str,
        table_name: str, 
        database_name: str,
        region: str,
        AWS_ACCESS_KEY_ID: str,
        AWS_SECRET_ACCESS_KEY: str
    ):
    print(f"{YELLOW_START}{BOLD_START}\n⏳ Criando tabela de controle da ROSIE ...{END}")
    glue_client = boto3.client('glue', region_name=region, aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    try:
        response = glue_client.get_table(DatabaseName=database_name, Name=table_name)
    except Exception as e:
        response = None

    if response:
        print(f"{BLUE_START}{BOLD_START}🔵 Tabela de controle já existe na conta!{END}")
        print(f"{YELLOW_START}{BOLD_START}⏳ Atualizando tabela de controle ...{END}")
        try:
            glue_client.update_table(
                DatabaseName=database_name,
                TableInput={
                    'Name': table_name,
                    'Description': 'Tabela de controle da ROSIE',
                    'StorageDescriptor': {
                        'Columns': schema,
                        'Location': f"s3://{bucket}/ROSIE/{table_name}/",
                        'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
                        'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                        'Compressed': False,
                        'SerdeInfo': {
                            'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
                        }
                    },
                    'TableType': 'EXTERNAL_TABLE',
                    'PartitionKeys': partitions,
                    'Parameters': {
                        'classification': 'parquet',
                        'parquet.compression': 'SNAPPY'
                    }
                }
            )
            print(f"{GREEN_START}{BOLD_START}✅ Tabela de controle atualizada com sucesso!{END}")
        except Exception as e:
            print(f"{RED_START}{BOLD_START}>>> Erro ao atualizar a tabela de controle: {e}{END}")
    else:   
        try:
            response = glue_client.create_table(
                DatabaseName=database_name,
                TableInput={
                    'Name': table_name,
                    'Description': 'Tabela de controle da ROSIE',
                    'StorageDescriptor': {
                        'Columns': schema,
                        'Location': f"s3://{bucket}/ROSIE/{table_name}/",
                        'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
                        'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                        'Compressed': False,
                        'SerdeInfo': {
                            'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
                        }
                    },
                    'TableType': 'EXTERNAL_TABLE',
                    'PartitionKeys': partitions,
                    'Parameters': {
                        'classification': 'parquet',
                        'parquet.compression': 'SNAPPY'
                    }
                }
            )
            print(f"{GREEN_START}{BOLD_START}✅ Tabela de controle criada com sucesso!{END}")
            return response
        except Exception as e:
            print(f"{RED_START}{BOLD_START}>>> Erro ao criar a tabela de controle: {e}{END}")