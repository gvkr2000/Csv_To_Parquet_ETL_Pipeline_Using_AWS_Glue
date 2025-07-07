import json
import time
import logging
from utils.boto_clients import sqs_client, dynamodb_client, glue_client
from utils.config import  DYNAMODB_TABLE, GLUE_JOB_NAME, S3_BUCKET, AWS_REGION
from utils.config_loader import load_runtime_config

config = load_runtime_config()
SQS_QUEUE_URL = config["SQS_QUEUE_URL"]

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s: %(message)s')

def create_glue_job_if_not_exists():
    try:
        glue_client.get_job(JobName=GLUE_JOB_NAME)
        logging.info(f"[+] Glue job '{GLUE_JOB_NAME}' already exists.")
    except glue_client.exceptions.EntityNotFoundException:
        logging.info(f"[!] Glue job '{GLUE_JOB_NAME}' not found. Creating now...")
        script_location = f's3://{S3_BUCKET}/scripts/csv_to_parquet_glue_job.py'
        try:
            glue_client.create_job(
                Name=GLUE_JOB_NAME,
                Role='AWSGlueServiceRole-ETL-gvkr',  
                ExecutionProperty={"MaxConcurrentRuns": 1},
                Command={
                    "Name": "glueetl",
                    "ScriptLocation": script_location,
                    "PythonVersion": "3"
                },
                DefaultArguments={
                    "--job-language": "python",
                    "--TempDir": f"s3://{S3_BUCKET}/temp/"
                },
                GlueVersion="4.0"
            )
            logging.info(f"[+] Glue job '{GLUE_JOB_NAME}' created successfully.")
        except Exception as e:
            logging.error(f"[!] Failed to create Glue job: {e}")

def process_s3_event(event):
    for record in event.get('Records', []):
        s3_bucket = record['s3']['bucket']['name']
        s3_key = record['s3']['object']['key']

        logging.info(f"[+] New file uploaded: s3://{s3_bucket}/{s3_key}")

        dynamodb_client.put_item(
            TableName=DYNAMODB_TABLE,
            Item={
                'FileName': {'S': s3_key},
                'Bucket': {'S': s3_bucket},
                'Status': {'S': 'RECEIVED'},
                'Timestamp': {'S': str(time.time())}
            }
        )

        create_glue_job_if_not_exists()

 
        try:
            response = glue_client.start_job_run(
                JobName=GLUE_JOB_NAME,
                Arguments={
                    '--s3_input': f's3://{s3_bucket}/{s3_key}',
                    '--s3_output': f's3://{s3_bucket}/output/'
                }
            )
            logging.info(f"[+] Triggered Glue job run ID: {response['JobRunId']}")
        except Exception as e:
            logging.error(f"[!] Failed to trigger Glue job: {e}")

def poll_sqs_forever():
    logging.info("[+] SQS polling started...")
    while True:
        try:
            response = sqs_client.receive_message(
                QueueUrl=SQS_QUEUE_URL,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=10
            )
            for msg in response.get('Messages', []):
                body = json.loads(msg['Body'])
                s3_event = json.loads(body.get('Message', '{}'))
                process_s3_event(s3_event)

                # Delete the message after processing
                sqs_client.delete_message(
                    QueueUrl=SQS_QUEUE_URL,
                    ReceiptHandle=msg['ReceiptHandle']
                )
        except Exception as e:
            logging.error(f"[!] Polling error: {e}")
        time.sleep(5)

if __name__ == "__main__":
    poll_sqs_forever()