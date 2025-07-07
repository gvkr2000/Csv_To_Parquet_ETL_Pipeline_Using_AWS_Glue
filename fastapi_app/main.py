from fastapi import FastAPI, UploadFile, File, BackgroundTasks, HTTPException
from fastapi.responses import FileResponse
import boto3
import os
import shutil
from utils.config import S3_BUCKET, AWS_REGION, GLUE_JOB_NAME  

app = FastAPI()
s3 = boto3.client("s3", region_name=AWS_REGION)
sqs = boto3.client("sqs", region_name=AWS_REGION)
glue = boto3.client("glue", region_name=AWS_REGION)

UPLOAD_DIR = "temp_uploads"
PARQUET_DIR = "temp_parquet"
os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(PARQUET_DIR, exist_ok=True)

@app.get("/")
def root():
    return {"message": "FastAPI is running"}

@app.post("/upload")
async def upload_file(file: UploadFile = File(...), background_tasks: BackgroundTasks = BackgroundTasks()):
    if not file.filename:
        raise HTTPException(status_code=400, detail="No filename provided in upload.")
    local_path = os.path.join(UPLOAD_DIR, file.filename)
    with open(local_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    s3_key = f"input/{file.filename}"
    s3.upload_file(local_path, S3_BUCKET, s3_key)
    return {"message": f"Uploaded {file.filename} to S3 at {s3_key}"}

@app.get("/download")
async def download_parquet(file: str):
    if not file.endswith(".parquet"):
        file += ".parquet"
    s3_key = f"output/{file}"
    local_path = os.path.join(PARQUET_DIR, file)
    try:
        s3.download_file(S3_BUCKET, s3_key, local_path)
        return FileResponse(local_path, media_type='application/octet-stream', filename=file)
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))
    
@app.post("/trigger")
def trigger_glue_job(s3_input: str, s3_output: str):
    response = glue.start_job_run(
        JobName=GLUE_JOB_NAME,
        Arguments={"--s3_input": s3_input, "--s3_output": s3_output}
    )
    return {"JobRunId": response["JobRunId"]}

@app.post("/setup")
def setup_resources():
    try:
        
        s3.create_bucket(
            Bucket=S3_BUCKET
        )
    except s3.exceptions.BucketAlreadyOwnedByYou:
        pass

  
    queue_name = "s3-event-queue"
    sqs_response = sqs.create_queue(QueueName=queue_name)
    queue_url =  sqs.get_queue_url(queue_name)
    queue_arn = sqs.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=['QueueArn']
    )['Attributes']['QueueArn']

    
    s3.put_bucket_notification_configuration(
        Bucket=S3_BUCKET,
        NotificationConfiguration={
            'QueueConfigurations': [
                {
                    'QueueArn': queue_arn,
                    'Events': ['s3:ObjectCreated:*'],
                    'Filter': {
                        'Key': {
                            'FilterRules': [
                                {'Name': 'prefix', 'Value': 'input/'},
                                {'Name': 'suffix', 'Value': '.csv'}
                            ]
                        }
                    }
                }
            ]
        }
    )
    return {
        "message": "S3 bucket, SQS queue, and event trigger set up successfully",
        "s3_bucket": S3_BUCKET,
        "sqs_queue_url": queue_url,
        "sqs_queue_arn": queue_arn
    }

