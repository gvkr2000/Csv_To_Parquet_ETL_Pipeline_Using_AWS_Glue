import os
from dotenv import load_dotenv
load_dotenv()

AWS_REGION = os.getenv('AWS_REGION')
S3_BUCKET = os.getenv('S3_BUCKET')
AWS_ACCESS_KEY_ID=os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY=os.getenv('AWS_SECRET_ACCESS_KEY')
# SQS_QUEUE_URL= os.getenv('SQS_QUEUE_URL')
DYNAMODB_TABLE = os.getenv('DYNAMODB_TABLE')
GLUE_JOB_NAME = os.getenv('GLUE_JOB_NAME')