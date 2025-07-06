import sys
from awsglue.utils import getResolvedOptions
from pyspark import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from utils.config import S3_BUCKET

args = getResolvedOptions(sys.argv, ['JOB_NAME','s3_input','s3_output'])

s3_input=f"s3://{S3_BUCKET}/input/competitors_raw_data.csv"
s3_output=f"s3://{S3_BUCKET}/output/"
sc=SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job= Job(glueContext)
job.init(args['JOB_NAME'], args)

input_path = args['s3_input']
output_path = args['s3_output']

logger = glueContext.get_logger()
logger.info("Starting AWS Glue job...")

try:
    df = spark.read.format("csv").option("header", "true").load(input_path)
    df.write.format("parquet").save(output_path)
    logger.info("AWS Glue job completed successfully.")
    job.commit()

except Exception as e:
    logger.error(f"Error during Glue job execution: {e}")