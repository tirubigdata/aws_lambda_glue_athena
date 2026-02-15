import json
import boto3
import pandas as pd
import io
from datetime import datetime

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    glue = boto3.client('glue')

    bucket_name = event['Records'][0]['s3']['bucket']['name']
    file_name = event['Records'][0]['s3']['object']['key']

    # Read JSON from S3
    response = s3.get_object(Bucket=bucket_name, Key=file_name)
    content = response['Body'].read().decode('utf-8')

    # Parse JSON
    data = json.loads(content)

    # Convert to DataFrame
    df = pd.DataFrame(data)
    print(df)

    # Convert DataFrame to Parquet in memory
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine='pyarrow')

    # Timestamp for unique file name
    timestamp = datetime.now().strftime("%y-%m-%d-%H-%M-%S")

    # Output S3 key
    key_staging = f'orders-parquet-datalake/orders-ETL-{timestamp}.parquet'

    # Upload Parquet to S3
    s3.put_object(
        Bucket=bucket_name,
        Key=key_staging,
        Body=buffer.getvalue()
    )

    # Trigger Glue Crawler
    try:
        glue.start_crawler(Name='yetl_pipeline_crawler')
        print("Glue crawler started")
    except glue.exceptions.CrawlerRunningException:
        print("Crawler already running")

    return {
        'statusCode': 200,
        'body': json.dumps('Parquet file written and crawler triggered')
    }
