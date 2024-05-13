import boto3
from botocore.exceptions import ClientError
from botocore.client import Config

def create_s3_client(access_key, secret_key, endpoint, region):
    """
    Create a boto3 client configured for Minio or any S3-compatible service.

    :param access_key: S3 access key
    :param secret_key: S3 secret key
    :param endpoint: Endpoint URL for the S3 service
    :param region: Region to use, defaults to us-east-1
    :return: Configured S3 client
    """
    return boto3.client(
        's3',
        region_name=region,
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version='s3v4')
    )

def create_bucket_if_not_exists(s3_client, bucket_name):
    """
    Check if an S3 bucket exists, and if not, create it.

    :param s3_client: Configured S3 client
    :param bucket_name: Name of the bucket to create or check
    :return: None
    """
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' already exists.")
    except ClientError as e:
        error_code = int(e.response['Error']['Code'])
        if error_code == 404:
            # Bucket does not exist, create it
            try:
                s3_client.create_bucket(Bucket=bucket_name)
                print(f"Bucket '{bucket_name}' created.")
            except ClientError as error:
                print(f"Failed to create bucket: {error}")
        else:
            print(f"Error: {e}")

# Credentials and Connection Info
access_key = 'minio'
secret_key = 'minio123'
endpoint = 'http://minio:9000'
region = 'us-east-1'

# Client creation and usage
try:
    s3_client = create_s3_client(access_key, secret_key, endpoint, region)
    bucket_name = 'tpch'# Replace with your bucket name
    create_bucket_if_not_exists(s3_client, bucket_name)
    bucket_name = 'rainforest'# Replace with your bucket name
    create_bucket_if_not_exists(s3_client, bucket_name)
except:
    print("Full catch, check bucket creation script at create_buckets.py")

