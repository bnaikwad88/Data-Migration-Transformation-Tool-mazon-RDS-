import botocore.exceptions
import requests
import zipfile
import boto3
import json
import io

# Set your AWS access key and secret key
access_key = "AKIAX37W6DV72SGBXBVV"
secret_key = "M32TkNWln0DTnqgCh0zCl13pjkZD9+D5zRiqgaPu"
region_name = "ap-south-1"


# Set your predefined S3 bucket and DynamoDB table names
table_name = "jsonData"
bucket_name = "project-6-s3-dynamodb"

# Create an AWS session using your credentials and region
session = boto3.Session(
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
    region_name=region_name
)

# Use the session to create the S3 client
s3 = session.client('s3')

# Downloading zip content
url = "https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip"
headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36 Edg/110.0.1587.63'}
response_data = requests.get(url, headers=headers, stream=True)
response_content = response_data.content

# Uploading the zip file contents from the web URL to the predefined S3 bucket
s3.upload_fileobj(io.BytesIO(response_content), bucket_name, "test.zip")
print("Zip file uploaded successfully")

# Extracting the contents of the zip file and uploading to S3
zip_obj = s3.get_object(Bucket=bucket_name, Key="test.zip")
zip_file = zipfile.ZipFile(io.BytesIO(zip_obj['Body'].read()))

extracted_files = []
for i, file in enumerate(zip_file.namelist()):
    if i >= 20:
        break

    file_name = file.split('/')[-1]
    extracted_files.append(file_name)
    s3.upload_fileobj(zip_file.open(file), bucket_name, f"unzip/{file_name}")
    print(f'In progress - {file_name}')
print("Zip file extracted and files uploaded to S3 successfully")

# Listing the filenames of the extracted files in the S3 bucket
s3_objects = s3.list_objects(Bucket=bucket_name, Prefix="unzip/")['Contents']
extracted_filenames = [obj['Key'].split('/')[-1] for obj in s3_objects]

# Create a boto3 client to access the DynamoDB service
dynamodb = boto3.resource('dynamodb', region_name=region_name,
                          aws_access_key_id=access_key,
                          aws_secret_access_key=secret_key)

# Retrieve the DynamoDB table
table = dynamodb.Table(table_name)
print("Table object:", table)

# Limit the number of files processed to 100
for i, file in enumerate(extracted_filenames):
    if i >= 20:
        break

    s3_object = s3.get_object(Bucket=bucket_name, Key=f"unzip/{file}")
    s3_data = s3_object['Body'].read().decode('utf-8')
    json_data = json.loads(s3_data)

    # Convert the data to the correct format for DynamoDB
    dynamodb_data = {k: json.dumps(v) if isinstance(v, dict) else str(v) for k, v in json_data.items()}

    if 'cik' in dynamodb_data:
        table.put_item(Item=dynamodb_data)
        print(f'Success - {file}')
    else:
        print(f'Skipped - {file}')

print("Data moved into DynamoDB successfully")
