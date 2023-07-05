# Data Migration and Transformation Tool for Amazon RDS Data Warehouses
## Objective :
This URL (https://www.sec.gov/edgar/sec-api-documentation) points to a zip file. The zip file contains multiple JSON files. The JSON files contain multiple documents with various data structures. Your goal is to download the zip file from the URL, extract the data from the JSON files, store it in Amazon S3, and load it into Amazon RDS. You want to use Python or PySpark to perform these tasks. You may use any libraries or tools that are necessary to complete the task.
## Approach:
1. To extract the data from a zip file that is available at a URL and load it into Amazon S3 and Amazon RDS (NoSQL), you can follow these steps: 
2. Use the requests library to download the zip file from the URL.
3. Use the zipfile module to extract the data from the zip file.
4. Use the boto3 library or PySpark to store the data in Amazon S3.
5. Use the pandas library and sqlalchemy or PySpark to load the data from S3 into Amazon RDS (NoSQL).
### Expected Results:
- The result of following these steps should be that the data from the zip file is extracted and stored in a list of dictionaries (if you are using Python) or a - - - DataFrame (if you are using PySpark). Each dictionary or DataFrame row will represent a document from one of the JSON files in the zip file. 
- The data in the list or DataFrame will then be stored in Amazon S3 as JSON files. You will be able to access these JSON files using the boto3 library or the Amazon S3 web interface. 
- The data from the JSON files will also be loaded into Amazon RDS (NoSQL). You will be able to access the data in RDS using SQL queries. The data will be stored in a table in RDS, and the schema of the table will be determined by the structure of the JSON documents. 
## s3_dynamodb.py
```python
import botocore.exceptions
import requests
import zipfile
import boto3
import json
import io

# Set your AWS access key and secret key
access_key = "<your aws access key>"
secret_key = "<your aws secret key>"
region_name = "<your aws region>"


# Set your predefined S3 bucket and DynamoDB table names
table_name = "<your dynamodb table>"
bucket_name = "<your s3 bucket name>"

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

```

## Screenshots
### Zip file(test.zip) downloaded to S3
![s3_zip_downloaded](https://github.com/bnaikwad88/Data-Migration-Transformation-Tool-mazon-RDS-/assets/116859424/2d6a3a4b-3a73-4075-821f-5013885dcac9)
### JSON files extracted from the Zip file in the same S3(unzip folder)
![s3_zip_unzipped](https://github.com/bnaikwad88/Data-Migration-Transformation-Tool-mazon-RDS-/assets/116859424/298de8a3-1de5-46d7-a04c-7bf1ff41dd1b)
### dynamoDB table-jsonData
![dynamo_table](https://github.com/bnaikwad88/Data-Migration-Transformation-Tool-mazon-RDS-/assets/116859424/016b5eaa-f324-4ff0-9acb-0286c43f8de2)
### Extracted json files inserted into dynamoDB table-jsonData
![json_inserted_dynamodb](https://github.com/bnaikwad88/Data-Migration-Transformation-Tool-mazon-RDS-/assets/116859424/fc73dd33-c2e4-4a2b-a42b-609c3850b988)
![json_inserted_dynamodb1](https://github.com/bnaikwad88/Data-Migration-Transformation-Tool-mazon-RDS-/assets/116859424/7d4ce9e1-9997-4804-8d56-6310badff3d2)
