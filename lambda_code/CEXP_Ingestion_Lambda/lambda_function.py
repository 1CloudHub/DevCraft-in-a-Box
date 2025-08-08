# # INGESTION LAMBDA
import requests
import json, os
import boto3, time 
import psycopg2

# Environment Variables
# fast_api_url = "http://54.166.212.102:8000/ingest-check"
instance_id = os.environ["instance_id"]
schema = os.environ["schema"]
region_used = os.environ["region_used"]
FILE_METADATA_TABLE = os.environ["FILE_METADATA_TABLE"]
FILE_VERSION_TABLE = os.environ["FILE_VERSION_TABLE"]
QUEUE_URL = os.environ["QUEUE_URL"]                                                                  
# Boto3 Clients
sqs = boto3.client('sqs',region_name = region_used)
# DB
db_database = os.environ["db_name"]
db_user = os.environ["db_user"]
db_password = os.environ["db_password"]
db_host = os.environ["endpoint"]
db_port = os.environ["port"]
app_metadata_table = os.environ["app_metadata_table"]
# KB Datasources
# data_source_id = os.environ["data_source_id"]
s3_client = boto3.client('s3',region_name = region_used)
ec2_client = boto3.client('ec2', region_name=region_used)
def update_db(query): #update
    try:
        connection = psycopg2.connect(
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,  
            database=db_database
        )                                  
        cursor = connection.cursor()
        cursor.execute(query)
        connection.commit()
        cursor.close()
        connection.close()  
        return {"status" : "success"}
    except Exception as e:
        result = []
        re = {
            "statusCode": 500,
            "message": f"The following exception occurred: {str(e)}"
        }
        result.append(re)
        print(result)
        return {"status" : "failed"}
def db_select(query): #select
    try:
        connection = psycopg2.connect(
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,  # Replace with the SSH tunnel local bind port
            database=db_database
        )                       
        cursor = connection.cursor()                                                                                        
        cursor.execute(query)
        result = cursor.fetchall()
        connection.commit()
        cursor.close()
        connection.close()
        return result
    except Exception as e:
        result = []
        re = {
            "statusCode": 500,
            "status" : "failed",
            "message": f"The following exception occurred: {str(e)}"
        }
        result.append(re)
        return result
        

def start_ingestion_job(knowledgebaseid, datasourceid):
    aws_region =region_used
    client_bedrock_agent = boto3.client('bedrock-agent', region_name=region_used)
    retries = 0
    MAX_RETRIES = 150  # Maximum number of retries                    
    RETRY_DELAY = 2
    while retries < MAX_RETRIES:
        try:
            start_ingestion_response = client_bedrock_agent.start_ingestion_job(                                            
                knowledgeBaseId=knowledgebaseid,
                dataSourceId=datasourceid,
            )
            ingestion_job_id = start_ingestion_response['ingestionJob']['ingestionJobId']
            return ingestion_job_id
        except Exception as e:
            retries += 1
            print(f"An exception occurred: {e}")
            print(f"Retrying in {RETRY_DELAY} seconds... (Attempt {retries}/{MAX_RETRIES})")
            time.sleep(RETRY_DELAY)
    
    raise Exception(f"Maximum retries ({MAX_RETRIES}) exceeded. Failed to start ingestion job.")
  

def lambda_handler(event, context):
                                                                                                    
    print(event) 
    
    file_identifier = json.loads(event['Records'][0]['body'])
    KB_ID = file_identifier["kb_id"]
    data_source_id = file_identifier["datasource_id"]
    action = file_identifier["trigger_action"]
    file_name = file_identifier['fileKey']
    reciept_handle = event['Records'][0]['receiptHandle']                       
    app_name =  file_identifier['app_name']
    app_id = file_identifier["app_id"]
    env = "Development"   
    bucket_name = file_identifier['bucketName']
    print(file_name)
    client_bedrock_agent = boto3.client('bedrock-agent', region_name=region_used)
    data_sourceid = data_source_id  
    try:      
        if action == "Put" or action == "Copy": 
            print("IN INGESTION CONDITION")
            print('file_name',file_name)  
            query_select1 = f"""
            SELECT *
            FROM {schema}.{FILE_METADATA_TABLE}
            WHERE active_file_name = '{file_name}'
            AND app_id = '{app_id}'
            AND env = '{env}'
            AND delete_status = 0;
            """
                
            # Execute the first SELECT query
            response = db_select(query_select1)
            print(response, 'response1')                
            id = response[0][0]

            ingestion_job_1 =start_ingestion_job(KB_ID,data_sourceid)                                    
            print(f"Started ingestion job with ID: {ingestion_job_1}")

            payload = {
                "app_id": app_id,
                "app_name":app_name,
                "sqs_receipt_handle":reciept_handle,
                "ingestion_job_id": ingestion_job_1,
                "KB_ID": KB_ID,
                "data_source_id": data_sourceid,
                "action":"Put",
                "file_id":id,
                "bucket_name":bucket_name,
                "env":env
            }
            response = ec2_client.describe_instances(InstanceIds=[instance_id])
            public_ip = response['Reservations'][0]['Instances'][0].get('PublicIpAddress', 'No Public IP assigned')
            print(f"Public IP: {public_ip}")
            fast_api_url = f"http://{public_ip}:8000/ingest-check"    
            fast_api_request = requests.post(fast_api_url,json=payload)
            print(fast_api_request.json)

            result = {
                    "statusCode": 200
                }
            print(result)
            return  result
        elif action == 'Delete':
            print("IN DELETION CONDITION")
            print("file_name: ",file_name)
            query_select1 = f"""
            SELECT *
            FROM {schema}.{FILE_METADATA_TABLE}
            WHERE active_file_name = '{file_name}'
            AND app_id = '{app_id}'
            AND env = '{env}'
            AND delete_status = 0;
            """
            # Execute the first SELECT query
            response = db_select(query_select1)
            print(response, 'response1')                
            id = response[0][0]

            ingestion_job_1 =start_ingestion_job(KB_ID,data_sourceid)                                    
            print(f"Started ingestion job with ID: {ingestion_job_1}")

            payload = {
                "app_id": app_id,
                "app_name":app_name,
                "sqs_receipt_handle":reciept_handle,
                "ingestion_job_id": ingestion_job_1,
                "KB_ID": KB_ID,
                "data_source_id": data_sourceid,
                "action":"Delete",
                "file_id":id,
                "bucket_name":bucket_name,
                "env":env
            }
            response = ec2_client.describe_instances(InstanceIds=[instance_id])
            public_ip = response['Reservations'][0]['Instances'][0].get('PublicIpAddress', 'No Public IP assigned')
            print(f"Public IP: {public_ip}")
            fast_api_url = f"http://{public_ip}:8000/ingest-check"      
            fast_api_request = requests.post(fast_api_url,json=payload)
            print(fast_api_request.json)
            print(fast_api_request.text)
            print(fast_api_request.status_code)
            result = {
                    "statusCode": 200
                }
            return  result

    except Exception as e:                                                                      
        print("EXCEPTION OCCURRED : ",e)        
        sqs_deletion = sqs.delete_message(
            QueueUrl=QUEUE_URL,
            ReceiptHandle=reciept_handle
        )
        print('deleted')   
        result = {
            "statusCode": 500
        }
        print(result)
        return result 

                                     