import json,boto3
import os
import uuid
import secrets
import string
import psycopg2

# Environment Variables
region_used = os.environ["region_used"]
sqs = boto3.client('sqs',region_name = region_used)     
QUEUE_URL = os.environ["QUEUE_URL"]
schema = os.environ["schema"]
app_metadata_table = os.environ["app_meta_data_table"]
db_database = os.environ["db_database"]
db_user = os.environ["db_user"]
db_password = os.environ["db_password"]
db_host = os.environ["endpoint"]
db_port = os.environ["db_port"]
KB_ID = os.environ["KB_ID"]
DS_ID = os.environ["DS_ID"]




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
        # result["status"] = "success"
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
        
def db_update(query): #update
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
        
def db_insert(query): #insert
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

 
def generate_random_id(length=100):
    characters = string.ascii_letters + string.digits
    return ''.join(secrets.choice(characters) for _ in range(length))


def lambda_handler(event, context):
    print(event)
    
    bucket = event["bucket_name"]
    print(bucket)
    key = event["file_name"]
    app_name = event["app_name"]
    print("APP NAME : ",app_name)
    action = event['trigger_action']
    
    # bucket = event['Records'][0]['s3']['bucket']['name']
    # print(bucket)
    # key = event['Records'][0]['s3']['object']['key'] 
    # app_name = key.split("/")[0]
    # print("APP NAME : ",app_name)
    # action = event['Records'][0]['eventName'].split(":")[-1]
    
    app_query = f"SELECT dev_kb_id, dev_datasource_id, app_id from {schema}.{app_metadata_table} WHERE app_name = '{app_name}' and delete_status = 0;"
    app_name_result = db_select(app_query)
    print("DB RESULT : ",app_name_result)
    datasource_id = DS_ID   
    app_id = app_name_result[0][2]
    
    print(key)                                                  

    message_body = {                                    
    'event_type':'sqs',                                                                         
    'bucketName': bucket,   
    'fileKey': key ,
    'kb_id' : KB_ID,
    'datasource_id' : datasource_id,
    'app_id' : app_id,
    "trigger_action":action,
    "app_name":app_name
    }      
    
    random_uuid = generate_random_id()  
    print('random_uuid',random_uuid)           
    response = sqs.send_message(
        QueueUrl=QUEUE_URL,
        MessageBody=json.dumps(message_body),
        MessageGroupId='FileUploadEvents',  # Use the same MessageGroupId for all messages related to file uploads
        MessageDeduplicationId = str(random_uuid)                                                               
        # MessageDeduplicationId=f"{bucket}/{key}"  # Use a unique MessageDeduplicationId for each message
    )
    print('response',response)                               
    if response['ResponseMetadata']['HTTPStatusCode'] == 200:
        print(f"Message sent successfully with MessageId: {response['MessageId']}")
    else:
        print(f"Failed to send message. Error: {response}")
    # print('response',response)                                        