import pandas as pd
import re
from urllib.parse import urlparse  
from bs4 import BeautifulSoup
import boto3, json
import warnings
from io import StringIO, BytesIO  
import base64, random ,string
from datetime import *
from botocore.exceptions import ClientError
import time                                   
from decimal import Decimal    
import os
import sys 
import psycopg2


schema = os.environ["schema"]
region_used = os.environ["region_used"]
bucket_name = os.environ["bucket_name"]
db_database = os.environ["db_database"]
db_user = os.environ["db_user"]
db_password = os.environ["db_password"]
db_host = os.environ["db_host"]
db_port = os.environ["db_port"]
file_version_table = os.environ["file_version_table"]
file_metadata_Table = os.environ["file_metadata_Table"]
file_log_table = os.environ["file_log_table"]


s3_client = boto3.client('s3',region_name = region_used)
s3_resource = boto3.resource('s3',region_name = region_used) 

# DB EXECUTE FUNCTION


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

def lambda_handler(event, context):
    print("EVENT : ",event)
    event_type = event["event_type"]
    
    # NEW CSV FILE PROCESSING
    if event_type == "csv_file_process":
        print("NEW CSV UPLOAD INVOKE LAMBDA")
        object_key = event["object_key"]
        file_name = event["file_name"]
        uploaded_by = event["uploaded_by"]
        access_type = event["access_type"]
        file_id = event["file_id"]
        file_type = event["file_type"]
        app_id = event["app_id"]
        app_name = event["app_name"]
        env = event["env"]
        current_time = datetime.now(timezone.utc).isoformat()
        try:
                
            if event["file_type"] == "csv":
                csv_content = s3_client.get_object(Bucket=bucket_name, Key=object_key)['Body'].read().decode('latin1')                      
                print('csv_content',csv_content)
                df = pd.read_csv(StringIO(csv_content))
            
            if event["file_type"] == "xlsx":
                # xlsx_content = s3_client.get_object(Bucket=bucket_name, Key=object_key)['Body'].read().decode('ISO-8859-1')  
                xlsx_content = s3_client.get_object(Bucket=bucket_name, Key=object_key)['Body'].read()                                          
        
                print('xlsx_content', xlsx_content)  # You can print a message instead if the content is too large
                
                # df = pd.read_excel(BytesIO(xlsx_content)) 
                df = pd.read_excel(BytesIO(xlsx_content), engine='openpyxl')                        
            total_line_count = df.shape[0]
            
            #update into dynamo_db file_metadata
            query = f"""
            UPDATE {schema}.{file_metadata_Table}
            SET count_in_kb = '{total_line_count}'
            WHERE id = '{file_id}' AND app_id = '{app_id}' AND env = '{env}';
            """
            # Execute the query
            update_response = db_update(query)  
            print("File version table updated")
            # insert into dynamo_db file_versions
            file_version_id = ''.join(random.choices(string.ascii_uppercase + string.digits, k=11))
            query = f"""
            INSERT INTO {schema}.{file_version_table} (
                id, actual_file_name, active_file_name, version,
                access_type, last_updated_time, delete_status, uploaded_by, count_in_kb,app_id,env
            ) VALUES (
                '{file_version_id}','{file_name}', '{file_name}', 1,
                '{access_type}', '{current_time}', 0, '{uploaded_by}', '{total_line_count}','{app_id}','{env}'
            );
            """ 
    
            # Execute the query
            put_response = db_insert(query)
            print("FILE VERSION DB INSERT RESPONSE : ",put_response)
                                                    
            df.columns = df.columns.str.lower()
            print("In df_columns",df.columns)    
            
            loop_executed = False
            for index, row in df.iterrows():   
                try:
                    loop_executed = True
                    print('file_name',file_name)
                    # Extract the file name from the index or any other relevant column
                    file_name_1 = f"{file_name.split('.')[0]}_rowwss{index}"   
                    
                    # Create a DataFrame for the current row                                
                    row_df = pd.DataFrame([row])
                    
                    # Convert the row DataFrame to a CSV
                    if file_type == "csv":
                        csv_buffer = StringIO()
                    if file_type == "xlsx":
                        csv_buffer = BytesIO()
 
                    # Write column names as the first row
                    row_df.columns = df.columns
                    if file_type == "csv":
                        row_df.to_csv(csv_buffer, index=False)
                    if file_type == "xlsx":
                        row_df.to_excel(csv_buffer, index=False)
                    # Reset buffer position to beginning
                    csv_buffer.seek(0)
                    if file_type == "csv":
                        object_key_csv = f"{app_name}/Dev_Documents_kb/{file_name}/{file_name_1}.csv"
                    if file_type == "xlsx":
                        object_key_csv = f"{app_name}/Dev_Documents_kb/{file_name}/{file_name_1}.xlsx"
                    print('object_key_csv',object_key_csv)
                    s3_client.put_object(Bucket=bucket_name, Key=object_key_csv, Body=csv_buffer.getvalue())
                    
                    # Initialize metadata dictionary
                    metadata = {}
                    
                    if access_type == 'private':
                        metadata = {
                        "Access":"private"
                        }
                        
                    else:
                        metadata = {
                        "Access":"public"
                        }
                    filtered_metadata = {k: v for k, v in metadata.items() if v and not v.isspace()}
                    # Create metadata JSON
                    metadata_dict = {"metadataAttributes": filtered_metadata}
                    metadata_content = json.dumps(metadata_dict, indent=4)
                    print(metadata_content)
                    if file_type == "csv":
                        object_key_metadata = f"{app_name}/Dev_Documents_kb/{file_name}/{file_name_1}.csv.metadata.json"  
                    if file_type == "xlsx":
                        object_key_metadata = f"{app_name}/Dev_Documents_kb/{file_name}/{file_name_1}.xlsx.metadata.json"  
                    # Upload metadata to S3
                    s3_upload = s3_client.put_object(Bucket=bucket_name, Key=object_key_metadata, Body=metadata_content, ContentType='application/json')
    
                except Exception as e:
                    print("First exception")   
                    print(f"Error processing row {index}: {e}")
                    if file_id:
                        query = f"""
                        UPDATE {schema}.{file_metadata_Table}
                        SET sync_status = 'FAILED'
                        WHERE id = '{file_id}' app_id = '{app_id}' AND env = '{env}';
                        """
    
                        # Execute the query
                        update_response = db_update(query)
                    return {"status":"500","Message":"Upload failed","error":e}  
            
            
            lambda_client = boto3.client('lambda',region_name = region_used)
            lambda_response = lambda_client.invoke(
                FunctionName = "CEXP_Ingestion_Trigger",                                                                                
                Payload = json.dumps({
                    'bucket_name' :bucket_name,
                    "file_name" : file_name,
                    "app_name" : app_name,
                    "trigger_action" : "Put"
                }),   
                InvocationType = 'Event'
                )
            if not loop_executed:  
                print("LOOP NOT EXECUTED UPLOAD FAILED")
                if file_id:  
                    query = f"""
                    UPDATE {schema}.{file_metadata_Table}
                    SET sync_status = 'FAILED'
                    WHERE id = '{file_id}';
                    """
    
                    # Execute the query
                    update_response = db_update(query)
                return {"status":"500","Message":"Upload failed","error":e}
            print("FILE SPLIT AND UPLOAD SUCCESSFUL")
            return {"status":"200","Message":"Upload Successful"}
        except Exception as e:
            print("Error processing : ",e)
            if file_id:
                query = f"""
                UPDATE {schema}.{file_metadata_Table}
                SET sync_status = 'FAILED'
                WHERE id = '{file_id}';
                """
                # Execute the query
                update_response = db_update(query)
            return {"status":"500","Message":"Upload failed"}
                
        
        # NEW CSV VERSION PROCESSING
    if event_type == "new_csv_version_processing":
        object_key = event["object_key"]
        file_name = event["file_name"]
        uploaded_by = event["uploaded_by"]
        access_type = event["access_type"]
        file_type = event["file_type"]
        id_to_update = event["id_to_update"]
        actual_file_name = event["actual_file_name"]
        new_version = event["new_version"]
        app_id = event["app_id"]
        app_name = event["app_name"]
        env = event["env"]
        current_time = datetime.now(timezone.utc).isoformat()
        print("IN NEW CSV VERSION PROCESSING")
        if file_type == "csv":
            csv_content = s3_client.get_object(Bucket=bucket_name, Key=object_key)['Body'].read().decode('latin1')
            print('csv_content',csv_content)
            df = pd.read_csv(StringIO(csv_content))
    
        if file_type == "xlsx":
            xlsx_content = s3_client.get_object(Bucket=bucket_name, Key=object_key)['Body'].read()
            print('xlsx_content', xlsx_content)  # You can print a message instead if the content is too large
            df = pd.read_excel(BytesIO(xlsx_content)) 
        total_line_count = df.shape[0]
        
        # update in file_metadata
        query_update = f"""
        UPDATE {schema}.{file_metadata_Table}
        SET count_in_kb = '{total_line_count}'
        WHERE id = '{id_to_update}';
        """
        
        # Execute the UPDATE query using the existing db_update function
        db_update(query_update)
        file_version_id = ''.join(random.choices(string.ascii_uppercase + string.digits, k=11))
        # insert new version into file_version table
        query_insert = f"""
        INSERT INTO {schema}.{file_version_table} (id,actual_file_name, active_file_name, version, access_type, last_updated_time, delete_status, uploaded_by, count_in_kb,app_id,env)
        VALUES (
            '{file_version_id}',
            '{actual_file_name}',
            '{file_name}',
            '{new_version}',
            '{access_type}',
            '{current_time}',
            '0',
            '{uploaded_by}',
            '{total_line_count}',
            '{app_id}',
            '{env}'
        );
        """
        # Execute the INSERT query using the existing db_insert function
        db_insert(query_insert)
        query_insert = f"""
        INSERT INTO {schema}.{file_log_table} (id, actual_file_name, active_file_name, done_by, action,app_id,env)
        VALUES (
            '{id_to_update}',
            '{actual_file_name}',
            '{file_name}',
            '{uploaded_by}',
            'New Version Added',
            '{app_id}',
            '{env}'
        );
        """
        
        # Execute the INSERT query using the existing db_insert function
        db_insert(query_insert)
        
        df.columns = df.columns.str.lower()
        # required_columns = ['terminal', 'level', 'area']
        loop_executed = False
        for index, row in df.iterrows():
            try:
                loop_executed = True
                # Extract the file name from the index or any other relevant column
                file_name_1 = f"{file_name}_rowwss{index}"  
                
                # Create a DataFrame for the current row
                row_df = pd.DataFrame([row])
                
                # Convert the row DataFrame to a CSV
                if file_type == "csv":
                    csv_buffer = StringIO()
                if file_type == "xlsx":
                    csv_buffer = BytesIO()
                # Write column names as the first row
                row_df.columns = df.columns
                if file_type == "csv":
                    row_df.to_csv(csv_buffer, index=False)
                if file_type == "xlsx":
                    row_df.to_excel(csv_buffer, index=False)
                # Reset buffer position to beginning
                csv_buffer.seek(0)
                if file_type == "csv":
                    object_key_csv = f"{app_name}/Dev_Documents_kb/{file_name}/{file_name_1}.csv"
                if file_type == "xlsx":
                    object_key_csv = f"{app_name}/Dev_Documents_kb/{file_name}/{file_name_1}.xlsx"
                s3_client.put_object(Bucket=bucket_name, Key=object_key_csv, Body=csv_buffer.getvalue())
                
                # Initialize metadata dictionary
                metadata = {}
                
                if access_type == 'private':
                    metadata = {
                    "Access":"private"
                    }
                    
                else:
                    metadata = {
                    "Access":"public"
                    }
                filtered_metadata = {k: v for k, v in metadata.items() if v and not v.isspace()}
                # Create metadata JSON
                metadata_dict = {"metadataAttributes": filtered_metadata}   
                metadata_content = json.dumps(metadata_dict, indent=4)                    
                print(metadata_content)
                if file_type == "csv":
                    object_key_metadata = f"{app_name}/Dev_Documents_kb/{file_name}/{file_name_1}.csv.metadata.json"
                if file_type == "xlsx":
                    object_key_metadata = f"{app_name}/Dev_Documents_kb/{file_name}/{file_name_1}.xlsx.metadata.json"
                # Upload metadata to S3
                s3_upload = s3_client.put_object(Bucket=bucket_name, Key=object_key_metadata, Body=metadata_content, ContentType='application/json')
            
            except Exception as e:
                print(f"Error processing row {index}: {e}")
                if id_to_update:
                    query = f"""
                    UPDATE {schema}.{file_metadata_Table}
                    SET sync_status = 'FAILED'
                    WHERE id = '{id_to_update}';
                    """
                    # Execute the query
                    update_response = db_update(query)
                return {"status":"500","Message":"Upload failed"}
        lambda_client = boto3.client('lambda',region_name = region_used)
        lambda_response = lambda_client.invoke(
            FunctionName = "CEXP_Ingestion_Trigger",                                                                                
            Payload = json.dumps({
                'bucket_name' :bucket_name,
                "file_name" : file_name,
                "app_name" : app_name,
                "trigger_action" : "Put"
            }),   
            InvocationType = 'Event'
            )
        if not loop_executed:   
            print("LOOP NOT EXECUTED UPLOAD FAILED ")
            if id_to_update:
                query = f"""
                    UPDATE {schema}.{file_metadata_Table}
                    SET sync_status = 'FAILED'
                    WHERE id = '{id_to_update}';
                    """
                    # Execute the query
                update_response = db_update(query)
            return {"status":"500","Message":"Upload failed","error":str(e)}    
            
        return {"status":"200","Message":"Upload successful. Ingestion started ..."}      
        
