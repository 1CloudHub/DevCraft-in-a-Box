import json
import os
import psycopg2
import boto3
import time
import requests
from textractor import Textractor
from textractor.data.constants import TextractFeatures
import base64
import fitz  # PyMuPDF
from PIL import Image
import io   

db_user =os.environ['db_user']     
db_password = os.environ['db_password']             
db_host = os.environ['db_host']                         
db_port = os.environ['db_port']
db_database = os.environ['db_database']  
schema = os.environ['schema']
document_type_table = os.environ['document_type_table']
document_processing_table = os.environ['document_processing_table']
bucket_name = os.environ['bucket_name']
region_name = os.environ['region_name']
model_id = os.environ['model_id']
prompt_metadata_table = os.environ['prompt_metadata_table']
ai_suggestion_table = os.environ['ai_suggestion_table']
temp_document_processing_table = os.environ['temp_document_processing_table']
cexp_ocr_ai_key_extraction_details_table = os.environ['cexp_ocr_ai_key_extraction_details_table']   

s3_client = boto3.client('s3',region_name = region_name)
bedrock_client = boto3.client('bedrock-runtime',region_name = region_name)

def select_db(query):
    connection = psycopg2.connect(  
        user=db_user,
        password=db_password,
        host=db_host,
        port=db_port,
        database=db_database
    )                      
    cursor = connection.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    connection.commit()
    cursor.close()
    connection.close()
    return result
    
def insert_db(query,values):
    try:
        connection = psycopg2.connect(
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
            database=db_database
        )                                                                                                                                    
        cursor = connection.cursor()
        cursor.execute(query,values)
        connection.commit()
        cursor.close()
        connection.close()
        return {"status":"insert query successful"}
    except Exception as e:
        print("Exception occurred while insert query : ",e)
        return None
 
def update_db(query):
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
        return {"status":"update query successful"}
    except Exception as e:
        print("Exception occurred while update query : ",e)
        return None

def pdf_to_base64_images(s3_path):
    print("PDF TO BASE64 CALLED")
    # Download PDF from S3
    pdf_obj = s3_client.get_object(Bucket=bucket_name, Key=s3_path)
    pdf_bytes = pdf_obj['Body'].read()

    # Open PDF from bytes
    doc = fitz.open("pdf", pdf_bytes)

    base64_images = []

    # Loop through each page
    for page_num in range(doc.page_count):
        page = doc.load_page(page_num)
        pix = page.get_pixmap()

        # Convert pixmap to PIL image
        img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)

        # Save image to a BytesIO buffer
        img_buffer = io.BytesIO()
        img.save(img_buffer, format="PNG")
        img_buffer.seek(0)

        # Encode the image as base64
        img_base64 = base64.b64encode(img_buffer.read()).decode("utf-8")

        # Append the base64 string to the result list
        base64_images.append(img_base64)

    return base64_images

def key_extraction_funtion_llm(page_texts,doc_type,doc_name,doc_id,file_extension):
    try:
        print("PAGE TEXT EXTRACTED : ",page_texts)

        #EXTRACT THE DOC_TYPE PROMPT
        select_query = f'''
                       SELECT 
                            document_json
                       FROM {schema}.{document_type_table}
                       WHERE delete_status = 0  and name = '{doc_type}'
                        '''
        document_prompt_details = select_db(select_query)[0][0]   
        json_document_details = json.loads(json.loads(document_prompt_details))
        print("json_prompt : ",json_document_details)
        print("json_prompt type: ",type(json_document_details))            
        document_key_schema = json_document_details['fields']
        document_description = json_document_details['documentDesc']    

        #EXTRACT THE BASE PROMPT
        select_query = f'''
                SELECT prompt_template 
                FROM {schema}.{prompt_metadata_table} 
                WHERE prompt_type = 'base_prompt';
                '''
        base_prompt = select_db(select_query)[0][0]
        final_prompt = f'''
                        {base_prompt}   
                        Document Type: {doc_type}
                        Document Description and Document Type Specific Instructions: {document_description}
                        Required Fields: {document_key_schema}

                        Input Document:
                        {page_texts}
                        '''
        print("FINAL PROMPT : ",final_prompt)    
        final = invoke_model_function(final_prompt)

        if 'usage' in final and 'input_tokens' in final['usage'] and 'output_tokens' in final['usage']:
            input_tokens = final['usage']['input_tokens']
            output_tokens = final['usage']['output_tokens']
        else:
            input_tokens = 0
            output_tokens = 0

        update_query = f'''UPDATE {schema}.{document_processing_table} SET total_input_tokens = {input_tokens}, total_output_tokens = {output_tokens} WHERE doc_id = '{doc_id}' and delete_status = 0;'''
        update_db(update_query)

        if 'content' in final and len(final['content']) > 0 and 'text' in final['content'][0]:
            extracted_json = final['content'][0]['text']
            print("EXTRACTED JSON BEFORE LOADS : ", extracted_json)
            try:
                extracted_json = json.loads(extracted_json)
                print("EXTRACTED JSON AFTER LOADS : ", extracted_json)
            except Exception as e:
                print("Exception occurred while converting string to json: ", e)
                extracted_json = {}
                
        else:
            extracted_json = {}
        
        return extracted_json

    except Exception as e:
        print("Exception occurred while extracting key entities: ", e)
        # return {}
        raise e




def text_extract_llm(base_64_array,file_extension):
    print("TEXT EXTRACT LLM CALLED")
    input_prompt = '''extract and provide the text present in the image in a neat formatted manner which can be used for nlp tasks. the answer format should only be the all extracted text in a neat formatted manner from the image without any other information. Ensure to double check the numbers extracted from the image.'''
    page_results = []
    for i in base_64_array:
        response = bedrock_client.invoke_model(contentType='application/json', body=json.dumps({
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": 3000,
            "temperature": 0,
            "top_p": 0.8,
            "top_k":100,
            "system":input_prompt,
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "image",
                            "source": {
                                "type": "base64",
                                "media_type": "image/png",
                                "data": i
                            }
                        },
                        # {
                        #     "type": "text",
                        #     "text": input_prompt
                        # }
                    ]
                }
            ]
        }), modelId=model_id)
        
        inference_result = response['body'].read().decode('utf-8')
        final = json.loads(inference_result)
        print("FINAL : ",final)   
        extracted_content = final['content'][0]['text']
        page_results.append(extracted_content)
    return page_results   


def encode_image_to_base64(doc_type,doc_id,file_extension):
    key = f"CEXP_OCR/{doc_type}/INPUT/{doc_id}.{file_extension}"
    response = s3_client.get_object(Bucket=bucket_name, Key=key)
    image_data = response['Body'].read()
    encoded_image = base64.b64encode(image_data).decode("utf-8")
    return encoded_image


# Create a folder in the S3 bucket (optional step)
def create_s3_folder(bucket_name, folder_name):
    s3_client.put_object(
        Bucket=bucket_name,
        Key=f"{folder_name}/"
    )
    print(f"Folder '{folder_name}' created in bucket '{bucket_name}'.")

def invoke_model_function(final_prompt):
    max_retries = 4
    retries = 1
    while retries <= max_retries:
        try:
            response = bedrock_client.invoke_model(contentType='application/json', body=json.dumps({
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": 3000,
                "temperature": 0,
                "top_p": 0.999,
                "top_k":250,
                "messages": [
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "text",
                                "text": final_prompt
                            }
                        ]
                    }
                ]
            }), modelId=model_id)

            if 'body' in response:
                inference_result = response['body'].read().decode('utf-8')
                final = json.loads(inference_result)
            else:
                final = {}
            return final
            break
        except Exception as e:
            print("ERROR OCCURRED IN INVOKE LLM FUNCTION")
            print(f"An error occurred: {e}")
            print("Retrying...")
            time.sleep(1)
            retries += 1
    else:
        print("Maximum retries exceeded. Unable to retrieve response.")
        return {}


def key_extraction_funtion(doc_type,doc_name,doc_id,file_extension):
    try:
        path = f"s3://{bucket_name}/CEXP_OCR/{doc_type}/INPUT/{doc_id}.{file_extension}"
        print(path)
        extractor = Textractor(region_name=region_name)
        document = extractor.start_document_analysis(
                file_source=path,
                features=[TextractFeatures.LAYOUT, TextractFeatures.TABLES],
                save_image=False)
        
        page_texts = []
        for i, page in enumerate(document.pages):
        #     print(f"Page {i + 1}:\n{page.get_text()}\n")
            page_texts.append(page.get_text())
        
        print("PAGE TEXT EXTRACTED : ",page_texts)

        #EXTRACT THE DOC_TYPE PROMPT
        select_query = f'''
                       SELECT 
                            document_json
                       FROM {schema}.{document_type_table}
                       WHERE delete_status = 0  and name = '{doc_type}'
                        '''
        document_prompt_details = select_db(select_query)[0][0]   
        json_document_details = json.loads(json.loads(document_prompt_details))
        print("json_prompt : ",json_document_details)
        print("json_prompt type: ",type(json_document_details))            
        document_key_schema = json_document_details['fields']
        document_description = json_document_details['documentDesc']    

        #EXTRACT THE BASE PROMPT
        select_query = f'''
                SELECT prompt_template 
                FROM {schema}.{prompt_metadata_table} 
                WHERE prompt_type = 'base_prompt';
                '''
        base_prompt = select_db(select_query)[0][0]
        final_prompt = f'''
                        {base_prompt}   
                        Document Type: {doc_type}
                        Document Description and Document Type Specific Instructions: {document_description}
                        Required Fields: {document_key_schema}

                        Input Document:
                        {page_texts}
                        '''
        print("FINAL PROMPT : ",final_prompt)    
        final = invoke_model_function(final_prompt)

        if 'usage' in final and 'input_tokens' in final['usage'] and 'output_tokens' in final['usage']:
            input_tokens = final['usage']['input_tokens']
            output_tokens = final['usage']['output_tokens']
        else:
            input_tokens = 0
            output_tokens = 0

        update_query = f'''UPDATE {schema}.{document_processing_table} SET total_input_tokens = {input_tokens}, total_output_tokens = {output_tokens} WHERE doc_id = '{doc_id}' and delete_status = 0;'''
        update_db(update_query)

        if 'content' in final and len(final['content']) > 0 and 'text' in final['content'][0]:
            extracted_json = final['content'][0]['text']
            print("EXTRACTED JSON BEFORE LOADS : ", extracted_json)
            try:
                extracted_json = json.loads(extracted_json)
                print("EXTRACTED JSON AFTER LOADS : ", extracted_json)
            except Exception as e:
                print("Exception occurred while converting string to json: ", e)
                extracted_json = {}
                
        else:
            extracted_json = {}
        
        return extracted_json

    except Exception as e:
        print("Exception occurred while extracting key entities: ", e)
        # return {}
        raise e

def test_key_extraction_funtion(doc_type,doc_name,doc_id,file_extension,document_prompt_details):
    try:
        path = f"s3://{bucket_name}/CEXP_OCR/{doc_type}/INPUT/{doc_id}.{file_extension}"
        print(path)
        extractor = Textractor(region_name=region_name)
        document = extractor.start_document_analysis(
                file_source=path,
                features=[TextractFeatures.LAYOUT, TextractFeatures.TABLES],
                save_image=False)
        
        page_texts = []
        for i, page in enumerate(document.pages):
        #     print(f"Page {i + 1}:\n{page.get_text()}\n")
            page_texts.append(page.get_text())
        
        print("PAGE TEXT EXTRACTED : ",page_texts)

        print("Document prompt Details : ", json.loads(document_prompt_details))

        json_document_details = json.loads(document_prompt_details)
        print("json_prompt : ",json_document_details)
        print("json_prompt type: ",type(json_document_details))            
        document_key_schema = json_document_details['fields']
        document_description = json_document_details['documentDesc']    

        #EXTRACT THE BASE PROMPT
        select_query = f'''
                SELECT prompt_template 
                FROM {schema}.{prompt_metadata_table} 
                WHERE prompt_type = 'base_prompt';
                '''
        base_prompt = select_db(select_query)[0][0]
        final_prompt = f'''
                        {base_prompt}   
                        Document Type: {doc_type}
                        Document Description and Document Type Specific Instructions: {document_description}
                        Required Fields: {document_key_schema}

                        Input Document:
                        {page_texts}
                        '''
        print("FINAL PROMPT : ",final_prompt)    
        final = invoke_model_function(final_prompt)

        if 'usage' in final and 'input_tokens' in final['usage'] and 'output_tokens' in final['usage']:
            input_tokens = final['usage']['input_tokens']
            output_tokens = final['usage']['output_tokens']
        else:
            input_tokens = 0
            output_tokens = 0

        update_query = f'''UPDATE {schema}.{temp_document_processing_table} SET total_input_tokens = {input_tokens}, total_output_tokens = {output_tokens} WHERE doc_id = '{doc_id}' and delete_status = 0;'''
        update_db(update_query)

        if 'content' in final and len(final['content']) > 0 and 'text' in final['content'][0]:
            extracted_json = final['content'][0]['text']
            print("EXTRACTED JSON BEFORE LOADS : ", extracted_json)
            try:
                extracted_json = json.loads(extracted_json)
                print("EXTRACTED JSON AFTER LOADS : ", extracted_json)
            except Exception as e:
                print("Exception occurred while converting string to json: ", e)
                extracted_json = {}
                
        else:
            extracted_json = {}
        
        return extracted_json

    except Exception as e:
        print("Exception occurred while extracting key entities: ", e)
        # return {}
        raise e

def test_encode_image_to_base64(doc_type,doc_id,file_extension):
    print("TEST PNG->BASE64")
    key = f"CEXP_OCR/Temp/INPUT/{doc_id}.{file_extension}"
    response = s3_client.get_object(Bucket=bucket_name, Key=key)
    image_data = response['Body'].read()
    encoded_image = base64.b64encode(image_data).decode("utf-8")
    return encoded_image

def test_key_extraction_funtion_llm(page_texts,doc_type,doc_name,doc_id,file_extension,document_prompt_details):
    try: 
        print("TEST KEY EXTRACTION FUNCTION LLM")       
        print("PAGE TEXT EXTRACTED : ",page_texts)

        print("Document prompt Details : ", json.loads(document_prompt_details))

        json_document_details = json.loads(document_prompt_details)
        print("json_prompt : ",json_document_details)
        print("json_prompt type: ",type(json_document_details))            
        document_key_schema = json_document_details['fields']
        document_description = json_document_details['documentDesc']    

        #EXTRACT THE BASE PROMPT
        select_query = f'''
                SELECT prompt_template 
                FROM {schema}.{prompt_metadata_table} 
                WHERE prompt_type = 'base_prompt';
                '''
        base_prompt = select_db(select_query)[0][0]
        final_prompt = f'''
                        {base_prompt}   
                        Document Type: {doc_type}
                        Document Description and Document Type Specific Instructions: {document_description}
                        Required Fields: {document_key_schema}

                        Input Document:
                        {page_texts}
                        '''
        print("FINAL PROMPT : ",final_prompt)    
        final = invoke_model_function(final_prompt)

        if 'usage' in final and 'input_tokens' in final['usage'] and 'output_tokens' in final['usage']:
            input_tokens = final['usage']['input_tokens']
            output_tokens = final['usage']['output_tokens']
        else:
            input_tokens = 0
            output_tokens = 0

        update_query = f'''UPDATE {schema}.{temp_document_processing_table} SET total_input_tokens = {input_tokens}, total_output_tokens = {output_tokens} WHERE doc_id = '{doc_id}' and delete_status = 0;'''
        update_db(update_query)

        if 'content' in final and len(final['content']) > 0 and 'text' in final['content'][0]:
            extracted_json = final['content'][0]['text']
            print("EXTRACTED JSON BEFORE LOADS : ", extracted_json)
            try:
                extracted_json = json.loads(extracted_json)
                print("EXTRACTED JSON AFTER LOADS : ", extracted_json)
            except Exception as e:
                print("Exception occurred while converting string to json: ", e)
                extracted_json = {}
                
        else:
            extracted_json = {}
        
        return extracted_json

    except Exception as e:
        print("Exception occurred while extracting key entities: ", e)
        # return {}
        raise e

def ai_key_extractiont_function(doc_name,doc_id,file_extension,document_type,document_description):
    try:
        path = f"s3://{bucket_name}/CEXP_OCR/ai_key_extraction/{document_type}/INPUT/{doc_id}.{file_extension}"
        print(path)
        extractor = Textractor(region_name=region_name)
        document = extractor.start_document_analysis(
                file_source=path,
                features=[TextractFeatures.LAYOUT, TextractFeatures.TABLES],
                save_image=False)
        
        page_texts = []
        for i, page in enumerate(document.pages):
        #     print(f"Page {i + 1}:\n{page.get_text()}\n")
            page_texts.append(page.get_text())
        
        # print("PAGE TEXT EXTRACTED : ",page_texts)

        #EXTRACT THE BASE PROMPT
        select_query = f'''
                SELECT prompt_template 
                FROM {schema}.{prompt_metadata_table} 
                WHERE prompt_type = 'ai_field_extract_prompt';
                '''
        base_prompt = select_db(select_query)[0][0]
        final_prompt = f'''
                        {base_prompt}
                        User Given Document Type: {document_type}
                        User Given Document Description: {document_description} 
                        Input Document:
                        {page_texts}
                        '''
        print("FINAL PROMPT : ",final_prompt)    
        final = invoke_model_function(final_prompt)

        if 'usage' in final and 'input_tokens' in final['usage'] and 'output_tokens' in final['usage']:
            input_tokens = final['usage']['input_tokens']
            output_tokens = final['usage']['output_tokens']
        else:
            input_tokens = 0
            output_tokens = 0

        update_query = f'''UPDATE {schema}.{cexp_ocr_ai_key_extraction_details_table} SET total_input_tokens = {input_tokens}, total_output_tokens = {output_tokens} WHERE doc_id = '{doc_id}' and delete_status = 0;'''
        update_db(update_query)

        if 'content' in final and len(final['content']) > 0 and 'text' in final['content'][0]:
            extracted_json = final['content'][0]['text']
            print("EXTRACTED JSON BEFORE LOADS : ", extracted_json)
            try:
                extracted_json = json.loads(extracted_json)
                print("EXTRACTED JSON AFTER LOADS : ", extracted_json)
                extracted_json['documentType'] = document_type
                extracted_json['documentDesc'] = document_description
                print("FINAL JSON : ",extracted_json)       
            except Exception as e:
                print("Exception occurred while converting string to json: ", e)
                extracted_json = {
                                    'documentType': document_type,
                                    'documentDesc': document_description,
                                    'fields': []
                                }
                
        else:
            extracted_json = {
                            'documentType': document_type,
                            'documentDesc': document_description,
                            'fields': []
                            }  
        
        return extracted_json

    except Exception as e:
        print("Exception occurred while extracting key entities: ", e)
        # return {}
        raise e


def generate_presigned_url(object_key, expiration=3600):
    s3_client = boto3.client('s3',region_name = region_name)

    try:
        url = s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': bucket_name, 'Key': object_key},
            ExpiresIn=expiration
        )
        return url
    except Exception as e:
        print(f"Error generating pre-signed URL: {e}")
        return None

def convert_response(key, value):
    keys = key.split(".") 
    nested_dict = value

    for k in reversed(keys):
        nested_dict = {k: nested_dict}

    return nested_dict


def lambda_handler(event, context):
    print("EVENT : ",event)
    start_time = time.time()
    print("START TIME : ",start_time)

    event_type = event['event_type']
    if event_type == "add_document_type":
        try:
            email_id = event['email_id']
            document_type = event['document_type'].lower().strip()
            document_json = event['document_json']
            extraction_method = event['extraction_method']
            human_intervention = event['human_intervention']
            connector_type = event['connector_type']
            config = event.get("config", None)

            if document_type == "":
                return {
                    "status_code" : 400,
                    "message" : "Document Type Name Can't be Empty"
                }

            query = f"SELECT * FROM {schema}.{document_type_table} WHERE name = '{document_type}' and delete_status = 0"
            response = select_db(query)
            print("RESPONSE : ",response)   

            if response:
                return {
                    "status_code" : 403,
                    "message" : "Document Type Already Exists"
                }

            query = f'''
            INSERT INTO {schema}.{document_type_table} (name, created_by, document_json, human_intervention, connector_type, config, extraction_method, created_at, delete_status) VALUES (%s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, 0)    
            '''
            values = (document_type, email_id, document_json, human_intervention, connector_type, json.dumps(config), extraction_method)   

            insert_db(query, values)

            #FOLDER CREATION IN S3
            folder_path = f"CEXP_OCR/{document_type}/INPUT/"
            create_s3_folder(bucket_name, folder_path) 
            folder_path = f"CEXP_OCR/{document_type}/OUTPUT/"
            create_s3_folder(bucket_name, folder_path)

            return {
                "status_code" : 200, 
                "message" : "Document Type Added Successfully" 
            }
        except Exception as e:
            print(f"Error Occured in {event_type} : {e}")
            return {
                "status_code" : 500,
                "message" : "An Error Occured While Adding Document Type"
            }   

    if event_type == "list_document_type":
        try:
            query = f"select json_agg(row_to_json(row_values)) from (SELECT * FROM {schema}.{document_type_table} WHERE delete_status = 0 order by created_at desc) as row_values"
            response = select_db(query)
            print(response)

            if not response:
                return {
                    "status_code" : 200,
                    "result" : []
                }
            
            return {
                    "status_code" : 200,
                    "result" : response[0][0]
                }
        
        except Exception as e:
            print(f"Error Occured in {event_type} : {e}")
            return {
                "status_code" : 500,
                "message" : "An Error Occured While Retriving Document Type"
            }

    if event_type == "suggest_description":
        try:
            document_json = event['document_json']
            user_input = event["user_input"]
            select_query = f'''
                SELECT prompt_template
                FROM {schema}.{prompt_metadata_table}
                where prompt_type = 'json_formatting_prompt';
                '''
            prompt = select_db(select_query)[0][0]
            final_prompt = f'''
                            {prompt}
                            {user_input}
                            Now, please generate a similar JSON with field descriptions for the document type:
                            {json.dumps(document_json)}
                            '''
            
            response = bedrock_client.invoke_model(contentType='application/json', 
                body=json.dumps({
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": 1000,
                "messages": [
                    {
                        "role": "user",
                        "content": [
                            {"type": "text", "text": final_prompt},
                        ]
                    }
                ],
            }), modelId=model_id)

            response_body = json.loads(response['body'].read().decode('utf-8'))
            print('response_body',response_body)    
            if 'usage' in response_body and 'input_tokens' in response_body['usage'] and 'output_tokens' in response_body['usage']:
                input_tokens = response_body['usage']['input_tokens']
                output_tokens = response_body['usage']['output_tokens']
            else:
                input_tokens = 0
                output_tokens = 0
            
            insert_query = f'''INSERT INTO {schema}.{ai_suggestion_table}
                                (document_json, input_tokens, output_tokens, user_input, created_on)
                                VALUES(%s, %s, %s, %s, CURRENT_TIMESTAMP);'''
            string_doc_json = json.dumps(document_json)   
            insert_values =(string_doc_json, str(input_tokens), str(output_tokens), user_input)      
            insert_db(insert_query, insert_values)

            response_body = response_body['content'][0]['text']

             
            return {
                "status_code" : 200,
                "response" : response_body
            }
        except Exception as e:
            print(f"Error Occured in {event_type} : {e}")
            return {
                "status_code" : 500,
                "message" : "An Error Occured While Suggestion Genaration"
            }

    if event_type == "edit_document_type":
        try:
            document_type = event['document_type'].lower().strip()
            document_json = event['document_json']
            extraction_method = event['extraction_method']
            connector_type = event['connector_type']
            config = event.get("config", None)
            document_json = document_json.replace("'","''")
                
            query = f"SELECT * FROM {schema}.{document_type_table} WHERE name = '{document_type}' and delete_status = 0"
            response = select_db(query)

            if not response:
                return {
                    "status_code" : 404,
                    "message" : "Document Type Doesn't Exist"
                }
            
            query = f'''
            UPDATE {schema}.{document_type_table} 
            SET 
            document_json = '{document_json}',
            extraction_method = '{extraction_method}',
            connector_type = '{connector_type}',
            config = '{json.dumps(config)}'
            WHERE name = '{document_type}'  and delete_status = 0  
            '''

            response = update_db(query)

            return {
                "status_code" : 200, 
                "message" : "Document Updated Successfully"
            }

        except Exception as e:
            print(f"Error Occured in {event_type} : {e}")
            return {
                "status_code" : 500,
                "message" : "An Error Occured While Updating Document Type"
            }

    if event_type == "delete_document_type":
        try:
            document_type = event['document_type'].lower()

            query = f"SELECT * FROM {schema}.{document_type_table} WHERE name = '{document_type}' and delete_status = 0"
            response = select_db(query)

            if not response:
                return {
                    "status_code" : 404,
                    "message" : "Document Type Doesn't Exist"
                }

            query = f'''
            UPDATE {schema}.{document_type_table} 
            SET 
            delete_status = 1
            WHERE name = '{document_type}'
            '''
            response = update_db(query)

            return {
                "status_code" : 200, 
                "message" : "Document Type Deleted Successfully"
            }


        except Exception as e:
            print(f"Error Occured in {event_type} : {e}")
            return {
                "status_code" : 500,
                "message" : "An Error Occured While Deleting Document Type"
            }

    if event_type == "list_documents":
        try:
            query = f"select json_agg(row_to_json(row_values)) from (SELECT * FROM {schema}.{document_processing_table} WHERE delete_status = 0 order by created_on desc) as row_values;"   
            response = select_db(query)
            print(response)

            if not response:
                return {
                    "status_code" : 200,
                    "result" : []
                }
            
            return {
                    "status_code" : 200,
                    "result" : response[0][0]
                }
        
        except Exception as e:
            print(f"Error Occured in {event_type} : {e}")
            return {
                "status_code" : 500,
                "message" : "An Error Occured While Retriving Documents"
            }

    if event_type == "view_document":
        try:
            doc_id = event['doc_id']
            doc_name = event['doc_name']
            doc_type = event['doc_type']
            uploaded_by = event['uploaded_by']
            file_extension = doc_name.split('.')[-1]
            key = f"CEXP_OCR/{doc_type}/INPUT/{doc_id}.{file_extension}"

            presigned_url = generate_presigned_url(key)   

            json_file_path = f"CEXP_OCR/{doc_type}/OUTPUT/{doc_id}.json"
            
            response = s3_client.get_object(Bucket=bucket_name, Key=json_file_path)
            json_content = response['Body'].read().decode('utf-8')  
            json_data = json.loads(json_content)            
            # print(json_data)    

            return {
                "statusCode":200,
                "presigned_url":presigned_url,
                "json_data":json_data,
                "document_id":doc_id   
            }    
        except Exception as e:
            print("Failed to process doc_view api due to : ",e)
            return {"statusCode":500,"presigned_url":"","json_data":{}}
    
    if event_type == 'doc_upload':
        doc_id = event['document_id']
        doc_name = event['document_name']
        doc_type = event['document_type']
        uploaded_by = event['uploaded_by']
        file_extension = doc_name.split('.')[-1]

        if file_extension not in ['pdf' ,'jpg', 'png']:
            print("INVALID FILE EXTENSION")
            return {"statusCode":200,"message":"Invalid file type"}

        query = f'''
            SELECT connector_type, config, human_intervention, extraction_method FROM {schema}.{document_type_table} WHERE delete_status = 0  and name = '{doc_type}'
        '''
        response = select_db(query)
        print("RESPONSE: ",response)    

        if not response:
            return {
                "status_code" : 404,
                "message" : "Document Type Can't be Found"
            }
        
        connector_type = response[0][0]
        connector_config = response[0][1]
        human_intervention = response[0][2]
        extraction_method = response[0][3]
        verified = "NO_HUMAN_INTERVENTION" if human_intervention == 0 or human_intervention == "0" else "NOT_VERIFIED"
        connector_config = json.loads(connector_config)

        select_query = f'''SELECT doc_id from {schema}.{document_processing_table} where doc_id = '{doc_id}' and delete_status = 0; '''
        select_result = select_db(select_query)

        if select_result != []:
            print("DOCUMENT ALREADY EXISTS")
            return {"statusCode":200,"message":"Document already exists"}
        else:
            print("DOCUMENT PROCESSING INITIATED")


        insert_query = f'''INSERT INTO {schema}.{document_processing_table}   
                        (doc_id, doc_name, created_on, delete_status, created_by, updated_on, doc_type, doc_status, status_description, updated_by, verified, total_input_tokens, total_output_tokens)
                        VALUES(%s, %s, CURRENT_TIMESTAMP, 0, %s, CURRENT_TIMESTAMP, %s, %s, %s, %s, %s, 0, 0);'''   
        insert_values = (doc_id, doc_name, uploaded_by, doc_type, "In Progress", "Key extraction In Progress", uploaded_by, verified)
        insert_result = insert_db(insert_query, insert_values)

        print("NEW DOCUMENT RECORD ADDED SUCCESSSFULLY")

        try:
            if extraction_method == 'Textract':
                print("TEXTRACT METHOD CALLED")
                final_output_json = key_extraction_funtion(doc_type,doc_name,doc_id,file_extension)
                print("KEY EXTRACTION SUCCESSFULLY")
            if extraction_method == 'LLM':
                print("LLM CALLED")
                if file_extension == 'pdf':
                    print("PDF CALLED")
                    s3_path= f"CEXP_OCR/{doc_type}/INPUT/{doc_id}.{file_extension}"
                    base64_array = pdf_to_base64_images(s3_path)
                    print("BASE_64_ARRAY: ",base64_array)
                    page_results = text_extract_llm(base64_array,file_extension)
                    print("PAGE RESULTS : ",page_results)
                    final_output_json = key_extraction_funtion_llm(page_results,doc_type,doc_name,doc_id,file_extension)
                    print("FINAL_JSON: ",final_output_json)
                    
                elif file_extension in ['png','jpg']:
                    print("PNG CALLED")
                    #call the llm directly with the image
                    encoded_image = encode_image_to_base64(doc_type,doc_id,file_extension)
                    base64_array = [encoded_image]
                    page_results = text_extract_llm(base64_array,file_extension)
                    final_output_json = key_extraction_funtion_llm(page_results,doc_type,doc_name,doc_id,file_extension)

                else:
                    print("INVALID FILE EXTENSION")
                    return {"statusCode":200,"message":"Invalid file type"}


            final_output_json_content = json.dumps(final_output_json, indent=4, ensure_ascii=False)
            final_output_json_path  = f"CEXP_OCR/{doc_type}/OUTPUT/{doc_id}.json"
            s3_upload = s3_client.put_object(Bucket=bucket_name, Key=final_output_json_path, Body=final_output_json_content, ContentType='application/json')
            print(f"json file uploaded successfully ")

            if connector_type == "API":
                api_url = connector_config['api_url']
                api_key = connector_config['api_key']
                output_key = connector_config['output_key']

                headers = {
                    "x-api-key": api_key,
                    "Content-Type": "application/json"
                }

                data = convert_response(output_key, final_output_json)

                # Making the POST request
                response = requests.post(api_url, json=data, headers=headers)
                print("Response for API : ", response)

            end_time = time.time()
            latency = end_time-start_time
            status = "Not Verified" if verified == "NOT_VERIFIED" else "Completed"
            update_query = f'''UPDATE {schema}.{document_processing_table} SET doc_status = '{status}', status_description = 'Key extraction successful', latency = {str(latency)} WHERE doc_id = '{doc_id}' and delete_status = 0;'''
            update_db(update_query)
            return {"statusCode":200,"message":"Key extraction successful"}
        except Exception as e:
            print("An exception occurred while key extraction : ",e)   
            end_time = time.time()
            latency = end_time-start_time
            final_output_json = {}     
            final_output_json_content = json.dumps(final_output_json, indent=4, ensure_ascii=False)
            final_output_json_path  = f"CEXP_OCR/{doc_type}/OUTPUT/{doc_id}.json"
            s3_upload = s3_client.put_object(Bucket=bucket_name, Key=final_output_json_path, Body=final_output_json_content, ContentType='application/json')
            update_query = f'''UPDATE {schema}.{document_processing_table} SET doc_status = 'Failed', status_description = 'Key extraction failed due to : {str(e)}', latency = {str(latency)} WHERE doc_id = '{doc_id}' and delete_status = 0;'''
            update_db(update_query)
            return {"statusCode":200,"message":"Key extraction failed"} 
    
    
    # if event_type == 'doc_upload':   
    #     doc_id = event['document_id']
    #     doc_name = event['document_name']
    #     doc_type = event['document_type']
    #     uploaded_by = event['uploaded_by']
    #     file_extension = doc_name.split('.')[-1]

    #     if file_extension not in ['pdf' ,'jpg', 'png']:
    #         print("INVALID FILE EXTENSION")
    #         return {"statusCode":200,"message":"Invalid file type"}

    #     query = f'''
    #         SELECT connector_type, config, human_intervention, extraction_method FROM {schema}.{document_type_table} WHERE delete_status = 0  and name = '{doc_type}'
    #     '''
    #     response = select_db(query)
    #     print("RESPONSE: ",response)    

    #     if not response:
    #         return {
    #             "status_code" : 404,
    #             "message" : "Document Type Can't be Found"
    #         }
        
    #     connector_type = response[0][0]
    #     connector_config = response[0][1]
    #     human_intervention = response[0][2]
    #     extraction_method = response[0][3]
    #     verified = "NO_HUMAN_INTERVENTION" if human_intervention == 0 or human_intervention == "0" else "NOT_VERIFIED"
    #     connector_config = json.loads(connector_config)

    #     select_query = f'''SELECT doc_id from {schema}.{document_processing_table} where doc_id = '{doc_id}' and delete_status = 0; '''
    #     select_result = select_db(select_query)

    #     if select_result != []:
    #         print("DOCUMENT ALREADY EXISTS")
    #         return {"statusCode":200,"message":"Document already exists"}
    #     else:
    #         print("DOCUMENT PROCESSING INITIATED")


    #     insert_query = f'''INSERT INTO {schema}.{document_processing_table}   
    #                     (doc_id, doc_name, created_on, delete_status, created_by, updated_on, doc_type, doc_status, status_description, updated_by, verified, total_input_tokens, total_output_tokens)
    #                     VALUES(%s, %s, CURRENT_TIMESTAMP, 0, %s, CURRENT_TIMESTAMP, %s, %s, %s, %s, %s, 0, 0);'''   
    #     insert_values = (doc_id, doc_name, uploaded_by, doc_type, "In Progress", "Key extraction In Progress", uploaded_by, verified)
    #     insert_result = insert_db(insert_query, insert_values)

    #     print("NEW DOCUMENT RECORD ADDED SUCCESSSFULLY")

    #     try:
    #         final_output_json = key_extraction_funtion(doc_type,doc_name,doc_id,file_extension)
    #         print("KEY EXTRACTION SUCCESSFULLY")

    #         final_output_json_content = json.dumps(final_output_json, indent=4, ensure_ascii=False)
    #         final_output_json_path  = f"CEXP_OCR/{doc_type}/OUTPUT/{doc_id}.json"
    #         s3_upload = s3_client.put_object(Bucket=bucket_name, Key=final_output_json_path, Body=final_output_json_content, ContentType='application/json')
    #         print(f"json file uploaded successfully ")

    #         if connector_type == "API":
    #             api_url = connector_config['api_url']
    #             api_key = connector_config['api_key']
    #             output_key = connector_config['output_key']

    #             headers = {
    #                 "x-api-key": api_key,
    #                 "Content-Type": "application/json"
    #             }

    #             data = convert_response(output_key, final_output_json)

    #             # Making the POST request
    #             response = requests.post(api_url, json=data, headers=headers)
    #             print("Response for API : ", response)

    #         end_time = time.time()
    #         latency = end_time-start_time
    #         status = "Not Verified" if verified == "NOT_VERIFIED" else "Completed"
    #         update_query = f'''UPDATE {schema}.{document_processing_table} SET doc_status = '{status}', status_description = 'Key extraction successful', latency = {str(latency)} WHERE doc_id = '{doc_id}' and delete_status = 0;'''
    #         update_db(update_query)
    #         return {"statusCode":200,"message":"Key extraction successful"}
    #     except Exception as e:
    #         print("An exception occurred while key extraction : ",e)
    #         end_time = time.time()
    #         latency = end_time-start_time
    #         final_output_json = {}     
    #         final_output_json_content = json.dumps(final_output_json, indent=4, ensure_ascii=False)
    #         final_output_json_path  = f"CEXP_OCR/{doc_type}/OUTPUT/{doc_id}.json"
    #         s3_upload = s3_client.put_object(Bucket=bucket_name, Key=final_output_json_path, Body=final_output_json_content, ContentType='application/json')
    #         update_query = f'''UPDATE {schema}.{document_processing_table} SET doc_status = 'Failed', status_description = 'Key extraction failed due to : {str(e)}', latency = {str(latency)} WHERE doc_id = '{doc_id}' and delete_status = 0;'''
    #         update_db(update_query)
    #         return {"statusCode":200,"message":"Key extraction failed"} 


    
    # if event_type == 'test_doc_upload':
    #     doc_id = event['document_id']
    #     doc_name = event['document_name']
    #     doc_type = event['document_type']
    #     extraction_method = event['extraction_method']
    #     uploaded_by = event['uploaded_by']
    #     document_prompt_details = event['document_prompt_details']

    #     file_extension = doc_name.split('.')[-1]     

    #     if file_extension not in ['pdf' ,'jpg', 'png']:
    #         print("INVALID FILE EXTENSION")
    #         return {"statusCode":200,"message":"Invalid file type"}

    #     select_query = f'''SELECT doc_id from {schema}.{temp_document_processing_table} where doc_id = '{doc_id}' and delete_status = 0; '''
    #     select_result = select_db(select_query)

    #     if select_result != []:
    #         print("DOCUMENT ALREADY EXISTS")
    #         return {"statusCode":200,"message":"Document already exists"}
    #     else:
    #         print("DOCUMENT PROCESSING INITIATED")


    #     insert_query = f'''INSERT INTO {schema}.{temp_document_processing_table}   
    #                     (doc_id, doc_name, created_on, delete_status, created_by, updated_on, doc_type, doc_status, status_description, updated_by, verified, total_input_tokens, total_output_tokens)
    #                     VALUES(%s, %s, CURRENT_TIMESTAMP, 0, %s, CURRENT_TIMESTAMP, %s, %s, %s, %s, %s, 0, 0);'''   
    #     insert_values = (doc_id, doc_name, uploaded_by, doc_type, "In Progress", "Key extraction In Progress", uploaded_by, "")
    #     insert_result = insert_db(insert_query, insert_values)

    #     print("NEW DOCUMENT RECORD ADDED SUCCESSSFULLY")

    #     try:
    #         final_output_json = test_key_extraction_funtion(doc_type,doc_name,doc_id,file_extension,document_prompt_details)
    #         print("KEY EXTRACTION SUCCESSFULLY")

    #         final_output_json_content = json.dumps(final_output_json, indent=4, ensure_ascii=False)
    #         final_output_json_path  = f"CEXP_OCR/{doc_type}/OUTPUT/{doc_id}.json"
    #         s3_upload = s3_client.put_object(Bucket=bucket_name, Key=final_output_json_path, Body=final_output_json_content, ContentType='application/json')
    #         print(f"json file uploaded successfully ")

    #         end_time = time.time()
    #         latency = end_time-start_time
    #         status = "Completed"
    #         update_query = f'''UPDATE {schema}.{temp_document_processing_table} SET doc_status = '{status}', status_description = 'Key extraction successful', latency = {str(latency)} WHERE doc_id = '{doc_id}' and delete_status = 0;'''
    #         update_db(update_query)
    #         return {"statusCode":200,"message":"Key extraction successful"}
    #     except Exception as e:
    #         print("An exception occurred while key extraction : ",e)
    #         end_time = time.time()
    #         latency = end_time-start_time
    #         final_output_json = {}     
    #         final_output_json_content = json.dumps(final_output_json, indent=4, ensure_ascii=False)
    #         final_output_json_path  = f"CEXP_OCR/{doc_type}/OUTPUT/{doc_id}.json"
    #         s3_upload = s3_client.put_object(Bucket=bucket_name, Key=final_output_json_path, Body=final_output_json_content, ContentType='application/json')
    #         update_query = f'''UPDATE {schema}.{temp_document_processing_table} SET doc_status = 'Failed', status_description = 'Key extraction failed due to : {str(e)}', latency = {str(latency)} WHERE doc_id = '{doc_id}' and delete_status = 0;'''
    #         update_db(update_query)
    #         return {"statusCode":200,"message":"Key extraction failed"} 
    
    if event_type == 'test_doc_upload':
        doc_id = event['document_id']
        doc_name = event['document_name']
        doc_type = event['document_type']
        extraction_method = event['extraction_method']
        uploaded_by = event['uploaded_by']
        document_prompt_details = event['document_prompt_details']

        file_extension = doc_name.split('.')[-1]     

        if file_extension not in ['pdf' ,'jpg', 'png']:
            print("INVALID FILE EXTENSION")
            return {"statusCode":200,"message":"Invalid file type"}

        select_query = f'''SELECT doc_id from {schema}.{temp_document_processing_table} where doc_id = '{doc_id}' and delete_status = 0; '''
        select_result = select_db(select_query)

        if select_result != []:
            print("DOCUMENT ALREADY EXISTS")
            return {"statusCode":200,"message":"Document already exists"}
        else:
            print("DOCUMENT PROCESSING INITIATED")


        insert_query = f'''INSERT INTO {schema}.{temp_document_processing_table}   
                        (doc_id, doc_name, created_on, delete_status, created_by, updated_on, doc_type, doc_status, status_description, updated_by, verified, total_input_tokens, total_output_tokens)
                        VALUES(%s, %s, CURRENT_TIMESTAMP, 0, %s, CURRENT_TIMESTAMP, %s, %s, %s, %s, %s, 0, 0);'''   
        insert_values = (doc_id, doc_name, uploaded_by, doc_type, "In Progress", "Key extraction In Progress", uploaded_by, "")
        insert_result = insert_db(insert_query, insert_values)

        print("NEW DOCUMENT RECORD ADDED SUCCESSSFULLY")

        try:
            # final_output_json = test_key_extraction_funtion(doc_type,doc_name,doc_id,file_extension,document_prompt_details)
            # print("KEY EXTRACTION SUCCESSFULLY")

            if extraction_method == 'Textract':
                print("TEST TEXTRACT")
                final_output_json = test_key_extraction_funtion(doc_type,doc_name,doc_id,file_extension,document_prompt_details)
                print("TEST KEY EXTRACTION SUCCESSFULLY")
            if extraction_method == 'LLM':
                print("TEST LLM")
                if file_extension == 'pdf':
                    print("TEST PDF")
                    s3_path= f"CEXP_OCR/Temp/INPUT/{doc_id}.{file_extension}"
                    print("TEST S3_PATH: ",s3_path)
                    base64_array = pdf_to_base64_images(s3_path)
                    print("TEST BASE64 ARRAY:",base64_array)
                    page_results = text_extract_llm(base64_array,file_extension)
                    print("TEST page_results returned: ",page_results)
                    final_output_json = test_key_extraction_funtion_llm(page_results,doc_type,doc_name,doc_id,file_extension,document_prompt_details)
                    print("TEST KEY EXTRACTION SUCCESSFULLY : ",final_output_json)
                    
                elif file_extension in ['png','jpg']:
                    print("TEST PNG,JPG")
                    #call the llm directly with the image
                    encoded_image = test_encode_image_to_base64(doc_type,doc_id,file_extension)
                    base64_array = [encoded_image]
                    print("TEST BASE64 ARRAY: ",base64_array)
                    page_results = text_extract_llm(base64_array,file_extension)   
                    print("TEST PAGE RESULTS: ",page_results)
                    final_output_json = test_key_extraction_funtion_llm(page_results,doc_type,doc_name,doc_id,file_extension)
                    print("TEST KEY EXTRACTION SUCCESSFULLY : ",final_output_json)

                else:   
                    print("INVALID FILE EXTENSION")
                    return {"statusCode":200,"message":"Invalid file type"}

            final_output_json_content = json.dumps(final_output_json, indent=4, ensure_ascii=False)
            final_output_json_path  = f"CEXP_OCR/{doc_type}/OUTPUT/{doc_id}.json"
            s3_upload = s3_client.put_object(Bucket=bucket_name, Key=final_output_json_path, Body=final_output_json_content, ContentType='application/json')
            print(f"json file uploaded successfully ")

            end_time = time.time()
            latency = end_time-start_time
            status = "Completed"
            update_query = f'''UPDATE {schema}.{temp_document_processing_table} SET doc_status = '{status}', status_description = 'Key extraction successful', latency = {str(latency)} WHERE doc_id = '{doc_id}' and delete_status = 0;'''
            update_db(update_query)
            return {"statusCode":200,"message":"Key extraction successful"}
        except Exception as e:
            print("An exception occurred while key extraction : ",e)
            end_time = time.time()
            latency = end_time-start_time
            final_output_json = {}     
            final_output_json_content = json.dumps(final_output_json, indent=4, ensure_ascii=False)
            final_output_json_path  = f"CEXP_OCR/{doc_type}/OUTPUT/{doc_id}.json"
            s3_upload = s3_client.put_object(Bucket=bucket_name, Key=final_output_json_path, Body=final_output_json_content, ContentType='application/json')
            update_query = f'''UPDATE {schema}.{temp_document_processing_table} SET doc_status = 'Failed', status_description = 'Key extraction failed due to : {str(e)}', latency = {str(latency)} WHERE doc_id = '{doc_id}' and delete_status = 0;'''
            update_db(update_query)
            return {"statusCode":200,"message":"Key extraction failed"} 
    

    if event_type == 'check_document_status':
        doc_name = event['doc_name']
        doc_id = event['document_id']

        select_query = f'''SELECT doc_status from {schema}.{temp_document_processing_table} where doc_id = '{doc_id}' and delete_status = 0; '''
        select_result = select_db(select_query)[0][0]

        print("Status : ", select_result)

        if(select_result == "Completed"):
            json_file_path = f"CEXP_OCR/Temp/OUTPUT/{doc_id}.json"
            
            response = s3_client.get_object(Bucket=bucket_name, Key=json_file_path)
            json_content = response['Body'].read().decode('utf-8')  
            json_data = json.loads(json_content) 

            file_extension = doc_name.split('.')[-1]
            key = f"CEXP_OCR/Temp/INPUT/{doc_id}.{file_extension}"

            presigned_url = generate_presigned_url(key)   

            return {
                "statusCode" : 200, 
                "status" : select_result,
                "data" : json_data,
                "presigned_url" : presigned_url
            }

        return {
            "statusCode" : 200, 
            "status" : select_result
        }

    if event_type == 'doc_delete':
        doc_id = event['doc_id']
        doc_name = event['doc_name']   
        doc_type = event['doc_type']
        uploaded_by = event['uploaded_by']
        file_extension = doc_name.split('.')[-1]

        select_query = f'''SELECT doc_id from {schema}.{document_processing_table} where doc_id = '{doc_id}' and delete_status = 0; '''
        select_result = select_db(select_query)

        if select_result == []:
            print("INVALID DOCUMENT DELETION REQUEST")
            return {"statusCode":200,"message":"Invalid document deletion request"}
        
        else:
            print("DOCUMENT DELETION PROCESS INITIATED")

            input_folder_path = f"CEXP_OCR/{doc_type}/INPUT/{doc_id}.{file_extension}"
            s3_client.delete_object(Bucket=bucket_name, Key=input_folder_path)

            output_folder_path = f"CEXP_OCR/{doc_type}/OUTPUT/{doc_id}.json"
            s3_client.delete_object(Bucket=bucket_name, Key=output_folder_path)

            delete_query = f'''UPDATE {schema}.{document_processing_table} SET delete_status = 1, updated_on = CURRENT_TIMESTAMP, updated_by = '{uploaded_by}' where doc_id = '{doc_id}';'''
            update_db(delete_query)
            return {"statusCode":200,"message":"Document deleted successfully"}
    
    if event_type == 'doc_edit':
        try:
            doc_id = event['doc_id']
            doc_name = event['doc_name']
            doc_type = event['doc_type']
            uploaded_by = event['uploaded_by']
            updated_json = event['updated_json']

            update_query = f'''update {schema}.{document_processing_table} set updated_on = CURRENT_TIMESTAMP, updated_by = '{uploaded_by}' where doc_id = '{doc_id}' and delete_status = 0;'''
            update_db(update_query)

            final_output_json_content = json.dumps(updated_json, indent=4, ensure_ascii=False)
            final_output_json_path  = f"CEXP_OCR/{doc_type}/OUTPUT/{doc_id}.json"
            s3_upload = s3_client.put_object(Bucket=bucket_name, Key=final_output_json_path, Body=final_output_json_content, ContentType='application/json')
            print(f"json file uploaded successfully ")

            return {"statusCode":200,"message":"Document updated successfully"}
               
        except Exception as e:
            print("Failed to process doc_edit api due to : ", e)
            return {"statusCode":500,"message":"Failed to update document"}    

    if event_type == "verify_document":
        try: 
            doc_id = event['doc_id']
            doc_name = event['doc_name']
            doc_type = event['doc_type']
            uploaded_by = event['uploaded_by']
            
            query = f'''
            UPDATE {schema}.{document_processing_table} 
            SET
            doc_status = 'Verified',
            verified = 'VERIFIED',
            updated_by = '{uploaded_by}'
            where doc_id = '{doc_id}' and delete_status = 0
            '''
            
            update_db(query)

            return {
                "statusCode":200,
                "message":"Document Verified"
            }


        except Exception as e:
            print("Failed to process verify_document api due to : ", e)
            return {"statusCode":500,"message":"Failed to verify document"}

    if event_type == 'ai_key_extraction':
        document_type = event['document_type']
        document_description = event['document_description']   
        doc_id = event['doc_id']
        doc_name = event['doc_name']
        uploaded_by = event['uploaded_by']

        file_extension = doc_name.split('.')[-1]

        if file_extension not in ['pdf' ,'jpg', 'png']:
            print("INVALID FILE EXTENSION")
            return {"statusCode":200,"message":"Invalid file type"}

        select_query = f'''SELECT doc_id from {schema}.{cexp_ocr_ai_key_extraction_details_table} where doc_id = '{doc_id}' and delete_status = 0; '''
        select_result = select_db(select_query)

        if select_result != []:
            print("DOCUMENT ALREADY EXISTS")
            return {"statusCode":200,"message":"Document already exists"}
        else:
            print("DOCUMENT PROCESSING INITIATED")


        insert_query = f'''INSERT INTO {schema}.{cexp_ocr_ai_key_extraction_details_table}   
                        (doc_id, doc_name, created_on, delete_status, created_by, updated_on, doc_type, doc_status, status_description, updated_by, total_input_tokens, total_output_tokens)
                        VALUES(%s, %s, CURRENT_TIMESTAMP, 0, %s, CURRENT_TIMESTAMP, %s, %s, %s, %s, 0, 0);'''   
        insert_values = (doc_id, doc_name, uploaded_by, document_type, "In Progress", "Key extraction In Progress", uploaded_by)
        insert_result = insert_db(insert_query, insert_values)

        print("NEW DOCUMENT RECORD ADDED SUCCESSSFULLY")

        try:
            final_output_json = ai_key_extractiont_function(doc_name,doc_id,file_extension,document_type,document_description)
            print("KEY EXTRACTION SUCCESSFULLY")

            final_output_json_content = json.dumps(final_output_json, indent=4, ensure_ascii=False)
            final_output_json_path  = f"CEXP_OCR/ai_key_extraction/{document_type}/OUTPUT/{doc_id}.json"
            s3_upload = s3_client.put_object(Bucket=bucket_name, Key=final_output_json_path, Body=final_output_json_content, ContentType='application/json')
            print(f"json file uploaded successfully ")

            end_time = time.time()
            latency = end_time-start_time
            status = "Completed"
            update_query = f'''UPDATE {schema}.{cexp_ocr_ai_key_extraction_details_table} SET doc_status = '{status}', status_description = 'Key extraction successful', latency = {str(latency)} WHERE doc_id = '{doc_id}' and delete_status = 0;'''
            update_db(update_query)
            return {"statusCode":200,"message":"Key extraction successful"}
        
        except Exception as e:
            print("An exception occurred while key extraction : ",e)  
            end_time = time.time()
            latency = end_time-start_time
            final_output_json = {
                                'documentType': document_type,
                                'documentDesc': document_description,
                                'fields': []
                                }    
            final_output_json_content = json.dumps(final_output_json, indent=4, ensure_ascii=False)
            final_output_json_path  = f"CEXP_OCR/{document_type}/OUTPUT/{doc_id}.json"
            s3_upload = s3_client.put_object(Bucket=bucket_name, Key=final_output_json_path, Body=final_output_json_content, ContentType='application/json')
            update_query = f'''UPDATE {schema}.{cexp_ocr_ai_key_extraction_details_table} SET doc_status = 'Failed', status_description = 'Key extraction failed due to : {str(e)}', latency = {str(latency)} WHERE doc_id = '{doc_id}' and delete_status = 0;'''
            update_db(update_query)
            return {"statusCode":200,"message":"Key extraction failed"}    
    
    if event_type == 'check_ai_field_extract_status':  
        doc_name = event['doc_name']
        doc_id = event['document_id']
        doc_type = event['document_type']

        select_query = f'''SELECT doc_status from {schema}.{cexp_ocr_ai_key_extraction_details_table} where doc_id = '{doc_id}' and delete_status = 0; '''
        select_result = select_db(select_query)[0][0]

        print("Status : ", select_result)

        if select_result == "Completed":   
            json_file_path = f"CEXP_OCR/ai_key_extraction/{doc_type}/OUTPUT/{doc_id}.json"
            
            response = s3_client.get_object(Bucket=bucket_name, Key=json_file_path)
            json_content = response['Body'].read().decode('utf-8')  
            json_data = json.loads(json_content) 

            file_extension = doc_name.split('.')[-1]
            key = f"CEXP_OCR/ai_key_extraction/{doc_type}/INPUT/{doc_id}.{file_extension}"

            presigned_url = generate_presigned_url(key)   

            return {
                "statusCode" : 200, 
                "status" : select_result,
                "data" : json_data,
                "presigned_url" : presigned_url   
            }

        return {
            "statusCode" : 200, 
            "status" : select_result
        }  
