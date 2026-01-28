import os
import io
import re
import json
import math
import boto3
import pypdfium2 as pdfium
from textractor import Textractor
from textractor.data.constants import TextractFeatures
from textractor.data.html_linearization_config import HTMLLinearizationConfig
import time
import psycopg2

S3_BUCKET = os.environ['bucket_name']
db_user =os.environ['db_user']     
db_password = os.environ['db_password']             
db_host = os.environ['db_host']                         
db_port = os.environ['db_port']
db_database = os.environ['db_database']
region_name = os.environ['region_name']

schema = os.environ['schema']
model_id = os.environ['model_id']
ocr_document_types_table = os.environ['cexp_ocr_document_types']
job_table = os.environ['job_table']
ocr_prompt_metadata_table  = os.environ['prompt_metadata_table']
ocr_document_upload_table = os.environ['ocr_document_upload_table']


s3_client = boto3.client("s3", region_name=region_name)
bedrock_client = boto3.client("bedrock-runtime", region_name=region_name)
extractor = Textractor(region_name=region_name)


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
        return {"status" : "Insert failed"}
        return None

def update_db_values(query,values):
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
        return {"status":"update query successful"}
    except Exception as e:
        print("Exception occurred while update query : ",e)
        return None

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

def extract_via_LLM(s3_path):
    def pdf_to_image_bytes_from_s3(s3_path: str):
        try:
            pdf_processing_stime = time.time()
            obj = s3_client.get_object(Bucket=S3_BUCKET, Key=s3_path)
            pdf_bytes = obj["Body"].read()

            pdf = pdfium.PdfDocument(pdf_bytes)

            scale = 1.5
            jpeg_quality = 70
            use_grayscale = True

            image_bytes_list = []
            
            for i in range(len(pdf)):
                page = pdf.get_page(i)

                bitmap = page.render(scale=scale)
                pil_img = bitmap.to_pil()

                if use_grayscale:
                    pil_img = pil_img.convert("L")
                else:
                    pil_img = pil_img.convert("RGB")

                buffer = io.BytesIO()
                pil_img.save(
                    buffer,
                    format="JPEG",
                    quality=jpeg_quality,
                    optimize=True
                )

                image_bytes_list.append(buffer.getvalue())
                
                buffer.close()
                pil_img.close()
                bitmap.close()
                page.close()

            pdf.close()
            
            print("Time Taken to Process pdf to bytes : ", time.time() - pdf_processing_stime)
            return image_bytes_list

        except Exception as e:
            print("Error in pdf_to_image_bytes_from_s3 : ", e)
            return []

    def extract_text_from_images(image_bytes_list, extraction_prompt):
        try:
            extraction_time = time.time()
            content_blocks = []

            # Add image blocks
            for image_bytes in image_bytes_list:
                content_blocks.append({
                    "image": {
                        "format": "jpeg",
                        "source": {
                            "bytes": image_bytes
                        }
                    }
                })

            response = bedrock_client.converse(
                modelId=model_id,
                system = [
                    {
                        "text": extraction_prompt
                    }
                ],
                messages=[
                    {
                        "role": "user",
                        "content": content_blocks
                    }
                ]
            )

            # ---- Parse response ----
            output_blocks = response["output"]["message"]["content"]

            extracted_text = ""
            for block in output_blocks:
                if "text" in block:
                    extracted_text += block["text"]
            
            _response = None
            try:
                _response = json.loads(extracted_text)
            except Exception as e:
                print("Retrying with regex: ", e, extracted_text)
                match = re.search(
                    r"```(?:json)?\s*([\s\S]*?)\s*```",
                    extracted_text,
                    re.IGNORECASE
                )
                json_str = match.group(1) if match else "{}"
                _response = json.loads(json_str)

            usage = response.get("usage", {})

            print("Time Taken to Extract Text : ", time.time() - extraction_time)
            return _response, usage
        
        except Exception as e:
            print("Error in extract_text_from_images : ", e)
            return [], {}

    try:
        print("Converting PDF to Bytes")
        image_bytes_list = pdf_to_image_bytes_from_s3(s3_path)
        
        if image_bytes_list == []:
            print("ERROR: image_bytes_list is empty")
            return []
        
        extraction_prompt_query = f"""
        SELECT prompt_template from {schema}.{ocr_prompt_metadata_table} WHERE prompt_type = 'extraction_prompt'
        """
        extraction_prompt = select_db(extraction_prompt_query)[0][0]
        
        print("Extracting Text form the Images via LLM")
        extracted_pages, usage = extract_text_from_images(image_bytes_list, extraction_prompt)

        total_input_tokens = usage.get("inputTokens", 0)
        total_output_tokens = usage.get("outputTokens", 0)
        
        print(f"Input Tokens for Extraction : {total_input_tokens}")
        print(f"Output Tokens for Extraction : {total_output_tokens}")
        
        return extracted_pages
    
    except Exception as e:
        print("Error in extract_via_LLM : ", e)
        return []
    
def get_templates(doc_type):
    query = f'''SELECT document_json from {schema}.{ocr_document_types_table} WHERE name = '{doc_type}' and delete_status = 0;'''
    
    response = select_db(query)     
    print("Response", response)

    event = json.loads(response[0][0])
    print("Event : ", event)

    nested_json_template = {}
    document_descriptions = {}
    master_json_template = {}

    for doc_key, doc_info in event.items():
        if doc_key == "documentDesc":
            continue

        doc_type = doc_info["documentType"].lower()

        # store document description once
        document_descriptions[doc_type] = doc_info["documentDesc"]

        # build field templates in one pass
        fields_dict = {}
        for field in doc_info['fields']:
            fields_dict.update({
                field['name']: "",
                f"{field['name']}_field_description": field['description']
            })

        print("Fields_Dict :", fields_dict)
        nested_json_template[doc_type] = fields_dict

        # update master template
        master_json_template.update(fields_dict)


    return nested_json_template, document_descriptions, master_json_template

def process_page_batch(pdf_uri,
            page_map,
            nested_json_template,
            master_json_template,
            document_descriptions,
            total_documents,
            doc_type
    ):
    
    pdf_key = pdf_uri.split("/", 3)[3]
    print("PDF key: ", pdf_key)
    page_key = page_map.split("/", 3)[3]
    print("Page map key: ", page_key)

    response = s3_client.get_object(Bucket=S3_BUCKET, Key=page_key)
    page_map = json.loads(response["Body"].read().decode("utf-8"))
    print("Fetched page map from S3:", json.dumps(page_map, indent=3))

    extraction_method_query = f"""
    SELECT extraction_method from {schema}.{ocr_document_types_table} where name = '{doc_type}'
    """

    extraction_method = select_db(extraction_method_query)[0][0]

    print("Extraction Method: ", extraction_method)

    try:

        pages_json = []
        if extraction_method.lower() == 'textract':
            document = extractor.start_document_analysis(
                file_source=pdf_uri,
                features=[TextractFeatures.LAYOUT, TextractFeatures.TABLES],
                save_image=False,
            )

            print("Document extracted: ", document.pages)
            print("Document length: ", len(document.pages))

            config = HTMLLinearizationConfig()

            for i in range(len(document.pages)):
                text = document.pages[i].get_text(config=config)

                pages_json.append({
                    "page": page_map[f"{i + 1}"],
                    "extracted_text": text
                })
        else:
            # LLM Flow
            response_list = extract_via_LLM(pdf_key)
            print("Response List: ", response_list)

            for i in range(len(response_list)):
                pages_json.append({
                    "page": page_map[f"{i + 1}"],
                    "extracted_text": response_list[i]['extracted_text']
                })
            
        print("Pages JSON prepared:", pages_json)
    except Exception as e:
        print("Error during document extraction: ", e)
        return {
            'statusCode': 500,
            'body': json.dumps({'message': 'Document extraction failed'})
        }
    
    return invoke_model_function(pages_json, total_documents, document_descriptions, nested_json_template, master_json_template)


def invoke_model_function(pages_json, total_documents, document_descriptions, nested_json_template, master_json_template):

    select_query = f'''
                SELECT prompt_template 
                FROM {schema}.{ocr_prompt_metadata_table}
                WHERE prompt_type = 'worker_prompt';
                '''
    worker_prompt = select_db(select_query)[0][0]
    print("Worker Prompt :\n", worker_prompt)
    max_retries = 4
    retries = 1
    final_prompt = f"""{worker_prompt}

<pages_json>
{pages_json}
</pages_json>

<total_documents>
{total_documents}
</total_documents>

<document_descriptions>
{document_descriptions}
</document_descriptions>

<nested_json>
{nested_json_template}
</nested_json>

<master_json>
{master_json_template}
</master_json>
"""


    while retries <= max_retries:
        try:
            response = bedrock_client.invoke_model(contentType='application/json', body=json.dumps({
                "max_tokens": 4000,
                "temperature": 0,
                "top_p": 0.999,
                # "top_k":250,
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

            print("\nFINAL:\n", final)
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
        return {
            'statusCode': 500,
            'body': json.dumps({'message': 'LLM Maximum retries exceeded'})
        }

def process_page_batch_solo(pdf_uri, page_map, template_json, doc_type):
    pdf_key = pdf_uri.split("/", 3)[3]
    print("PDF key: ", pdf_key)
    page_key = page_map.split("/", 3)[3]
    print("Page map key: ", page_key)

    response = s3_client.get_object(Bucket=S3_BUCKET, Key=page_key)
    page_map = json.loads(response["Body"].read().decode("utf-8"))
    print("Fetched page map from S3:", json.dumps(page_map, indent=3))

    extraction_method_query = f"""
    SELECT extraction_method from {schema}.{ocr_document_types_table} where name = '{doc_type}'
    """

    extraction_method = select_db(extraction_method_query)[0][0]
    print("Extraction Method: ", extraction_method)

    try:

        pages_json = []
        if extraction_method.lower() == 'textract':
            document = extractor.start_document_analysis(
                file_source=pdf_uri,
                features=[TextractFeatures.LAYOUT, TextractFeatures.TABLES],
                save_image=False,
            )

            print("Document extracted: ", document.pages)
            print("Document length: ", len(document.pages))

            config = HTMLLinearizationConfig()

            for i in range(len(document.pages)):
                text = document.pages[i].get_text(config=config)

                pages_json.append({
                    "page": page_map[f"{i + 1}"],
                    "extracted_text": text
                })
        else:
            # LLM Flow
            # pages_json = extract_via_LLM(pdf_key)

            response_list = extract_via_LLM(pdf_key)

            print("Response List: ", response_list)

            for i in range(len(response_list)):
                pages_json.append({
                    "page": page_map[f"{i + 1}"],
                    "extracted_text": response_list[i]['extracted_text']
                })
            
        print("Pages JSON prepared:", pages_json)
    except Exception as e:
        print("Error during document extraction: ", e)
        return {
            'statusCode': 500,
            'body': json.dumps({'message': 'Document extraction failed'})
        }
    return invoke_singleDoc_model_function(pages_json, template_json)

def invoke_singleDoc_model_function(pages_json, template_json):

    select_query = f'''
                SELECT prompt_template 
                FROM {schema}.{ocr_prompt_metadata_table}
                WHERE prompt_type = 'single_doc_prompt';
                '''
    singleDoc_prompt = select_db(select_query)[0][0]

    max_retries = 4
    retries = 1
    final_prompt = f"""{singleDoc_prompt}

    <user_requirement>
    {template_json}
    </user_requirements>

    <pages_json>
    {pages_json}
    </pages_json>
    """

    print("FINAL PROMPT:", final_prompt)


    while retries <= max_retries:
        try:
            response = bedrock_client.invoke_model(contentType='application/json', body=json.dumps({
                "max_tokens": 4000,
                "temperature": 0,
                "top_p": 0.999,
                # "top_k":250,
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

            print("\nFINAL:\n", final)
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
        return {
            'statusCode': 500,
            'body': json.dumps({'message': 'LLM Maximum retries exceeded'})
        }
    

def lambda_handler(event, context):
    print("Event received: ", event)
    
    event_type = event.get('event_type', '')
    pdf_uri = event["pdf_uri"]
    page_map = event["page_map"]
    # nested_json_template = event["nested_json_template"]
    # master_json_template = event["master_json_template"]
    # document_descriptions = event["document_descriptions"]
    # total_documents = event["total_documents"]
    doc_id = event["doc_id"]
    doc_type = event["doc_type"]

    if event_type == 'process_document':
        try:
            nested_json_template, document_descriptions, master_json_template = get_templates(doc_type)
            total_documents = len(document_descriptions)
            result = process_page_batch(
                pdf_uri,
                page_map,
                nested_json_template,
                master_json_template,
                document_descriptions,
                total_documents,
                doc_type
            )

            page_content = result["choices"][0]["message"]["content"]
            input_tokens = result["usage"]["prompt_tokens"]
            output_tokens = result["usage"]["completion_tokens"]

            data = {
                "page_content" : page_content,
                "input_tokens": input_tokens,
                "output_tokens": output_tokens
            }   

            query = f"""INSERT INTO {schema}.{job_table}
            (doc_id, data)
            VALUES(%s, %s)"""

            insert_values = (doc_id, json.dumps(data))
            response = insert_db(query, insert_values)

            print("Insert Response: ", response['status'])


            return {
                'statusCode': 200,
                'body': json.dumps(result)
            }
        except Exception as e:
            print("Error during processing document: ", e)
            update_query = f"""UPDATE {schema}.{ocr_document_upload_table} SET doc_status = 'Failed' WHERE doc_id = %s AND delete_status = 0;"""
            values = (doc_id,)
            update_db_values(update_query, values)
            return {
                'statusCode': 500,
                'body': json.dumps({'message': 'Document processing failed'})
            }
    
    if event_type == 'process_single_document':
        try:
            query = f"""SELECT document_json FROM {schema}.{ocr_document_types_table} WHERE name = '{doc_type}'"""

            result = select_db(query)

            template_json = json.loads(result[0][0])

            print("Final: ", template_json)

            result = process_page_batch_solo(
                pdf_uri,
                page_map,
                template_json,
                doc_type
            )

            print("Result: ", result)

            page_content = result["choices"][0]["message"]["content"]
            input_tokens = result["usage"]["prompt_tokens"]
            output_tokens = result["usage"]["completion_tokens"]

            data = {
                "page_content" : page_content,
                "input_tokens": input_tokens,
                "output_tokens": output_tokens
            }   

            query = f"""INSERT INTO {schema}.{job_table}
            (doc_id, data)
            VALUES(%s, %s)"""

            insert_values = (doc_id, json.dumps(data))
            response = insert_db(query, insert_values)

            print("Insert Response: ", response['status'])


            return {
                'statusCode': 200,
                'body': json.dumps(result)
            }
        
        except Exception as e:
            print("Error during processing a single document: ", e)
            
            update_query = f"""UPDATE {schema}.{ocr_document_upload_table} SET doc_status = 'Failed' WHERE doc_id = %s AND delete_status = 0;"""
            values = (doc_id,)
            update_db_values(update_query, values)

            return {
                'statusCode': 500,
                'body': json.dumps({'message': 'Single document processing failed'})
            }
    
    return {
        'statusCode': 400,
        'body': json.dumps({'message': 'Invalid event type'})
    }