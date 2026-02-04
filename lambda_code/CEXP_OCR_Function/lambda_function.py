import json
import os
import re
import psycopg2
import boto3
import time
import math
import requests
from textractor import Textractor
from textractor.data.constants import TextractFeatures
import base64
from botocore.config import Config
import fitz  # PyMuPDF
from PIL import Image
import io   

db_user =os.environ['db_user']     
db_password = os.environ['db_password']             
db_host = os.environ['db_host']                         
db_port = os.environ['db_port']
db_database = os.environ['db_database']  
bucket_name = os.environ['bucket_name']
S3_BUCKET = bucket_name
region_name = os.environ['region_name']
WORKER_NAME = os.environ['worker_name']
ORCHESTRATOR_NAME = os.environ['orchestrator_name']

schema = os.environ['schema']
document_type_table = os.environ['document_type_table']
job_table = os.environ['job_table']
document_processing_table = os.environ['document_processing_table']
orchestrator_model_id = os.environ['orchestrator_model_id']
extraction_model_id = os.environ['extraction_model_id']
prompt_metadata_table = os.environ['prompt_metadata_table']
ai_suggestion_table = os.environ['ai_suggestion_table']
temp_document_processing_table = os.environ['temp_document_processing_table']
cexp_ocr_ai_key_extraction_details_table = os.environ['cexp_ocr_ai_key_extraction_details_table']   

s3_client = boto3.client('s3',region_name = region_name)
bedrock_client = boto3.client('bedrock-runtime',region_name = region_name)
boto3_session = boto3.Session(region_name = region_name)

lambda_config = Config(
    retries={
        'max_attempts': 0,
        'mode': 'standard'
    },
    connect_timeout=30,
    read_timeout=180
)

lambda_client = boto3_session.client("lambda", config=lambda_config)


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

def s3_put(key, body, content_type):
    try:
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=key,
            Body=body,
            ContentType= f"application/{content_type}"
        )
    except Exception as e:
        print("Exception in s3_put: ", e)

def split_pdf(s3_input_key, doc_name, chunk_size = 5, output_prefix = "splits/"):
    
    try:
        obj = s3_client.get_object(Bucket=bucket_name, Key=s3_input_key)
        pdf_bytes = obj["Body"].read()

        # Load PDF from bytes (NO temp file)
        file_title = doc_name.split(".")[0]
        pdf = fitz.open(stream=pdf_bytes, filetype="pdf")
        total_pages = pdf.page_count

        uploaded_batches = []

        for i in range(0, total_pages, chunk_size):
            new_pdf = fitz.open()

            start = i
            end = min(i + chunk_size, total_pages)

            page_map = {}
            batch_page_index = 1

            for page_num in range(start, end):
                new_pdf.insert_pdf(pdf, from_page=page_num, to_page=page_num)

                # batch_page_number : actual_page_number (1-based)
                page_map[batch_page_index] = page_num + 1
                batch_page_index += 1

            pdf_part_bytes = new_pdf.tobytes()

            part_number = (i // chunk_size) + 1
            pdf_key = f"{output_prefix}_part_{part_number}.pdf"
            map_key = f"{output_prefix}_part_{part_number}_page_map.json"

            # upload PDF
            # s3_client.put_object(
            #     Bucket=S3_BUCKET,
            #     Key=pdf_key,
            #     Body=pdf_part_bytes,
            #     ContentType="application/pdf",
            # )

            s3_put(pdf_key, pdf_part_bytes, "pdf")

            # upload page map JSON
            # s3_client.put_object(
            #     Bucket=S3_BUCKET,
            #     Key=map_key,
            #     Body=json.dumps(page_map),
            #     ContentType="application/json",
            # )

            print("Page Map:", json.dumps(page_map, indent = 2))

            s3_put(map_key, json.dumps(page_map), "json")

            uploaded_batches.append(
                {
                    "pdf_uri": f"s3://{S3_BUCKET}/{pdf_key}",
                    "page_map_uri": f"s3://{S3_BUCKET}/{map_key}",
                    "page_map": page_map,
                }
            )

            del pdf_part_bytes
            new_pdf.close()

        pdf.close()
        del pdf_bytes

        return uploaded_batches
    except Exception as e:
        print("Exception in split_pdf: ", e)
        return []


def get_templates(doc_type):
    query = f'''SELECT document_json from {schema}.{document_type_table} WHERE name = '{doc_type}' and delete_status = 0;'''
    # select_values = (,)
    
    response = select_db(query)     
    print("Response", response)

    event = json.loads(response[0][0])
    print("Event : ", event)

    nested_json_template = {}
    document_descriptions = {}
    # master_json_template = {}

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
        # master_json_template.update(fields_dict)


    return nested_json_template, document_descriptions


def invoke_worker(payload, worker_name):
    print("Inside Invoke Worker for key , ", payload)    

    lambda_client.invoke(
        FunctionName=worker_name,
        InvocationType='Event',
        Payload=json.dumps(payload).encode("utf-8"),
    )

    # return { "response": "Lambda Invoked" }

def invoke_workers_parallel(s3_keys, doc_id, doc_type, worker_lambda_name, event_type, max_workers=5):
    try:
        max_workers = min(max_workers, len(s3_keys))
        print("Inside Invoke Parallel with max workers : ", max_workers)
        total_input_tokens = 0
        total_output_tokens = 0
        
        # nested_json_template, document_descriptions, master_json_template = get_templates(doc_type)
        # total_documents = len(document_descriptions)

        for batch in s3_keys:
            payload = {
                'event_type': event_type,
                'pdf_uri' : batch["pdf_uri"],
                'page_map' : batch["page_map_uri"],
                # 'nested_json_template' : nested_json_template,
                # 'master_json_template' : master_json_template,
                # 'document_descriptions' : document_descriptions,
                # 'total_documents' : total_documents,
                'doc_id' : doc_id,
                'doc_type' : doc_type,
            }
            invoke_worker(
                payload,
                worker_name=worker_lambda_name
            )

        return {
            'result' : f"{max_workers} Lambda(s) Invoked",
        }
    except Exception as e:
        return {
            'result': f"Exception occurred while invoking lambdas in parallel: {e}",
        }  

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

        # # Convert pixmap to PIL image
        # img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)

        # # Save image to a BytesIO buffer
        # img_buffer = io.BytesIO()
        # img.save(img_buffer, format="PNG")
        # img_buffer.seek(0)

        # # Encode the image as base64
        # img_base64 = base64.b64encode(img_buffer.read()).decode("utf-8")

        # # Append the base64 string to the result list
        # base64_images.append(img_base64)
        # ------------------------------------------
        # Directly get PNG bytes from fitz
        png_bytes = pix.tobytes("png")

        # Encode as base64
        img_base64 = base64.b64encode(png_bytes).decode("utf-8")
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

def detect_image_format_from_bytes_pil(image_base64: str) -> str:
    img_bytes = base64.b64decode(image_base64)
    with Image.open(io.BytesIO(img_bytes)) as img:
        return img.format.lower()


def text_extract_llm(base_64_array,file_extension):
    print("TEXT EXTRACT LLM CALLED")
    input_prompt = '''extract and provide the text present in the image in a neat formatted manner which can be used for nlp tasks. the answer format should only be the all extracted text in a neat formatted manner from the image without any other information. Ensure to double check the numbers extracted from the image.'''
    page_results = []
    for i in base_64_array:
        # response = bedrock_client.invoke_model(contentType='application/json', body=json.dumps({
        #     "anthropic_version": "bedrock-2023-05-31",
        #     "max_tokens": 3000,
        #     "temperature": 0,
        #     "top_p": 0.8,
        #     "top_k":100,
        #     "system":input_prompt,
        #     "messages": [
        #         {
        #             "role": "user",
        #             "content": [
        #                 {
        #                     "type": "image",
        #                     "source": {
        #                         "type": "base64",
        #                         "media_type": "image/png",
        #                         "data": i
        #                     }
        #                 },
        #                 # {
        #                 #     "type": "text",
        #                 #     "text": input_prompt
        #                 # }
        #             ]
        #         }
        #     ]
        # }), modelId=model_id)

        if type(i) == dict:
            messages = [
                {
                    "role": "user",
                    "content": [
                        {
                            "image": {
                                "format": i['format'],
                                "source": {
                                    "bytes": i['bytes']
                                }
                            }   
                        }
                    ]
                }
            ]
        else:
            _format = detect_image_format_from_bytes_pil(i)
            messages = [
                {
                    "role": "user",
                    "content": [
                        {
                            "image": {
                                "format": _format,
                                "source": {
                                    "bytes": "b"+i
                                }
                            }   
                        }
                    ]
                }
            ]


        # if type(i) == dict:
        #     request_body = {
        #         "schemaVersion": "messages-v1",
        #         "system": [
        #             {
        #                 "text": input_prompt
        #             }
        #         ],
        #         "messages": [
        #             {
        #                 "role": "user",
        #                 "content": [
        #                     {
        #                         "image": {
        #                             "format": i['format'],
        #                             "source": {
        #                                 "bytes": i['bytes']
        #                             }
        #                         }   
        #                     }
        #                 ]
        #             }
        #         ],
        #         "inferenceConfig": {
        #             "maxTokens": 3000,
        #             "temperature": 0.0,
        #             "topP": 0.8
        #         }
        #     }
        # else:
        #     _format = detect_image_format_from_bytes_pil(i)
        #     request_body = {
        #         "schemaVersion": "messages-v1",
        #         "system": [
        #             {
        #                 "text": input_prompt
        #             }
        #         ],
        #         "messages": [
        #             {
        #                 "role": "user",
        #                 "content": [
        #                     {
        #                         "image": {
        #                             "format": _format,
        #                             "source": {
        #                                 "bytes": "b"+i
        #                             }
        #                         }   
        #                     }
        #                 ]
        #             }
        #         ],
        #         "inferenceConfig": {
        #             "maxTokens": 3000,
        #             "temperature": 0.0,
        #             "topP": 0.8
        #         }
        #     }
        
        # response = orchestrator_bedrock_client.invoke_model(
        #     modelId = extraction_model_id,
        #     body = json.dumps(request_body),
        #     contentType = 'application/json'
        # )

        response = bedrock_client.converse(
            modelId=extraction_model_id,
            system = [
                {
                    "text": input_prompt
                }
            ],
            messages=messages
        )
        
        # inference_result = response['body'].read().decode('utf-8')
        # final = json.loads(inference_result)
        # print("FINAL : ",final)   
        # extracted_content = final['output']['message']['content'][0]['text']

        output_blocks = response["output"]["message"]["content"]
        extracted_text = ""
        for block in output_blocks:
            if "text" in block:
                extracted_text += block["text"]
        print("Extracted Text: ", extracted_text)
        page_results.append(extracted_text)
    return page_results   


def encode_image_to_base64(doc_type,doc_id,file_extension):
    key = f"CEXP_OCR/{doc_type}/INPUT/{doc_id}.{file_extension}"
    response = s3_client.get_object(Bucket=bucket_name, Key=key)
    image_data = response['Body'].read()
    # encoded_image = base64.b64encode(image_data).decode("utf-8")
    return image_data


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
            request = {
                "modelId": orchestrator_model_id,
                "messages": [
                    {
                        "role": "user",
                        "content": [{"text": final_prompt}]
                    }
                ],
                "inferenceConfig": {
                    "maxTokens": 3000,
                    "temperature": 0.0
                }
            }

            response = bedrock_client.converse_stream(**request)

            print('Response from LLM extraction: ', response)

            full_text = ""
            usage = {"input_tokens": 0, "output_tokens": 0}

            for event in response.get("stream", []):
                if "contentBlockDelta" in event:
                    delta = event["contentBlockDelta"]["delta"]
                    if "text" in delta:
                        full_text += delta["text"]

                if "responseStop" in event:
                    stop = event["responseStop"]
                    if "usage" in stop:
                        usage = {
                            "input_tokens": stop["usage"].get("inputTokens", 0),
                            "output_tokens": stop["usage"].get("outputTokens", 0)
                        }

            return {
                "usage": usage,
                "content": [{"text": full_text}]
            }

        except Exception as e:
            print("ERROR IN CONVERSE STREAM:", e)
            print("Retrying...")
            time.sleep(1)
            retries += 1

    print("Max retries exceeded.")
    return {"usage": {"input_tokens": 0, "output_tokens": 0}, "content": [{"text": ""}], "status": "Failed"}

def key_extraction_funtion_invoke(doc_type,doc_name,doc_id,file_extension):
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

def extract_text_from_image(image_bytes, file_extension, extraction_prompt):
        try:
            extraction_time = time.time()
            content_blocks = []

            response = bedrock_client.converse(
                modelId=extraction_model_id,
                system = [
                    {
                        "text": extraction_prompt
                    }
                ],
                messages=[
                    {
                        "role": "user",
                        "content": [
                            {
                                "image": {
                                    "format": "jpeg" if file_extension in ["jpg", "jpeg"] else file_extension,
                                    "source": {
                                        "bytes": image_bytes
                                    }
                                }
                            }
                        ]
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

def key_extraction_funtion(doc_type, doc_name, doc_id, file_extension, extraction_method):
    try:
        page_texts = []
        if extraction_method.lower() == "textract":
            print("Extracting Through Textract - key_extraction_funtion")
            path = f"s3://{bucket_name}/CEXP_OCR/{doc_type}/INPUT/{doc_id}.{file_extension}"
            extractor = Textractor(region_name=region_name)

            document = extractor.start_document_analysis(
                file_source=path,
                features=[TextractFeatures.LAYOUT, TextractFeatures.TABLES],
                save_image=False
            )

            page_texts = [page.get_text() for page in document.pages]
        
        else:
            print("Extracting Through LLM - key_extraction_funtion")
            extraction_prompt_query = f"""
            SELECT prompt_template from {schema}.{prompt_metadata_table} WHERE prompt_type = 'extraction_prompt'
            """
            extraction_prompt = select_db(extraction_prompt_query)[0][0]
            
            encoded_image = encode_image_to_base64(doc_type, doc_id,file_extension)
            page_texts, _ = extract_text_from_image(encoded_image, file_extension, extraction_prompt)

        select_query = f"""
            SELECT document_json
            FROM {schema}.{document_type_table}
            WHERE delete_status = 0 AND name = '{doc_type}'
        """
        document_prompt_details = select_db(select_query)[0][0]
        json_document_details = json.loads(json.loads(document_prompt_details))

        document_key_schema = json_document_details["fields"]
        document_description = json_document_details["documentDesc"]

        base_prompt = select_db(f"""
            SELECT prompt_template 
            FROM {schema}.{prompt_metadata_table}
            WHERE prompt_type = 'base_prompt';
        """)[0][0]
        print(base_prompt,"base_prompt")
        print(doc_type,"doc_type")
        print(document_description,"document_description")
        print(document_key_schema,"document_key_schema")
        final_prompt = f"""
            {base_prompt}
            Document Type: {doc_type}
            Document Description and Document Type Specific Instructions: {document_description}
            Required Fields: {document_key_schema}

            Input Document:
            {page_texts}
        """

        final = invoke_model_function(final_prompt)

        input_tokens = final.get("usage", {}).get("input_tokens", 0)
        output_tokens = final.get("usage", {}).get("output_tokens", 0)
        status = final.get("status", "")
        print("Status: ", status)

        update_query = f"""
            UPDATE {schema}.{document_processing_table}
            SET total_input_tokens = {input_tokens},
                total_output_tokens = {output_tokens}
            WHERE doc_id = '{doc_id}' AND delete_status = 0;
        """
        
        update_db(update_query)

        # Extract content safely
        raw_output = final.get("content", [{}])[0].get("text", "").strip()

        # Remove markdown json fences if present
        if raw_output.startswith("```"):
            raw_output = raw_output.split("```")[1]
            raw_output = raw_output.replace("json", "").strip()

        try:
            extracted_json = json.loads(raw_output)
        except Exception as e:
            print("Error parsing JSON:", e)
            extracted_json = {}

        print("Extracted JSON: ", extracted_json)
        return extracted_json

    except Exception as e:
        print("Exception in key extraction:", e)
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
        print("AI Response : ", final)

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

def has_any_value(nested_json: dict) -> bool:
    """
    Return True only if at least one actual FIELD key (not *_field_description,
    not classification) has a non-empty value.
    """

    if not isinstance(nested_json, dict):
        return False

    for doc_type, doc_block in nested_json.items():
        # skip classification entirely
        if doc_type == "classification":
            continue

        if not isinstance(doc_block, dict):
            continue

        for key, value in doc_block.items():
            # ignore description keys
            if key.endswith("_field_description"):
                continue

            # non-empty value check
            if value not in ("", None, [], {}):
                return True

    return False

def final_json_curation(extractions):

    final_output = {
        "nested_jsons": {},
        "unclassified_master_jsons": []
    }
    input_tokens = 0
    output_tokens = 0

    nested_counter = 1

    for row in extractions:
        # row is already a JSON string
        outer = json.loads(row)

        # page_content itself is a string containing TWO JSON blocks
        page_content = outer["page_content"]
        input_tokens += outer["input_tokens"]
        output_tokens += outer["output_tokens"]

        # print("Page Content: \n", page_content)

        # split the two JSON objects safely
        page_content = page_content.strip()
        content = json.loads(page_content)
        print("Content: \n", json.dumps(content, indent = 2))
        nested_json = content.get("nested_json")
        unclassified = content.get("unclassified_master_jsons", [])

        # print("Nested JSON: \n", nested_json)
        # print("Unclassified JSON: \n", unclassified)


        # add nested_json only if it has at least one real value
        if nested_json and has_any_value(nested_json):
            key = f"nested_json_{nested_counter}"
            final_output["nested_jsons"][key] = nested_json
            nested_counter += 1

        # always aggregate non-empty unclassified master jsons
        if unclassified:
            final_output["unclassified_master_jsons"].extend(unclassified)
        
    input_cost = (input_tokens / 1000) * 0.00053
    output_cost = (output_tokens / 1000) * 0.00266  

    print(f"Final output:\n {final_output}")  
    print(f"Input Cost:\n {input_cost}")  
    print(f"Output Cost:\n {output_cost}")  

    return final_output, input_cost, output_cost

def final_json_curation_singleDoc(extractions):
    input_tokens = 0
    output_tokens = 0
    final_json = {}
    for idx, row in enumerate(extractions, start=1):
        outer = json.loads(row)
        page_key = f"page_{idx}"
        page_content_str = outer.get("page_content", "{}")
        final_json[page_key] = json.loads(page_content_str)
        input_tokens += outer["input_tokens"]
        output_tokens += outer["output_tokens"]

    input_cost = (input_tokens / 1000) * 0.00053
    output_cost = (output_tokens / 1000) * 0.00266  

    print(f"Final output:\n {final_json}")  
    print(f"Input Cost:\n {input_cost}")  
    print(f"Output Cost:\n {output_cost}")
    return final_json, input_cost, output_cost

def extract_json_after_reasoning(text: str) -> str:
    try:
        """
        Extracts and returns the JSON string that appears strictly
        after the </reasoning> tag.
        """
        print("Here")
        marker = "</reasoning>"
        idx = text.find(marker)
        if idx == -1:
            raise ValueError("Missing </reasoning> tag")

        return text[idx + len(marker):].strip()
    except Exception as e:
        print(f"Error at reasoning filtering: {e}")
        return text


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

    if event_type == "add_multi_document_type":
        try:
            email_id = event['email_id']
            document_type = event['document_type'].lower().strip()
            extraction_method = event['extraction_method']
            document_desc = event['document_desc']
            document_list = event['document_list']
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

            # Prepare for the Document Json
            sql_list = ", ".join("'" + x.replace("'", "''") + "'" for x in document_list)

            select_doc_type_query = f"""
                SELECT jsonb_object_agg(name, document_json) AS doc_map
                FROM {schema}.{document_type_table}
                WHERE name IN ({sql_list})
            """

            doc_type_response = select_db(select_doc_type_query)
            doc_map = doc_type_response[0][0]

            for k, v in doc_map.items():
                doc_map[k] = json.loads(json.loads(v))

            doc_map['documentDesc'] = document_desc

            query = f'''
            INSERT INTO {schema}.{document_type_table} (name, created_by, document_json, human_intervention, connector_type, config, extraction_method, created_at, delete_status, is_multi_document) VALUES (%s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, 0, 1)    
            '''
            values = (document_type, email_id, json.dumps(doc_map), human_intervention, connector_type, json.dumps(config), extraction_method)   

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
            
            # response = bedrock_client.invoke_model(contentType='application/json', 
            #     body=json.dumps({
            #     "anthropic_version": "bedrock-2023-05-31",
            #     "max_tokens": 1000,
            #     "messages": [
            #         {
            #             "role": "user",
            #             "content": [
            #                 {"type": "text", "text": final_prompt},
            #             ]
            #         }
            #     ],
            # }), modelId=model_id)

            final = invoke_model_function(final_prompt)

            if 'usage' in final and 'input_tokens' in final['usage'] and 'output_tokens' in final['usage']:
                input_tokens = final['usage']['input_tokens']
                output_tokens = final['usage']['output_tokens']
            else:
                input_tokens = 0
                output_tokens = 0

            if 'content' in final and len(final['content']) > 0 and 'text' in final['content'][0]:
                response_body = final['content'][0]['text']
                print("EXTRACTED JSON BEFORE LOADS : ", response_body)
                try:
                    response_body = json.loads(response_body)
                    print("EXTRACTED JSON AFTER LOADS : ", response_body)
                except Exception as e:
                    print("Exception occurred while converting string to json: ", e)
                    response_body = {}
            
            insert_query = f'''INSERT INTO {schema}.{ai_suggestion_table}
                                (document_json, input_tokens, output_tokens, user_input, created_on)
                                VALUES(%s, %s, %s, %s, CURRENT_TIMESTAMP);'''
            string_doc_json = json.dumps(document_json)   
            insert_values =(string_doc_json, str(input_tokens), str(output_tokens), user_input)      
            insert_db(insert_query, insert_values)
             
            return {
                "status_code" : 200,
                "response" : json.dumps(response_body)
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
            is_multi_document = event.get("is_multi_document", False)
                
            query = f"SELECT * FROM {schema}.{document_type_table} WHERE name = '{document_type}' and delete_status = 0"
            response = select_db(query)

            if not response:
                return {
                    "status_code" : 404,
                    "message" : "Document Type Doesn't Exist"
                }

            # if is_multi_document:
            #     document_list = event.get("document_list", [])
            #     document_desc = event.get("document_desc", "")

            #     print("Document List : ", document_list)

            #     sql_list = ", ".join("'" + x.replace("'", "''") + "'" for x in document_list)

            #     select_doc_type_query = f"""
            #         SELECT jsonb_object_agg(name, document_json) AS doc_map
            #         FROM {schema}.{document_type_table}
            #         WHERE name IN ({sql_list})
            #     """

            #     doc_type_response = select_db(select_doc_type_query)
            #     doc_map = doc_type_response[0][0]

            #     for k, v in doc_map.items():
            #         doc_map[k] = json.loads(json.loads(v))

            #     doc_map['documentDesc'] = document_desc

            #     query = f'''
            #     UPDATE {schema}.{document_type_table} 
            #     SET 
            #     document_json = %s,
            #     extraction_method = %s,
            #     connector_type = %s,
            #     config = %s
            #     WHERE name = '{document_type}' and delete_status = 0  
            #     '''

            #     values = (json.dumps(doc_map), extraction_method, connector_type, json.dumps(config))
            #     response = update_db_values(query, values)

            #     return {
            #         "status_code" : 200, 
            #         "message" : "Document Updated Successfully"
            #     }

            # else:
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

            select_status_query = f"""
            SELECT doc_status, verified from {schema}.{document_processing_table} where doc_id = '{doc_id}'
            """
            status_response = select_db(select_status_query)

            presigned_url = generate_presigned_url(key)   

            json_file_path = f"CEXP_OCR/{doc_type}/OUTPUT/{doc_id}.json"
            
            response = s3_client.get_object(Bucket=bucket_name, Key=json_file_path)
            json_content = response['Body'].read().decode('utf-8')  
            json_data = json.loads(json_content)            
            # print(json_data)    

            return {
                "statusCode":200,
                "presigned_url": f"https://{bucket_name}.s3.{region_name}.amazonaws.com/{key}",
                "json_data":json_data,
                "document_id":doc_id,
                "verified": status_response[0][1],
                "doc_status": status_response[0][0]
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
            SELECT connector_type, config, human_intervention, extraction_method, is_multi_document FROM {schema}.{document_type_table} WHERE delete_status = 0  and name = '{doc_type}'
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
        multidocument = response[0][4]
        verified = "NO_HUMAN_INTERVENTION" if human_intervention == 0 or human_intervention == "0" else "NOT_VERIFIED"
        connector_config = json.loads(connector_config)

        print("Extraction method:", extraction_method)    

        select_query = f'''SELECT doc_id from {schema}.{document_processing_table} where doc_id = '{doc_id}' and delete_status = 0; '''
        select_result = select_db(select_query)
        
        if select_result != []:
            print("DOCUMENT ALREADY EXISTS")
            return {"statusCode":200,"message":"Document already exists"}
        else:
            print("DOCUMENT PROCESSING INITIATED")  
            
        try:
            if file_extension == 'pdf':
                print("Processing the PDF")
                input_path = f"CEXP_OCR/{doc_type}/INPUT/{doc_id}.{file_extension}"
                obj = s3_client.get_object(Bucket=bucket_name, Key=input_path)
                pdf_bytes = obj["Body"].read()

                doc = fitz.open(stream=pdf_bytes, filetype="pdf")
                total_pages = doc.page_count
                doc.close()

                total_parts = math.ceil(total_pages / 5)

                print("Total Pages: ", total_pages)
                print("Total Parts: ", total_parts)    

                insert_query = f'''INSERT INTO {schema}.{document_processing_table}
                                (doc_id, doc_name, created_on, delete_status, created_by, updated_on, doc_type, doc_status, status_description, updated_by, verified, total_input_tokens, total_output_tokens, total_parts)
                                VALUES(%s, %s, CURRENT_TIMESTAMP, 0, %s, CURRENT_TIMESTAMP, %s, %s, %s, %s, %s, 0, 0, %s);'''   
                insert_values = (doc_id, doc_name, uploaded_by, doc_type, "In Progress", "Key extraction In Progress", uploaded_by, verified, total_parts)
                insert_result = insert_db(insert_query, insert_values)
                
                # Invoking Workers 
                output_path = f"CEXP_OCR/{doc_type}/OUTPUT/{doc_id}_splits/"
                s3_keys = split_pdf(input_path, doc_name, output_prefix=output_path)    
                print("Invoking the Lambdas")

                # Call the Lambdas 
                responses = invoke_workers_parallel(
                    s3_keys=s3_keys,
                    doc_id=doc_id,
                    doc_type=doc_type,
                    worker_lambda_name=WORKER_NAME,
                    event_type = 'process_document' if multidocument == 1 or multidocument == '1' else 'process_single_document',
                    max_workers=5
                )

                print("Lambda Invocation status: ", responses['result'])
                
                return {
                    "statusCode" : 200,
                    "message" : "Key extraction In Progress"
                }
                
            elif file_extension in ['png','jpg']:
                # Images - Textract & LLM Extraction - Happens in Main Lambda itself 
                print("Processing the Image file")
                insert_query = f'''INSERT INTO {schema}.{document_processing_table}
                                (doc_id, doc_name, created_on, delete_status, created_by, updated_on, doc_type, doc_status, status_description, updated_by, verified, total_input_tokens, total_output_tokens)
                                VALUES(%s, %s, CURRENT_TIMESTAMP, 0, %s, CURRENT_TIMESTAMP, %s, %s, %s, %s, %s, 0, 0);'''   
                insert_values = (doc_id, doc_name, uploaded_by, doc_type, "In Progress", "Key extraction In Progress", uploaded_by, verified)
                insert_result = insert_db(insert_query, insert_values)
                
                final_output_json = key_extraction_funtion(doc_type, doc_name, doc_id, file_extension, extraction_method)
                
                print("Final Output JSON: ", final_output_json)
                if len(final_output_json) > 0:
                    final_output_json_content = json.dumps(final_output_json, indent=4, ensure_ascii=False)
                    final_output_json_path  = f"CEXP_OCR/{doc_type}/OUTPUT/{doc_id}.json"
                    s3_upload = s3_client.put_object(Bucket=bucket_name, Key=final_output_json_path, Body=final_output_json_content, ContentType='application/json')
                    print(f"json file uploaded successfully ")
                    update_query = f"""
                        UPDATE {schema}.{document_processing_table}
                        SET doc_status = 'Completed'
                        WHERE doc_id = '{doc_id}' AND delete_status = 0;
                    """
                    update_db(update_query)
                else:
                    print(f"json file uploaded failed ")

                    update_query = f"""
                        UPDATE {schema}.{document_processing_table}
                        SET doc_status = 'Failed'
                        WHERE doc_id = '{doc_id}' AND delete_status = 0;
                    """
                    update_db(update_query)

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
            update_query = f'''UPDATE {schema}.{document_processing_table} SET doc_status = '{status}', status_description = 'Key extraction successful', latency = {str(0)} WHERE doc_id = '{doc_id}' and delete_status = 0;'''
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
                    base64_array = [{"bytes" : encoded_image, "format" : file_extension}]
                    print("TEST BASE64 ARRAY: ",base64_array)
                    page_results = text_extract_llm(base64_array,file_extension)   
                    print("TEST PAGE RESULTS: ",page_results)
                    final_output_json = test_key_extraction_funtion_llm(page_results,doc_type,doc_name,doc_id,file_extension,document_prompt_details)
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
                "presigned_url" : f"https://{bucket_name}.s3.{region_name}.amazonaws.com/{key}"
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
                "presigned_url" : f"https://{bucket_name}.s3.{region_name}.amazonaws.com/{key}"   
            }
    
    
    

        return {
            "statusCode" : 200, 
            "status" : select_result
        }     
    
    if event_type == 'handle_doc_job':
        try:
            doc_id = event['doc_id']
            sec_event = {
                'doc_id': doc_id,
                "event_type": "handle_doc_job2"
            }

            lambda_client.invoke(
                FunctionName=ORCHESTRATOR_NAME,
                InvocationType='Event',
                Payload=json.dumps(sec_event).encode("utf-8"),
            )
        except Exception as e:
            print("Error invoking handle_doc_job2 lambda: ", e)
            return{
                "statusCode": 500,
                "status": "Error invoking handle_doc_job2 lambda"
            }

    if event_type == 'handle_doc_job2':
        try:
            doc_id = event['doc_id']
            query = f'''SELECT doc_type, doc_name FROM {schema}.{document_processing_table} WHERE doc_id = '{doc_id}';'''
            response = select_db(query)
            doc_type = response[0][0]
            doc_name = response[0][1]

            connector_query = f'''
            SELECT connector_type, config, human_intervention FROM {schema}.{document_type_table} WHERE delete_status = 0 and name = '{doc_type}'
            '''
            connector_response = select_db(connector_query)

            connector_type = connector_response[0][0]
            connector_config = connector_response[0][1]
            human_intervention = connector_response[0][2]
            verified = "NO_HUMAN_INTERVENTION" if human_intervention == 0 or human_intervention == "0" else "NOT_VERIFIED"
            connector_config = json.loads(connector_config)

            print("Doc_Type: ", doc_type)
            print("Doc_Name: ", doc_name)

            multidocument_check_query = f"""SELECT is_multi_document FROM {schema}.{document_type_table} WHERE name = '{doc_type}'"""

            is_multi_document = select_db(multidocument_check_query)[0][0]

            print('Is Multi Document: ', is_multi_document)

            query = f"""SELECT data FROM {schema}.{job_table} WHERE doc_id = '{doc_id}'"""
            # select_values = (doc_id, )

            time.sleep(5)

            data_response = select_db(query)

            print("Data_response : ", data_response)

            results = []

            for entry in data_response:
                results.append(entry[0])

            print("Results of all the Lambdas:" , results)

            if is_multi_document == '1' or is_multi_document == 1:

                final_output, qwen_input_cost, qwen_output_cost = final_json_curation(results)

                # print("Final Output: ", final_output)

                nested_json_template, document_descriptions = get_templates(doc_type)
                total_documents = len(document_descriptions)
                print("Total Documents: ", total_documents)

                select_query = f'''
                    SELECT prompt_template 
                    FROM {schema}.{prompt_metadata_table}
                    WHERE prompt_type = 'orchestrator_prompt';
                    '''
                orchestrator_prompt = select_db(select_query)[0][0]
                print("Orchestrator Prompt: \n", orchestrator_prompt)

                final_orchestrator_prompt = orchestrator_prompt + f"""
                <user_required_fields>
                {nested_json_template}
                </user_required_fields>
                <output_json>
                {final_output}
                </output_json>
                <total_documents>
                {total_documents}
                </total_documents>
                <document_descriptions>
                {document_descriptions}
                </document_descriptions>
                """

                response = bedrock_client.invoke_model(
                    modelId=orchestrator_model_id,
                    contentType="application/json",
                    accept="application/json",
                    body=json.dumps({
                        "messages": [
                            {"role": "system", "content": final_orchestrator_prompt},
                        ],
                        "max_tokens": 4000,
                        "temperature": 0.2
                    })
                )

                result = response["body"].read().decode("utf-8")

                print("Result:", result)

                answer = json.loads(result)

                oss_input_tokens = answer['usage']['prompt_tokens']
                oss_output_tokens = answer['usage']['completion_tokens']

                oss_input_cost = (oss_input_tokens / 1000) * 0.00015
                oss_output_cost = (oss_output_tokens / 1000) * 0.00060

                print(F"OSS Input Tokens: ${oss_input_cost}")
                print(F"OSS Output Tokens: ${oss_output_cost}")

                total_input_cost = qwen_input_cost + oss_input_cost
                total_output_cost = qwen_output_cost + oss_output_cost  

                print(f"Total Input Cost: ${total_input_cost}")          
                print(f"Total Output Cost: ${total_output_cost}") 

                total_cost = total_input_cost + total_output_cost

                total_cost = '$' + str(round(total_cost, 6))

                print(f"Total Cost: {total_cost}")           

                output = extract_json_after_reasoning(answer['choices'][0]['message']['content'])
                print("Output: ", output)

                final = json.loads(output)
                print("Final Curated JSON:", final)
            else:
                final_output, qwen_input_cost, qwen_output_cost = final_json_curation_singleDoc(results)

                print("Final Output: ", final_output)

                select_query = f'''
                    SELECT prompt_template 
                    FROM {schema}.{prompt_metadata_table}
                    WHERE prompt_type = 'curation_prompt';
                    '''
                curation_prompt = select_db(select_query)[0][0]
                print("Curation Prompt: \n", curation_prompt)

                query = f"""SELECT document_json FROM {schema}.{document_type_table} WHERE name = '{doc_type}'"""
                result = select_db(query)

                template_json = json.loads(result[0][0])

                print("Template_JSON: ", template_json)

                final_orchestrator_prompt = curation_prompt + f"""
                <user_required_fields>
                {template_json}
                </user_required_fields>
                <output_json>
                {final_output}
                </output_json>"""

                response = bedrock_client.invoke_model(
                    modelId=orchestrator_model_id,
                    contentType="application/json",
                    accept="application/json",
                    body=json.dumps({
                        "messages": [
                            {"role": "system", "content": final_orchestrator_prompt},
                        ],
                        "max_tokens": 4000,
                        "temperature": 0.2
                    })
                )

                result = response["body"].read().decode("utf-8")

                print("Result:", result)

                answer = json.loads(result)

                oss_input_tokens = answer['usage']['prompt_tokens']
                oss_output_tokens = answer['usage']['completion_tokens']

                oss_input_cost = (oss_input_tokens / 1000) * 0.00015
                oss_output_cost = (oss_output_tokens / 1000) * 0.00060

                print(F"OSS Input Tokens: ${oss_input_cost}")
                print(F"OSS Output Tokens: ${oss_output_cost}")

                total_input_cost = qwen_input_cost + oss_input_cost
                total_output_cost = qwen_output_cost + oss_output_cost  

                print(f"Total Input Cost: ${total_input_cost}")          
                print(f"Total Output Cost: ${total_output_cost}") 

                total_cost = total_input_cost + total_output_cost

                total_cost = '$' + str(round(total_cost, 6))

                print(f"Total Cost: {total_cost}")           

                output = extract_json_after_reasoning(answer['choices'][0]['message']['content'])
                print("Output: ", output)

                final = json.loads(output)
                print("Final Curated Single Doc JSON:", final)
                
            s3_client.put_object(
                Bucket=bucket_name,
                Key=f"CEXP_OCR/{doc_type}/OUTPUT/{doc_id}.json",
                Body=json.dumps(final, indent=4, ensure_ascii=False),
                ContentType='application/json'
            )

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

            status = "Not Verified" if verified == "NOT_VERIFIED" else "Completed"
            update_query = f"""UPDATE {schema}.{document_processing_table} SET doc_status = %s, status_description = 'Key extraction successful', total_cost = %s WHERE doc_id = %s and delete_status = 0;"""  

            values = (status, total_cost, doc_id)

            update_db_values(update_query, values)

            return {
                "statusCode": 200,
                "status": "Key extraction successful"
            }
        except Exception as e:
            print("An exception occurred while key extraction : ",e)   
            update_query = f"""UPDATE {schema}.{document_processing_table} SET doc_status = 'Failed', status_description = 'Key extraction failed due to : {str(e)}' WHERE doc_id = '{doc_id}' and delete_status = 0;"""  
            values = (doc_id,)
            update_db_values(update_query, values)
            return {
                "statusCode": 500,
                "status": "Key extraction failed"
            }
