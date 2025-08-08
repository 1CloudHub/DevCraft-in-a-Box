import json 
import os
import psycopg2
import boto3  
import time
import secrets
import string
from datetime import *
from zoneinfo import ZoneInfo

gateway_url = os.environ['gateway_url']
db_user = os.environ['db_user']
db_password = os.environ['db_password']
db_host = os.environ['db_host']                         
db_port = os.environ['db_port']
db_database = os.environ['db_database']
region_used = os.environ["region_used"] 
schema = os.environ['schema']
app_meta_data = os.environ['app_meta_data']
config_table = os.environ['config_table']
user_table = os.environ['user_table']
ui_chat_session = os.environ['ui_chat_session']     

KB_ID = os.environ['KB_ID']

s3_client = boto3.client("s3",region_name = region_used)

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

def update_db(query):
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
    
def insert_db(query,values):
    connection = psycopg2.connect(
        user=db_user,
        password=db_password,
        host=db_host,
        port=db_port,  # Replace with the SSH tunnel local bind port
        database=db_database
    )    
                                                                            
    cursor = connection.cursor()
    cursor.execute(query,values)
    connection.commit()
    cursor.close()
    connection.close()

def split_s3_uri(s3_uri):
    if not s3_uri.startswith("s3://"):
        raise ValueError("The URI must start with 's3://'.")
    
    # Remove the "s3://" prefix
    stripped_uri = s3_uri[5:]
    
    # Split into bucket name and object key
    parts = stripped_uri.split("/", 1)
    bucket_name = parts[0]
    object_key = parts[1] if len(parts) > 1 else ""
    
    return bucket_name, object_key


def generate_presigned_url(s3_uri, expiration=3600):
    
    # Generate the presigned URL
    try:
        bucket_name,object_key = split_s3_uri(s3_uri)
        print("Bucket Name: ",bucket_name)
        print("Object Key: ",object_key)    
        file_name = object_key.split('/')[-1]
        presigned_url = s3_client.generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket_name, "Key": object_key},
            ExpiresIn=expiration
        )
        return presigned_url, file_name
    except Exception as e:
        raise RuntimeError(f"Error generating presigned URL: {e}")


def get_prompt(app_id, env,prompt_type):
    prompt_query = f'''select description from {schema}.{config_table} where app_id = '{app_id}' and environment = '{env}' and prompt_type = '{prompt_type}';'''
    # if env == "first_layer":
    #     prompt_query = f'''select description from {schema}.{config_table} where app_id = '{env}' and environment = '{env}';'''
    try:
        final_prompt = select_db(prompt_query)
        if final_prompt != []:
            final_prompt = final_prompt[0][0]
    except Exception as e:
        print("An exception occurred in prompt_query : ",e)
        final_prompt = ''
    return final_prompt
    
def generate_session_id(length=100):
    characters = string.ascii_letters + string.digits
    return ''.join(secrets.choice(characters) for _ in range(length))

def retrieve_chunks(KB_ID, question):
    retrieve_client = boto3.client("bedrock-agent-runtime", region_name=region_used)
    chunk_size = 20
    sourceArray = []
    view_source_list = []
    updated_source_list = []
    
    # for tag in metadata:
    try:
        print("Processing METADATA TAG:")
        accessFilter = {
            "equals": {
                "key": "Access",
                "value": "private"
            }
        }
        
        response_chunks = retrieve_client.retrieve(
            retrievalQuery={'text': question},
            knowledgeBaseId=KB_ID,
            retrievalConfiguration={
                'vectorSearchConfiguration': {
                    'filter': accessFilter,
                    'numberOfResults': chunk_size,
                    'overrideSearchType': 'HYBRID'
                }
            }
        )
        
        retrieval_results = response_chunks.get('retrievalResults', [])
        if not retrieval_results:
            print(f"No chunks retrieved for tag, moving to next tag.")
        
        print('response_chunks:', retrieval_results)
        for chunk in retrieval_results:
            temp = {
                "inlineDocumentSource": {
                    "textDocument": {"text": chunk["content"].get("text", "")},
                    "type": "TEXT"
                },
                "type": "INLINE"
            }
            sourceArray.append(temp)
            
            url, file_name, pg_num = '', '', ''
            if 'metadata' in chunk:
                metadata = chunk['metadata']
                if 'x-amz-bedrock-kb-source-uri' in metadata:
                    url, file_name = generate_presigned_url(metadata['x-amz-bedrock-kb-source-uri'])
                if 'x-amz-bedrock-kb-document-page-number' in metadata:
                    pg_num = str(metadata['x-amz-bedrock-kb-document-page-number'])
            
            view_source_list.append({'chunk': chunk['content'].get('text', ''), 'file_name': file_name, 'pg': pg_num, 'presigned_url': url})
            
    except retrieve_client.exceptions.ValidationException as e:
        print(f"ValidationException encountered for tag : {e}")
    except Exception as e:
        print(f"Unexpected error while processing tag : {e}")

    if not sourceArray:
        return {"reranked_chunks": [], "updated_source_list": [], "message": "No relevant chunks retrieved."}
    
    reranked_chunks = []
    updated_source_list = []
    client = boto3.client('bedrock-agent-runtime', region_name=region_used)
    number_of_reranked_chunks = min(len(sourceArray), 20)
    
    if number_of_reranked_chunks > 0:
        try:
            response = client.rerank(
                nextToken='string',
                queries=[{'textQuery': {'text': question}, 'type': 'TEXT'}],
                rerankingConfiguration={
                    'bedrockRerankingConfiguration': {
                        'modelConfiguration': {
                            'modelArn': "arn:aws:bedrock:us-west-2::foundation-model/amazon.rerank-v1:0",
                        },
                        'numberOfResults': number_of_reranked_chunks
                    },
                    'type': 'BEDROCK_RERANKING_MODEL'
                },
                sources=sourceArray
            )
            
            for index in response.get("results", []):
                reranked_chunks.append(sourceArray[index["index"]]["inlineDocumentSource"]["textDocument"]["text"])
                updated_source_list.append({
                    "chunk": view_source_list[index["index"]]["chunk"],
                    "file_name": view_source_list[index["index"]]["file_name"],
                    "pg": view_source_list[index["index"]]["pg"],
                    "presigned_url": view_source_list[index["index"]]["presigned_url"]
                })
            
            return {"reranked_chunks": reranked_chunks, "updated_source_list": updated_source_list, "message": "Chunks retrieved and reranked successfully."}
        
        except Exception as e:
            print(f"Error during reranking: {e}")
            return {"reranked_chunks": [], "updated_source_list": [], "message": f"Error during reranking: {str(e)}"}
    
    return {"reranked_chunks": [], "updated_source_list": [], "message": "No valid chunks available for reranking."}



def first_layer_action(question, session_id, app_id, env, model_id):
    chat_history = []
    
    try:
        select_query = f'''
                SELECT 
                    question, 
                    answer
                FROM 
                    {schema}.{ui_chat_session}
                WHERE 
                    session_id = '{session_id}'
                    and app_id = '{app_id}'
                    and environment = '{env}'
                ORDER BY 
                    last_updated_time DESC
                LIMIT 4;
        '''

        # GET CHAT SESSION HISTORY 
        history_response = select_db(select_query)
        print("CHAT HISTORY : ", history_response)
        
        for i in history_response:
            temp = {"user": i[0], "bot": i[1]}
            chat_history.append(temp)
        
        print("PREVIOUS CHAT HISTORY : ", chat_history[::-1])
        first_layer_prompt = get_prompt("first_layer","first_layer", "first_layer")
        
        # if env == "Production":
        #     system_role = get_prompt(app_id,env,"prod_persona")
        # else:
        #     system_role = get_prompt(app_id,env,"dev_persona")
        # print("SYSTEM ROLE : ",system_role)
        final_prompt_template = f'''
            {first_layer_prompt}
            Here is the chat history format and the user chat history to analyze before choosing the correct core function to perform:
            Chat History Format:
                The chat history is ordered by ascending time. The most recent chat is in the last index.
                Format of chat history:
                user : Question from the user. 
                bot : Answer from the Connect Experience chatbot
            
            User Chat History::\n{chat_history[::-1]}\n
            User's Latest Query: : {question}\n

            '''
        print(final_prompt_template)

        # INVOKE 1ST LLM LAYER
        invoke_model_client = boto3.client('bedrock-runtime', region_name=region_used)
        
        try:
            response = invoke_model_client.invoke_model(
                contentType='application/json', 
                body=json.dumps({
                    "anthropic_version": "bedrock-2023-05-31",
                    "max_tokens": 1000,    
                    "temperature": 0.3,
                    "top_p": 0.9,             
                    "top_k": 40,
                    "messages": [
                        {                                    
                            "role": "user",
                            "content": [
                                {"type": "text", "text": final_prompt_template},
                            ]
                        }
                    ],         
                }), modelId=model_id
            )
        except Exception as e:
            print(f"Error invoking model: {e}")
            return {"question": "", "ask_back": "", "message": f"Error invoking model: {str(e)}"}
        
        print("QUESTION FRAME LL RESPONSE : ", response)
        
        if 'body' in response:
            inference_result = response['body'].read().decode('utf-8')
            final = json.loads(inference_result)
            print("FINALL : ", final)
            
            first_layer_input_tokens = final['usage']['input_tokens']
            first_layer_output_tokens = final['usage']['output_tokens']
            print("1st LAYER INPUT TOKENS : ", first_layer_input_tokens)
            print("1st LAYER OUTPUT TOKENS : ", first_layer_output_tokens)
            
            reframed_response = json.loads(final['content'][0]['text'])   
            print("REFRAMED RESPONSE : ", reframed_response)
            
            reframed_question = reframed_response.get('question', '')
            ask_back = str(reframed_response.get('ask_back', ''))
            
            print("REFRAMED QUESTION : ", reframed_question)
            print(f"ASK BACK : {ask_back}. TYPE : {type(ask_back)}")

            return {
                "question": reframed_question,
                "ask_back": ask_back,
                "message": "Successfully processed question.",
                "first_layer_input_tokens" : first_layer_input_tokens,
                "first_layer_output_tokens" : first_layer_output_tokens
                
            }
        else:
            return {"question": "", "ask_back": "", "message": "No response body received from model." , "first_layer_input_tokens" : 0, "first_layer_output_tokens" : 0 }
        
    except KeyError as e:
        print(f"KeyError encountered: {e}")
        return {"question": "", "ask_back": "", "message": f"KeyError: {str(e)}","first_layer_input_tokens" : 0, "first_layer_output_tokens" : 0}
    except json.JSONDecodeError as e:
        print(f"JSON decoding error: {e}")
        return {"question": "", "ask_back": "", "message": f"JSON decoding error: {str(e)}","first_layer_input_tokens" : 0, "first_layer_output_tokens" : 0}
    except Exception as e:
        print(f"Unexpected error: {e}")
        return {"question": "", "ask_back": "", "message": f"Unexpected error: {str(e)}","first_layer_input_tokens" : 0, "first_layer_output_tokens" : 0}


def lambda_handler(event, context):
    print("EVENT : ",event)
    api_gateway_client = boto3.client('apigatewaymanagementapi', endpoint_url=gateway_url, region_name=region_used)
    connectionId = event["requestContext"]["connectionId"]
    e = json.loads(event["body"])
    
    print(e)
    
    app_id = e['app_id']
    environment = e['env']
    chat = e['chat']
    chat_type = e.get("chat_type", "")
    email_id = e.get("user_detail", "")
    answer = ""
    environment = "Development"
    # if environment == "Production":
    #     prompt = get_prompt(app_id, environment,"pg_prompt")
    # else:
        # prompt = get_prompt(app_id, environment,"pg_prompt")
    prompt = get_prompt(app_id,environment,"pg_prompt")
    print("KB PROMPT")
    print("INITIAL PROMPT : ",prompt)
    
    query = f''' 
    select prod_kb_id, dev_kb_id, active_model_id, conversational, streaming, unique_identifier, active_pg_model_id from {schema}.{app_meta_data} where app_id = '{app_id}';
    '''
    res = select_db(query)
    print(res)
    res = res[0]
    model_id = res[2]
    convertional = res[3]
    streaming = res[4]
    unique_identifier = res[5]
    active_pg_model_id = res[6]
    output_tokens = 0
    input_tokens = 0
    if convertional == 1 and streaming == 1:
        try:
            print("CONVERSATIONAL: 1, STREAMING: 1")
            session_id = e['session_id']
            if session_id == None or session_id == "null":
                session_id = generate_session_id()
            first_layer_response = first_layer_action(chat,session_id,app_id,environment,model_id)
            print("FIRST LAYER RESPONSE : ",first_layer_response)
            
            if any(word in first_layer_response["message"] for word in ["error", "Error", "KeyError"]):
                print(first_layer_response["message"])
                ans = "An Unkown error occurred. Please try again after some time."
                view_source = {'session_id': session_id, 'view_source': []}
                res = api_gateway_client.post_to_connection(ConnectionId = connectionId, Data = ans)
                res = api_gateway_client.post_to_connection(ConnectionId = connectionId, Data = "$$$"+json.dumps(view_source)) 
                return {'statusCode': 200}
            reframed_question = first_layer_response["question"]
            ask_back = first_layer_response["ask_back"]
            first_layer_input_tokens = first_layer_response["first_layer_input_tokens"]
            first_layer_output_tokens = first_layer_response["first_layer_output_tokens"]
            print("TYPE OF ASK BACK : ",type(ask_back))
            if ask_back == "True":
                print("ASK BACK TRUE")
                view_source = {'session_id': session_id, 'view_source': []}
                res = api_gateway_client.post_to_connection(ConnectionId = connectionId, Data = reframed_question)
                res = api_gateway_client.post_to_connection(ConnectionId = connectionId, Data = "$$$"+json.dumps(view_source))
                answer = reframed_question
            else:
                retriever_response = retrieve_chunks(KB_ID,reframed_question)
                chunks = retriever_response["reranked_chunks"]
                view_source_list = retriever_response["updated_source_list"]
                if "Error during reranking" in retriever_response["message"]:
                    print(retriever_response["message"])
                    view_source = {'session_id': session_id, 'view_source': []}
                    ans = "An Unkown error occurred. Please try again after some time."
                    res = api_gateway_client.post_to_connection(ConnectionId = connectionId, Data = ans)
                    res = api_gateway_client.post_to_connection(ConnectionId = connectionId, Data = "$$$"+json.dumps(view_source)) 
                    return {'statusCode': 200}
                if prompt == '' or prompt == [] :
                    final_prompt_template = f'''
                        <relevant_information>
                        {chunks}
                        </relevant_information>
                        And here is the user's query:
                        <user_query>
                        {reframed_question}
                        </user_query>
                        First, carefully review the relevant information provided and do a step-by-step thought process for how you will answer the user's query using only the relevant information. Do not output the thought process block to the user.
                        Now, generate a well-formatted answer to the user's query. Your answer should be based strictly on the relevant information provided above. Do not mention in your answer that it is based on the provided information. Output your answer.
                        If, after reviewing the relevant information, you determine that there is not enough information provided to satisfactorily answer the user's query, instead output:
                        "Sorry I dont have enough information to answer that question"
                        Remember, the user is expecting a helpful answer based only on the information provided, so do not attempt to answer if you do not have enough relevant information to do so.
                        <output format instruction>
                        1. Strictly always check if the answer framed is only using information from the relevant information provided to you. If not please provide "Sorry I dont have enough information to answer that question"
                        </output format instructions>
                    '''
                else:
                    final_prompt_template = f'''
                    {prompt}
                    Use the following data to answer queries:
                    <data_to_answer_queries> 
                    {chunks}              
                    </data_to_answer_queries>
                
                    Respond to the following query:
                    <query> 
                    {reframed_question}  
                    </query>  
                    ''' 
    
                native_request = {
                    "anthropic_version": "bedrock-2023-05-31",
                    "max_tokens": 1000,
                    "temperature": 0.5,
                    "messages": [
                        {
                            "role": "user",
                            "content": [{"type": "text", "text": final_prompt_template}],
                        }
                    ],
                }
                
                # Convert the native request to JSON.
                request = json.dumps(native_request)
                
                client = boto3.client("bedrock-runtime", region_name = region_used)
                
                try:
                    # Invoke the model with the request.
                    streaming_response = client.invoke_model_with_response_stream(
                        modelId=active_pg_model_id, body=request  
                    )
                    # print("STREAMING RESPONSE : ",streaming_response)  
        
                    full_answer = ''
                    for sres in streaming_response["body"]:
                        chunk = json.loads(sres["chunk"]["bytes"])
                        if chunk["type"] == "content_block_delta":
                            ans = chunk['delta']['text']  
                            res = api_gateway_client.post_to_connection(ConnectionId = connectionId, Data = ans)
                            full_answer += ans
                        if 'amazon-bedrock-invocationMetrics' in chunk:
                            print("INPUT TOKENS : ",chunk['amazon-bedrock-invocationMetrics']['inputTokenCount'])
                            print("OUTPUT TOKENS : ",chunk['amazon-bedrock-invocationMetrics']['outputTokenCount']) 
                            input_tokens = chunk['amazon-bedrock-invocationMetrics']['inputTokenCount']
                            output_tokens = chunk['amazon-bedrock-invocationMetrics']['outputTokenCount']
                            
                    print("FULL ANSWER:",full_answer)  
                    
                    if chat_type != 'plugin':
                        view_source = {'session_id':session_id,'view_source':view_source_list}
                    else:
                        view_source = {'session_id':session_id,'view_source':[]}
                except Exception as e:
                    final = {'error': "An unexpected error occurred"}
                    view_source = {'session_id': session_id, 'view_source': []}
                    ans = "An Unknown error occurred. Sorry for the inconvenience caused. Please try again after sometime"
                    res = api_gateway_client.post_to_connection(ConnectionId = connectionId, Data = answer)
                    res = api_gateway_client.post_to_connection(ConnectionId = connectionId, Data = "$$$"+json.dumps(view_source)) 
                res = api_gateway_client.post_to_connection(ConnectionId = connectionId, Data = "$$$"+json.dumps(view_source))
                answer = full_answer
        except Exception as e:
            final = {'error': "An unexpected error occurred"}
            view_source = {'session_id': session_id, 'view_source': []}
            ans = "An Unknown error occurred. Sorry for the inconvenience caused. Please try again after sometime"
            res = api_gateway_client.post_to_connection(ConnectionId = connectionId, Data = answer)
            res = api_gateway_client.post_to_connection(ConnectionId = connectionId, Data = "$$$"+json.dumps(view_source)) 

    
    print("QUESTION : ",chat)
    print("ANSWER : ",answer)
    print("EMAIL : ",email_id)
    print("ENVIRONMENT : ",environment)
    print("APP ID : ",app_id)
    print("SESSION ID : ",session_id)
    # CHAT LOGGING
    query = f'''
    insert into {schema}.{ui_chat_session} (question, answer, user_details, environment, app_id, handoff, session_id, output_token, input_token,first_layer_input_tokens,first_layer_output_tokens, first_layer_model, kb_model, last_updated_time) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s,CURRENT_TIMESTAMP);
    '''
    values = (chat, answer, email_id, environment, app_id, 1, session_id,output_tokens, input_tokens,first_layer_input_tokens, first_layer_output_tokens,model_id,active_pg_model_id)
    res = insert_db(query, values)
    
    return {'statusCode': 200}
