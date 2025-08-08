import boto3
import json
import psycopg2
import uuid
import os 

socket_endpoint = os.environ["gateway_url"]
db_user = os.environ['db_user']
db_password = os.environ['db_password']
db_host = os.environ['db_host']                         
db_port = os.environ['db_port']
db_database = os.environ['db_database']

region_used = os.environ["region_used"]
rerank_region = "us-west-2"

schema = os.environ['schema']
chat_history_table = os.environ['chat_history_table']               # yet to create
transcript_history_table = os.environ['transcript_history_table']   # yet to create
prompt_metadata_table = os.environ['config_table']

app_meta_data = os.environ['app_meta_data']
KB_ID = os.environ['KB_ID']

INIT_CHUNK_SIZE = 20
FINAL_CHUNK_SIZE = 10

retrieve_client = boto3.client('bedrock-agent-runtime', region_name=region_used)   
bedrock_client = boto3.client('bedrock-runtime', region_name=region_used)
rerank_client = boto3.client('bedrock-agent-runtime', region_name=rerank_region)
api_gateway_client = boto3.client('apigatewaymanagementapi', endpoint_url=socket_endpoint)

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

def update_db_ans(query,values):
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
    except Exception as e:
        print("An error occurred : ",e)
        if connection:
            connection.rollback()
        return []
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()
    
def insert_db(query,values):
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

def last_inserted_id(query,values):   
        connection = psycopg2.connect(
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
            database=db_database
        )                                                                        
        cursor = connection.cursor()
        cursor.execute(query,values)
        last_inserted_id = cursor.fetchone()[0]
        print(" IN DB FUNCTION Last inserted ID:", last_inserted_id)   
        connection.commit()
        cursor.close()
        connection.close()
        return last_inserted_id 

def get_prompt(app_id, env, prompt_type):
    prompt_query = f'''select description from {schema}.{prompt_metadata_table} where app_id = '{app_id}' and environment = '{env}' and prompt_type = '{prompt_type}';'''
    try:
        final_prompt = select_db(prompt_query)
        if final_prompt != []:
            final_prompt = final_prompt[0][0]
    except Exception as e:
        print("An exception occurred in prompt_query : ",e)
        final_prompt = ''
    return final_prompt

def get_information_chunks(chat, org_question):
    print("in retrieval function")
    chunks = []
    try:

        response_chunks = retrieve_client.retrieve(
                    retrievalQuery={                                                                                
                        'text': chat
                    },
                    knowledgeBaseId = KB_ID,
                    retrievalConfiguration={
                        'vectorSearchConfiguration': {                         
                            'numberOfResults': INIT_CHUNK_SIZE,
                            'overrideSearchType': 'HYBRID'
                }
            }
        )
        
        for chunk_item in response_chunks['retrievalResults']:  # Iterate over the list
            if 'content' in chunk_item and 'text' in chunk_item['content'] :  # Check if keys exist
                chunks.append(chunk_item)

        #rekrank chunks
        rerank_response = rerank_client.rerank(
                    queries=[
                        {
                            'textQuery': {
                                'text': org_question
                            },
                            'type': 'TEXT'
                        },
                    ],
                    rerankingConfiguration={
                        'bedrockRerankingConfiguration': {
                                        'modelConfiguration': {
                                'modelArn': f"arn:aws:bedrock:{rerank_region}::foundation-model/amazon.rerank-v1:0",
                            },
                            'numberOfResults': FINAL_CHUNK_SIZE
                        },
                        'type': 'BEDROCK_RERANKING_MODEL'
                    },
                    sources=[
                        {
                            'inlineDocumentSource': {
                                'jsonDocument': chunk,
                                'type': 'JSON'
                            },
                            'type': 'INLINE'
                        }
                    for chunk in chunks]
                )

        reranked_chunks = []
        for chunk in rerank_response['results']:
            reranked_chunks += [chunks[chunk['index']]]

        return reranked_chunks

    except Exception as e:
        print("An error occurred : ",e)

def get_information(input, session_id, connectionId, final_ai_response, org_question, model_id):
    print("IN GET INFORMATION tool")
    try:
        processed_question = input.get('query', "")
        iti_input_tokens = 0
        iti_output_tokens = 0

        retrieval_prompt = get_prompt("qFDnG5nR", "Development", "pg_prompt")
        
        if processed_question != "":
            chunk_response = get_information_chunks(processed_question, org_question)
            prompt = f"""
                        {retrieval_prompt}

                        Original Question from the user : {org_question}
                        Processed Question : {processed_question}

                        Chunks to be used to generate answer: {chunk_response}
                        """

            response = bedrock_client.invoke_model_with_response_stream(contentType='application/json', body=json.dumps({
                    "anthropic_version": "bedrock-2023-05-31",
                    "max_tokens": 3000,
                    "temperature": 0,
                    "messages": [
                        {
                            "role": "user",
                            "content": [
                                {
                                    "type": "text",
                                    "text": prompt
                                }
                            ]
                        }
                    ]
                    }), 
                    modelId=model_id)
            
            streamed_content = ""   
            content_block = None
            assistant_response = []

            # initialize websocket connection
            # res = api_gateway_client.post_to_connection(ConnectionId = connectionId,Data = "@@@")
            for item in response['body']:
                content = json.loads(item['chunk']['bytes'].decode())

                print(content)  # For debugging

                if content['type'] == 'content_block_start':
                    content_block = content['content_block']

                elif content['type'] == 'content_block_stop':
                    if content_block['type'] == 'text':
                        content_block['text'] = streamed_content
                        final_ai_answer = content_block['text']
                        assistant_response.append(content_block)
                    elif content_block['type'] == 'tool_use':
                        content_block['input'] = json.loads(streamed_content)
                        assistant_response.append(content_block)

                    streamed_content = ""

                elif content['type'] == 'content_block_delta':
                    if content['delta']['type'] == 'text_delta':
                        streamed_content += content['delta']['text']
                        #stream to frontend
                        res = api_gateway_client.post_to_connection(ConnectionId = connectionId,Data = content['delta']['text']) 
                        # res = api_gateway_client.post_to_connection(ConnectionId = connectionId,Data = json.dumps({"session_id":session_id,"end_token":"$$$"}))
                    elif content['delta']['type'] == 'input_json_delta':
                        streamed_content += content['delta']['partial_json']
                elif content['type'] == 'message_stop':
                    iti_input_tokens += content['amazon-bedrock-invocationMetrics']['inputTokenCount']
                    iti_output_tokens += content['amazon-bedrock-invocationMetrics']['outputTokenCount']
            return {
                "streamed_to_user": True,
                "tool_response": final_ai_answer,      
                "input_tokens":iti_input_tokens,    
                "output_tokens":iti_output_tokens
            }     
        else:
            return {
                "streamed_to_user": False,
                "tool_response": json.dumps({"information_tool_response":"knowledge_base_retrieval_question is missing."}),      
                "input_tokens":0,    
                "output_tokens":0
            } 

    except Exception as e:
        print("Information retrieval error : ",e)
        raise

def agent_invoke_tool(message_history, session_id, connectionId, last_inserted_id_value, org_question, model_id):
    print("in agent invoke tool function")
    try:
        input_tokens = 0
        output_tokens = 0
        print("In agent_invoke")
        
        base_prompt = get_prompt("first_layer", "first_layer", "first_layer")
        
        information_retrieval_schema = {
            "name": "information_retrieval",
            "description": "Use the tool to answer general user FAQs",
            "input_schema" : {
                "type" : "object",
                "properties" : {
                            "query": {
                                "type": "string",
                                "description": "The query asked by the user to interact with the agent."
                            }
                        },
                "required": ["query"]
            }
        }

        model_loop_limit = 0
        tool_functions = {"information_retrieval":get_information}
        tool_schemas   = [information_retrieval_schema]
        while model_loop_limit < 3:
            model_loop_limit += 1
            individual_input_tokens = 0
            individual_output_tokens = 0
            response = bedrock_client.invoke_model_with_response_stream(
                contentType='application/json',
                body=json.dumps({
                    "anthropic_version": "bedrock-2023-05-31",
                    "max_tokens": 3000,
                    "temperature": 0,
                    "system": base_prompt,
                    "tools": tool_schemas,
                    "messages": message_history
                }),
                modelId=model_id
            )

            streamed_content = ''
            assistant_response = []
            content_block = None
            final_ai_response = ''
            # if model_loop_limit:
            #     res = api_gateway_client.post_to_connection(ConnectionId = connectionId,Data = "@@@")   
            for item in response['body']:
                content = json.loads(item['chunk']['bytes'].decode())
                print(content)

                if content['type'] == 'content_block_start':
                    content_block = content['content_block']

                elif content['type'] == 'content_block_delta':
                    if content['delta']['type'] == 'text_delta':
                        streamed_content += content['delta']['text']
                        #stream response
                        res = api_gateway_client.post_to_connection(ConnectionId = connectionId,Data = content['delta']['text']) 
                    elif content['delta']['type'] == 'input_json_delta':
                        streamed_content += content['delta']['partial_json']

                elif content['type'] == 'content_block_stop':
                    if content_block['type'] == 'text':
                        content_block['text'] = streamed_content
                        assistant_response.append(content_block)
                        final_ai_response = streamed_content
                    elif content_block['type'] == 'tool_use':
                        content_block['input'] = json.loads(streamed_content)
                        assistant_response.append(content_block)
                    streamed_content = ''

                elif content['type'] == 'message_stop':
                    input_tokens += content['amazon-bedrock-invocationMetrics']['inputTokenCount']
                    output_tokens += content['amazon-bedrock-invocationMetrics']['outputTokenCount']
                    individual_input_tokens = content['amazon-bedrock-invocationMetrics']['inputTokenCount']
                    individual_output_tokens = content['amazon-bedrock-invocationMetrics']['outputTokenCount']

            message_history.append({'role': 'assistant', 'content': assistant_response})
            if not assistant_response:
                print("empty assistant_response: ",assistant_response)
            print(f"ASSISTANT RESPONSE {model_loop_limit}: ", assistant_response)
            update_query = f"update {schema}.{chat_history_table} set answer = %s, input_tokens = %s, output_tokens = %s where id = %s; "
            values = (json.dumps(assistant_response),str(individual_input_tokens),str(individual_output_tokens),str(last_inserted_id_value))          
            update_db_ans(update_query,values)
            individual_input_tokens = 0
            individual_output_tokens = 0
            tool_calls = [a for a in assistant_response if a['type'] == 'tool_use']

            print("Tool calls: ", tool_calls)

            if not tool_calls:
                #api gateway client with $$$
                view_source = {'session_id': session_id, 'view_source': []}
                res = api_gateway_client.post_to_connection(ConnectionId = connectionId,Data = "$$$"+json.dumps(view_source))
                break

            tool_result_blocks = []
            streamed_assistant_message = None   

            for action in tool_calls:
                #close websocket with &&&
                # res = api_gateway_client.post_to_connection(ConnectionId = connectionId,Data = "&&&")     
                tool_name = action['name']
                tool_input = action['input']
                tool_use_id = action.get('tool_use_id') or action.get('id')

                fn = tool_functions.get(tool_name)
                print("function: ", fn)
                if fn:
                    try:
                        dict_response = fn(tool_input, session_id, connectionId, final_ai_response, org_question, model_id)    
                        individual_input_tokens += dict_response['input_tokens']
                        individual_output_tokens += dict_response['output_tokens']  
                    except Exception as e:
                        print("An exception occurred: ",e)
                        dict_response = {"tool_response": json.dumps({"error": str(e)}), "streamed_to_user": False}
                        #stream error to frontend
                        res = api_gateway_client.post_to_connection(ConnectionId = connectionId,Data = "Something went wrong while processing your request. Please try again shortly.")
                        #update db with error
                        update_query = f"update {schema}.{chat_history_table} set answer = %s, input_tokens = %s, output_tokens = %s where id = %s; "
                        values = (f"Input : {tool_input}\nError at {tool_name} : {str(e)}","0","0",str(last_inserted_id_value))          
                        update_db_ans(update_query,values)     
                        return {"input_tokens":0,"output_tokens":0,"session_id":session_id, "final_ai_response":f"Error during individual tool call : {str(e)}"}
                else:
                    print("UNKNOWN TOOL")  
                    dict_response = {"tool_response": json.dumps({"error": f"Unknown tool {tool_name}"}), "streamed_to_user": False}

                response_text = dict_response["tool_response"]
                streamed_flag = dict_response.get("streamed_to_user", False)

                tool_result_blocks.append({
                    "type": "tool_result",
                    "tool_use_id": tool_use_id,
                    "content": [{"type": "text", "text": response_text}]
                })

                if streamed_flag:
                    final_ai_response = response_text
                    streamed_assistant_message = {
                        "role": "assistant",
                        "content": [{"type": "text", "text":response_text}]
                    }
                    print("ASSISTANT RESPONSE: ",streamed_assistant_message)

            # Append tool result first as user
            message_history.append({"role": "user", "content": tool_result_blocks})
            insert_query = f'''
                    INSERT INTO {schema}.{chat_history_table}
                    (session_id, question, answer, input_tokens, output_tokens, created_on, updated_on)
                    VALUES( %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) RETURNING id;   
                    '''
            values = (str(session_id),json.dumps(tool_result_blocks), str(''), str('0'), str('0'))
            last_inserted_id_value = last_inserted_id(insert_query, values)
            print("last_inserted_id: ",last_inserted_id_value)

            # Then append assistant if response was streamed to user
            if streamed_assistant_message:
                message_history.append(streamed_assistant_message)
                if not streamed_assistant_message["content"]:
                    print("empty streamed_assistant_message[content]:",streamed_assistant_message["content"])    
                update_query = f"update {schema}.{chat_history_table} set answer = %s, input_tokens = %s, output_tokens = %s where id = %s; "
                values = (json.dumps(streamed_assistant_message["content"]),str(individual_input_tokens),str(individual_output_tokens),str(last_inserted_id_value))          
                update_db_ans(update_query,values)

            if streamed_flag:
                print("[INFO] Tool streamed response directly to user, stopping further LLM calls.")
                #api gateway client with $$$
                view_source = {'session_id': session_id, 'view_source': []}
                res = api_gateway_client.post_to_connection(ConnectionId = connectionId,Data = "$$$"+json.dumps(view_source))
                input_tokens += individual_input_tokens
                output_tokens += individual_output_tokens  
                break
        return {"input_tokens":input_tokens,"output_tokens":output_tokens,"session_id":session_id,"final_ai_response":final_ai_response}
    except Exception as e:
        print("An error occurred: ",e)
        #stream error to frontend
        res = api_gateway_client.post_to_connection(ConnectionId = connectionId,Data = "")
        # res = api_gateway_client.post_to_connection(ConnectionId = connectionId,Data = "@@@")   
        res = api_gateway_client.post_to_connection(ConnectionId = connectionId,Data = "Something went wrong please try again later.")   
        view_source = {'session_id': session_id, 'view_source': []}
        res = api_gateway_client.post_to_connection(ConnectionId = connectionId,Data = "$$$"+json.dumps(view_source))
        return {"input_tokens":0,"output_tokens":0,"session_id":session_id, "final_ai_response":"Something went wrong please try again later."}  
    

def lambda_handler(event, context):
    print("Event: ", event)
    e = json.loads(event["body"])
    
    print("chat request")
    app_id = e['app_id']
    environment = "Development"
    chat_type = e.get("chat_type", "")
    email_id = e.get("user_detail", "")
    
    user_question = e['chat']
    message_history = []
    connectionId = event["requestContext"]["connectionId"]
    session_id = e['session_id']
    
    model_query = f''' 
    select active_pg_model_id from {schema}.{app_meta_data} where app_id = '{app_id}';
    '''
    res = select_db(model_query)
    print(res)
    model_id = res[0][0]

    if  session_id == 'null' :
        session_id = str(uuid.uuid4())
        print("session_id: ",session_id)
    else:
        query = f'''select question,answer
                from {schema}.{chat_history_table}
                where session_id = '{session_id}'
                order by created_on desc limit {4 + 1};'''
        history_response = select_db(query)
        # print("history_response: ", history_response)

        # If the earliest message (last in list) has tool_result, prepend the assistant reply
        if history_response:
            earliest = json.loads(history_response[-1][0])
            if any(i["type"] == "tool_result" for i in earliest):
                print("FLAG TO APPEND IS TRUEEEEE")
                if history_response[-1][1]:
                    message_history.append({'role': 'assistant', 'content': json.loads(history_response[-1][1])})
                history_response = history_response[:-1]  # Remove the extra from normal loop

        # Add remaining messages in reverse (oldest to newest)
        for chat_session in reversed(history_response):
            message_history.append({'role': 'user', 'content': json.loads(chat_session[0])})
            if chat_session[1]:
                message_history.append({'role': 'assistant', 'content': json.loads(chat_session[1])})

    # Append current user question
    message_history.append({'role': 'user', 'content': [{"type": "text", 'text': user_question}]})
    print("FINAL CHAT HISTORY : ",message_history)
    insert_query = f'''
            INSERT INTO {schema}.{chat_history_table}
            (session_id, question, answer, input_tokens, output_tokens, created_on, updated_on)
            VALUES( %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) RETURNING id;   
            '''
    values = (str(session_id),json.dumps([{"type" : "text",'text': user_question}]), str(''), str('0'), str('0'))
    last_inserted_id_value = last_inserted_id(insert_query, values)    
    tool_response = agent_invoke_tool(message_history, session_id, connectionId, last_inserted_id_value, user_question, model_id)  
    print("FINAL OUTPUT RESPONSE: ", tool_response)     
    # insert into transcript_history_table
    query = f'''
            INSERT INTO {schema}.{transcript_history_table}    
            (session_id, question, answer, input_tokens, output_tokens, created_on, updated_on)
            VALUES( %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
            '''
    values = (str(session_id),str(user_question), str(tool_response['final_ai_response']), str(tool_response['input_tokens']), str(tool_response['output_tokens']))   
    res = insert_db(query, values) 
    print(res)
    return {"statusCode": 200}