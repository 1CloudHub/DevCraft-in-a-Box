import psycopg2
import os                                               
import boto3
import random,string        
import json
from datetime import *              
from calendar import monthrange    
import secrets
import string
import re

db_host = os.environ["db_host"]   
db_port = os.environ["db_port"]
db_database = os.environ["db_database"]
db_user = os.environ["db_user"]             
db_password = os.environ["db_password"]
region_used = os.environ["region_used"]
# cognito_idp = boto3.client('cognito-idp',region_name = region_used)
token_optix_schema = os.environ['schema']
USER_MANAGEMENT_TABLE = os.environ["USER_MANAGEMENT_TABLE"]
# BOT_USER_MANAGEMENT_TABLE = os.environ["BOT_USER_MANAGEMENT_TABLE"]  
# app_meta_data_table = os.environ["app_meta_data_table"]
# model_metadata_table = os.environ["model_metadata_table"]
# app_integration_details_table = os.environ["app_integration_details_table"]
# subscription_metadata_table = os.environ["subscription_metadata_table"] 
# token_details_table = os.environ["token_details_table"]
# integration_details_table = os.environ["integration_details_table"] 
# UserPoolId = os.environ["UserPoolId"]  
# cexp_bot_logs = os.environ['cexp_bot_logs']
# deployment_lambda = os.environ['deployment_lambda']
# CHAT_SESSION_TABLE = os.environ['CHAT_SESSION_TABLE']
# CONFIG_TABLE = os.environ['CONFIG_TABLE']
# FILE_METADATA_TABLE  = os.environ["FILE_METADATA_TABLE"]        
# FILE_VERSIONS_TABLE = os.environ["FILE_VERSIONS_TABLE"]
# FILE_METADATA_TABLE = os.environ["FILE_METADATA_TABLE"]

    
def generate_random_string(length):
    characters = string.ascii_letters
    random_string = ''.join(random.choice(characters) for _ in range(length)) +str(random.randint(0, 10000000000000))
    return random_string

def add_months(source_date, months):
    month = source_date.month - 1 + months
    year = int(source_date.year + month / 12)
    month = month % 12 + 1
    day = min(source_date.day, [31,
        29 if year % 4 == 0 and not year % 100 == 0 or year % 400 == 0 else 28,
        31, 30, 31, 30, 31, 31, 30, 31, 30, 31][month-1])
    return source_date.replace(year=year, month=month, day=day)

                        
def calculate_cost(input_tokens, output_tokens,input_cost,output_cost):
    prompt_cost = input_tokens * input_cost
    completion_cost = output_tokens * output_cost                   
    total_cost = prompt_cost + completion_cost              
    return total_cost    
    

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

def select_db(query):
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
    
def insert_ret_db(query,values):
    connection = psycopg2.connect(
        user=db_user,
        password=db_password,
        host=db_host,
        port=db_port,  # Replace with the SSH tunnel local bind port
        database=db_database
    )                                                                         
    cursor = connection.cursor()
    
    cursor.execute(query,values)
    last_inserted_id = cursor.fetchone()[0]
    #print("Last inserted ID:", last_inserted_id)
    connection.commit()
    cursor.close()
    connection.close()
    return last_inserted_id
    

def update_user_name(email_id, new_user_name):
    print("in update username function ",new_user_name)
    query = f'''update {token_optix_schema}.{USER_MANAGEMENT_TABLE} set user_name  = '{new_user_name}' where email_id  = '{email_id}' and delete_status = 0;'''
    result = update_db(query)
    return result      
    
def generate_session_id(length=100):
    characters = string.ascii_letters + string.digits
    return ''.join(secrets.choice(characters) for _ in range(length))
    
    
def extract_sections(llm_response):
    # Define the regular expression pattern for each section
    patterns = {
    "Conversation Type": r'"Conversation Type":\s*"([^"]+)"',
    "Conversation Summary Explanation": r'"Conversation Summary Explanation":\s*"([^"]+)"',
    "Detailed Summary": r'"Detailed Summary":\s*"([^"]+)"',
    "Conversation Sentiment": r'"Conversation Sentiment":\s*"([^"]+)"',
    "Conversation Sentiment Generated Details" :r'"Conversation Sentiment Generated Details":\s*"([^"]+)"',
    "Lead Sentiment": r'"Lead Sentiment":\s*"([^"]+)"',
    "Leads Generated Details": r'"Leads Generated Details":\s*"([^"]+)"',
    "Action to be Taken": r'"Action to be Taken":\s*"([^"]+)"',
    "Email Creation": r'"Email Creation":\s*"([^"]+)"'
    }

    extracted_data = {}
    for key, pattern in patterns.items():
        match = re.search(pattern, llm_response, re.DOTALL)
        if match:
            extracted_data[key] = match.group(1)

    if len(extracted_data) > 0:
        return extracted_data
    else:
        return None

                    
def lambda_handler(event,context):
    #result = []   
    print(event)  
    if 'requestContext' in event:
        # socket disconnect
        
        if event['requestContext']['eventType'] == "DISCONNECT":
            print("SOCKET DISCONNECT")
            connectionId = event["requestContext"]["connectionId"]
            print("SUMMARY GENERATION ")
            
            session_query = f"select session_id, app_id, environment from {token_optix_schema}.{cexp_bot_logs} where connectionid ='{connectionId}';"
            session_response = select_db(session_query)
            print("SESSION ID : ",session_response)
            session_id = session_response[0][0]
            app_id = session_response[0][1]
            environment = session_response[0][2]

            client = boto3.client('bedrock-runtime', region_name = region_used)

            chat_query = f'''
            SELECT question,answer
            FROM {token_optix_schema}.{CHAT_SESSION_TABLE} 
            WHERE session_id = '{session_id}';
            '''   
            model_id = "anthropic.claude-3-5-sonnet-20240620-v1:0"
        
            chat_details = select_db(chat_query)
            print("CHAT DETAILS : ",chat_details)
            history = ""
        
            for chat in chat_details[::-1]:
                history1 = "Human: "+chat[0]
                history2 = "Bot: "+chat[1]
                history += "\n"+history1+"\n"+history2+"\n"
            print("HISTORY : ",history)
            prompt_query = f"SELECT description from {token_optix_schema}.{CONFIG_TABLE} where prompt_type = 'summary';"
            prompt_response = select_db(prompt_query)
            prompt_template = prompt_response[0][0]
            print("PROMPT : ",prompt_template)
            template = f'''
            <Conversation>
            {history}
            </Conversation>
            {prompt_template}
            '''
        
            # - Ensure the email content is formatted correctly with new lines. USE ONLY "\n" for new lines. 
            #         - Ensure the email content is formatted correctly for new lines instead of using new line characters.
            response = client.invoke_model(contentType='application/json', body=json.dumps({
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": 1000,
                "messages": [
                    {
                        "role": "user",
                        "content": [
                            {"type": "text", "text": template},
                        ]
                    }
                ],
            }), modelId=model_id)                                                                                                                       
        
            inference_result = response['body'].read().decode('utf-8')
            final = json.loads(inference_result)
            out=final['content'][0]['text']
            print(out)
            llm_out = extract_sections(out)
        
            conversation_type = ""
            conversation_summary_explanation = ""
            detailed_summary = ""
            conversation_sentiment = ""
            conversation_sentiment_generated_details = ""
            lead_sentiment = ""
            leads_generated_details = ""
            action_to_be_taken = ""
            email_creation = ""
            
            try:
                if 'Conversation Type' in llm_out:
                    conversation_type = llm_out['Conversation Type']
                    if conversation_type == "N/A":
                        enquiry, complaint = (0, 0)
                    else:
                        enquiry, complaint = (1, 0) if conversation_type == "Enquiry" else (0, 1)
            except:
                enquiry , complaint = 0,0
                
            try:
                if 'Conversation Summary Explanation' in llm_out:
                    conversation_summary_explanation = llm_out['Conversation Summary Explanation']
            except:
                conversation_summary_explanation= ""
            
            try:
                if 'Detailed Summary' in llm_out:
                    detailed_summary = llm_out['Detailed Summary']
            except:
                detailed_summary = ""
            
            try:
                if 'Conversation Sentiment' in llm_out:
                    conversation_sentiment = llm_out['Conversation Sentiment']
            except:
                conversation_sentiment = ""
            
            try:
                if 'Conversation Sentiment Generated Details' in llm_out:
                    conversation_sentiment_generated_details = llm_out['Conversation Sentiment Generated Details']
            except:
                conversation_generated_details = ""
                
            try:
                if 'Lead Sentiment' in llm_out:
                    lead_sentiment = llm_out['Lead Sentiment']
                    lead = 1 if lead_sentiment == "Hot" else 0
            except:
                lead = 0
            
            try:
                if 'Leads Generated Details' in llm_out:
                    leads_generated_details = llm_out['Leads Generated Details']
            except:
                leads_generated_details = ""
            
            try:
                if 'Action to be Taken' in llm_out:
                    action_to_be_taken = llm_out['Action to be Taken']
            except:
                action_to_be_taken = ""
            
            try:
                if 'Email Creation' in llm_out:
                    email_creation = llm_out['Email Creation']
            except:
                email_creation = ""
            detailed_summary = detailed_summary.replace("'", "''")
            email_creation = email_creation.replace("'", "''")
            action_to_be_taken = action_to_be_taken.replace("'", "''")
            leads_generated_details = leads_generated_details.replace("'", "''")
            conversation_sentiment_generated_details = conversation_sentiment_generated_details.replace("'", "''")        
            
            print("LEAD : ",lead)
            print("ENQUIRY : ",enquiry)
            print("COMPLAINT : ",complaint)
            print("conversation_type:", conversation_type)
            print("Sentiment Explanation:", conversation_summary_explanation)
            print("Detailed summary:", detailed_summary)
            print("CONVERSATION SENTIMENT :",conversation_sentiment)
            print("CONVERSATION SENTIMENT DETAILS:",conversation_sentiment_generated_details)
            print("lead Sentiment:", lead_sentiment)
            print("lead explanation:", leads_generated_details)
            print("next_best_action:",action_to_be_taken)
            print("email_content:",email_creation)
            session_time = datetime.now()
            update_query = f'''UPDATE {token_optix_schema}.{cexp_bot_logs}
            SET 
                lead = {lead},
                lead_explanation = '{leads_generated_details}',
                sentiment = '{conversation_sentiment}',
                sentiment_explanation = '{conversation_sentiment_generated_details}',
                session_time = '{session_time}',
                enquiry = {enquiry},
                complaint = {complaint},
                summary = '{detailed_summary}',
                email_content = '{email_creation}',
                next_best_action = '{action_to_be_taken}'
            WHERE 
                session_id = '{session_id}' AND app_id = '{app_id}' AND environment = '{environment}'
                '''
            update_db(update_query)
            
            print("Summary successfully generated")
            
            return {
                    "statusCode" : 200,
                    "message" : "Summary Successfully Generated"
                }
                            
    event_type = event['event_type']

    # if event_type == "update_user":
    #     email_id = event['email_id']
    #     new_user_name = event['user_name']
    #     print("event ",new_user_name)
    #     response = update_user_name(email_id, new_user_name)
        
    #     if response:
    #         function_response = {
    #             "data": "updated success"
    #         }
    #         return {                                        
    #                 'statusCode': 200,
    #                 'body': function_response
    #             }
    #     else:
    #         function_response = {
    #                 "status" : "update failed"
    #             }
    #         return {
    #                 'statusCode': 500,
    #                 'body': function_response
    #             }
    
    if event_type == 'deploy_app':
        app_id = event['app_id']
        email_id = event['email_id']
        lambda_client = boto3.client("lambda", region_name=region_used)
        lambda_response = lambda_client.invoke(
            FunctionName=deployment_lambda, 
            Payload=json.dumps({ "app_id" : app_id ,"event_type":"create_app","email_id":email_id}),
            InvocationType = 'Event'
        )
        current_time = datetime.now(timezone.utc).isoformat()
        log_insert_query = f"insert into {schema}.{action_logs_table} (email_id,app_id,env,action,last_updated_time) values (%s, %s, %s, %s, %s)"
        values = (email_id, app_id, "token_optix","APP DEPLOYED",current_time)
        log_insert_response = insert_db(log_insert_query,values)
        print("LOG INSERTED : ",log_insert_response)
        print(f"LAMBDA INVOCATION RESPONSE : {lambda_response}")
        return {
            "statusCode" : 200, 
            "msg" : "app deploying"
        }
    
    if event_type == 'delete_app':  
        app_id = event['app_id']
        email_id = event['email_id']
        lambda_client = boto3.client("lambda", region_name=region_used)
        lambda_response = lambda_client.invoke(
            FunctionName=deployment_lambda, 
            Payload=json.dumps({ "app_id" : app_id ,"event_type":"delete_app","email_id":email_id}),
            InvocationType = 'Event'
        )
        current_time = datetime.now(timezone.utc).isoformat()
        log_insert_query = f"insert into {schema}.{action_logs_table} (email_id,app_id,env,action,last_updated_time) values (%s, %s, %s, %s, %s)"
        values = (email_id, app_id, "token_optix","APP DELETED",current_time)
        log_insert_response = insert_db(log_insert_query,values)
        print("LOG INSERTED : ",log_insert_response)
        print(f"LAMBDA INVOCATION RESPONSE : {lambda_response}")
        return{
            "statusCode" : 200,
            "msg":"app deleting"
        }

         
    
    if event_type == 'register_user':
        app_id = event['app_id']
        user_data = event['user_data']
        env = event["env"]
        connection_id = event['connection_id']
        session_id = generate_session_id()                                      
        query = f'''
        INSERT into {token_optix_schema}.{cexp_bot_logs} (user_data, app_id, session_id,environment, connectionid) values (%s, %s, %s, %s, %s)
        '''
        values = (user_data, app_id, session_id, env, connection_id)
        
        res = insert_db(query, values)
        
        query = f'''
        SELECT conversational, streaming from {token_optix_schema}.{app_meta_data_table} where app_id = '{app_id}';
        '''
        res = select_db(query)[0]
        print("APP DETAILS : ",res)
        conversational = res[0]
        streaming = res[1]
        
        # if conversational == 1 and streaming == 1:
        #     return {
        #         'statusCode' : 200,
        #         'route' : 'chat_stream',
        #         'session_id' : session_id
        #     }
        # else:
        return {
            'statusCode' : 200,
            'route' : 'chat',
            'session_id' : session_id
        }
    
    
    if event_type == 'get_ws_link':
        app_id = event['app_id']
        query = f'''
        SELECT conversational, streaming from {token_optix_schema}.{app_meta_data_table} where app_id = '{app_id}';
        '''
        res = select_db(query)[0]
        conversational = res[0]
        streaming = res[1]
        
        # if conversational == 1 and streaming == 1:
        #     return {
        #         'statusCode' : 200,         
        #         'route' : 'chat_stream'
        #     }
        # else:
        return {
            'statusCode' : 200,
            'route' : 'chat'
        }
        

    if event_type=='top_model_cost':
        result=[]
        date_time=event['date_time']
        # date_time='2024-04-09'
        #query=f'''select sum(cast(meta_value as float)), app_name  from token_optix.token_details td join token_optix.token_details_meta_data tdmd on td.id = tdmd.token_details_id left join token_optix.app_meta_data as md on td.app_id = md.id where (meta_key = 'total_cost' or meta_key = 'api_cost') and td.app_id is not null AND EXTRACT(YEAR FROM td.date_time) = EXTRACT(YEAR FROM '{date_time}'::DATE) AND EXTRACT(MONTH FROM td.date_time) = EXTRACT(MONTH FROM '{date_time}'::DATE) and td.delete_status = 0 GROUP BY md.app_name;'''
        query = f'''
        select sum(total_cost), md.app_name
        from {token_optix_schema}.token_details td
        left join {token_optix_schema}.app_meta_data as md
        on td.app_id = md.app_id
        where EXTRACT(YEAR FROM td.date_time) = EXTRACT(YEAR FROM '{date_time}'::DATE)
        AND EXTRACT(MONTH FROM td.date_time) = EXTRACT(MONTH FROM '{date_time}'::DATE)
        and td.delete_status = 0 GROUP BY md.app_name;'''
        
        res=select_db(query)
        for i in res:       
            app_name = i[1]                             
            total_cost = i[0]
            data = {"app_name":app_name,"total_cost":'{:.4f}'.format(float(total_cost))}  
            result.append(data)
            
    if event_type == 'get_user_access':
        try:
            email_id = event['email_id']
            app_id = event['app_id']
            query = f'''
            select row_to_json(row_values) FROM (select * from {token_optix_schema}.{USER_MANAGEMENT_TABLE} where email_id = '{email_id}' and app_id = '{app_id}' and delete_status = 0) row_values;
            '''
            user_data = select_db(query)
            
            return {
                'statusCode' : 200, 
                'data' : user_data
            }
        except Exception as e:
            print(e)
            return {
                'statusCode' : 500,
                'msg' : f"something went wrong : {e}"
            }
        
            

    if event_type == 'user_auth':                      
        email_id = event["email_id"]
        query = f'''SELECT row_to_json(row_values) FROM (SELECT * FROM {token_optix_schema}.dev_cexp_user_management WHERE email_id='{email_id}' and delete_status = 0) row_values;'''
        user_data = select_db(query)
        print(user_data)                                                                                    
        if user_data:     
            _user_data = []
            for user in user_data:
                _user_data.append(user[0])
            user_data = user_data[0][0]
            Access_type = user_data['Access_type']                                                                                                                  
            print(Access_type)
            user_type = user_data['User_type']
            print(user_type)
            date_created = datetime.strptime(user_data['created_date'], '%Y-%m-%dT%H:%M:%S.%f').date()
            current_date = datetime.now(timezone.utc).date()

            expiry_date = add_months(date_created, 3)                               
            validity = 0
            if current_date >= expiry_date:
                print("Password expired")
                validity = 0
            else:
                print("Password valid")
                validity = 1
            user_data['validity'] = 1
            function_response = {
                "access_info" : _user_data,
                "data": user_data,
                "message": "success"     
            }
            print("USER DATA : ",function_response)
            return {
                'statusCode': 200,
                'body': function_response
            }
        else:
            print("Failed to retrieve user data.")
            function_response = {
                "status" : "User does not exist"
            }
            return {
                    'statusCode': 500,
                    'body': function_response
                    }   
                    
                    
    if event_type == "list_users_count":
        user_type = event['User_type']
        search_result = event['search_result']
        if user_type == 'bot_user':
            bot_user_query = f'''select count(distinct user_unique_id)  
                                from {token_optix_schema}.{BOT_USER_MANAGEMENT_TABLE}
                                where "user_type" = '{user_type}' and delete_status = 0 and user_name ilike '%{search_result}%';'''
            try:        
                bot_user_count = select_db(bot_user_query)[0][0]
            except Exception as e:
                print("An exception occurred in bot_user_query: ",e)
                bot_user_count = 0
            function_response = {"bot_user_count":bot_user_count}
    
        elif user_type == 'user':
            user_query = f'''select count(distinct user_unique_id)   
                            from {token_optix_schema}.{USER_MANAGEMENT_TABLE}
                            where "User_type" = '{user_type}' and delete_status = 0 and user_name ilike '%{search_result}%';'''
            try:
                user_count = select_db(user_query)[0][0]
            except Exception as e:
                print("An exception occurred in user_query : ",e)
                user_count = 0
            function_response = {"user_count":user_count}
        else:
            user_query = f'''select count(*)   
                            from {token_optix_schema}.{USER_MANAGEMENT_TABLE}
                            where "User_type" = '{user_type}' and delete_status = 0 and user_name ilike '%{search_result}%';'''
            try:
                user_count = select_db(user_query)[0][0]
            except Exception as e:
                print("An exception occurred in user_query : ",e)
                user_count = 0
            function_response = {"user_count":user_count}
    
        return {
                'statusCode': 200,
                'body': function_response
            }   
    
    if event_type == 'list_users':  
        user_type = event['User_type']
        search_result = event['search_result']
        limit = event['limit']
        page = event['page']
        if user_type == 'bot_user':
            # bot_user_query = f'''select json_agg(row_to_json(row_values))
            #                 from
            #                 (select *
            #                 from {token_optix_schema}.{BOT_USER_MANAGEMENT_TABLE}
            #                 where "user_type" = '{user_type}' and delete_status = 0 and user_name ilike '%{search_result}%'
            #                 order by created_at desc
            #                 limit {limit} offset ({page} - 1) * {limit})row_values;'''
            bot_user_query = f'''select json_agg(row_to_json(row_values))
                                from(
                                SELECT DISTINCT user_unique_id, "user_type", user_name
                                FROM (
                                    SELECT user_unique_id, "user_type", user_name, created_at
                                    FROM {token_optix_schema}.{BOT_USER_MANAGEMENT_TABLE}
                                    WHERE "user_type" = '{user_type}' 
                                      AND delete_status = 0 
                                      AND user_name ILIKE '%{search_result}%'
                                    ORDER BY created_at DESC        
                                )LIMIT {limit} OFFSET ({page} - 1) * {limit})row_values;'''
            bot_user_data = select_db(bot_user_query)[0][0]  
            function_response = {"user_data":bot_user_data}
    
        else:
            # user_query = f'''select json_agg(row_to_json(row_values))
            #                 from
            #                 (select *
            #                 from {token_optix_schema}.{USER_MANAGEMENT_TABLE}
            #                 where "User_type" = '{user_type}' and delete_status = 0 and user_name ilike '%{search_result}%'
            #                 order by created_date desc
            #                 limit {limit} offset ({page} - 1) * {limit})row_values;'''
            email_id = event["email_id"]
            select_query = f"SELECT company_name from {token_optix_schema}.{USER_MANAGEMENT_TABLE} where email_id = '{email_id}' and delete_status = 0;"
            company_name_response = select_db(select_query)
            company_name = company_name_response[0][0]
            print("COMPANY AME : ",company_name)
            function_response = {}
            # if user_type == "User"
            if company_name == "1CloudHub":
                if user_type == "user":
                    user_query = f'''select json_agg(row_to_json(row_values))
                                 from(
                                SELECT DISTINCT user_unique_id, "User_type", user_name,email_id   
                                FROM (
                                    SELECT user_unique_id, "User_type", user_name, created_date,email_id  
                                    FROM {token_optix_schema}.{USER_MANAGEMENT_TABLE}
                                    WHERE ("User_type" != 'admin' or "User_type" != 'super_admin')
                                      AND delete_status = 0 
                                      AND user_name ILIKE '%{search_result}%'
                                    ORDER BY created_date DESC
                                )LIMIT {limit} OFFSET ({page} - 1) * {limit})row_values;'''
                else:
                    user_query = f'''select json_agg(row_to_json(row_values))
                                     from(
                                    SELECT DISTINCT user_unique_id, "User_type", user_name,email_id   
                                    FROM (
                                        SELECT user_unique_id, "User_type", user_name, created_date,email_id  
                                        FROM {token_optix_schema}.{USER_MANAGEMENT_TABLE}
                                        WHERE "User_type" = '{user_type}' 
                                          AND delete_status = 0 
                                          AND user_name ILIKE '%{search_result}%'
                                        ORDER BY created_date DESC
                                    )LIMIT {limit} OFFSET ({page} - 1) * {limit})row_values;'''   
                user_data = select_db(user_query)[0][0]
                function_response = {"user_data":user_data}
            else:
                
                if user_type == "user":
                    user_query = f'''select json_agg(row_to_json(row_values))
                                 from(
                                SELECT DISTINCT user_unique_id, "User_type", user_name,email_id   
                                FROM (
                                    SELECT user_unique_id, "User_type", user_name, created_date,email_id  
                                    FROM {token_optix_schema}.{USER_MANAGEMENT_TABLE}
                                    WHERE ("User_type" != 'admin' or "User_type" != 'super_admin')
                                      AND company_name = '{company_name}'
                                      AND delete_status = 0 
                                      AND user_name ILIKE '%{search_result}%'
                                    ORDER BY created_date DESC
                                )LIMIT {limit} OFFSET ({page} - 1) * {limit})row_values;'''   
                else:
                    user_query = f'''select json_agg(row_to_json(row_values))
                                     from(
                                    SELECT DISTINCT user_unique_id, "User_type", user_name,email_id   
                                    FROM (
                                        SELECT user_unique_id, "User_type", user_name, created_date,email_id  
                                        FROM {token_optix_schema}.{USER_MANAGEMENT_TABLE}
                                        WHERE "User_type" = '{user_type}' 
                                          AND company_name = '{company_name}'
                                          AND delete_status = 0 
                                          AND user_name ILIKE '%{search_result}%'
                                        ORDER BY created_date DESC
                                    )LIMIT {limit} OFFSET ({page} - 1) * {limit})row_values;'''   
                user_data = select_db(user_query)[0][0]
                function_response = {"user_data":user_data}
        
            print("USER FUNCTION RESPONSE : ", user_data)
        return {
                'statusCode': 200,
                'body': function_response
            }
    
    if event_type == "add_user":   
        user_name = event['user_name']
        user_unique_id = ''.join(random.choices(string.ascii_letters + string.digits, k=10))
        if 'email_id' in event:
            email_id = event['email_id']
        else:
            email_id = ''                                                                                                                                                                
        role_type = event['user_type']                                                  
        if role_type in ['super_admin', 'user', 'admin']:
            
            query = f'''select count(*) from {token_optix_schema}.{USER_MANAGEMENT_TABLE} where email_id='{email_id}' and delete_status = 0;'''
            user_exists = select_db(query)[0][0]   
            
            if user_exists == 0:
                if role_type == "super_admin" or role_type == "admin":
                    app_access = "all_access"
                    app_name = "all_access"
                    company_name = "1CloudHub"
                    if role_type == "admin":
                        company_name = event["company_name"]
                    query = f'''insert into {token_optix_schema}.{USER_MANAGEMENT_TABLE}(email_id, delete_status, user_name, "User_type", app_access, created_date, user_unique_id, app_name, company_name) values (%s, 0, %s, %s,%s,NOW(), %s,%s, %s)'''    
                    values = (email_id, user_name, role_type,app_access,user_unique_id,app_name,company_name)         
                    result = insert_db(query, values)
                if role_type == "user":
                    company_name = event["company_name"]
                    app_access_details = event['app_access_details']
                    for app in app_access_details:
                        query = f'''insert into {token_optix_schema}.{USER_MANAGEMENT_TABLE}(email_id, delete_status,user_name, "User_type", created_date,app_access,user_unique_id,app_name,company_name) values (%s, 0, %s, %s,NOW(),%s,%s,%s,%s)'''
                        values = (email_id,user_name,app["app_role"],app['app_id'],user_unique_id,app['app_name'],company_name)
                        result = insert_db(query, values)
                print("User added to User Table")
                try:
                    random_string_length = 10
                    rand =  generate_random_string(random_string_length)                                                                   
                    temp_pass ='CEXP@'+rand
                    cognito_user = cognito_idp.admin_create_user(
                    UserPoolId=UserPoolId,
                    Username=email_id,
                        UserAttributes=[
                        {
                            'Name': 'email',
                            'Value': email_id                     
                        },
                    ],
                    TemporaryPassword=temp_pass,
                    ForceAliasCreation=False,
                    DesiredDeliveryMediums=[
                        'EMAIL',
                    ],
                
                    )
                    cognito_pass = cognito_idp.admin_set_user_password(
                        UserPoolId=UserPoolId,
                        Username=email_id,
                        Password=temp_pass,
                        Permanent=True
                    )               
                    function_response = {
                        "message" : "user added"
                    }
                    return {
                        'statusCode': 200,
                        'body': function_response
                        }
                except Exception as e:
                    print("Exception:",e)  
                    function_response = {"message": "Error: Could not create user in cognito","Exception":str(e)}      
                    return {
                        'statusCode': 500,
                        'body': function_response
                        }
                
            else:
                print("user already exists")
                function_response = {
                    "message" : "user already exists"
                }
                return {                                                                
                    'statusCode': 200,
                    'body': function_response
                    }
        elif role_type == 'bot_user':
            app_access_details = event['app_access_details']
            for app in app_access_details:
                access_type = app['access_type']
                app_name = app['app_name']
                app_id = app['app_id']
                integration_value = app['integration_details']
                user_identifier = integration_value['identifier']
                integration_type = integration_value['integration_type']
                
                query = f'''
                INSERT INTO {token_optix_schema}.{BOT_USER_MANAGEMENT_TABLE} 
                (user_id, access_type, created_at, delete_status, user_name, user_type, app_access, user_unique_id, app_name,integration_type)
                VALUES (%s, %s, CURRENT_TIMESTAMP, 0, %s, %s, %s, %s, %s, %s);
                '''
                values = (user_identifier, access_type, user_name, role_type, app_id, user_unique_id, app_name, integration_type)
                result = insert_db(query, values)
            
            function_response = {
                "message"  : "bot user added"
            }
            return {
                'statusCode': 200,
                'body': function_response
                }
                           
            
        else:
            function_response = {
                "message":"Invalid user type"
            }
            return{
                "statusCode":200,
                "body":function_response
            }    

    
    
    # DELETE USER
    # if event_type == 'delete_user':
    #         email_id = event['email_id']
    #         role_type = event['User_type']
    #         mobile_number=event['mobile_number']
    #         if role_type =='super_admin' or role_type == 'user':
    #             query = f'''update {token_optix_schema}.{USER_MANAGEMENT_TABLE} set delete_status = 1 where email_id  = '{email_id}' and delete_status = 0;'''
    #             result = update_db(query)
    #             try:
    #                 if result is not None:
    #                     response = cognito_idp.admin_delete_user(
    #                             UserPoolId=UserPoolId,
    #                             Username=email_id                           
    #                         )
    #                     function_response = {"message": "User deleted successfully"}
    #                     return {
    #                         'statusCode': 200,
    #                         'body': function_response
    #                     }
    #                 else:
    #                     function_response = {"message": f"Error: Could not delete user {email_id}"}
    #                     return {
    #                         'statusCode': 500,
    #                         'body': function_response
    #                     }
    #             except:
    #                 function_response = {"message": f"Error: Could not delete user {email_id}"}
    #                 return {
    #                     'statusCode': 500,
    #                     'body': function_response
    #                 }
    #         else:
    #             query = f'''update {token_optix_schema}.{BOT_USER_MANAGEMENT_TABLE} set delete_status = 1 where mobile_number  = '{mobile_number}' and delete_status = 0;'''
    #             result = update_db(query)
    #             if result is not None:
    #                 function_response = {"message": "User deleted successfully"}
    #                 return {
    #                     'statusCode': 200,
    #                     'body': function_response
    #                 }
    #             else:
    #                 function_response = {"message": f"Error: Could not delete user {mobile_number}"}
    #                 return {
    #                     'statusCode': 500,
    #                     'body': function_response
    #                 }
    if event_type == 'delete_user':  
        if 'email_id' in event:
            email_id = event['email_id']
        role_type = event['user_type']                                                                                          
        user_unique_id = event['user_unique_id']  
        if role_type =='super_admin' or role_type == 'admin' or role_type == 'user':                                                                                         
            query = f'''update {token_optix_schema}.{USER_MANAGEMENT_TABLE} set delete_status = 1 where email_id  = '{email_id}' and delete_status = 0 and user_unique_id = '{user_unique_id}' ;'''
            result = update_db(query)
            try:
                
                response = cognito_idp.admin_delete_user(
                        UserPoolId=UserPoolId,
                        Username=email_id                           
                    )
                function_response = {"message": "User deleted successfully"}
                return {
                    'statusCode': 200,
                    'body': function_response
                }
                
            except Exception as e:
                print("Exception:",e)   
                function_response = {"message": f"Error: Could not delete user {email_id}"}  
                return {
                    'statusCode': 500,
                    'body': function_response
                }
        else:
            query = f'''update {token_optix_schema}.{BOT_USER_MANAGEMENT_TABLE} set delete_status = 1 where user_unique_id  = '{user_unique_id}' and delete_status = 0;'''
            result = update_db(query)
           
            function_response = {"message": "User deleted successfully"}
            return {
                'statusCode': 200,
                'body': function_response
            }  
            
    # if event_type == 'update-user':
    #     email_id = event['email_id']
    #     user_name =  event['user_name']                                     
    #     role_type = event['User_type']
    #     access = event['access']
    #     mobile_number=event['mobile_number']   
    #     config_access =event['config_access']

    #     if role_type =='super_admin' or role_type == 'user':
    #         if role_type == 'super_admin':
    #             config_access = "allow"
    #             dev_access = "allow"
    #             prod_access = "allow"
    #         else:
    #             environment = event['environment']
    #             prod_access = "allow" if "Prod" in environment else "deny"
    #             dev_access = "allow" if "Dev" in environment else "deny"
                
    #         query = f'''update {token_optix_schema}.{USER_MANAGEMENT_TABLE} set Access_type = '{access}', User_type = '{role_type}', mobile_number = '{mobile_number}', config_access ='{config_access}', prod_access = '{prod_access}', dev_access = '{dev_access}', user_name='{user_name}' where email_id ='{email_id}' and delete_status = 0; '''
    #     else:
    #         # environment = event['environment']
    #         query = f'''update {token_optix_schema}.{BOT_USER_MANAGEMENT_TABLE} set Access_type = '{access}', email_id = '{email_id}', user_name = '{user_name}', environment = 'prod' where mobile_number = '{mobile_number}' and delete_status = 0'''

    #     result = update_db(query)
    #     if result is not None:  
    #         function_response = {
    #             "message" : 'update successfully'
    #         }
    #         return {
    #             'statusCode': 200,
    #             'body': function_response                                                                                                       
    #         }
    #     else:
    #         function_response = {
    #             "message" : 'something went wrong'
    #         }
    #         return {
    #             'statusCode': 500,
    #             'body': function_response
    #         }
              
    
        
    if event_type == 'update_date_user_auth':
        email_id = event['email_id']  
        query = f'''update {token_optix_schema}.{USER_MANAGEMENT_TABLE} set created_date = NOW() where email_id ='{email_id}' and delete_status =0'''
        result = update_db(query)
        if result is not None:
            return {"statusCode":200,"message":"Date updated successfully"}    
        else:
            return {"statusCode":500,"message":f"Error: Unable to update date for user {email_id}"}  
    
    if event_type == 'list_apps':
        drop_down = event['drop_down']
        text = event['text']
        limit = event['limit']              
        page = event['page']
        # company_name = event['company_name']
        
        # if company_name == "OCH":
        #     check_company = ""
        # else:
        #     check_company = f"company_name = '{company_name}' and "
            
        email_id = event['email_id']                    
        # query = f'''select id, email_id, app_name, description, created_on, app_id,status, integration, unique_identifier,status_description,company_name,active_app_name
        #             from {token_optix_schema}.{app_meta_data_table}
        #             where delete_status = 0 and {drop_down} ilike '%{text}%' and 
        #             (
        #                 'all_access' = ANY (SELECT app_access 
        #                                     FROM {token_optix_schema}.{USER_MANAGEMENT_TABLE} 
        #                                     WHERE email_id = '{email_id}' and delete_status = 0)
        #                 OR app_id IN (SELECT app_access 
        #                               FROM {token_optix_schema}.{USER_MANAGEMENT_TABLE} 
        #                               WHERE email_id = '{email_id}' and delete_status = 0)
        #               )
        #             order by created_on desc 
        #             limit {limit} offset ({page} - 1) * {limit};'''
        query = f'''
                select 
                    id, email_id, app_name, description, created_on, app_id, status, integration, unique_identifier, 
                    status_description, company_name, active_app_name,conversational
                from 
                    {token_optix_schema}.{app_meta_data_table}
                where 
                    delete_status = 0 
                    and (
                        '1CloudHub' = ANY (
                            SELECT company_name 
                            FROM {token_optix_schema}.{USER_MANAGEMENT_TABLE} 
                            WHERE email_id = '{email_id}' and delete_status = 0
                        )
                        OR company_name IN (
                            SELECT company_name 
                            FROM {token_optix_schema}.{USER_MANAGEMENT_TABLE} 
                            WHERE email_id = '{email_id}' and delete_status = 0
                        )
                    )
                    and {drop_down} ilike '%{text}%' 
                    and (
                        'all_access' = ANY (
                            SELECT app_access 
                            FROM {token_optix_schema}.{USER_MANAGEMENT_TABLE} 
                            WHERE email_id = '{email_id}' and delete_status = 0
                        )
                        OR app_id IN (
                            SELECT app_access 
                            FROM {token_optix_schema}.{USER_MANAGEMENT_TABLE} 
                            WHERE email_id = '{email_id}' and delete_status = 0
                        )
                    )
                order by 
                    created_on desc 
                limit 
                    {limit} offset ({page} - 1) * {limit};
        '''
        result1 = select_db(query)
        print("DB RESULT ALLOWED APPS : ",result1)
        result = []
        for record in result1:
            record_dict = {
                "id": record[0],
                "email_id": record[1],
                "app_name": record[2],
                "description": record[3],
                "created_on": str(record[4]),
                "app_id":record[5],
                "status":record[6],
                "integration":record[7],
                "unique_identifier" : record[8],
                "status_description" : record[9],
                "company_name": record[10],
                "active_app_name" : record[11],
                "conversational" : record[12]
            }           
            result.append(record_dict)
            
    if event_type == 'app_pageno_recent':
        drop_down = event['drop_down']
        text = event['text']
        email_id = event['email_id']
        # company_name = event['company_name']
        result = []
        
        # if company_name == "OCH":
        #     check_company = ""
        # else:           
        #     check_company = f"company_name = '{company_name}' and "
            
        query = f'''select count(*)
                    from {token_optix_schema}.app_meta_data
                    where delete_status = 0 and {drop_down} ilike '%{text}%' and 
                    (
                        'all_access' = ANY (SELECT app_access 
                                            FROM {token_optix_schema}.{USER_MANAGEMENT_TABLE} 
                                            WHERE email_id = '{email_id}')
                        OR app_id IN (SELECT app_access 
                                      FROM {token_optix_schema}.{USER_MANAGEMENT_TABLE} 
                                      WHERE email_id = '{email_id}')
                     );'''
        
        result1 = select_db(query)
        count = result1[0][0]
        result.append(count)
    
    
    # if event_type == 'insert-app':
    #     model_name = event['model_name']
    #     region_value = event['region']
    #     embedding_model = event["embedding_model"]
    #     model_name= json.loads(model_name)
    #     app_name =  event['app_name']
    #     email_id = event['email_id']
    #     description  = event['description']
    #     title  = event['title']
    #     sub_type = event['sub_type']
    #     credit_count = event['credit_count']
    #     result = []
    #     res1 = []
    #     query1 = f"select id from token_optix.app_meta_data where app_name ='{app_name}' and delete_status = 0;" 
    #     res1 = select_db(query1)             
    #     if res1 == []:
    #         query = f'''
    #         INSERT INTO token_optix.app_meta_data
    #         (email_id,app_name, description, title, created_on, updated_on,delete_status,embedding_model,region)
    #         VALUES( %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP,%s,%s,%s) RETURNING id;
    #     '''
    #         insert_values = (email_id,app_name, description, title,0,embedding_model,region_value)
    #         app_id = insert_ret_db(query,insert_values)
    #         model_list = []
    #         provider_list= []
    #         model_id_list = []
    #         for item in model_name:
    #             model_list.append(item['model_name'])
    #             provider_list.append(item['provider'])
    #             model_id_list.append(item['model_id_name'])   
    #         for mod,prov,mod_id in zip(model_list,provider_list,model_id_list):
    #             convo_json = {}
    #             convo_id = ''.join(random.choices(string.ascii_letters + string.digits, k=10))
    #             query = f'''
    #                 INSERT INTO token_optix.model_metadata
    #                 (app_id, convo_id, model_name, provider, created_on, updated_on, delete_status,app_name,model_id)
    #                 VALUES(%s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, %s,%s,%s) ;
    #                 '''
    #             insert_values =(app_id,convo_id,mod,prov,0,app_name,mod_id)
    #             insert_db(query,insert_values)
    #             convo_json['model_name'] = mod
    #             convo_json['convo_id'] = convo_id
    #             convo_json['mod_id'] = mod_id
    #             result.append(convo_json)
            
    #         #Add the app to the subscription table
    #         model_id = result[0]['mod_id']  
    #         convo_id = result[0]['convo_id']
    #         model_name_1 = result[0]['model_name']
    #         if sub_type == 'monthly':
    #             time_interval = 'month'
    #         elif sub_type == 'yearly':
    #             time_interval = 'year'
    #         query = f'''
    #                 INSERT INTO token_optix.subscription_metadata
    #                 (app_id, assigned_credits,remaining_credits, subscription_type, start_date, expiry_date, delete_status,model_id,convo_id,model_name)
    #                 VALUES(%s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP + INTERVAL '1 {time_interval}', %s,%s,%s,%s);'''
    #         subscript_values = (app_id,credit_count,credit_count,sub_type,0,model_id,convo_id,model_name_1)  
    #         insert_db(query,subscript_values)     

    #     else:
    #         re =  {
    #         'statusCode': 200,
    #         'body': 'Record already exist.'
    #         }   
    #         result.append(re)
    
    if event_type == 'insert-app':
        api_link = "https://cnfhx7ltg3.execute-api.us-east-1.amazonaws.com/dev"
        status = "active"
        model_name = event['model_name']
        integration_values = event['integration_values']
        integration_values =json.loads(integration_values)  
        region_value = event['region']
        embedding_model = event["embedding_model"]
        model_name= json.loads(model_name)        
        app_name =  event['app_name']
        email_id = event['email_id']
        description  = event['description']
        title  = event['title']
        sub_type = event['sub_type']
        credit_count = event['credit_count']
        conversational = event['conversational']
        streaming = event['streaming']
        result = []
        res1 = []
        query1 = f"select id from {token_optix_schema}.{app_meta_data_table} where app_name ='{app_name}' and delete_status = 0;" 
        res1 = select_db(query1)             
        if res1 == []:
            unique_app_id = ''.join(random.choices(string.ascii_letters + string.digits, k=8))
            query = f'''
            INSERT INTO {token_optix_schema}.{app_meta_data_table}
            (app_id,email_id,app_name, description, title, created_on, updated_on,delete_status,embedding_model,region,conversational,streaming,api_gateway_link,status)
            VALUES( %s,%s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP,%s,%s,%s,%s,%s,%s,%s);
        '''
            insert_values = (unique_app_id,email_id,app_name, description, title,0,embedding_model,region_value,int(conversational),int(streaming),api_link,status)  
            insert_db(query,insert_values)
            model_list = []
            provider_list= []
            model_id_list = []
            for item in model_name:
                model_list.append(item['model_name'])
                provider_list.append(item['provider'])
                model_id_list.append(item['model_id_name'])   
            for mod,prov,mod_id in zip(model_list,provider_list,model_id_list):
                convo_json = {}
                convo_id = ''.join(random.choices(string.ascii_letters + string.digits, k=10))
                query = f'''
                    INSERT INTO {token_optix_schema}.{model_metadata_table}
                    (app_id, convo_id, model_name, provider, created_on, updated_on, delete_status,app_name,model_id)
                    VALUES(%s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, %s,%s,%s) ;
                    '''
                insert_values =(unique_app_id,convo_id,mod,prov,0,app_name,mod_id)
                insert_db(query,insert_values)
                convo_json['model_name'] = mod
                convo_json['convo_id'] = convo_id
                convo_json['mod_id'] = mod_id
                result.append(convo_json)
                
            #Add integration details
            integration_type_list = []
            integration_access_id_list = []
            integration_chat_access_list = []
            for item in integration_values:
                integration_type_list.append(item['integration_type'])
                integration_access_id_list.append(item['integration_access_id'])
                integration_chat_access_list.append(item['integration_chat_access'])
            for integration_type,integration_access_id,integration_chat_access in zip(integration_type_list,integration_access_id_list,integration_chat_access_list):
                #integration_json = {}  
                integration_id = ''.join(random.choices(string.ascii_letters + string.digits, k=12))
                query = f'''INSERT INTO {token_optix_schema}.{app_integration_details_table}
                            (app_id, integration_type, access_id, delete_status, chat_access, integration_id,created_on,updated_on)
                            VALUES(%s, %s, %s, 0, %s,%s,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP);'''
                insert_values = (unique_app_id,integration_type,integration_access_id,integration_chat_access,integration_id)
                insert_db(query,insert_values)
                # integration_json['integration_type'] = integration_type
                # integration_json['integration_access_id'] = integration_access_id
                # integration_json['integration_chat_access'] = integration_chat_access
                # integration_json['integration_id'] = integration_id   
    
            #Add the app to the subscription table
            model_id = result[0]['mod_id']  
            convo_id = result[0]['convo_id']
            model_name_1 = result[0]['model_name']
            if sub_type == 'monthly':
                time_interval = 'month'
            elif sub_type == 'yearly':
                time_interval = 'year'
            query = f'''
                    INSERT INTO {token_optix_schema}.{subscription_metadata_table}
                    (app_id, assigned_credits,remaining_credits, subscription_type, start_date, expiry_date, delete_status,model_id,convo_id,model_name)
                    VALUES(%s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP + INTERVAL '1 {time_interval}', %s,%s,%s,%s);'''
            subscript_values = (unique_app_id,credit_count,credit_count,sub_type,0,model_id,convo_id,model_name_1)  
            insert_db(query,subscript_values)
            current_time = datetime.now(timezone.utc).isoformat()
            log_insert_query = f"insert into {schema}.{action_logs_table} (email_id,app_id,env,action,last_updated_time) values (%s, %s, %s, %s, %s)"
            values = (email_id, unique_app_id, "token_optix","APP INSERTED",current_time)
            log_insert_response = insert_db(log_insert_query,values)
            print("LOG INSERTED : ",log_insert_response)
            result = [{'statusCode': 200,'body': 'App created successfully.','app_id':unique_app_id,'app_name':app_name}]            
    
        else:
            re =  {
            'statusCode': 200,
            'body': 'Record already exist.'
            }   
            result.append(re)
            
    if event_type == "list_company":
        company_query = f"SELECT DISTINCT company_name FROM {token_optix_schema}.{app_meta_data_table};"
        company_response = select_db(company_query)
        print(company_response)
        companies = []
        
        for company in company_response:
            if company[0] is not None:
                companies.append(company[0])
        
        result = {
            "message" : "success",
            "companies" : companies
        }   
        
    if event_type == "list_company_and_apps":
        email_id = event["email_id"]
        try:
            select_query = f'''
                select json_agg(row_to_json(row_values))
                from(
                select 
                    app_id,company_name, active_app_name
                from 
                    {token_optix_schema}.{app_meta_data_table}
                where 
                    delete_status = 0 
                    and status = 'DEPLOYED'
                    and (
                        '1CloudHub' = ANY (
                            SELECT company_name 
                            FROM {token_optix_schema}.{USER_MANAGEMENT_TABLE} 
                            WHERE email_id = '{email_id}' and delete_status = 0
                        )
                        OR company_name IN (
                            SELECT company_name 
                            FROM {token_optix_schema}.{USER_MANAGEMENT_TABLE} 
                            WHERE email_id = '{email_id}' and delete_status = 0
                        )
                    )
                    and (
                        'all_access' = ANY (
                            SELECT app_access 
                            FROM {token_optix_schema}.{USER_MANAGEMENT_TABLE} 
                            WHERE email_id = '{email_id}' and delete_status = 0
                        )
                        OR app_id IN (
                            SELECT app_access 
                            FROM {token_optix_schema}.{USER_MANAGEMENT_TABLE} 
                            WHERE email_id = '{email_id}' and delete_status = 0
                        )
                    )
                order by 
                    created_on desc 
                )row_values;
        '''
            
            response = select_db(select_query)[0][0]
            print("DB RESPONSE : ",response)
            app_response = {}
            
            for item in response:
                company = item['company_name']
                if company not in app_response:
                    app_response[company] = []
                app_response[company].append({
                    'app_id': item['app_id'],
                    'active_app_name': item['active_app_name']
                })
            
            # Convert the dictionary into a list of dictionaries with the desired format
            final_response = [{company: details} for company, details in app_response.items()]
            return {
                "statusCode" : 200,
                "response" : app_response
            }
    
        except Exception as e:
            print("EXCEPTION OCCURRED : ",e)
            return {
                "statusCode" : 500,
                "response" : "Error while fetching data"
            }

    if event_type == "list_tags_count":
        env = event["env"]
        app_id = event["app_id"]
        company_name = event["company_name"]
        search_result = event["search"]
        try:
            select_query = f'''
                    select count(*) from {token_optix_schema}.{TAGS_TABLE} where
                    app_id = '{app_id}' and company_name = '{company_name}' and env = '{env}' and delete_status = 0 and tag_name ilike '%{search_result}%'
            '''
            count = select_db(select_query)[0][0]
            return {
                "statusCode" : 200,
                "response" : count
            }
    
        except Exception as e:
            print("EXCEPTION OCCURRED : ",e)
            return {
                "statusCode" : 500,
                "response" : f"Error while fetching data  {e}"
            }
            
    if event_type == "list_tags":
        env = event["env"]
        app_id = event["app_id"]
        limit = event["limit"]
        page = event["page"]
        search_result = event["search"]
        
        try:
            select_query = f'''
                select company_name from {token_optix_schema}.{app_meta_data_table} where app_id = '{app_id}'
            '''
            company_name = select_db(select_query)[0][0]
            select_query = f'''
                            select json_agg(row_to_json(row_values))
                from(
                    select * from {token_optix_schema}.{TAGS_TABLE} where
                    app_id = '{app_id}' and company_name = '{company_name}' and env = '{env}' and delete_status = 0 and  tag_name ilike '%{search_result}%' 
                    ORDER BY created_at DESC LIMIT {limit} OFFSET ({page} - 1) * {limit}
                                    )row_values;
            '''
            tags = select_db(select_query)[0][0]
            
            if tags == None:
                tags = []
            return {
                "statusCode" : 200,
                "response" : tags
            }
    
        except Exception as e:
            print("EXCEPTION OCCURRED : ",e)
            return {
                "statusCode" : 500,
                "response" : f"Error while fetching data  {e}"
            }
    
    
            
    if event_type == "add_tag":
        env = event["env"]
        tag_name = event["tag_name"]
        app_id = event["app_id"]
        company_name = event["company_name"]
        created_by = event["email_id"]
        try:
            select_query = f'''
            select count(*) from {token_optix_schema}.{TAGS_TABLE} where app_id = '{app_id}' and env = '{env}' and company_name = '{company_name}' and tag_name = '{tag_name}' and delete_status = 0;
            '''
            count = select_db(select_query)
            
            if count[0][0] > 0:
                return {
                "statusCode" : 200,
                "response" : "Tag already exsists"
                }
            else:
                insert_query = f'''
                insert into {token_optix_schema}.{TAGS_TABLE} (tag_name, app_id, company_name, created_by, created_at, delete_status,env) values(%s,%s,%s,%s,CURRENT_TIMESTAMP,0,%s)
                '''
                values = (tag_name, app_id, company_name,created_by,env)
                insert_db(insert_query,values)
                current_time = datetime.now(timezone.utc).isoformat()
                log_insert_query = f"insert into {schema}.{action_logs_table} (email_id,app_id,env,action,last_updated_time) values (%s, %s, %s, %s, %s)"
                values = (created_by, app_id, env,f"TAG {tag_name} Created.",current_time)
                log_insert_response = insert_db(log_insert_query,values)
                print("LOG INSERTED : ",log_insert_response)
                return {
                    "statusCode" : 200,
                    "response" : "Successfully added tag"
                }
        except Exception as e:
            return {
                "statusCode" : 500,
                "response" : f"Error while adding tag {e}"
            }
            
    if event_type == "delete_tag":
        tag_name = event["tag_name"]
        app_id = event["app_id"]
        company_name = event["company_name"]
        env = event["env"]
        email_id = event["email_id"]
        try:
            select_query = f'''
                SELECT SUM(total_count) AS total_associated_count
                FROM (
                    -- Count tagged users
                    SELECT COUNT(*) AS total_count
                    FROM {token_optix_schema}.{USER_MANAGEMENT_TABLE} 
                    WHERE (app_access = '{app_id}' OR app_access = 'all_access')
                      AND delete_status = 0
                      AND '{tag_name}' = ANY(STRING_TO_ARRAY(metadata, ',,,'))
                    
                    UNION ALL
                    
                    -- Count tagged documents
                    SELECT COUNT(*) AS total_count
                    FROM {token_optix_schema}.{FILE_VERSIONS_TABLE} 
                    WHERE app_id = '{app_id}'
                      AND delete_status = 0
                      AND '{tag_name}' = ANY(STRING_TO_ARRAY(metadata, ',,,'))
                      AND env = '{env}'
                ) AS combined_counts;
            '''
            count = select_db(select_query)[0][0]
            if count > 0:
                return{
                "statusCode" : 201,
                "response" : f"Ensure to delete Users and documents tagged with tag {tag_name}"
                }
            
            update_query = f'''
            update {token_optix_schema}.{TAGS_TABLE} 
            set delete_status = '1'
            where app_id = '{app_id}' and company_name = '{company_name}' and env = '{env}' and tag_name = '{tag_name}' and delete_status = 0
            '''
            delete_response = update_db(update_query)
            current_time = datetime.now(timezone.utc).isoformat()
            log_insert_query = f"insert into {schema}.{action_logs_table} (email_id,app_id,env,action,last_updated_time) values (%s, %s, %s, %s, %s)"
            values = (email_id, app_id, env,f"TAG {tag_name} Deleted.",current_time)
            log_insert_response = insert_db(log_insert_query,values)
            print("LOG INSERTED : ",log_insert_response)
            
            return {
                "statusCode" : 200,
                "response" : f"Successfully deleted tag {tag_name}"
            }
        except Exception as e:
            return {
                "statusCode" : 500,
                "response" : f"Error while deleting tag due to {e}"
            }
            
        
            
    if event_type == "count_of_tagged_users_or_documents":
        app_id = event["app_id"]
        company_name = event["company_name"]
        search_result = event["search"]
        tag_name = event["tag_name"]
        entity_type = event["entity_type"]
        env = event["env"]
        try:
            count = 0
            if entity_type == "users":
                select_query = f'''select count(*) from {token_optix_schema}.{USER_MANAGEMENT_TABLE} 
                where (app_access = '{app_id}' or app_access = 'all_access')
                and company_name = '{company_name}'
                and delete_status = '0'
                and email_id ilike '%{search_result}%'
                and "User_type" NOT IN ('super_admin', 'admin')
                '''
                count =  select_db(select_query)[0][0]
            else:
                select_query = f'''select count(*) 
                FROM {token_optix_schema}.{FILE_METADATA_TABLE}
                    WHERE app_id = '{app_id}'
                      AND delete_status = 0
                      AND env = '{env}'
                      AND active_file_name ILIKE '%{search_result}%'
                      AND '{tag_name}' = ANY(STRING_TO_ARRAY(metadata, ',,,'));
                '''
                
                count =  select_db(select_query)[0][0]
            
            return {
                "statusCode" : 200,
                "count" : count
            }
        
        except Exception as e:
            return {
                "statusCode" : 500,
                "response" : f"Error while fetching count tag due to {e}"
            }
            
    if event_type == "list_of_tagged_users_or_documents":
        app_id = event["app_id"]
        company_name = event["company_name"]
        search_result = event["search"]
        tag_name = event["tag_name"]
        limit = event["limit"]
        page = event["page"]
        entity_type = event["entity_type"]
        env = event["env"]
        try:
            if entity_type == "users":
                select_query = f'''
                   SELECT json_agg(row_to_json(row_values))
                    FROM (
                        SELECT 
                            email_id,
                            user_name,
                            company_name,
                            app_access,
                            CASE 
                                WHEN '{tag_name}' = ANY(STRING_TO_ARRAY(metadata, ',,,')) THEN 1 
                                ELSE 0 
                            END AS is_tagged
                        FROM {token_optix_schema}.{USER_MANAGEMENT_TABLE}
                        WHERE (app_access = '{app_id}' OR app_access = 'all_access')
                          AND company_name = '{company_name}'
                          AND email_id ILIKE '%{search_result}%'
                          AND delete_status = 0
                          AND "User_type" NOT IN ('super_admin', 'admin')  
                        ORDER BY is_tagged DESC, user_name
                        LIMIT {limit} OFFSET ({page} - 1) * {limit}
                    ) row_values;
                '''
                
                tagged_users = select_db(select_query)
                
                if len(tagged_users) > 0:
                    tagged_users = tagged_users[0][0]
                print("TAGGED USERS : ",tagged_users)
                return {
                    "statusCode" : 200,
                    "tagged_data" : tagged_users
                }
            else:
                
                select_query = f'''
                SELECT json_agg(row_to_json(row_values))
                    FROM (
                    SELECT * 
                    FROM {token_optix_schema}.{FILE_METADATA_TABLE}
                    WHERE app_id = '{app_id}'
                      AND delete_status = 0
                      AND env = '{env}'
                      AND active_file_name ILIKE '%{search_result}%'
                      AND '{tag_name}' = ANY(STRING_TO_ARRAY(metadata, ',,,'))
                       ) row_values;
                '''
                tagged_documents = select_db(select_query)
                print("TAGGED DOCUMENTS : ",tagged_documents)
                if len(tagged_documents) > 0:
                    tagged_documents = tagged_documents[0][0]
                print("TAGGED DOCUMENTS : ",tagged_documents)
                return {
                    "statusCode" : 200,
                    "tagged_data" : tagged_documents
                }
                
        except Exception as e:
            return {
                "statusCode" : 500,
                "response" : f"Error while fetching data tag due to {e}"
            }
            
    if event_type == "update_user_tag":
        app_id = event["app_id"]
        company_name = event["company_name"]
        tag_name = event["tag_name"]
        data = event["data"]
        emails = list(data.keys())
        env = event["env"]
        done_by = event["email_id"]
        print("EMAILS : ",emails)
        try:
            for email in emails:
                print(f"EMAIL : {email} - ACTION : {data[email]['action']}")
                if data[email]["action"] == True:
                    select_tag_query = f'''
                    select metadata from {token_optix_schema}.{USER_MANAGEMENT_TABLE} where email_id = '{email}' and app_access = '{app_id}' and company_name = '{company_name}' and delete_status = 0
                    '''
                    metadata = select_db(select_tag_query)
                    print(f"METADATA FOR USER {email} : ",metadata)
                    
                    if metadata[0][0] is None:
                        update_tag_query = f'''
                        update {token_optix_schema}.{USER_MANAGEMENT_TABLE}
                        set metadata = ',,,{tag_name}'
                        where email_id = '{email}' and app_access = '{app_id}' and company_name = '{company_name}' and delete_status = 0
                        '''
                    else:
                        metadata = metadata[0][0]
                        metadata = metadata + ",,," + tag_name
                        update_tag_query = f'''
                        update {token_optix_schema}.{USER_MANAGEMENT_TABLE}
                        set metadata = '{metadata}'
                        where email_id = '{email}' and app_access = '{app_id}' and company_name = '{company_name}' and delete_status = 0
                        '''
                    update_response = update_db(update_tag_query)
                    print(f"METADATA HAS BEEN SUCCESSFULLY UPDATED FOR EMAIL {email} : ",update_response)
                if data[email]["action"] == False:
                    select_tag_query = f'''
                    select metadata from {token_optix_schema}.{USER_MANAGEMENT_TABLE} where email_id = '{email}' and app_access = '{app_id}' and company_name = '{company_name}' and delete_status = 0
                    '''
                    metadata = select_db(select_tag_query)
                    print(f"METADATA FOR USER {email} : ",metadata)
                    
                    if metadata[0][0] is None:
                        pass
                    else:
                        metadata = metadata[0][0]
                        if tag_name in metadata:
                            metadata = metadata.replace(f",,,{tag_name}","")
                            update_tag_query = f'''
                            update {token_optix_schema}.{USER_MANAGEMENT_TABLE}
                            set metadata = '{metadata}'
                            where email_id = '{email}' and app_access = '{app_id}' and company_name = '{company_name}' and delete_status = 0
                            '''
                            update_response = update_db(update_tag_query)
                            print(f"METADATA HAS BEEN SUCCESSFULLY UPDATED FOR EMAIL {email} : ",update_response)
                    
            current_time = datetime.now(timezone.utc).isoformat()
            log_insert_query = f"insert into {schema}.{action_logs_table} (email_id,app_id,env,action,last_updated_time) values (%s, %s, %s, %s, %s)"
            values = (done_by, app_id, env,f"TAG {tag_name} Updated.",current_time)
            log_insert_response = insert_db(log_insert_query,values)
            print("LOG INSERTED : ",log_insert_response)     
            return{
                "statusCode" : 200,
                "response" : f"Users has been successfully updated for tag : {tag_name}"
            }
        except Exception as e:
            print("EXCEPTION E : ",e)
            return {
                "statusCode" : 500,
                "response" : f"Error while adding tag to user due to {e}"
            }
    


    if event_type == 'list_apps_company':
        query = f"""
        SELECT 
            company_name, 
            jsonb_agg(jsonb_build_object('app_name', app_name, 'active_app_name', active_app_name, 'app_id', app_id)) AS apps
        FROM 
            {token_optix_schema}.{app_meta_data_table}
        WHERE 
            delete_status = 0 and status = 'DEPLOYED'
        GROUP BY 
            company_name;

        """                
        company_response = select_db(query)
        print(company_response)
        companies = {}
        
        for company in company_response:
            if company[0] is not None:
                companies[company[0]] = company[1]
        
        result = {
            "message" : "success",
            "companies" : companies
        }  

    if event_type == 'add-app':
        status = "NOT_DEPLOYED"
        region_value = 'us-east-1'
        
        integration = event['integration']
        embedding_model = event["embedding_model"]
        app_name =  event['app_name']
        email_id = event['email_id']
        description  = event['description']
        conversational = event['conversational']
        streaming = event['streaming']
        unique_identifier = event['unique_identifier']
        company_name = event['company_name']
        
        model_name = event['model_name']
        model_name= json.loads(model_name)  
        active_model_id = model_name[0]['model_id_name']
        is_agent = event['isAgent']
        
        result = []      
        res1 = []
        query1 = f"select id from {token_optix_schema}.{app_meta_data_table} where app_name ='{app_name}' and delete_status = 0;" 
        res1 = select_db(query1)             
        if res1 == []:
            unique_app_id = ''.join(random.choices(string.ascii_letters + string.digits, k=8))
            query = f'''
            INSERT INTO {token_optix_schema}.{app_meta_data_table}
            (app_id,email_id,app_name, description, created_on, updated_on,delete_status,embedding_model,region,conversational,streaming,status,company_name,integration,unique_identifier,active_model_id,active_pg_model_id,active_app_name,is_agent)
            VALUES( %s,%s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
        '''
            print(query)
            insert_values = (unique_app_id,email_id,app_name, description,0,embedding_model,region_value,int(conversational),int(streaming),status,company_name,integration,unique_identifier,active_model_id,active_model_id,app_name,is_agent)  
            insert_db(query,insert_values)
            model_list = []
            provider_list= []
            model_id_list = []
            for item in model_name:    
                model_list.append(item['model_name'])
                provider_list.append(item['provider'])
                model_id_list.append(item['model_id_name'])   
            for mod,prov,mod_id in zip(model_list,provider_list,model_id_list):
                convo_json = {}
                convo_id = ''.join(random.choices(string.ascii_letters + string.digits, k=10))
                query = f'''
                    INSERT INTO {token_optix_schema}.{model_metadata_table}
                    (app_id, convo_id, model_name, provider, created_on, updated_on, delete_status,app_name,model_id)
                    VALUES(%s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, %s,%s,%s) ;
                    '''
                insert_values =(unique_app_id,convo_id,mod,prov,0,app_name,mod_id)
                insert_db(query,insert_values) 
            
            result = [{'statusCode': 200,'body': 'App created successfully.','app_id':unique_app_id,'app_name':app_name}]            
    
        else:
            re =  {
            'statusCode': 200,
            'body': 'Record already exist.'
            }   
            result.append(re) 
    
    # if event_type=="delete_app":
    #     app_id = event['app_id']
    #     result=[]
        
    #     query = f'''update {token_optix_schema}.{app_meta_data_table} set delete_status = 1 where app_id = '{app_id}';'''
    #     update_db(query)
        
    #     query=f'''update {token_optix_schema}.{model_metadata_table} set delete_status = 1 where app_id = '{app_id}';'''
    #     update_db(query)
        
    #     #delete integration_details
    #     query = f'''update {token_optix_schema}.{app_integration_details_table} set delete_status = 1 where app_id = '{app_id}';'''
    #     update_db(query)
        
    #     #delete subscription details
    #     query = f'''update {token_optix_schema}.{subscription_metadata_table} set delete_status = 1 where app_id = '{app_id}';'''
    #     update_db(query)

    #     #delete in token_details
    #     query =f'''update {token_optix_schema}.{token_details_table} set delete_status = 1 where app_id= '{app_id}';'''
    #     update_db(query)
        
    #     re =  {
    #         'statusCode': 200,
    #         'body': f'deleted successfully.'
    #     }  
    #     result.append(re)   
        
                                                                    
    if event_type == 'insert_token_details':
        convo_id = event['convo_id']
        user_name = event['user_name']
        input_tokens = event['input_tokens']
        output_tokens = event['output_tokens']
        total_cost = event['total_cost']
        result = []

        query = f'''
        INSERT INTO {token_optix_schema}.token_details
        (convo_id, user_name, input_tokens, output_tokens, total_tokens,timestamp,date_time,delete_status,total_cost)
        VALUES(%s,%s, %s, %s, %s,CURRENT_TIMESTAMP,CURRENT_DATE,%s,%s)
        '''
        insert_values = (convo_id, user_name, input_tokens, output_tokens, int(input_tokens) + int(output_tokens),0,total_cost)
     
        insert_db(query, insert_values)
        
        re =  {
            'statusCode': 200,
            'body': f'inserted successfully.'
        }  
        result.append(re)

    
    # if event_type == 'update-app':      
    #     app_name =  event['app_name']
    #     title = event['title']
    #     app_id = event['app_id']
    #     description = event['description']
    #     model_name = event['model_name']
    #     model_name= json.loads(model_name)
    #     new_model_info = [[d["model_id"], d["model_name"], d["provider"],d["model_id_name"]] for d in model_name] # converting list of dict to list of list
    #     #print(new_model_info)
    #     query = f'''select convo_id, model_name, provider, model_id from token_optix.model_metadata where delete_status = 0 and app_id = {app_id};'''
    #     res1 = select_db(query)
    #     res1_list= [list(t) for t in res1] #converting list of tuples to list
    #     #print(res1_list)
        
    #     #adding new models  
    #     for model in new_model_info:
    #         if model not in res1_list:
    #             convo_id = ''.join(random.choices(string.ascii_letters + string.digits, k=10))
    #             #insert in model_metadata
    #             query = f'''
    #                 INSERT INTO token_optix.model_metadata
    #                 (app_id, convo_id, model_name, provider, created_on, updated_on, delete_status,app_name,model_id)
    #                 VALUES(%s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, %s,%s,%s) ;
    #                 '''
    #             insert_values =(app_id,convo_id,model[1],model[2],0,app_name,model[3])  
    #             insert_db(query,insert_values)
                
    #     #removing deleted models
    #     for model in res1_list:
    #         if model not in new_model_info:
    #             #update delete status is 1 in model_metadata
    #             query = f'''update token_optix.model_metadata
    #                         set delete_status = 1 , app_name = '{app_name}'
    #                         where convo_id = '{model[0]}';'''
    #             update_db(query)
    #             #update delete status is 1 in token_details for that convo_id
    #             query = f'''update token_optix.token_details
    #                         set delete_status = 1
    #                         where convo_id = '{model[0]}';'''
    #             update_db(query)
    #         else:
    #             query = f'''update token_optix.model_metadata
    #                         set app_name = '{app_name}'
    #                         where convo_id = '{model[0]}';'''
    #             update_db(query)
    
    #     update_query = f'''
    #         UPDATE token_optix.app_meta_data 
    #         SET app_name = '{app_name}',description = '{description}',title = '{title}'  
    #         WHERE id = {app_id} and delete_status = 0;
    #     '''
    #     update_db(update_query)
        
        
    #     result = []  
    #     re =  {
    #         'statusCode': 200,
    #         'body': f'updated successfully.'
    #     } 
    #     result.append(re)
    
    if event_type == 'update-app':      
        app_id = event['app_id']
        description = event['description']
        app_name = event['app_name']        
        active_app_name = event["active_app_name"]
        model_name = event['model_name']
        model_name= json.loads(model_name)
        email_id = event["email_id"]
        new_model_info = [[d["model_id"], d["model_name"], d["provider"],d["model_id_name"]] for d in model_name] # converting list of dict to list of list

        query = f'''select convo_id, model_name, provider, model_id from {token_optix_schema}.{model_metadata_table} where delete_status = 0 and app_id = '{app_id}';'''
        res1 = select_db(query)
        res1_list= [list(t) for t in res1] #converting list of tuples to list
        #print(res1_list)
        app_name_update_query = f"update {token_optix_schema}.{app_meta_data_table} set active_app_name = '{active_app_name}' where delete_status = 0 and app_id = '{app_id}';"
        app_name_update = update_db(app_name_update_query)
        print("APP NAME UPDATED : ",app_name_update)
        #adding new models  
        for model in new_model_info:
            if model not in res1_list:
                convo_id = ''.join(random.choices(string.ascii_letters + string.digits, k=10))
                #insert in model_metadata
                query = f'''
                    INSERT INTO {token_optix_schema}.{model_metadata_table}
                    (app_id, convo_id, model_name, provider, created_on, updated_on, delete_status,app_name,model_id)
                    VALUES(%s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, %s,%s,%s) ;
                    '''
                insert_values =(app_id,convo_id,model[1],model[2],0,app_name,model[3])  
                insert_db(query,insert_values)
                
        #removing deleted models
        for model in res1_list:
            if model not in new_model_info:
                #update delete status is 1 in model_metadata
                query = f'''update {token_optix_schema}.{model_metadata_table}
                            set delete_status = 1 , app_name = '{app_name}'
                            where convo_id = '{model[0]}';'''
                update_db(query)
                #update delete status is 1 in token_details for that convo_id
                query = f'''update {token_optix_schema}.{token_details_table}
                            set delete_status = 1
                            where convo_id = '{model[0]}';'''
                update_db(query)
            else:
                query = f'''update {token_optix_schema}.{model_metadata_table}
                            set app_name = '{app_name}'
                            where convo_id = '{model[0]}';'''
                update_db(query)
    
        update_query = f'''
            UPDATE {token_optix_schema}.{app_meta_data_table} 
            SET description = '{description}' 
            WHERE app_id = '{app_id}' and delete_status = 0;
        '''
        update_db(update_query)
        current_time = datetime.now(timezone.utc).isoformat()
        log_insert_query = f"insert into {schema}.{action_logs_table} (email_id,app_id,env,action,last_updated_time) values (%s, %s, %s, %s, %s)"
        values = (email_id, app_id, env,f"APP {app_id} Updated.",current_time)
        log_insert_response = insert_db(log_insert_query,values)
        print("LOG INSERTED : ",log_insert_response)     
            
        result = []  
        re =  {
            'statusCode': 200,
            'body': f'updated successfully.'
        } 
        result.append(re)     
        
    
    if event_type=="token_monthwise":
        date_time = event['date_time']
        user_name = event['user_name']
        
        # query = f'''select convo_id, TO_CHAR(cast(date_time as DATE), 'YYYY-MM') as "month" ,sum(total_tokens)
        #             from token_optix.token_details td
        #             WHERE DATE_TRUNC('month', cast(date_time as DATE)) >= DATE_TRUNC('month','{date_time}'::DATE) - interval  '2 months'
        #             and DATE_TRUNC('month', cast(date_time as DATE)) <= DATE_TRUNC('month', '{date_time}'::DATE)
        #             and delete_status = 0
        #             GROUP by convo_id, month;                                                               
        #         '''
        # data = select_db(query)
                                                                            
        # query = f'''select distinct convo_id,app_name from token_optix.model_metadata where delete_status = 0 ;'''
        # re1 = select_db(query)
        
        # app_name_dict = {convo_id: name for convo_id, name in re1}
        # result = []                 
        # for convo_id, month, total_token_used in data:
        #     app_name = app_name_dict.get(convo_id, '')
        #     result.append({'app_name':app_name, 'month':month, 'total_token_used':int(total_token_used)})
        
        query = f'''select amd.app_name ,TO_CHAR(cast(date_time as DATE), 'YYYY-MM') as "month" ,sum(total_tokens)
                    from {token_optix_schema}.token_details td join {token_optix_schema}.app_meta_data amd on td.app_id = amd.app_id
                    WHERE DATE_TRUNC('month', cast(date_time as DATE)) >= DATE_TRUNC('month','{date_time}'::DATE) - interval  '2 months'
                    and DATE_TRUNC('month', cast(date_time as DATE)) <= DATE_TRUNC('month', '{date_time}'::DATE)
                    and td.delete_status = 0 and amd.delete_status = 0
                    GROUP by month, td.app_id,amd.app_name  ;'''
        res1 = select_db(query)
        result = []
        for res in res1:
            result.append({"app_name":res[0],"month":res[1],"total_token_used":res[2]})    
        
            
    if event_type == "daywise_token":
        date_time = event['date_time']
        user_name = event['user_name']
     
        query=f'''select distinct date_time , sum(total_tokens) from
                {token_optix_schema}.token_details td
                WHERE EXTRACT(YEAR FROM date_time) = EXTRACT(YEAR FROM '{date_time}'::DATE)
                AND EXTRACT(MONTH FROM date_time) = EXTRACT(MONTH FROM '{date_time}'::DATE)
                and delete_status = 0
                group by 1 order by date_time;'''
        result = []
        re1 = select_db(query)            
        print(re1)
        if re1 ==[]:
            result.append({'date_time':str(date_time), 'tokens_used':0})                                                                
        else:
            for i in re1:
                result.append({'date_time':str(i[0]), 'tokens_used':int(i[1])})
    
    if event_type == "app_wise_question_count":                     
        date_time = event['date_time']
        user_name = event['user_name']
        query = f'''select convo_id,TO_CHAR(cast(date_time as DATE), 'YYYY-MM') as month,count(input_tokens) as question_count from {token_optix_schema}.token_details  where DATE_TRUNC('month',
                cast(date_time as DATE)) >= DATE_TRUNC('month',
                '{date_time}'::DATE) - interval '2 months'
                and DATE_TRUNC('month',
                cast(date_time as DATE)) <= DATE_TRUNC('month',
                '{date_time}'::DATE) and delete_status = 0 group by 1,2 order by question_count desc;'''
        data = select_db(query)
        query = f'''select distinct convo_id as convo_id,app_name from {token_optix_schema}.model_metadata where delete_status = 0;'''
        re1 = select_db(query)
        app_name_dict = {convo_id: name for convo_id, name in re1}
     
        result = []
        for convo_id, month,question_count in data:
            app_name = app_name_dict.get(convo_id, '')
            result.append({'app_name':app_name,'month':month,'question_count':question_count})
            
    if event_type == "daywise_question_token":
        date_time = event['date_time']
        user_name = event['user_name']
        result = []
        query = f"SELECT DISTINCT date_time,count(input_tokens) FROM {token_optix_schema}.token_details WHERE EXTRACT(YEAR FROM date_time) = EXTRACT(YEAR FROM '{date_time}'::DATE) AND EXTRACT(MONTH FROM date_time) = EXTRACT(MONTH FROM '{date_time}'::DATE) and delete_status = 0 GROUP BY 1 order by date_time desc;"        
        re1 = select_db(query)
        if re1 ==[]:
            result.append({'date_time':str(date_time), 'question_count':0})
           
        else:
            for i in re1:
                result.append({'date_time':str(i[0]), 'question_count':int(i[1])})
    
    if event_type == "model_wise_question_count":
        date_time = event['date_time']
        user_name = event['user_name']
        result =[]
        query = f'''select distinct convo_id,count(input_tokens) as question_count,sum(total_tokens) as total_tokens from {token_optix_schema}.token_details WHERE
            EXTRACT(YEAR FROM date_time) = EXTRACT(YEAR FROM '{date_time}'::DATE)
            AND EXTRACT(MONTH FROM date_time) = EXTRACT(MONTH FROM '{date_time}'::DATE) and delete_status = 0
            group by 1 order by total_tokens desc;'''
        data = select_db(query)
        query = f'''select distinct convo_id as convo_id, model_name from {token_optix_schema}.model_metadata where delete_status = 0;'''
        re1 = select_db(query)
        model_name_dict = {convo_id: name for convo_id, name in re1}
        result1 = []
        for convo_id,question_count, total_tokens in data:
            model_name = model_name_dict.get(convo_id, '')
            result1.append({'model_name':model_name,'question_count':question_count,'total_tokens':total_tokens})
        combined_info = {}
        for entry in result1:
            model_name = entry['model_name']
            question_count = entry['question_count']
            total_tokens = entry['total_tokens']
    
            if model_name not in combined_info:
                combined_info[model_name] = {'question_count': 0, 'total_tokens': 0}
    
            combined_info[model_name]['question_count'] += question_count
            combined_info[model_name]['total_tokens'] += total_tokens
    
        result = [{'model_name': model_name, 'question_count': info['question_count'], 'total_tokens': info['total_tokens']} for model_name, info in combined_info.items()]

    if event_type == "top_user_token":
        user_name = event['user_name']
        year = event['year']
        month = event['month']
        result = []
        query = f'''select user_name, sum(total_tokens) as token_sum from {token_optix_schema}.token_details where extract(month from date_time) = '{month}' and extract(year from date_time) = '{year}'  and delete_status = 0 group by user_name order by token_sum desc limit 5;'''
        res1 = select_db(query)
        for i in res1:
            result.append({"user_id":i[0],"total_tokens":i[1]})
    
    if event_type == "year_total_count":
        year = event['year']
        user_name = event['user_name']
        result = []
        query = f'''select TO_CHAR(cast(date_time as DATE), 'YYYY') as year,TO_CHAR(cast(date_time as DATE), 'MM') as month , sum(total_tokens) as tokens_used from {token_optix_schema}.token_details  where extract(year from date_time) = '{year}' and delete_status = 0 group by 1,2;'''
        res= select_db(query)
        
        for i in res:
            result.append({"year":i[0],"month":i[1],"tokens_used":i[2]})
            
    if event_type == 'list_model_providers':
        region_value = event['region']       
        result_model = []
        res1 = []
        query = f'''select mp.model_name, mp.provider,mp.model_type,mp.actual_model_name from {token_optix_schema}.model_providers as mp  join {token_optix_schema}.region_details as rd on mp.region = rd.region where mp.region = '{region_value}' and rd.region = '{region_value}';'''  
        res1 = select_db(query)
        if res1 != []:
            for res in res1:
                re={}
                re['model_name'] = res[0]
                re['provider'] = res[1]
                re['api_type'] = res[2]
                re['model_id'] = res[3]  
                result_model.append(re)
        else:
            result_model = []  
    
        result_embed = []
        res1 = []
        query = f'''select embedding_provider, embedding_model,actual_model_name from {token_optix_schema}.embedding_model_providers as emp join {token_optix_schema}.region_details as rd on emp.region = rd.region where emp.region = '{region_value}' and rd.region = '{region_value}' ;'''   
        res1 = select_db(query)
        if res1 != []:
            for res in res1:
                re={}
                re['embedding_provider'] = res[0]
                re['embedding_model'] = res[1]
                re['actual_model_name'] = res[2]
                result_embed.append(re)
            
        else:
            result_embed = []
        
        result = {"models": result_model, "embedding_models": result_embed}   
            
    if event_type == 'list_regions':
        result = []
        res1 = []
        query = f'''select region, region_name from {token_optix_schema}.region_details;'''
        res1 = select_db(query)
        if res1 != []:
            for res in res1:
                re = {}
                re['region'] = res[0]
                re['region_name'] = res[1]
                result.append(re)
        else:
            result = []   
            
    if event_type =="list_model":
        app_id = event['app_id']
        result = []
        query = f'''select distinct md.convo_id,  md.model_name,  md.provider, mp.model_type,  md.model_id  from {token_optix_schema}.model_metadata md  join {token_optix_schema}.model_providers mp on mp.model_name = md.model_name where md.app_id = '{app_id}' and md.delete_status = 0;'''
        result1 = select_db(query)
        for res in result1:
            model_json={}
            model_json['convo_id'] = res[0]
            model_json['model_name'] = res[1]
            model_json['provider'] = res[2]
            model_json['model_type'] = res[3]
            result.append(model_json)
            
    if event_type == "list_credit":
        app_id = event['app_id']  
        result = []
        query = f'''select assigned_credits, remaining_credits, subscription_type, start_date, expiry_date 
                    from {token_optix_schema}.subscription_metadata 
                    where app_id = '{app_id}' and delete_status = 0;'''
        result1 = select_db(query)
        if result1 != []:
            for res in result1:
                credit_json = {}
                credit_json['assigned_credits'] = res[0]
                credit_json['remaining_credits'] = res[1]
                credit_json['subscription_type'] = res[2]
                credit_json['start_date'] = str(res[3])
                credit_json['expiry_date'] = str(res[4])
                result.append(credit_json)
        else:
            result.append({'assigned_credits':0,'remaining_credits':0,'subscription_type':'not-specified','start_date':'','expiry_date':''})

#     if event_type == 'recent_prompt':
#         limit = event['limit']
#         page = event['page']
#         drop_down = event['drop_down']
#         text = event['text']
#         result = []
      
#         if drop_down == 'user_name' : 
#             drop_down1 = 'td.' + drop_down
#         else:
#             drop_down1 = 'amd.' + drop_down
            
#         query = f'''select distinct td.user_name, td.user_session, amd.app_name,  max(td.timestamp) as timestamp, sum(cast(tdmd.meta_value as float4)) as total_cost, SUM(CAST(tdmdac.meta_value AS float4)) as api_cost from 
# token_optix.token_details td 
# left join token_optix.app_meta_data amd on td.app_id = amd.id
# left join token_optix.token_details_meta_data tdmd on tdmd.token_details_id = td.id and tdmd.meta_key = 'total_cost'
# left join token_optix.token_details_meta_data tdmdac on tdmdac.token_details_id = td.id and tdmdac.meta_key = 'api_cost'
# where td.user_session != '' and td.delete_status = 0 and amd.delete_status = 0 and CAST({drop_down1}  AS VARCHAR) ilike '%{text}%'
# group by td.user_name, td.user_session, amd.app_name
# order by timestamp  asc
# limit {limit} offset ({page} - 1) * {limit};'''
#         result1 = select_db(query)
#         for i in result1:  
#             r = {'user_name':i[0], 'user_session':i[1],'app_name': i[2],'timestamp':i[3].strftime('%Y-%m-%d %H:%M:%S.%f %z'),'total_cost':i[4],'api_cost':i[5]}
#             result.append(r)
    
    # if event_type == 'pageno_recent':
    #     drop_down = event['drop_down']
    #     text = event['text']
    #     result = []
    #     if drop_down == 'user_name' : 
    #         drop_down1 = 'td.' + drop_down
    #     else:
    #         drop_down1 = 'amd.' + drop_down
    #     query = f'''SELECT COUNT(*) 
    #                 FROM (
    #                     SELECT td.user_session
    #                     FROM token_optix.token_details td
    #                     LEFT JOIN token_optix.app_meta_data amd ON td.app_id = amd.id
    #                     WHERE td.user_session != '' 
    #                     AND td.delete_status = 0 
    #                     AND amd.delete_status = 0
    #                     AND CAST({drop_down1}  AS VARCHAR) ILIKE '%{text}%'
    #                     GROUP BY td.user_session
    #                 ) AS subquery;'''
    #     result1 = select_db(query)
    #     count = result1[0][0]
    #     result.append(count)
    
    if event_type == 'pageno_recent':    
        drop_down = event['drop_down']
        text = event['text']
        result = []
        if drop_down == 'user_name' : 
            drop_down1 = 'td.' + drop_down
        else:
            drop_down1 = 'amd.' + drop_down
        query = f'''select COUNT(*)
                    from token_optix.token_details as td join token_optix.app_meta_data as amd on td.app_id = amd.app_id
                    where td.delete_status = 0 and CAST('{drop_down}' AS VARCHAR) ILIKE '%{text}%' and amd.delete_status = 0;'''
        result1 = select_db(query)
        count = result1[0][0]
        result.append(count)
    
    if event_type == 'recent_prompt':
        limit = event['limit']
        page = event['page']
        drop_down = event['drop_down']
        text = event['text']
        result = []
      
        if drop_down == 'user_name' : 
            drop_down1 = 'td.' + drop_down
        else:
            drop_down1 = 'amd.' + drop_down
            
        query = f'''select td."timestamp" , amd.app_name , td.user_name , td.total_cost, td.convo_id, td.id
                    from token_optix.token_details as td join token_optix.app_meta_data as amd on td.app_id = amd.app_id
                    where td.delete_status = 0 and amd.delete_status = 0 and CAST('{drop_down}' AS VARCHAR) ILIKE '%{text}%'
                    order by timestamp  desc
                    limit {limit} offset ({page} - 1) * {page};'''
        
        result1 = select_db(query)
        for i in result1:  
            r = {'user_name':i[2],'app_name': i[1],'timestamp':i[0].strftime('%Y-%m-%d %H:%M:%S.%f %z'),'total_cost':i[3],'convo_id':i[4],'id':i[5]}     
            result.append(r)
        
#     if event_type == 'get_token_details':
#         id = event['id']
#         api_type = event['api_type']
#         if api_type == 'llm' :
#             query = f'''select distinct tdmdit.meta_value as input_tokens, tdmdot.meta_value as output_tokens, mm.model_name, tdmdq.meta_value as question , tdmda.meta_value as answer
# from token_optix.token_details td 
# left join token_optix.model_metadata mm on mm.convo_id = td.convo_id
# left join token_optix.token_details_meta_data tdmdit on tdmdit.token_details_id = td.id and tdmdit.meta_key = 'input_tokens'
# left join token_optix.token_details_meta_data tdmdot on tdmdot.token_details_id = td.id and tdmdot.meta_key = 'output_tokens'
# left join token_optix.token_details_meta_data tdmdq on tdmdq.token_details_id = td.id and tdmdq.meta_key = 'question'
# left join token_optix.token_details_meta_data tdmda on tdmda.token_details_id = td.id and tdmda.meta_key = 'answer'   
# where td.user_session = '{id}' and td.api_type = 'llm' group by 1,2,3,4,5;'''
#             result1 = select_db(query)
#             print(len(result1))
#             result = []
#             for i in result1:
#                 input_text = i[3].strip() if i[3] is not None else ''
#                 input_tokens = i[0]
#                 output_text = i[4].strip() if i[4] is not None else ''
#                 output_tokens = i[1]
#                 model_name = i[2]
#                 input_data = {'input': input_text, 'input_tokens': input_tokens, 'model_name': model_name}
#                 output_data = {'output': output_text, 'output_tokens': output_tokens, 'model_name': model_name}
#                 result.append(input_data)
#                 result.append(output_data)
#         else :
#             query = f'''select SUM(CAST(REPLACE(tdmdpg.meta_value, ',', '.') AS decimal(10, 2))) as pg_count,
#                         SUM(CAST(REPLACE(tdmdcc.meta_value, ',', '.') AS decimal(10, 2))) as char_count,
#                         mm.model_name
#                         from token_optix.token_details td 
#                         left join token_optix.model_metadata mm on mm.convo_id = td.convo_id
#                         left join token_optix.token_details_meta_data tdmdpg on tdmdpg.token_details_id = td.id and tdmdpg.meta_key = 'pg_count'
#                         left join token_optix.token_details_meta_data tdmdcc on tdmdcc.token_details_id = td.id and tdmdcc.meta_key = 'char_count'
#                         where td.user_session = '{id}' and td.api_type != 'llm' group by mm.model_name'''
#             result1 = select_db(query)
#             result = []
#             for i in result1:
#                 page_count = i[0]
#                 char_count = i[1]
#                 model_name = i[2]  
#                 input_data = {'page_count': page_count, 'char_count': char_count, 'model_name': model_name}
#                 result.append(input_data)
    
    if event_type == 'get_token_details':
        convo_id = event['convo_id']
        id_value = int(event['id_value'])
        query = f'''select question, answer, input_tokens, output_tokens
                    from token_optix.token_details 
                    where delete_status = 0  and convo_id = '{convo_id}' and id = {id_value};'''
        res1 = select_db(query)
        query = f'''select model_name from token_optix.model_metadata where delete_status = 0 and convo_id = '{convo_id}';'''
        res2 = select_db(query) 
        result = {"input":res1[0][0],"output":res1[0][1],"input_tokens":res1[0][2],"output_tokens":res1[0][3],"model_name":res2[0][0]}      
        
        
    if event_type  == 'token_calculation':
        api_type= event['api_type']
        if api_type=='llm':
            provider = event['provider']
            model_name = event['model_name']
            input_tokens = int(event['input_tokens'])
            output_tokens =  int(event['output_tokens'])               
            query = f"select prompt_input,completion_output from token_optix.model_providers where model_name ='{model_name}' and provider = '{provider}' and model_type='{api_type}';"
            print(query)
            query_result = select_db(query)
            input_cost = query_result[0][0]
            output_cost = query_result[0][1]
            final_response  = calculate_cost(input_tokens,output_tokens,input_cost,output_cost)
            total_cost = '{:.4f}'.format(final_response / 1000)
            return {"total_cost":total_cost}
        
        if api_type=='page':
            provider = event['provider']
            model_name = event['model_name']
            api_type = event['api_type']
            page=event['page']            
            query =f'''select
                            a.prompt_input,
                            a.completion_output,
                            a.model_api_type,
                            b.value
                        from
                            token_optix.model_providers a 

                            right join token_optix.page_max_value b on a.model_api_type = b.model_api_type
                            where
                            a.model_name = '{model_name}'
                            and a.provider = '{provider}'
                            and a.model_type='{api_type}';'''
            query_result = select_db(query)
            input_cost = query_result[0][0]
            output_cost = query_result[0][1]
            model_api_type=query_result[0][2]
            value=query_result[0][3]
        
            query1 = f"select SUM(CAST(REPLACE(tdmd.meta_value, ',', '.') AS decimal(10, 2))) AS total_value  from token_optix.token_details td left join token_optix.token_details_meta_data tdmd on tdmd.token_details_id = td.id and tdmd.meta_key = 'pg_count' where td.model_api_type='{model_api_type}';"
            total_pages = select_db(query1)[0][0] or 0
            if total_pages < int(value) :
                cost = calculate_cost(int(page),0,input_cost,0)
            else:
                cost = calculate_cost(0,int(page),output_cost,0)

            return {"total_cost":'{:.4f}'.format(cost)}
            
        if api_type=='character':
            provider = event['provider']
            model_name = event['model_name']
            input_character=event['input_character']
            output_character = event['output_character']
            
            query = f"select prompt_input,completion_output from token_optix.model_providers where model_name ='{model_name}' and provider = '{provider}' and model_type='character';"
            print(query)
            query_result = select_db(query)
            input_cost = query_result[0][0]
            output_cost = query_result[0][1]

            final_response  = cost = calculate_cost(int(input_character), int(output_character), input_cost, output_cost)  
            res = '{:.4f}'.format(final_response)
            return {"total_cost":res}
    
    if event_type=="price_monthwise":
        date_time = event['date_time']
        user_name = event['user_name']
     
        #query = f'''select sum(cast(meta_value as numeric)) as total, app_name, TO_CHAR(cast(td.date_time as DATE), 'YYYY-MM') from token_optix.token_details as td join token_optix.token_details_meta_data tdmd on td.id = tdmd.token_details_id join token_optix.app_meta_data as md on td.app_id = md.id where (meta_key = 'api_cost' or meta_key = 'total_cost') and extract(year from date_time) = extract(year from '{date_time}'::DATE) and td.delete_status = 0 and extract(month from date_time) = extract(month from '{date_time}'::DATE) group by 2,3;'''
        query = f'''   
        select sum(total_cost) as total, app_name, TO_CHAR(cast(td.date_time as DATE), 'YYYY-MM')
        from token_optix.token_details as td
        join token_optix.app_meta_data as md on td.app_id = md.app_id
        where extract(year from date_time) = extract(year from '{date_time}'::DATE) and td.delete_status = 0
        and extract(month from date_time) = extract(month from '{date_time}'::DATE) group by 2,3;
        '''
        data = select_db(query)     
     
        result = []         
        for i in data:                                      
            result.append({'app_name':i[1], 'month':i[2], 'total_cost':i[0]})
            
            
    if event_type == "application_count":
        result = []
        user_id = event['email_id']                                        
        date_string = event['date_time']                                                           
        # query = f'''select sum (cast(meta_value as numeric)) as total_cost from token_optix.token_details td join token_optix.token_details_meta_data tdmd on td.id = tdmd.token_details_id where (meta_key = 'total_cost' or meta_key = 'api_cost') and EXTRACT(YEAR FROM date_time) = EXTRACT(YEAR FROM '{date_string}'::DATE) AND EXTRACT(MONTH FROM date_time) = EXTRACT(MONTH FROM '{date_string}'::DATE) 
        # '''
        query = f'''SELECT COALESCE(SUM(total_cost), 0) AS total_cost 
                    FROM token_optix.token_details 
                    WHERE delete_status = 0 
                    AND EXTRACT(YEAR FROM date_time) = EXTRACT(YEAR FROM '{date_string}'::DATE) AND EXTRACT(MONTH FROM date_time) = EXTRACT(MONTH FROM '{date_string}'::DATE);'''
        res1 = select_db(query)
        for i in res1:                  
            result.append({"cost_for_month":i[0]})   
        
    if event_type == "pie_chart":
        result = []                                                                                                
        user_id = event['email_id']                                                                
        date_string = event['date_time']                                                                                                   
       
        # query = f'''SELECT distinct model_name, SUM(CAST(tdmd.meta_value  AS decimal)) as modelwise_cost
        #             FROM token_optix.model_metadata mm join token_optix.token_details td on mm.convo_id = td.convo_id  join token_optix.token_details_meta_data tdmd on td.id = tdmd.token_details_id 
        #             WHERE EXTRACT(YEAR FROM td.date_time) = EXTRACT(YEAR FROM '{date_string}'::DATE)
        #             AND EXTRACT(MONTH FROM td.date_time) = EXTRACT(MONTH FROM '{date_string}'::DATE) and tdmd.meta_key in ('total_cost','api_cost') and mm.delete_status = 0 and td. delete_status = 0
        #             group by mm.model_name;''' 
        
        query = f'''
                    SELECT distinct model_name, SUM(total_cost) as modelwise_cost
                    FROM token_optix.model_metadata mm join token_optix.token_details td
                    on mm.convo_id = td.convo_id
                    WHERE EXTRACT(YEAR FROM td.date_time) = EXTRACT(YEAR FROM '{date_string}'::DATE)
                    AND EXTRACT(MONTH FROM td.date_time) = EXTRACT(MONTH FROM '{date_string}'::DATE)   
                    and mm.delete_status = 0
                    and td. delete_status = 0
                    group by mm.model_name;'''
        res = select_db(query)
        for i in res:
            res1 = {"model_name": i[0], "cost": i[1]}
            result.append(res1)
            
    if event_type == "table":                                       
        result = []
        date_string = event['date_time']
        user_id = event['email_id']
        query = f'''select amd.app_name, count(td.question),sum(td.total_cost)
                    from token_optix.token_details td join token_optix.app_meta_data amd on td.app_id = amd.app_id 
                    where amd.delete_status = 0 and td.delete_status = 0 and EXTRACT(YEAR FROM td.date_time) = EXTRACT(YEAR FROM '{date_string}' :: DATE)
                    AND EXTRACT(MONTH FROM td.date_time) = EXTRACT(MONTH FROM '{date_string}' :: DATE)
                    group by amd.app_name ;'''  

    #     query = f'''SELECT
    #                 amd.app_name,
    #                 COUNT(CASE WHEN tdmd.meta_key = 'question' THEN 1 ELSE NULL END) AS question_count,
    #                 SUM(CASE WHEN tdmd.meta_key = 'total_cost' THEN CAST(tdmd.meta_value AS DECIMAL) ELSE 0 END) AS total_cost_count
    #                 FROM
    #                 token_optix.app_meta_data AS amd
    #                 JOIN
    #                 token_optix.model_metadata AS mm ON mm.app_id = amd.id
    #                 JOIN
    #                 token_optix.token_details AS td ON td.convo_id = mm.convo_id
    #                 JOIN
    #                 token_optix.token_details_meta_data AS tdmd ON td.id = tdmd.token_details_id 
    #                 WHERE
    #                 amd.delete_status = 0
    #                 AND EXTRACT(YEAR FROM td.date_time) = EXTRACT(YEAR FROM '{date_string}' :: DATE)
    #                 AND EXTRACT(MONTH FROM td.date_time) = EXTRACT(MONTH FROM '{date_string}' :: DATE)              
    #                 AND tdmd.meta_key in ('question', 'total_cost')                               
    #                 AND td.delete_status = 0
    #                 AND mm.delete_status = 0
    #                 GROUP BY
    #                 amd.app_name;       
    # '''     
        res = select_db(query) 
        result = [{"app_name": item[0], "question_count": item[1], "total_cost_count": item[2]} for item in res]     
          
    if event_type == "daily_data":
        result = []
        date_string = event['date_time']                                                                      
        # query = f'''SELECT td.convo_id,
        #         tm.app_name,
        #         tm.model_name,
        #         TO_CHAR(td.date_time, 'YYYY-MM-DD') AS formatted_date,
        #         SUM(CAST(meta_value AS numeric)) AS total_cost
        #     FROM
        #         token_optix.token_details td join token_optix.token_details_meta_data tdmd on td.id = tdmd.token_details_id 
        #     INNER JOIN
        #         token_optix.model_metadata tm ON td.convo_id = tm.convo_id
        #     where
        #     (meta_key = 'api_cost' or meta_key = 'total_cost')
        #         and EXTRACT(MONTH FROM td.date_time) = EXTRACT(MONTH FROM '{date_string}'::date)
        #         AND EXTRACT(YEAR FROM td.date_time) = EXTRACT(YEAR FROM '{date_string}'::date)
        #         AND td.delete_status = 0 
        #         and tm.delete_status = 0        
        #     GROUP BY 1, 2, 3, 4
        #     ORDER BY 4;
        # '''
        query = f'''select amd.app_name, sum(td.total_cost), td.date_time    
                    from token_optix.token_details td join token_optix.app_meta_data amd on td.app_id = amd.app_id
                    where amd.delete_status = 0 and td.delete_status = 0 and EXTRACT(MONTH FROM td.date_time) = EXTRACT(MONTH FROM '{date_string}'::date)
                    AND EXTRACT(YEAR FROM td.date_time) = EXTRACT(YEAR FROM '{date_string}'::date)
                    group by td.date_time,amd.app_name;'''          
        res = select_db(query)                                          
        result = {                                          
            "data": [                                                               
                    {
                        "app_name": item[0],  
                        "date_time": str(item[2]),                                                     
                        "total_cost": '{:.4f}'.format(float(item[1]))    # Convert Decimal to float   
                    } 
                    for item in res
                ]
            }
    
    if event_type=="api_token_monthwise":       
        date_time = event['date_time']
        user_name = event['user_name']
     
        query = f'''SELECT 
                    td.convo_id,TO_CHAR(cast(td.date_time as DATE), 'YYYY-MM') as month,
                    COALESCE(SUM(CAST(REPLACE(tdmd.meta_value, ',', '.') AS DECIMAL(10, 2))), 0) as pg_count,
                    COALESCE(SUM(CAST(REPLACE(tdmdcc.meta_value, ',', '.') AS DECIMAL(10, 2))), 0) as char_count
                    FROM  token_optix.token_details td
                    left join token_optix.token_details_meta_data tdmd on tdmd.token_details_id = td.id and tdmd.meta_key ='pg_count'
                    left join token_optix.token_details_meta_data tdmdcc on tdmdcc.token_details_id = td.id and tdmdcc.meta_key ='char_count'
                    WHERE DATE_TRUNC('month', cast(td.date_time as DATE)) >= DATE_TRUNC('month', '{date_time}'::DATE) - interval '2 months'
                    and DATE_TRUNC('month', cast(td.date_time as DATE)) <= DATE_TRUNC('month', '{date_time}'::DATE)       
                    and td.delete_status = 0  
                    AND tdmd.meta_value IS NOT NULL
                    GROUP by td.convo_id, month 
                    ORDER BY pg_count desc;
                '''             
        data = select_db(query)
     
        query = f'''select distinct convo_id,app_name from token_optix.model_metadata where delete_status = 0 ;'''
        re1 = select_db(query)
       
        app_name_dict = {convo_id: name for convo_id, name in re1}
        result = []                 
        for convo_id, month, pg_count,char_count in data:
            app_name = app_name_dict.get(convo_id, '')
            result.append({'app_name':app_name, 'month':month, 'total_pg_count':pg_count,'total_char_count':char_count})
    
    if event_type == "api_top_user_token":
        user_name = event['user_name']
        year = event['year']
        month = event['month']
        result = []
        query = f'''select user_name, sum(pg_count) as pg_count, sum(char_count) as char_count from token_optix.token_details where extract(month from date_time) = '{month}' and extract(year from date_time) = '{year}'  and delete_status = 0  group by user_name order by pg_count desc limit 5;'''
        res1 = select_db(query)
        for i in res1:      
            result.append({"user_id":i[0],"total_pg_count":i[1],"total_char_count":i[2]})
            
    if event_type == "api_year_total_count":
        year = event['year']
        user_name = event['user_name']
        result = []
        # query = f'''
        # select 
        # TO_CHAR(cast(td.date_time as DATE), 'YYYY') as year,
        # TO_CHAR(cast(td.date_time as DATE), 'MM') as month , 
        # COALESCE(SUM(CAST(REPLACE(tdmd.meta_value, ',', '.') AS DECIMAL(10, 2))), 0) as pg_count,
        # COALESCE(SUM(CAST(REPLACE(tdmdcc.meta_value, ',', '.') AS DECIMAL(10, 2))), 0) as char_count,
        # count(CAST(REPLACE(tdmd.meta_value, ',', '.') AS decimal(10, 2))) as count
        # from 
        # token_optix.token_details td
        # left join token_optix.token_details_meta_data tdmd on tdmd.token_details_id = td.id and tdmd.meta_key ='pg_count'
        # left join token_optix.token_details_meta_data tdmdcc on tdmdcc.token_details_id = td.id and tdmdcc.meta_key ='char_count'
        # where extract(year from date_time) = '{year}'   
        # and td.delete_status = 0  
        # group by 1,2;'''
        query = f'''
                select TO_CHAR(cast(td.date_time as DATE), 'YYYY') as year,
                TO_CHAR(cast(td.date_time as DATE), 'MM') as month , count(*)
                from token_optix.token_details td
                where extract(year from date_time) = '{year}'   
                and td.delete_status = 0  
                group by 1,2;'''   
 
        res= select_db(query)
        for i in res:
            result.append({"year":i[0],"month":i[1],"call_count":int(i[2])})              
            
    if event_type == "api_model_wise_question_count":
        date_time = event['date_time']
        user_name = event['user_name']
        result =[]
        
        query = f'''select model_name, count(*) from token_optix.token_details td
                    join token_optix.model_metadata mm
                    on td.convo_id = mm.convo_id
                    where td.delete_status =0
                    and mm.delete_status =0
                    and DATE_TRUNC('month', cast(date_time as DATE)) = DATE_TRUNC('month','{date_time}'::DATE)
                    and DATE_TRUNC('year', cast(date_time as DATE)) = DATE_TRUNC('year','{date_time}'::DATE)
                    group by model_name
                    order by 2;'''    
        data = select_db(query)
        # query = f'''select distinct convo_id as convo_id, model_name from token_optix.model_metadata where delete_status = 0;'''
        # re1 = select_db(query)
        # model_name_dict = {convo_id: name for convo_id, name in re1}
        result1 = []            
        for model_name, call_count in data:
            # model_name = model_name_dict.get(convo_id, '')
            result1.append({'model_name':model_name,'call_count':call_count})
        combined_info = {}
        for entry in result1:
            model_name = entry['model_name']   
            call_count = entry['call_count']
            if model_name not in combined_info:
                combined_info[model_name] = {'call_count': 0}
    
            combined_info[model_name]['call_count'] += call_count
            
            result = [{'model_name': model_name, 'call_count': int(info['call_count'])} for model_name, info in combined_info.items()]          
    
    if event_type == 'app_details':  
        result = {}
        app_id = event['app_id']
        query = f'''select app_name,description,embedding_model,region,conversational,streaming,app_id,integration,unique_identifier,company_name,active_app_name  from {token_optix_schema}.{app_meta_data_table} where app_id = '{app_id}' and delete_status = 0;'''
        res1 = select_db(query)
        result['app_name'] = res1[0][0]
        result['description'] = res1[0][1]
        result['embedding_model'] = res1[0][2]
        result['region'] = res1[0][3]
        result['conversational'] = res1[0][4]
        result['streaming'] = res1[0][5] 
        result['app_id'] = res1[0][6]   
        result['integration'] = res1[0][7]
        result['company_name'] = res1[0][9]
        result['unique_identifier'] = res1[0][8]
        result['active_app_name'] = res1[0][10]
        query = f'''select model_name, provider, model_id, convo_id from {token_optix_schema}.{model_metadata_table} where app_id = '{app_id}' and delete_status = 0;'''
        res2 = select_db(query)
        models = []
        for res in res2:
            models_json = {}
            models_json['model_name'] = res[0]
            models_json['provider'] = res[1]
            models_json['model_id'] = res[2]
            models_json['convo_id'] = res[3]      
            models.append(models_json)
        result['model_info'] = models
            
    if event_type == 'list_integration_types':
        query = f'''select json_agg(row_to_json(row_values))FROM
                    (select integration_type,access_id from {token_optix_schema}.{integration_details_table})row_values;'''      
        res = select_db(query)[0][0]   
        result = []
        if res != []:
            re = {
                "statusCode":200,
                "body":res
            }
            result.append(re)
        else:
            re = {
                "statusCode":200,
                "body":res
            }
            result.append(re)
            
    if event_type == 'app_unique_test':   
        app_name = event['app_name']
        query = f'''select app_name from token_optix.app_meta_data where delete_status = 0;'''
        res = select_db(query)
        result = {}
        if res == []:
            result = {"statusCode":200,"body":"True"}
        else:
            result = {"statusCode":200,"body":"False"}
    
    if event_type == 'list_app_integrations':
        query = f'''select app_name, app_id from {token_optix_schema}.{app_meta_data_table}  where delete_status = 0 '''
        app_details = select_db(query)
        result = []
        for app in app_details:
            app_json = {}
            app_json['app_name'] = app[0]
            app_json['app_id'] = app[1]
            query = f'''select json_agg(row_to_json(row_values))FROM (select integration_type, access_id, chat_access, integration_id from {token_optix_schema}.{app_integration_details_table} where delete_status = 0 and app_id = '{app[1]}')row_values;'''
            integration_values = select_db(query)[0][0]
            app_json['integration_values'] = integration_values
            result.append(app_json)   
    
    if event_type == 'list_all_apps':
        email_id = event["email_id"]
        select_query = f"SELECT company_name from {token_optix_schema}.{USER_MANAGEMENT_TABLE} where email_id = '{email_id}' and delete_status = 0;"
        company_name_response = select_db(select_query)
        company_name = company_name_response[0][0]
        print("COMPANY AME : ",company_name)
        if company_name == "1CloudHub":
            query = f'''select json_agg(row_to_json(row_values))FROM (select app_name, app_id, active_app_name from  {token_optix_schema}.{app_meta_data_table} amd where delete_status = 0 and status = 'DEPLOYED')row_values;'''
        else:
            query = f'''select json_agg(row_to_json(row_values))FROM (select app_name, app_id, active_app_name from  {token_optix_schema}.{app_meta_data_table} amd where delete_status = 0 and company_name = '{company_name}' and status = 'DEPLOYED')row_values;'''
        res  = select_db(query)[0][0]
        result = {"statusCode":200,"body":res}
    
    if event_type == "list_user_details":    
        user_unique_id = event['user_unique_id']
        user_type = event['user_type']
        if user_type == 'super_admin':
            query = f'''select json_agg(row_to_json(row_values))   
                        from
                        (select user_unique_id, "User_type", user_name,email_id,"Access_type",config_access,prod_access,dev_access,app_access,app_name from {token_optix_schema}.{USER_MANAGEMENT_TABLE} where delete_status = 0 and  user_unique_id = '{user_unique_id}' and "User_type" = '{user_type}')row_values;'''  
            res = select_db(query)[0][0][0]                         
            result = {"statusCode": 200,"body":res}
        elif user_type in ['user', 'admin']:
            query = f'''select json_agg(row_to_json(row_values))
                        from(select DISTINCT user_unique_id, "User_type", user_name,email_id, company_name from {token_optix_schema}.{USER_MANAGEMENT_TABLE} where delete_status = 0 and user_unique_id = '{user_unique_id}' )row_values;'''
            res = select_db(query)[0][0][0]
            query = f'''select json_agg(row_to_json(row_values))
                        from(select "Access_type",config_access,prod_access,dev_access,app_access,app_name from {token_optix_schema}.{USER_MANAGEMENT_TABLE} where delete_status = 0 and user_unique_id = '{user_unique_id}' )row_values;'''
            res1 = select_db(query)[0][0]
            result = {} 
            result['statusCode'] = 200
            result['body'] = res
            print("result",result)
            print("res1:",res1)    
            result['body']['app_access_details'] = res1
        else:
            query = f'''select json_agg(row_to_json(row_values))
                        from(select DISTINCT user_unique_id, "user_type" as "User_type", user_name FROM {token_optix_schema}.{BOT_USER_MANAGEMENT_TABLE}
                        WHERE "user_type" = '{user_type}' 
                            AND delete_status = 0
                            AND user_unique_id = '{user_unique_id}')row_values;'''
            res = select_db(query)[0][0][0]  
            query = f'''select json_agg(row_to_json(row_values))  
                        from(select  distinct app_access as app_id, access_type, app_name FROM {token_optix_schema}.{BOT_USER_MANAGEMENT_TABLE}
                        WHERE "user_type" = '{user_type}'          
                        AND delete_status = 0
                        AND user_unique_id = '{user_unique_id}')row_values;'''   
            res1 = select_db(query)[0][0]  
            query = f'''select json_agg(row_to_json(row_values))  
                        from(select app_access as app_id, user_id as access_value ,integration_type FROM {token_optix_schema}.{BOT_USER_MANAGEMENT_TABLE}
                        WHERE "user_type" = '{user_type}'        
                        AND delete_status = 0
                        AND user_unique_id = '{user_unique_id}')row_values;'''   
            res2 = select_db(query)[0][0] 
            result = {}
            result['statusCode'] = 200
            result['body'] = res
            
            print("res1:",res1)   
            print("res2",res2)       
            result['body']['app_access_details'] = res1
            
            for app_access in result['body']['app_access_details']:
                integration_details = [
                    {k: v for k, v in integration.items() if k != 'app_id'}
                    for integration in res2 if integration['app_id'] == app_access['app_id']
                ]
                app_access['integration_details'] = integration_details
                        
            print("result",result)
    
    if event_type == 'user_unique_email_test':
        email_id = event['email_id']
        query = f'''select * from {token_optix_schema}.{USER_MANAGEMENT_TABLE} where email_id='{email_id}' and delete_status = 0;'''
        res = select_db(query)
        if res == []:
            result = {"status":200,"body":"True"}
        else:
            result = {"status":200,"body":"False"}
    
    if event_type == 'update_user':   
        try:
            user_type = event["user_type"]
            user_name = event["user_name"]
            user_unique_id = event["user_unique_id"]
        
        
            if user_type == "super_admin" or user_type == "admin":
                email_id = event["email_id"]
                access_type = event["access_type"]
        
                update_query = f'''
                        UPDATE {token_optix_schema}.{USER_MANAGEMENT_TABLE}
                        SET  user_name = '{user_name}'
                        WHERE delete_status = 0 AND user_unique_id = '{user_unique_id}'
            '''
                update_db(update_query)
        
            if user_type == "bot_user_old":
                apps = []
                for appDetails in event["app_access_details"]:
                    integrations = []
                    access_type = appDetails["access_type"]
                    app_name = appDetails["app_name"]
                    app_id = appDetails["app_id"]
                    apps.append(app_id)
                    for integrationDetails in appDetails["integration_details"]:
                            integration_type = integrationDetails["integration_type"]
                            access_id = integrationDetails["access_id"]
                            access_value = integrationDetails["access_value"]
                            integration_id = integrationDetails["integration_id"]
                            integrations.append(integration_type)
                            select_query = f'''SELECT user_id from {token_optix_schema}.{BOT_USER_MANAGEMENT_TABLE}
                            WHERE app_access = '{app_id}' AND user_unique_id = '{user_unique_id}' AND integration_type = '{integration_type}' AND delete_status = 0;'''
                            select_response = select_db(select_query)
                            print("SELECT RESPONSE FOR BOT USER : ",select_response)
                            if select_response != []:
                                if select_response[0][0] == access_value:
                                    print(f"USER ALREADY EXSISTS FOR INTEGRATION TYPE {integration_type} WITH USER ID {user_unique_id} IN THE NAME {user_name}")
                                    update_query = f''' UPDATE {token_optix_schema}.{BOT_USER_MANAGEMENT_TABLE}
                                    SET  user_name = '{user_name}',
                                        access_type = '{access_type}'
                                    WHERE user_unique_id = '{user_unique_id}' AND app_access = '{app_id}' AND integration_type = '{integration_type}' AND delete_status = 0;
                                '''
                                    update_db(update_query)
                                    print(f"DB UPDATED FOR THE USER {user_name} WITH USER ID {user_unique_id} WITH INTEGRATION TYPE {integration_type}")
                            else:
                                query = f'''INSERT INTO {token_optix_schema}.{BOT_USER_MANAGEMENT_TABLE}
                                (user_id, access_type, created_at, delete_status, user_name, user_type, app_access, user_unique_id, app_name, user_id_type, integration_id,integration_type)
                                VALUES(%s, %s, CURRENT_TIMESTAMP, 0, %s, %s, %s,%s, %s, %s, %s,%s);'''   
                                values = (access_value,access_type,user_name,user_type,app_id,user_unique_id,app_name,access_id,integration_id,integration_type)
                                result = insert_db(query, values)
                                print(f"BOT USER {user_name} WITH USER ID {user_unique_id} CREATED WITH INTEGRATION TYPE {integration_type} FOR THE APP {app_id} {app_name}")
                    # DELETE
                    print("DELETING LOOP")
                    select_query = f''' SELECT integration_type FROM {token_optix_schema}.{BOT_USER_MANAGEMENT_TABLE}
                                    WHERE app_access = '{app_id}' AND user_unique_id = '{user_unique_id}' AND delete_status = 0;  
                '''
                    select_response = select_db(select_query)
                    print(f"SELECTED INTEGRATIONS FOR USER {user_unique_id} IN APP {app_id}",select_response)
                    deleted_integrations = [item for item in [item[0] for item in select_response] if item not in integrations]
                    print(f"INTEGRATION IN REQUEST FOR {user_unique_id} IN APP {app_id}",integrations)
                    print(f"DELETING INTEGRATIONS {deleted_integrations}")
                    for deleteIntegration in deleted_integrations:
                        delete_query = f'''
                        UPDATE {token_optix_schema}.{BOT_USER_MANAGEMENT_TABLE}
                        SET delete_status = 1
                        WHERE app_access = '{app_id}' AND user_unique_id = '{user_unique_id}' AND integration_type = '{deleteIntegration}' AND delete_status = 0;
                '''
                        update_db(delete_query)
                        print(f"ROW DELETED FOR THE USER {user_name} WITH USER ID {user_unique_id} WITH INTEGRATION TYPE {deleteIntegration}")
                select_query = f''' SELECT DISTINCT app_access FROM {token_optix_schema}.{BOT_USER_MANAGEMENT_TABLE}
                                WHERE user_unique_id = '{user_unique_id}'    
                '''
                select_response = select_db(select_query)
                print(f"APP ACCESS ALLOWED FOR USER {user_unique_id}",select_response)
                deleted_apps = [item for item in [item[0] for item in select_response] if item not in apps]
                print(f"UPDATED APP ACCESS ALLOWED FOR USER {user_unique_id}",deleted_apps)
                
    
                for deleteApp in deleted_apps:
                    select_query = f''' SELECT integration_type FROM {token_optix_schema}.{BOT_USER_MANAGEMENT_TABLE}
                                    WHERE app_access = '{deleteApp}' AND user_unique_id = '{user_unique_id}' AND delete_status = 0;   
                '''
                    select_response = select_db(select_query)
                    print(f"INTEGRATIONS ALLOWED FOR APP {deleteApp} FOR USER {user_unique_id}",select_response)
                    for integration in select_response:
                        delete_query = f''' 
                        UPDATE {token_optix_schema}.{BOT_USER_MANAGEMENT_TABLE}
                        SET delete_status = 1
                        WHERE app_access = '{deleteApp}' AND user_unique_id = '{user_unique_id}' AND integration_type = '{integration[0]}' AND delete_status = 0;
                '''
                        update_db(delete_query)
                    print(f"ROW DELETED FOR THE USER {user_name} WITH USER ID {user_unique_id} FOR APP {deleteApp}")
                    
            if user_type == "user":                                                 
                user_type = event["user_type"]
                email_id = event["email_id"]
                apps = []
                for details in event['app_access_details']:
                    print("APP DETAILS : ",details)
                    app_name = details["app_name"]
                    app_id = details["app_id"]      
                    apps.append(app_id)
                    app_role = details["app_role"]
                    select_query = f'''SELECT email_id from {token_optix_schema}.{USER_MANAGEMENT_TABLE}
                            WHERE app_access = '{app_id}' AND user_unique_id = '{user_unique_id}' AND delete_status = 0;'''
                    select_response = select_db(select_query)
                    print("SELECT RESPONSE FOR USER : ",select_response)
                    if select_response != []:
                        if select_response[0][0] == email_id:
                            print(f"USER ALREADY EXSISTS WITH USER ID {user_unique_id} IN THE NAME {user_name}")
                            update_query = f''' UPDATE {token_optix_schema}.{USER_MANAGEMENT_TABLE}
                                    SET  user_name = '{user_name}',
                                        "User_type" = '{app_role}'
                                    WHERE user_unique_id = '{user_unique_id}' AND app_access = '{app_id}' AND delete_status = 0;
                                '''
                            update_db(update_query)
                        
                            print(f"DB UPDATED FOR THE USER {user_name} WITH USER ID {user_unique_id} AND WITH APP ACCESS {app_id}")
                    else:
                        print(f"USER DOES NOT EXSISTS WITH USER ID {user_unique_id} IN THE NAME {user_name} FOR THE APP {app_id}")
                        insert_query = f'''insert into {token_optix_schema}.{USER_MANAGEMENT_TABLE}(email_id, delete_status,user_name, "User_type",created_date,app_access,user_unique_id,app_name) values (%s,0, %s, %s,NOW(), %s,%s,%s)'''
                        values = (email_id,user_name,app_role,app_id,user_unique_id,app_name)
                        result = insert_db(insert_query, values)
                        print(f"USER {user_name} ADDED WITH USER ID {user_unique_id} AND WITH APP ACCESS {app_id}")
                        print("RESULT : ",result)
                        
        
                select_query = f''' SELECT app_access FROM {token_optix_schema}.{USER_MANAGEMENT_TABLE}
                                        WHERE user_unique_id = '{user_unique_id}' AND delete_status = 0;   
                    '''
                select_response = select_db(select_query)
                print(f"SELECTED APPS FOR USER {user_unique_id} IN APP {app_id}",select_response)
                deleted_integrations = [item for item in [item[0] for item in select_response] if item not in apps]
                print(f"APPS IN REQUEST FOR {user_unique_id} IN APP {app_id}",apps)
                print(f"DELETING APPS {deleted_integrations}")
        
                for deleteIntegration in deleted_integrations:
                    delete_query = f'''
                    UPDATE {token_optix_schema}.{USER_MANAGEMENT_TABLE}
                    SET delete_status = 1
                    WHERE app_access = '{deleteIntegration}' AND user_unique_id = '{user_unique_id}' AND delete_status = 0;
            '''
                    update_db(delete_query)
                    print(f"ROW DELETED FOR THE USER {user_name} WITH USER ID {user_unique_id} WITH INTEGRATION TYPE {deleteIntegration}")
                    
            if user_type == "bot_user":
                # existing appdetails changes 
                # adding new app...
                # deleting existing app..
                
                # query for all app access...
                query = f'''
                SELECT app_access from {token_optix_schema}.{BOT_USER_MANAGEMENT_TABLE} WHERE user_unique_id = '{user_unique_id}' and delete_status = 0;
                '''
                old_app_access = list(select_db(query))
                old_app_access = [item for sublist in old_app_access for item in sublist]
                print("old_app_access : ", old_app_access)
                
                for app in event['app_access_details']:
                    if app['app_id'] not in old_app_access:
                        print("new app added: ", app['app_id'])
                        
                        access_type = app['access_type']
                        app_name = app['app_name']
                        app_id = app['app_id']
                        integration_value = app['integration_details']
                        user_identifier = integration_value['identifier']
                        integration_type = integration_value['integration_type']
                        
                        query = f'''
                        INSERT INTO {token_optix_schema}.{BOT_USER_MANAGEMENT_TABLE} 
                        (user_id, access_type, created_at, delete_status, user_name, user_type, app_access, user_unique_id, app_name,integration_type)
                        VALUES (%s, %s, CURRENT_TIMESTAMP, 0, %s, %s, %s, %s, %s, %s);
                        '''
                        values = (user_identifier, access_type, user_name, user_type, app_id, user_unique_id, app_name, integration_type)
                        result = insert_db(query, values)
                        
                    if app['app_id'] in old_app_access:
                        print("either update or delete : ", app['app_id'])
                        
                        access_type = app['access_type']
                        app_name = app['app_name']
                        app_id = app['app_id']
                        integration_value = app['integration_details']
                        user_identifier = integration_value['identifier']
                        integration_type = integration_value['integration_type']
                        
                        query = f'''
                        UPDATE {token_optix_schema}.{BOT_USER_MANAGEMENT_TABLE} 
                        SET user_id = '{user_identifier}',
                            access_type = '{access_type}',
                            user_name = '{user_name}'
                        WHERE user_unique_id = '{user_unique_id}' and app_access = '{app_id}';
                        '''
                        
                        result = update_db(query)
                        old_app_access.remove(app['app_id'])
                        
                if len(old_app_access) > 0:
                    print("apps to be deleted...", old_app_access)
                    for _app_id in old_app_access:
                        
                        query = f'''
                        UPDATE {token_optix_schema}.{BOT_USER_MANAGEMENT_TABLE} 
                        SET delete_status = 1
                        WHERE user_unique_id = '{user_unique_id}' and app_access = '{_app_id}';
                        '''
                        result = update_db(query) 
                    
            function_response = {
                        "message" : 'update successfully'
                    }
            return {
                'statusCode': 200,
                'body': function_response                                                                                                      
            }
        except Exception as e:
            print(f"EXCEPTION OCCURRED {e}")
            function_response = {
                        "message" : 'update failed'   
                    }
            return {
                'statusCode': 500,
                'body': function_response                                                                                                         
            }
            
            
    return result                                                                       