import boto3
from botocore.exceptions import ClientError
from time import sleep
import psycopg2
import json                                                                                             
import os   
from datetime import *
import random,string
import re

# Environment Variables
 
CHAT_SESSION_TABLE = os.environ["CHAT_SESSION_TABLE"]
BOT_USER_TABLE = os.environ["BOT_USER_TABLE"]
FILE_VERSIONS_TABLE = os.environ["FILE_VERSIONS_TABLE"]
USER_TABLE = os.environ["USER_TABLE"]
FILE_METADATA_TABLE = os.environ["FILE_METADATA_TABLE"]
CONFIG_TABLE = os.environ["CONFIG_TABLE"]                                                   
PREPROCESS_TABLE = os.environ["PREPROCESS_TABLE"]
CHAT_LOG_TABLE = os.environ["CHAT_LOG_TABLE"]
UserPoolId = os.environ["cognito_id"]                               
region_used = os.environ["region_used"]                     
bucket_name = os.environ["BUCKET_NAME"]
db_user =os.environ['db_user']
db_password = os.environ['db_password']
db_host = os.environ['db_host']                         
db_port = os.environ['db_port']
db_database = os.environ['db_database']  
cexp_schema = os.environ["schema"]
app_meta_data_table = os.environ["app_meta_data_table"]
# SOCKET_URL = os.environ["SOCKET_URL"]
model_metadata = os.environ['model_metadata']
subscription_metadata = os.environ['subscription_metadata']
token_optix_schema = os.environ['schema']
model_id = os.environ['model_id']
TAGS_TABLE = os.environ["TAGS_TABLE"]
CHAT_TRANSCRIPT_TABLE = os.environ["CHAT_TRANSCRIPT_TABLE"]
# Boto3 Clients
s3_client = boto3.client('s3',region_name = region_used)
# TAGS_DOCUMENT_ASSOCIATE_TABLE = os.environ["TAGS_DOCUMENT_ASSOCIATE_TABLE"]


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


# Helper Functions
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


# Bot user Stats
def count_bot_user_data(app_id):
 
    total_users_query = f'''SELECT COUNT(*)
                            FROM {cexp_schema}.{USER_TABLE}
                            WHERE created_at < CURRENT_DATE and delete_status = 0 and app_access = '{app_id}';'''
    try:
        total_users = select_db(total_users_query)[0][0]
    except Exception as e:
        print("An exception occurred for total_users select query :",e)
        total_users = 0
 
    active_users_today_query = f'''SELECT count(distinct email_id)
                                    FROM {cexp_schema}.{CHAT_SESSION_TABLE}
                                    WHERE last_updated_time  >= CURRENT_DATE
                                    AND last_updated_time  < CURRENT_DATE + INTERVAL '1 day'
                                    app_id = '{app_id}';'''
    try:
       active_users_today = select_db(active_users_today_query)[0][0]
    except Exception as e:
        print("An exception occurred for active_users_today select query :",e)
        active_users_today = 0
   
    new_users_today_query = f'''select count(*)
                                from {cexp_schema}.{USER_TABLE}
                                where created_at >= current_date
                                and created_at < current_date + interval '1 day' and delete_status = 0 and app_access = '{app_id}';'''
    try:
        new_users_today = select_db(new_users_today_query)[0][0]
    except Exception as e:
        print("An exception occurred for new_users_today select query :",e)
        new_users_today = 0
 
    new_users_past7days_query =f'''SELECT COUNT(*)
                                    FROM {cexp_schema}.{USER_TABLE}
                                    WHERE created_at >= CURRENT_DATE - INTERVAL '7 day'
                                    AND created_at < CURRENT_DATE and delete_status = 0 and  app_access = '{app_id}';'''
    try:
        new_users_past7days = select_db(new_users_past7days_query)[0][0]
    except Exception as e:
        print("An exception occurred for new_users_past7days select query :",e)
        new_users_past7days = 0
   
    active_users_past7days_query = f'''SELECT COUNT(distinct email_id)
                                        FROM {cexp_schema}.{CHAT_SESSION_TABLE}
                                        WHERE last_updated_time >= CURRENT_DATE - INTERVAL '7 day'
                                        AND last_updated_time < CURRENT_DATE and app_id = '{app_id}';'''
    try:
        active_users_past7days = select_db(active_users_past7days_query)[0][0]
    except Exception as e:
        print("An exception occurred for active_users_past7days select query :",e)
        active_users_past7days = 0
   
    return {
        'total_users': total_users,              
        'active_users_today' : active_users_today,
        'new_users_today': new_users_today,
        'new_users_past7days' : new_users_past7days,
        'active_users_past7days': active_users_past7days
    }

# Chatbot Stats
def count_questions_and_answers(app_id):   
    convo_query = f'''select count(*)
                    from {cexp_schema}.{CHAT_SESSION_TABLE} where app_id = '{app_id}';'''
    try:
        convo_count = select_db(convo_query)[0][0]
 
    except Exception as e:
        print("An exception occurred in convo_count: ",e)
        convo_count = 0
   
    total_answered_query = f'''select count(*)
                                from {cexp_schema}.{CHAT_SESSION_TABLE}
                                where handoff = 1 and app_id = '{app_id}';'''
    try:
        total_answered_count = select_db(total_answered_query)[0][0]
 
    except Exception as e:
        print("An exception occurred in total_answered_count: ",e)
        total_answered_count = 0
   
    total_unanswered_count_query = f'''select count(*)
                                        from {cexp_schema}.{CHAT_SESSION_TABLE}
                                        where handoff = 0 and app_id = '{app_id}';'''
    try:
        total_unanswered_count = select_db(total_unanswered_count_query)[0][0]
 
    except Exception as e:
        print("An exception occurred in total_unanswered_count: ",e)
        total_unanswered_count = 0
   
    last7days_convo_query = f'''select count(*)
                                from {cexp_schema}.{CHAT_SESSION_TABLE}
                                where last_updated_time >= CURRENT_DATE - INTERVAL '7 day'
                                AND last_updated_time < CURRENT_DATE and app_id = '{app_id}';'''
    try:
        last7days_convo = select_db(last7days_convo_query)[0][0]
   
    except Exception as e:
        print("An exception occurred in last7days_convo: ",e)
        last7days_convo = 0
   
    last7days_ans_query = f'''select count(*)
                                from {cexp_schema}.{CHAT_SESSION_TABLE}
                                where last_updated_time >= CURRENT_DATE - INTERVAL '7 day'
                                AND last_updated_time < CURRENT_DATE
                                AND handoff = 1 and app_id = '{app_id}';'''
    try:
        last7days_ans = select_db(last7days_ans_query)[0][0]
   
    except Exception as e:
        print("An exception occurred in last7days_ans: ",e)
        last7days_ans = 0
   
    last7days_unans_query = f'''select count(*)
                                from {cexp_schema}.{CHAT_SESSION_TABLE}
                                where last_updated_time >= CURRENT_DATE - INTERVAL '7 day'
                                AND last_updated_time < CURRENT_DATE
                                AND handoff = 0 and app_id = '{app_id}';'''
    try:
        last7days_unans = select_db(last7days_unans_query)[0][0]
   
    except Exception as e:
        print("An exception occurred in last7days_unans: ",e)
        last7days_unans = 0
   
    return{
        "convo": convo_count,
        "total_answers": total_answered_count,
        "number_ques_unans": total_unanswered_count,
        "last7days_convo": last7days_convo,
        "last7days_ans": last7days_ans,
        "last7days_unans": last7days_unans
    }


# Bot Users List
# def list_all_emails(app_id):
#     query = f'''select array(select format('%s - %s', user_name, mobile_number) from {cexp_schema}.{BOT_USER_TABLE} ccbum where environment='prod' and app_access = '{app_id}' order by user_name);'''                                                                
#     try:                                                        
#         email_ids = select_db(query)[0][0]
#     except:
#         print("Error in fetching Email IDs from Bot User Table")
#         email_ids = []                      
#     print(email_ids)            
#     return email_ids

def list_all_chat_users(app_id, environment):
    query = f'''
    select array(select distinct cs.user_details from {token_optix_schema}.{CHAT_SESSION_TABLE} cs where app_id = '{app_id}' and environment = '{environment}');
    '''
    try:
        res = select_db(query)[0]
    except:
        print("Error in fetching Email IDs from Bot User Table")
        res = []
    return res


# File Versions List
def get_all_file_versions(actual_file_name,app_id,env):
    query = f'''SELECT json_agg(row_to_json(row_values)) FROM (SELECT * FROM {cexp_schema}.{FILE_VERSIONS_TABLE} WHERE actual_file_name='{actual_file_name}' and delete_status = 0 and app_id = '{app_id}'and env = '{env}') row_values;'''
    try:
        file_versions = select_db(query)[0][0]
    except:                     
        print("Error in fetching File Versions from File Version Table")
        file_versions = []
    print("Response : ",file_versions)

    return file_versions
                                        

    
def generate_presigned_url(key, expiration=3600):
    presigned_url = s3_client.generate_presigned_url(
        ClientMethod='get_object',
        Params={
            'Bucket': bucket_name,
            'Key': key,
            'ResponseContentDisposition': 'inline'
        },
        ExpiresIn=expiration
    )
    return presigned_url


def update_user_name(email_id, new_user_name):
    print("in update username function ",new_user_name)
    query = f'''update {cexp_schema}.{USER_TABLE} set user_name  = '{new_user_name}' where email_id  = '{email_id}' and delete_status = 0;'''
    result = update_db(query)
    return result
    
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
   

def lambda_handler(event, context):
    model_id = os.environ['model_id']
    print(model_id)
    print("EVENT : ",event)
    event_type = event['event_type']
    
    if event_type == 'update_active_model':
        app_id = event['app_id']
        model_id = event['model_id']
        environment = event['env']
        
        if environment == 'Production':
            query = f'''
            update {cexp_schema}.{app_meta_data_table} 
            set active_model_id = '{model_id}'
            where app_id = '{app_id}';
            '''
        else:
            query = f'''
            update {cexp_schema}.{app_meta_data_table} 
            set active_pg_model_id = '{model_id}'
            where app_id = '{app_id}';
            '''
            
        update_db(query)
        
        return {
            'statusCode' : 200,
            'msg': 'model changed successfully'
        }

    if event_type == 'prompt-update':
        app_id = event['app_id']
        prompt = event['prompt']
        prompt = prompt.replace("'","''")
        prompt = prompt.replace('"','""')   
        prompt_type = "pg_prompt"
        
        environment = event['env']
        
        # check if prompt exists...
        query = f'''
        select description from {cexp_schema}.{CONFIG_TABLE} where prompt_type = '{prompt_type}' and app_id= '{app_id}';
        '''
        res = select_db(query)
        
        if res == []:
            query = f'''
            insert into {cexp_schema}.{CONFIG_TABLE} (prompt_type, description, environment, app_id) values (%s, %s, %s, %s);
            '''
            values = (prompt_type, prompt, environment, app_id)
            insert_db(query, values)
            
            return {
                "statusCode" : 200, 
                "msg" : "Prompt Configuartion Created Successfully"
            }
        
        
        query = f'''update {cexp_schema}.{CONFIG_TABLE}
                    set description = '{prompt}'
                    where prompt_type = '{prompt_type}' and app_id= '{app_id}';'''  
 
        data = update_db(query)
        print(data,"data")  
        if data is not None:                
            function_response = {
                "data": "updated success"
            }
            return {                                        
                    'statusCode': 200,
                    'body': function_response
                }
        else:
            function_response = {
                    "data" : "update failed"
                }
            return {
                    'statusCode': 500,
                    'body': function_response
                }

    if event_type == 'prompt-list':
        try:
            app_id = event['app_id']
            prompt_type = event['prompt_type']
            query = f'''select description from {cexp_schema}.{CONFIG_TABLE} where prompt_type = 'pg_prompt' and app_id = '{app_id}';'''  
            res = select_db(query)
            print("PROMPT RESULT : ",res)
            if res != []:
                function_response = {
                    "data" : res[0][0],
                    "message": "updated success"
                }                                                                                    
                return {                                        
                        'statusCode': 200,
                        'body': function_response
                    }
            else:
                function_response = {
                        "status" : "No configuration to display"
                    }
                return {
                        'statusCode': 500,
                        'body': function_response
                            }
        except Exception as e:
            print("An error occurred in the event_type - prompt-list :",e)
            function_response = {
                        "status" : "No configuration to display"
                    }
            return {
            	'statusCode': 500,
                'body': function_response
                }
            

    
    if event_type == "list_emails":
        app_id = event['app_id']
        environment = event['env']
        
        return {
            "statusCode": 200,
            "body": {
                "data": [
                    [
                        "demo@1cloudhub.com"
                    ]
                ]
            }
        }
        
        emails = list_all_chat_users(app_id, environment)
        if emails is not None:
            function_response = {
                "data": emails
            }
            return {
                    'statusCode': 200,
                    'body': function_response
                }
        else:
            function_response = {
                    "status" : "No users"
                }
            return {
                    'statusCode': 500,
                    'body': function_response
                        }
            
   
    if event_type == "user_count":
        app_id = event['app_id']
        users_count = count_bot_user_data(app_id)
        function_response = {
                "data": users_count
            }
        return {
                'statusCode': 200,
                'body': function_response
            }
                            
    if event_type == "count_chat":
        app_id = event['app_id']
        data = count_questions_and_answers(app_id)
        function_response = {
            "data": data
        }
        return {
                'statusCode': 200,
                'body': function_response
            } 
            
    if event_type == "list_chat_count":
        app_id = event['app_id']
        try:
            count_query = f'''select count(*) from {token_optix_schema}.{CHAT_LOG_TABLE} 
            WHERE app_id = '{app_id}' AND environment = 'Production' 
            ;'''
            result = select_db(count_query)
            count = result[0][0]
            return {
                "statusCode" : 200, 
                "count" : count
            }
        except Exception as e:
            print("a error while fetching count", e)
            return {
                "statusCode" : 500, 
                "message" : "something went wrong!"
            }
                
    if event_type == "list_chat":
        app_id = event['app_id']
        try:
            limit = event["limit"] 
            page = event["page"]
            offset = ((page - 1) * limit)
            
            list_query = f'''select json_agg(row_to_json(row_values)) FROM (select * from {token_optix_schema}.{CHAT_LOG_TABLE} WHERE app_id = '{app_id}' AND environment = 'Production' order by timestamp desc limit {limit} offset {offset}) row_values;'''
            print(list_query)
            result = select_db(list_query)[0][0]
            return {
                "statusCode" : 200, 
                "chat" : result
            }
        except Exception as e:
            print("a error while fetching chat", e)
            return {
                "statusCode" : 500, 
                "message" : "something went wrong!"
            }
            
    if event_type == "chat_history":
        try:
            session_id = event["session_id"]
            chat_history_query = f'''
            select json_agg(row_to_json(row_values)) from  
            (
            select mcu.user_data, ui.last_updated_time as date_time, question as user, answer as bot from {token_optix_schema}.{CHAT_SESSION_TABLE} as ui 
            join {token_optix_schema}.{CHAT_LOG_TABLE} as mcu 
            on mcu.session_id = ui.session_id 
            where ui.session_id = '{session_id}'
            order by last_updated_time
            ) row_values;'''
            res = select_db(chat_history_query)[0][0]
            if res == None:
                
                user_details_query = f'''
                select json_agg(row_to_json(row_values)) from (
                    select user_data from {token_optix_schema}.{CHAT_LOG_TABLE} where session_id = '{session_id}'
                ) row_values;
                '''
                
                user_details = select_db(user_details_query)[0][0]
                
                _result = {}
                _result["user_data"] = user_details[0]["user_data"]
                    
                return {
                    "statusCode" : 200, 
                    "data" : _result  
                }
            result = {
                "user_data" : "",
                "chat" : []
            }
            result["user_data"] = res[0]["user_data"]
            chat = []
            for r in res:
                chat.append({
                    "question" : r["user"],
                    "answer" : r["bot"],
                    "date_time" : r["date_time"]
                })
            result["chat"] = chat
            return {
                "statusCode" : 200,
                "data" : result
            }
            
            
        except Exception as e:
            print("error occured when trying to fetch chat history", e)
            return {
                "statusCode" : 500,
                "message" : f"error occured when trying to fetch chat history : {e}"
            }
               
    
            
    if event_type == "generate_summary":
        app_id = event['app_id']
        environment = event['env']
        print("SUMMARY GENERATION ")
        client = boto3.client('bedrock-runtime', region_name = region_used)
        session_id = event["session_id"]
        chat_query = f'''
        SELECT question,answer
        FROM {token_optix_schema}.{CHAT_SESSION_TABLE} 
        WHERE session_id = '{session_id}'
        AND handoff = 1;
        '''
        model_id = "anthropic.claude-3-5-sonnet-20240620-v1:0"
    
        chat_details = select_db(chat_query)
        print("CHAT DETAILS : ",chat_details)
        history = ""
    
        for chat in chat_details:
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
        update_query = f'''UPDATE {token_optix_schema}.{CHAT_LOG_TABLE}
        SET 
            lead = {lead},
            lead_explanation = '{leads_generated_details}',
            sentiment = '{conversation_sentiment}',
            sentiment_explanation = '{conversation_sentiment_generated_details}',
            enquiry = {enquiry},
            complaint = {complaint},
            summary = '{detailed_summary}',
            email_content = '{email_creation}',
            next_best_action = '{action_to_be_taken}'
        WHERE 
            session_id = '{session_id}' AND app_id = '{app_id}' AND environment = 'Production'
            '''
        update_db(update_query)
        return {
                "statusCode" : 200,
                "message" : "Summary Successfully Generated"
            }
        
        
    
    
    if event_type == "chat_history_count":
        user_detail = event["user_detail"]
        environment = event['env']
        app_id = event['app_id']
        query = f'''select count(*) from {token_optix_schema}.{CHAT_TRANSCRIPT_TABLE}' ;'''
        result = select_db(query)
        try:
            count = result[0][0]
            function_response = {"count" : count}
            return {
                    'statusCode': 200,
                    'body': function_response
                    }
        except:
            print("Error: Unable to fetch count of ui chat session table")
            function_response = {"status" : "Error: Unable to fetch count of ui chat session table"}
            return {
                    'statusCode': 500,
                    'body': function_response
                    }
 
    if event_type == "chat_history_data":
        limit = event["limit"]
        page = event["page"]
        
        user_detail = event["user_detail"]
        environment = event['env']
        app_id = event['app_id']
     
        # query = f'''SELECT json_agg(row_to_json(row_values)) FROM (select question, answer, last_updated_time from {dev_schema}.{CHAT_SESSION_TABLE} ccucs where mobile_number = '{user.split(' ')[-1]}' order by last_updated_time desc limit {limit} offset ({page} -1) * {limit}) row_values;'''  
        query = f'''SELECT json_agg(row_to_json(row_values)) FROM (select question, answer, TO_CHAR(updated_on, 'YYYY-MM-DD"T"HH24:MI:SS.US') as last_updated_time from {token_optix_schema}.{CHAT_TRANSCRIPT_TABLE} ccucs order by updated_on desc limit {limit} offset ({page} -1) * {limit}) row_values;  '''
        result = select_db(query)
        try:
            chat_session = result[0][0]
        except:
            chat_session = None
        if chat_session is not None:
            function_response = {
                "data" : chat_session  
            }
            return {
                    'statusCode': 200,
                    'body': function_response
                }
        else:
            function_response = {
                    "status" : "User has not initiated any chat"
                }
            return {
                    'statusCode': 500,
                    'body': function_response
                    }

    
    if event_type == 'user_auth':
        app_id = event['app_id']
        email_id = event["email_id"]   
        query = f'''SELECT row_to_json(row_values) FROM (SELECT * FROM {cexp_schema}.{USER_TABLE} WHERE email_id='{email_id}' and delete_status = 0 and app_access = '{app_id}') row_values;'''
        user_data = select_db(query)
        print(user_data)                                                                                    
        if user_data:                                                                       
            user_data = user_data[0][0]
            Access_type = user_data['Access_type']                                                                                                                  
            print(Access_type)
            user_type = user_data['User_type']
            print(user_type)
            # filter_expression = Attr('email_id').eq(email_id) 
            # response = scan_with_filter_expression(USER_TABLE,filter_expression)
            # date_created_str = response['Items'][0]['created_date']
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
            user_data['validity'] = validity
            function_response = {
                "data": user_data,
                "message": "success"     
            }
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

    if  event_type == "get_versions":
        app_id = event['app_id']
        env = event['env']
        actual_file_name = event['actual_file_name']
        file_versions = get_all_file_versions(actual_file_name,app_id,env)
        
        if file_versions is not None:
            function_response = {
                "data" : file_versions
            }
            return {
                'statusCode': 200,
                'body': function_response
            }
        else:
            function_response = {
                "status" : "no versions"
            }
            return {
                    'statusCode': 500,
                    'body': function_response
                    }
            
       
    if event_type == "list_users_count":
        app_id = event['app_id']
        user_type = event['User_type']
        search_result = event['search_result']
        if user_type == 'bot_user':
            bot_user_query = f'''select count(*)
                                from {cexp_schema}.{BOT_USER_TABLE}
                                where "user_type" = '{user_type}' and delete_status = 0 and user_name ilike '%{search_result}%' and app_access = '{app_id}';'''
            try:
                bot_user_count = select_db(bot_user_query)[0][0]
            except Exception as e:
                print("An exception occurred in bot_user_query: ",e)
                bot_user_count = 0
            function_response = {"bot_user_count":bot_user_count}
    
        else:
            user_query = f'''select count(*)   
                            from {cexp_schema}.{USER_TABLE}
                            where "User_type" = '{user_type}' and delete_status = 0 and user_name ilike '%{search_result}%' and app_access = '{app_id}';'''
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
    
            
    if event_type == "pagination_count":
        app_id = event['app_id']
        env = event['env']
        access_type = event['access_type']
        search_result =  event['search_result']
        query = f'''select count(*)
                    from {token_optix_schema}.{FILE_METADATA_TABLE}
                    where access_type = '{access_type}'
                    and active_file_name ilike '%{search_result}%'
                    and delete_status = 0
                    and app_id = '{app_id}'
                    and env = '{env}';'''
        if access_type == 'private':
            query = f'''select count(*)
                    from {token_optix_schema}.{FILE_METADATA_TABLE}
                    where active_file_name ilike '%{search_result}%'
                    and delete_status = 0
                    and app_id = '{app_id}'
                    and env = '{env}';'''
            
        try:
            document_count =  select_db(query)[0][0]
        except Exception as e:
            print("An error occurred in retrieving document count: ",e)
            document_count = 0
        function_response =  {"count":document_count}
        return {
            'statusCode': 200,
            'body': function_response
        }
        
    if event_type == "document_tags":
        app_id = event["app_id"]
        env = event["env"]
        active_file_name = event["active_file_name"]
        version = event["active_version"]
        try:
            select_query = f'''
           WITH AllTags AS (
            SELECT DISTINCT tag_name
            FROM {token_optix_schema}.{TAGS_TABLE}
            WHERE app_id = '{app_id}'
              AND env = '{env}'
              AND delete_status = 0
        ),
        TaggedTags AS (
            SELECT DISTINCT unnest(STRING_TO_ARRAY(metadata, ',,,')) AS tag_name
            FROM {token_optix_schema}.{FILE_VERSIONS_TABLE}
            WHERE app_id = '{app_id}'
              AND env = '{env}'
              AND active_file_name = '{active_file_name}'
              AND delete_status = 0
              AND version = '{version}' 
        )
        SELECT json_build_object(
            'file_name', '{active_file_name}',
            'tagged_tags', 
                COALESCE(json_agg(tt.tag_name) FILTER (WHERE tt.tag_name IS NOT NULL), '[]'),
            'untagged_tags', 
                COALESCE(json_agg(at.tag_name) FILTER (WHERE at.tag_name NOT IN (SELECT tag_name FROM TaggedTags)), '[]')
        )
        FROM AllTags at
        LEFT JOIN TaggedTags tt ON at.tag_name = tt.tag_name;



            '''
            print("QUERY : ",select_query)
            response = select_db(select_query)[0][0]
            print("DOCUMENT TAGS : ",response)
            return {
                "statusCode" : 200,
                "response" : response
            }
        except Exception as e:
            return {
                "statusCode" : 500,
                "response" : f"Error {e}..... while fetching document tags"
            }
        
                                                                                        
    if event_type == "pagination":
        app_id = event['app_id']
        env = event['env']
        access_type = event['access_type']
        search_result = event['search_result']
        limit = event['limit']
        page = event['page']
        query = f'''select json_agg(row_to_json(row_values))
                    FROM
                    (select *
                    from {token_optix_schema}.{FILE_METADATA_TABLE}
                    where access_type = '{access_type}'
                    and delete_status = 0
                    and active_file_name ilike '%{search_result}%'
                    and app_id = '{app_id}'
                    and env = '{env}'
                    order by last_updated_time desc
                    limit {limit} offset ({page} - 1) * {limit})row_values;'''
                    
        if access_type == 'private':
            query = f'''select json_agg(row_to_json(row_values))
                    FROM
                    (select *
                    from {token_optix_schema}.{FILE_METADATA_TABLE}
                    where delete_status = 0
                    and active_file_name ilike '%{search_result}%'
                    and app_id = '{app_id}'
                    and env = '{env}'
                    order by last_updated_time desc
                    limit {limit} offset ({page} - 1) * {limit})row_values;'''   
            
        try:
            print("QUERY : ", query)
            batch = select_db(query)
            print(batch)
            batch = batch[0][0]
       
        except Exception as e:
            print("An exception occurred in pagination: ",e)
            batch = []
       
        function_response = {"batch":batch}
        return {
            'statusCode' : 200,
            'body': function_response
        }
                
    
    if event_type == "generate_url":
        active_filename = event["active_filename"]
        fileName = event["fileName"]
        version = event["version"]
        status = event["status"]
        app_id = event["app_id"]
        app_query = f"SELECT app_name from {token_optix_schema}.{app_meta_data_table} WHERE app_id = '{app_id}';"
        app_name_result = select_db(app_query)
        app_name = app_name_result[0][0]
        env = event["env"]
        file_type = fileName.split('.')[-1]  
        if file_type == 'csv' or file_type == 'xlsx':
            if status == 1:
                key =  f'{app_name}/Documents_kbview/{active_filename}'                                                                                
            else:
                key = f'{app_name}/Documents/{fileName}_versions/{fileName}_version{version}/whole_files/{active_filename}'
            presigned_url = generate_presigned_url(key)
            if presigned_url:
                print(f'Pre-signed URL: {presigned_url}')
                function_response = {
                "url" : presigned_url
            }
                return {
                        'statusCode': 200,
                        'body': function_response
                        }
            else:
                print('Failed to generate pre-signed URL.')
                function_response = {
                "status" : "Failed to genrate url"
            }
                return {
                        'statusCode': 500,
                        'body': function_response
                        }
        if file_type == 'docx' or file_type == 'pdf':
            print("FILE_TYPE IS DOCX OR PDF")   
            if status == 1:
                key = f'{app_name}/Documents_kb/{active_filename}'
            else:
                key = f'{app_name}/Documents/{fileName}_versions/{fileName}_version{version}/{active_filename}'
            presigned_url = generate_presigned_url(key)
            if presigned_url:
                print(f'Pre-signed URL: {presigned_url}')
                function_response = {
                "url" : presigned_url
            }
                return {
                        'statusCode': 200,
                        'body': function_response
                        }
            else:
                print('Failed to generate pre-signed URL.')
                function_response = {
                "status" : "Failed to genrate url"
            }
                return {
                        'statusCode': 500,
                        'body': function_response
                        }
        else:
            print('Failed to generate pre-signed URL.')
            function_response = {
            "status" : "Failed to genrate url"
            }
            return {
                    'statusCode': 500,
                    'body': function_response
                    }
            
    if event_type == "generate-pre-process-url":
        app_id = event["app_id"]
        app_query = f"SELECT app_name from {token_optix_schema}.{app_meta_data_table} WHERE app_id = '{app_id}';"
        app_name_result = select_db(app_query)
        app_name = app_name_result[0][0]
        env = event["env"]
        fileName = event["file_name"]
        state = event["state"]
        if state == "output":
            if env == "Production":
                key = f"{app_name}/Document_Preprocessing/output_files/"
                fileName = "output_"+fileName+".csv"
            else:
                key = f"{app_name}/Dev_Document_Preprocessing/output_files/"
                fileName = "output_"+fileName+".csv"
        else:
            if env == "Production":
                key = f"{app_name}/Documents_Preprocessing/input_files/"  
            else:
                key = f"{app_name}/Dev_Documents_Preprocessing/input_files/"                
        key = key+fileName
        presigned_url = generate_presigned_url(key)                
        print("pre signed url",presigned_url)
        if presigned_url:
            print(f'Pre-signed URL: {presigned_url}')
            function_response = {
                "url" : presigned_url
            }
            return {
                    'statusCode': 200,
                    'body': function_response
                    }
        else:
            print('Failed to generate pre-signed URL.')
            function_response = {
                "status" : "Failed to genrate url"
            }
            return {
                    'statusCode': 500,
                    'body': function_response
                }    
        
    if event_type == 'update_date_user_auth':
        email_id = event['email_id'] 
        app_id = event['app_id']
        # today = datetime.now(timezone.utc).date().isoformat()
        # update_db(USER_TABLE,'email_id',email_id,'created_date',str(today))     
        query = f'''update {cexp_schema}.{USER_TABLE} set created_date = NOW() where email_id ='{email_id}' and delete_status =0 and app_access = '{app_id}';'''
        result = update_db(query)
        if result is not None:
            return {"statusCode":200,"message":"Date updated successfully"}    
        else:
            return {"statusCode":500,"message":f"Error: Unable to update date for user {email_id}"}  
                                            
    
    if event_type == "preprocess_count":
        app_id = event['app_id']
        env = event['env']
        search_result =event['search_result']
        query = f'''select count(*) from {cexp_schema}.{PREPROCESS_TABLE} ccpm where delete_status = 0 and input_filename ilike '%{search_result}%' and app_id = '{app_id}' and env = '{env}';'''   
        result = select_db(query)
        try:
            count = result[0][0]
            function_response = {"count" : count}
            return {
                    'statusCode': 200,
                    'body': function_response
                    }
        except:
            print("Error: Unable to fetch count of preprocess metadata table")
            function_response = {"status" : "Error: Unable to fetch count of preprocess metadata table"}
            return {
                    'statusCode': 500,
                    'body': function_response
                    }                                                                           
    
    if event_type == "preprocess-list":
        app_id = event['app_id']
        env = event['env']
        limit = event['limit']
        search_result =event['search_result']   
        page = event['page'] 
        query = f'''SELECT json_agg(row_to_json(row_values)) FROM (select * from {token_optix_schema}.{PREPROCESS_TABLE} ccpm where delete_status = 0  and input_filename ilike '%{search_result}%' and app_id = '{app_id}' and env = '{env}' order by last_updated_time desc limit {limit} offset ({page} -1) * {limit}) row_values;'''
        result = select_db(query)
        try:
            data = result[0][0]
            function_response = {
                "batch" : data,
            }
            return {
                    'statusCode': 200,
                    'body': function_response
                    }       
        except:
            print("Error: Unable to fetch data of preprocess metadata table")
            function_response = {
                "status" : "Error: Unable to fetch data of preprocess metadata table",
            }
            return {
                    'statusCode': 500,
                    'body': function_response
                    }
                    
    if event_type == 'available_models':
        result = []
        res1 = []
        #app_id = 26
        # app_id = os.environ["APP_ID"]    
        app_id = event['app_id']
        query = f'''select model_name, model_id from {cexp_schema}.{model_metadata} where app_id = '{app_id}' and delete_status = 0;''' 
        res1 = select_db(query)
        print("MODEL DETAILS : ",res1)
        environment = event['env']
        if environment == "Production":
            query = f'''select active_model_id from {cexp_schema}.{app_meta_data_table} where app_id = '{app_id}' and delete_status = 0;'''  
        else:
            query = f'''select active_pg_model_id from {cexp_schema}.{app_meta_data_table} where app_id = '{app_id}' and delete_status = 0;'''  
        res2 = select_db(query)
        print("ACTIVE MODEL : ",res2)
        current_model_id = res2[0][0]
        if res1 != []:
            for res in res1:
                res_json = {}
                res_json['model_name'] = res[0]
                res_json['model_id'] = res[1]  
                if current_model_id == res[1]:   
                    res_json['status'] = '1'
                else:
                    res_json['status'] = '0'   
                result.append(res_json)
        else:                                                                           
            result = []                   
        return result   
    
    if event_type == 'update_model_config':
        model_id = event['model_id']
        model_name = event['model_name']
        #app_id = 26
        app_id = event['app_id']
        query = f'''select model_id, model_name, convo_id from {cexp_schema}.{model_metadata} where app_id ='{app_id}' and delete_status = 0 and model_id = '{model_id}' and model_name = '{model_name}';'''   
        res = select_db(query)
        query = f'''update {cexp_schema}.subscription_metadata set model_id = '{model_id}', model_name = '{model_name}', convo_id = '{res[0][2]}' where delete_status = 0 and app_id = '{app_id}';'''
        update_db(query)
        function_response = {"status":"Record updated successfully"} 
        return {"statusCode":200,"body":function_response}
        
    if event_type == "list_credit":    
        result = []  
        # app_id = os.environ['APP_ID']
        app_id = event['app_id']
        query = f'''select assigned_credits, remaining_credits, subscription_type, start_date, expiry_date 
                    from {cexp_schema}.{subscription_metadata} 
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
        return result  
    
    if event_type == 'chat_type':   
        app_id = event['app_id']  
        query = f'''select streaming from {cexp_schema}.{app_meta_data_table} where app_id = '{app_id}' and delete_status = 0;'''
        res = select_db(query)[0][0]
        print("RESULT FROM DB : ",res)     
        if res == 1:  
            result = {"statusCode": 200,"body":{"streaming":'1',"socket_url":SOCKET_URL}}      
        else:
            result = {"statusCode": 200,"body":{"streaming":'0',"socket_url":""}}
        return result     
    
    if event_type == "preprocess_update":
        file_id = event["file_id"]
        app_id = event["app_id"]
        file_name = event["file_name"]
        status = event["status"]
        
        try:
            if status == "success":
                update_query = f"update {cexp_schema}.{PREPROCESS_TABLE} set preprocess_status = 'COMPLETED' where id = '{file_id}' and delete_status = 0"
            elif status == "error":
                update_query = f"update {cexp_schema}.{PREPROCESS_TABLE} set preprocess_status = 'FAILED' where id = '{file_id}' and delete_status = 0"
            update_response = update_db(update_query)
            return {
                "statusCode" : 200
            }
        except Exception as e:
            print("EXCEPTION OCCURRED : ",e)
            return {
                "statusCode" : 500
            }
        
        
    #doc_db_update event
    if event_type == 'doc_db_update':
        file_id = event['file_id']
        app_id = event['app_id']
        env = event['env']
        action = event['action']
        status = event['status']
        # Update the document in the database
        if action == 'Put' or action == 'Copy':
            query_update = f"""
            UPDATE {cexp_schema}.{FILE_METADATA_TABLE}
            SET sync_status = '{status}'
            WHERE id = '{file_id}' and delete_status = 0 and app_id = '{app_id}' and env = '{env}';
            """
            update_db(query_update)
        elif action == "metadata_delete":
            query_update = f"""
            UPDATE {cexp_schema}.{FILE_METADATA_TABLE}
            SET sync_status = 'INPROGRESS'
            WHERE id = '{file_id}' and delete_status = 0 and app_id = '{app_id}' and env = '{env}';
            """
            update_db(query_update)
        elif action == "metadata_update":
            query_update = f"""
            UPDATE {cexp_schema}.{FILE_METADATA_TABLE}
            SET sync_status = '{status}'
            WHERE id = '{file_id}' and delete_status = 0 and app_id = '{app_id}' and env = '{env}';
            """
            update_db(query_update)
        
        elif action == 'Delete':
            query_update = f"""
            UPDATE {cexp_schema}.{FILE_METADATA_TABLE}
            SET delete_status = 1,
            sync_status = '{status}'
            WHERE id = '{file_id}' and delete_status = 0 and app_id = '{app_id}' and env = '{env}';
            """
            update_db(query_update)
    
            if status != 'FAILED':
                query = f'''SELECT actual_filename FROM {cexp_schema}.{FILE_METADATA_TABLE} where id = '{file_id}' and app_id = '{app_id}' and env = '{env}';'''
                res_actual_filename = select_db(query)[0][0]
    
                query_update = f'''
                UPDATE {cexp_schema}.{FILE_VERSIONS_TABLE}
                SET delete_status = 1 
                WHERE actual_file_name = '{res_actual_filename}' and app_id = '{app_id}' and delete_status = 0 and env = '{env}';'''
                update_db(query_update)
        function_response = {"status":"Record updated successfully"} 
        return {"statusCode":200,"body":function_response} 
            
            
        