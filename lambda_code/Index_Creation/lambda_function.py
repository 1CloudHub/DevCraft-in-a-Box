import boto3,os,json                    
from requests_aws4auth import AWS4Auth
from opensearchpy import OpenSearch, RequestsHttpConnection
import time

sts = boto3.client('sts')
identity = sts.get_caller_identity()
print("Running as IAM Role:", identity['Arn'])
# def lambda_handler(event, context):
print("INDEX CREATION SCRIPT") 
# host=os.environ["host"]
# account_id=os.environ["account_id"]
region=os.environ["region"]
session = boto3.Session()


def lambda_handler(event, context):
  print("LAMBDA EVENTTT : ",event)
  # Get credentials from the Lambda execution environment                   
  credentials = session.get_credentials()
  print("CREDENTIALS : ",credentials)
  # print("CREDENTIALS :",credentials)
  # print(credentials.access_key)
  # print(credentials.secret_key)
  # Set up AWS authentication for OpenSearch
  awsauth = AWS4Auth(
      credentials.access_key,
      credentials.secret_key,
      region,
      'aoss',           
      session_token=credentials.token
  )

  # host = '54obgk8hzg10aj0k5xvj.us-east-1.aoss.amazonaws.com'
  # host = '54obgk8hzg10aj0k5xvj.us-east-1.aoss.amazonaws.com'
  # host = 'pe2ivn2lkil9y0fkqqjd.us-east-1.aoss.amazonaws.com'      
  # host ='54obgk8hzg10aj0k5xvj.us-east-1.aoss.amazonaws.com'     
  # host = '54obgk8hzg10aj0k5xvj.us-east-1.aoss.amazonaws.com'  
  # host = 'knv01psmxagjwzd3b4y1.us-east-1.aoss.amazonaws.com'
  host = os.environ["OPENSEARCH_ENDPOINT"]
  print("OPENSEARCH HOST : ",host)  
  client = OpenSearch(                  
      hosts=[{'host': host, 'port': 443}],
      http_auth=awsauth,
      use_ssl=True,
      verify_certs=True,
      http_compress=True,  # enables gzip compression for request bodies
      connection_class=RequestsHttpConnection
  )
  print("CLIENT INITIATED")
  # print(client)

  # Define the request body
  request_body_prod ={
  "settings": {
  "index": {
    "knn": True,  
    "knn.algo_param.ef_search": 512
  }
  },
  "mappings": {
  "properties": {
    "cexp_text_index": {
      "type": "knn_vector",
      "dimension": 1024,
      "method": {
        "name": "hnsw",
        "engine": "faiss",
        "parameters": {},
        "space_type": "l2"
      }
    }
  }
  }
  }

  try:  

    
    response_prod = client.indices.create(
        index='cexp_text_index',                
        body=request_body_prod
    )  
    
    print("SUCCESSFULLY CREATED INDEX : ",response_prod)
    time.sleep(30)
    print("SLEPT FOR 30 SECS")
    return response_prod    
    
    # print("response_prod",response_dev)
      

      
  except Exception as e:
    if 'resource_already_exists_exception' in str(e):
      print("st")
    print("exception occurred:",e)   
  except RequestError as e:
    
    print("errror")   
  # Check if the error is a resource_already_exists_exception
    if e.error == 'resource_already_exists_exception':  
      
        print(f"Error: Index 'ai-index' already exists.")  
