import json
import os
import psycopg2
from typing import Dict, Any
from http import HTTPStatus


db_user =os.environ['db_user']
db_password = os.environ['db_password']
db_host = os.environ['db_host']                         
db_port = os.environ['db_port']
db_database = os.environ['db_database'] 
schema = os.environ["schema"]
employee_details = os.environ["employee_details"]
employee_leave_records = os.environ['employee_leave_records']

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

# Mock employee leave data
employee_leaves = [
    {"empId": "EMP-001", "NoOfLeave": 5},
    {"empId": "EMP-002", "NoOfLeave": 3},
    {"empId": "EMP-003", "NoOfLeave": 8},
]

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    try:
        action_group = event.get('actionGroup', 'default')
        apiPath = event.get('apiPath', '/leave')
        httpMethod = event.get('httpMethod', 'GET')
        parameters = event.get('parameters', {})
        message_version = event.get('messageVersion', 1)

        if httpMethod.upper() == 'GET':
            if len(parameters) > 0 and parameters[0].get('name') == 'empId':
                empID = parameters[0]['value']
                empID = empID.upper()
            else:
                response_body = {
                    'application/json': {
                        'body': json.dumps({
                            "message": f"Can't Process request, EmpID isn't passed",
                            "remaining_balance": 0
                        })
                    }
                }

                action_response = {
                    'actionGroup': action_group,
                    'apiPath': apiPath,
                    'httpMethod': httpMethod,
                    'httpStatusCode': 200,
                    'responseBody': response_body
                }

                response = {
                    'response': action_response,
                    'messageVersion': message_version
                }

                return response

            query = f"select json_agg(row_to_json(row_values)) from (SELECT EmployeeName, EmployeeID, NoOfLeave from {schema}.{employee_details} WHERE EmployeeID = '{empID}') as row_values"
            res = select_db(query)

            if not res:
                response_body = {
                    'application/json': {
                        'body': json.dumps({
                            "message": f"Can't Process request, EmpID doesn't exist, Try with a proper Employee ID",
                            "remaining_balance": 0
                        })
                    }
                }

                action_response = {
                    'actionGroup': action_group,
                    'apiPath': apiPath,
                    'httpMethod': httpMethod,
                    'httpStatusCode': 200,
                    'responseBody': response_body
                }

                response = {
                    'response': action_response,
                    'messageVersion': message_version
                }

                return response

            # Return list of employee leaves
            response_body = {
                'application/json': {
                    'body': json.dumps(res[0][0])
                }
            }
            http_status = 200

        elif httpMethod.upper() == 'POST':
            try:
                body = event['requestBody']['content']['application/json']['properties']
                _parameters = {}
                for i in body:
                    _parameters[i['name']] = i['value']

                print("Inside Post : ", _parameters)
                # Extract mock input parameters
                empId = _parameters.get('empId', '')
                empId = empId.upper()
                reason = _parameters.get('reason', 'Personal')
                noOfDays = int(_parameters.get('noOfDays', 1))

                select_query = f"SELECT  NoOfLeave from {schema}.{employee_details} WHERE EmployeeID = '{empId}'"
                select_response = select_db(select_query)

                if not select_response:
                    response_body = {
                        'application/json': {
                            'body': json.dumps({
                                "message": f"Can't Process request, EmpID doesn't exist",
                                "remaining_balance": 0
                            })
                        }
                    }
                    action_response = {
                        'actionGroup': action_group,
                        'apiPath': apiPath,
                        'httpMethod': httpMethod,
                        'httpStatusCode': 200,
                        'responseBody': response_body
                    }

                    response = {
                        'response': action_response,
                        'messageVersion': message_version
                    }

                    return response

                _NoOfLeave = select_response[0][0]

                if _NoOfLeave < noOfDays:
                    response_body = {
                        'application/json': {
                            'body': json.dumps({
                                "message": f"Can't Process request, Not enough Leave available",
                                "remaining_balance": _NoOfLeave
                            })
                        }
                    }
                    action_response = {
                        'actionGroup': action_group,
                        'apiPath': apiPath,
                        'httpMethod': httpMethod,
                        'httpStatusCode': 200,
                        'responseBody': response_body
                    }

                    response = {
                        'response': action_response,
                        'messageVersion': message_version
                    }

                    return response

                _NoOfLeave = _NoOfLeave - noOfDays

                update_query = f"""
                UPDATE {schema}.{employee_details}
                SET NoOfLeave = '{_NoOfLeave}'
                WHERE EmployeeID = '{empId}'
                """

                update_db(update_query)

                insert_query = f"INSERT INTO {schema}.{employee_leave_records} (EmployeeID, Reason, NoOfDays) VALUES (%s, %s, %s);"
                insert_values = (empId, reason, noOfDays)
                insert_response = insert_db(insert_query, insert_values)

                # Mock response
                response_body = {
                    'application/json': {
                        'body': json.dumps({
                            "message": f"Leave applied successfully for {empId}.",
                            "remaining_balance": _NoOfLeave
                        })
                    }
                }
                http_status = 200
            except Exception as e:
                print(e)

        else:
            # Unsupported HTTP method
            response_body = {
                'application/json': {
                    'body': json.dumps({"message": f"HTTP method {httpMethod} not supported."})
                }
            }
            http_status = 400

        # Build action response
        action_response = {
            'actionGroup': action_group,
            'apiPath': apiPath,
            'httpMethod': httpMethod,
            'httpStatusCode': http_status,
            'responseBody': response_body
        }

        response = {
            'response': action_response,
            'messageVersion': message_version
        }

        return response

    except Exception as e:
        return {
            'statusCode': HTTPStatus.INTERNAL_SERVER_ERROR,
            'body': 'Internal server error'
        }
