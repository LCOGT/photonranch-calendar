import json
import os
import boto3
import decimal
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError


dynamodb = boto3.resource('dynamodb')

projects_table = os.environ['PROJECTS_TABLE']


#=========================================#
#=======     Helper Functions     ========#
#=========================================#

def create_response(statusCode, message):
    return { 
        'statusCode': statusCode,
        'headers': {
            # Required for CORS support to work
            'Access-Control-Allow-Origin': '*',
            # Required for cookies, authorization headers with HTTPS
            'Access-Control-Allow-Credentials': 'true',
        },
        'body': message
    }

# Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)


#=========================================#
#=======       API Endpoints      ========#
#=========================================#

def addNewProject(event, context):
    
    event_body = json.loads(event.get("body", ""))
    table = dynamodb.Table(projects_table)

    print("event_body:")
    print(event_body)

    # Check that all required keys are present.
    required_keys = ['project_name', 'user_id', 'created_at']
    actual_keys = event_body.keys()
    for key in required_keys:
        if key not in actual_keys:
            print(f"Error: missing requied key {key}")
            return {
                "statusCode": 400,
                "body": f"Error: missing required key {key}",
                "headers": {
                    "Access-Control-Allow-Origin": "*",
                    "Access-Control-Allow-Credentials": "true",
                },
            }

    table_response = table.put_item(Item=event_body)

    message = json.dumps({
        'table_response': table_response,
        'new_project': event_body,
    })
    return create_response(200, message)

def getProject(event, context):

    event_body = json.loads(event.get("body", ""))
    table = dynamodb.Table(projects_table)

    print("event_body:")
    print(event_body)

    project_name = event_body['project_name']
    created_at = event_body['created_at']

    response = table.get_item(
        Key={
            "project_name": project_name,
            "created_at": created_at,
        }
    )
    print(f"getProject response: {response}")
    if 'Item' in response:
        project = json.dumps(response['Item'], cls=DecimalEncoder)
        return create_response(200, project)
    else: 
        return create_response(404, "Project not found.")

def getAllProjects(event, context):
    '''
    example python code that uses this endpoint:

        import requests
        url = "https://projects.photonranch.org/dev/get-all-projects"
        all_projects = requests.post(url).json()

    '''

    table = dynamodb.Table(projects_table)

    response = table.scan()
    data = response['Items']

    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        data.extend(response['Items'])

    return create_response(200, json.dumps(data, cls=DecimalEncoder))


def getUserProjects(event, context):

    event_body = json.loads(event.get("body", ""))
    table = dynamodb.Table(projects_table)

    print("event_body:")
    print(event_body)

    # Check that all required keys are present.
    required_keys = ['user_id']
    actual_keys = event_body.keys()
    for key in required_keys:
        if key not in actual_keys:
            print(f"Error: missing requied key {key}")
            return {
                "statusCode": 400,
                "body": f"Error: missing required key {key}",
                "headers": {
                    "Access-Control-Allow-Origin": "*",
                    "Access-Control-Allow-Credentials": "true",
                },
            }

    response = table.query(
        IndexName="userid-createdat-index",
        KeyConditionExpression=Key('user_id').eq(event_body['user_id'])
    )
    print(response)
    user_projects = json.dumps(response['Items'], cls=DecimalEncoder)

    return create_response(200, user_projects)

def addProjectData(event, context):
    '''
    When an observatory captures and uploads an image requested in a project,
    it should use this endpoint to update the projects completion status.
    '''

    event_body = json.loads(event.get("body", ""))
    table = dynamodb.Table(projects_table)
    #table = dynamodb.Table("photonranch-projects")

    print("event")
    print(json.dumps(event))

    # unique project identifier
    project_name = event_body["project_name"]
    created_at = event_body["created_at"]

    # Indices for where to save the new data in project_data
    target_index = event_body["target_index"]
    exposure_index = event_body["exposure_index"]

    # Data to save
    base_filename = event_body["base_filename"]


    # First, get the 'project_data' and 'remaining' arrays we want to update
    # 'project_data[target_index][exposure_index]' stores filenames of completed exposures
    # 'remaining[target_index][exposure_index[' is the number of exposures remaining
    resp1 = table.get_item(
        Key={
            "project_name": project_name,
            "created_at": created_at,
        }
    ) 
    project_data = resp1["Item"]["project_data"]
    remaining = resp1["Item"]["remaining"]

    # Next, add our new information
    project_data[target_index][exposure_index].append(base_filename)
    remaining[target_index][exposure_index] = int(remaining[target_index][exposure_index]) - 1

    print("updated values: ")
    print(project_data)
    print(remaining)
    
    # Finally, update the dynamodb project entry with the revised 'project_data' and 'remaining'
    resp2 = table.update_item(
        Key={
            "project_name": project_name,
            "created_at": created_at,
        },
        UpdateExpression="SET #project_data = :project_data_updated, #remaining = :remaining_updated",
        ExpressionAttributeNames={
            "#project_data": "project_data",
            "#remaining": "remaining",
        },
        ExpressionAttributeValues={
            ":project_data_updated": project_data,
            ":remaining_updated": remaining,
        }
    )
    if resp2["ResponseMetadata"]["HTTPStatusCode"] == 200:
        return create_response(200, json.dumps({"message": "success"}))
    else:
        return create_response(500, json.dumps({"message": "failed to update project in dynamodb"}))


def deleteProject(event, context):

    event_body = json.loads(event.get("body", ""))
    table = dynamodb.Table(projects_table)

    print("event")
    print(json.dumps(event))

    # Get the user's roles provided by the lambda authorizer
    userMakingThisRequest = event["requestContext"]["authorizer"]["principalId"]
    print(f"userMakingThisRequest: {userMakingThisRequest}")
    userRoles = json.loads(event["requestContext"]["authorizer"]["userRoles"])
    print(f"userRoles: {userRoles}")

    # Check if the requester is an admin
    requesterIsAdmin="false"
    if 'admin' in userRoles:
        requesterIsAdmin="true"
    print(f"requesterIsAdmin: {requesterIsAdmin}")

    # Specify the event with our pk (project_name) and sk (created_at)
    project_name = event_body['project_name']
    created_at = event_body['created_at']

    try:
        response = table.delete_item(
            Key={
                "project_name": project_name,
                "created_at": created_at
            },
            ConditionExpression=":requesterIsAdmin = :true OR user_id = :requester_id",
            ExpressionAttributeValues = {
                ":requester_id": userMakingThisRequest, 
                ":requesterIsAdmin": requesterIsAdmin,
                ":true": "true"
            }
        )
    except ClientError as e:
        print(f"error deleting project: {e}")
        if e.response['Error']['Code'] == "ConditionalCheckFailedException":
            print(e.response['Error']['Message'])
            return create_response(403, "You may only delete your own projects.")
        return create_response(403, e.response['Error']['Message'])
    
    message = json.dumps(response, indent=4, cls=DecimalEncoder)
    print(f"success deleting project; message: {message}")
    return create_response(200, message)


if __name__=="__main__":

    projects_table = "photonranch-projects"

    time = "2020-05-12T16:40:00Z" # This should be during 'cool cave' at ALI-sim
    site = "ALI-sim"
    user_id = "google-oauth2|100354044221813550027"

    event = {
        "body": json.dumps({
            "user_id": user_id,
            "site": site,
            "time": time
        })
    }

    event = {
        "body": json.dumps({
            "project_name": "m101",
            "created_at": "2020-06-24T16:53:56Z",
            "target_index": 0,
            "exposure_index": 0,
            "base_filename": "test_filename",
        })
    }
    addProjectData(event, {})

    #print(isUserScheduled(event, {}))
    #print(getUserEventsEndingAfterTime(event, {}))




