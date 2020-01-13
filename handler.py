import json
import os
import boto3
import decimal
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError


dynamodb = boto3.resource('dynamodb')


#=========================================#
#=======     Helper Functions     ========#
#=========================================#

def create_200_response(message):
    return { 
        'statusCode': 200,
        'headers': {
            # Required for CORS support to work
            'Access-Control-Allow-Origin': '*',
            # Required for cookies, authorization headers with HTTPS
            'Access-Control-Allow-Credentials': 'true',
        },
        'body': message
    }

def create_403_response(message):
    return { 
        'statusCode': 403,
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
#=======       API Functions      ========#
#=========================================#

def addNewEvent(event, context):
    
    try:
        event_body = json.loads(event.get("body", ""))
        table = dynamodb.Table(os.environ['DYNAMODB_CALENDAR'])

        print("event_body:")
        print(event_body)

        # Check that all required keys are present.
        required_keys = ['event_id', 'start', 'site']
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
            'new_calendar_event': event_body,
        })
        return create_200_response(message)

    except Exception as e: 
        print(f"Exception: {e}")
        return create_200_response(json.dumps(e))

#def modifyEvent(event, context):
    #event_body = json.loads(event.get("body", ""))
    #table = dynamodb.Table(os.environ['DYNAMODB_CALENDAR'])

    ## If the start time is new, we need to delete and recreate the item
    ## (since start time is the sort key for our table)

    ## If the start time has not changed, a simple update will work.
    #response = table.update_item(
        #Key={
            #'event_id': '',
            #'start': ''
        #},
        #UpdateExpression="set "
    #)


def deleteEventById(event, context):

    event_body = json.loads(event.get("body", ""))
    table = dynamodb.Table(os.environ['DYNAMODB_CALENDAR'])

    print("event")
    print(json.dumps(event))
    userMakingThisRequest = event["requestContext"]["authorizer"]["principalId"]
    print(f"userMakingThisRequest: {userMakingThisRequest}")

    eventToDelete = event_body['event_id']
    startTime = event_body['start']

    try:
        response = table.delete_item(
            Key={
                'event_id': eventToDelete,
                'start': startTime
            },
            ConditionExpression="creator_id = :user_id",
            ExpressionAttributeValues = {
                ":user_id": userMakingThisRequest, 
            }
        )
    except ClientError as e:
        if e.response['Error']['Code'] == "ConditionalCheckFailedException":
            print(e.response['Error']['Message'])
            return create_403_response("You are not authorized to delete this event.")
        return create_403_response(json.dumps(e))
    
    else:
        message = json.dumps(response, indent=4, cls=DecimalEncoder)
        return create_200_response({"delete_item response": message})

    #except Exception as e: 
        #print(f"Exception: {e}")
        #return create_200_response(json.dumps(e))


def getWMDEvents(event, context):
    
    site = "wmd"
    table = dynamodb.Table(os.environ['DYNAMODB_CALENDAR'])

    res = table.query(
        IndexName="site_events",
        KeyConditionExpression=Key('site').eq(site)
    )

    message = json.dumps({'results':res})
    return create_200_response(message)

def getSiteEventsInDateRange(event, context):

    event_body = json.loads(event.get("body", ""))
    table = dynamodb.Table(os.environ['DYNAMODB_CALENDAR'])

    # Check that all required keys are present.
    required_keys = ['site', 'start', 'end']
    actual_keys = event_body.keys()
    for key in required_keys:
        if key not in actual_keys:
            print(f"Error: missing required key {key}")
            return {
                "statusCode": 400,
                "body": f"Error: missing required key {key}",
                "headers": {
                    "Access-Control-Allow-Origin": "*",
                    "Access-Control-Allow-Credentials": "true",
                },
            }

    start_date = event_body['start']
    end_date = event_body['end']
    site = event_body['site']

    table_response = table.query(
        IndexName="site_events",
        KeyConditionExpression=Key('site').eq(site) & Key('start').between(start_date, end_date)
    )

    message = json.dumps({
        'table_response': table_response,
        'new_calendar_event': event_body,
    })

    return create_200_response(message)







