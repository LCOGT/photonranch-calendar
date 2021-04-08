import json
import os
import boto3
import decimal
import requests
import datetime
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError


dynamodb = boto3.resource('dynamodb')

calendar_table_name = os.environ['DYNAMODB_CALENDAR']


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
        if isinstance(o, set):
            return list(o)
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)


def get_utc_iso_time():
    return datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")


def getEvent(eventId, eventStart):

    print(f'eventId: {eventId}')
    print(f'eventStart: {eventStart}')
    table = dynamodb.Table(calendar_table_name)
    try: 
        response = table.get_item(
            Key={
                'event_id': eventId,
                'start': eventStart,
            }
        )
        print(f"getEvent response: {response}")
        return response['Item']
    except Exception as e:
        print(f"error with getEvent")
        print(e)
    return ''
        
def getEventsDuringTime(time, site):
    ''' 
    Get any calendar events at a site that are active during the given time 
    Args:
        time: UTC datestring (eg. '2020-05-14T17:30:00Z')
    '''
    table = dynamodb.Table(calendar_table_name)
    response = table.query(
        IndexName="site-end-index",
        KeyConditionExpression=
                Key('site').eq(site)
                & Key('end').gte(time),
        FilterExpression=Key('start').lte(time)
    )
    #table = dynamodb.Table('photonranch-calendar')
    print(f"Items during {time}: {response['Items']}")
    return response['Items']

def getProject(project_name, created_at):
    url = "https://projects.photonranch.org/dev/get-project"
    body = json.dumps({
        "project_name": project_name,
        "created_at": created_at,
    })
    response = requests.post(url, body)
    if response.status_code == 200:
        return response.json()
    else:
        return "Project not found."

#=========================================#
#=======       API Endpoints      ========#
#=========================================#

def addNewEvent(event, context):
    
    try:
        event_body = json.loads(event.get("body", ""))
        table = dynamodb.Table(calendar_table_name)

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

        # add creation date
        event_body["last_modified"] = get_utc_iso_time()

        table_response = table.put_item(Item=event_body)

        message = json.dumps({
            'table_response': table_response,
            'new_calendar_event': event_body,
        })
        return create_200_response(message)

    except Exception as e: 
        print(f"Exception: {e}")
        return create_200_response(json.dumps(e))

def modifyEvent(event, context):
    table = dynamodb.Table(calendar_table_name)
    event_body = json.loads(event.get("body", ""))

    originalEvent = event_body['originalEvent']
    modifiedEvent = event_body['modifiedEvent']

    originalId =  originalEvent['event_id']
    originalStart = originalEvent['start']
    modifiedStart = modifiedEvent['start']

    # Make sure the user is admin, or modifying their own event
    creatorId = getEvent(originalId, originalStart)['creator_id']
    userMakingThisRequest = event["requestContext"]["authorizer"]["principalId"]
    userRoles = json.loads(event["requestContext"]["authorizer"]["userRoles"])
    if creatorId != userMakingThisRequest and 'admin' not in userRoles:
        return create_403_response("You may only modify your own events.")

    # If the start time is new, we need to delete and recreate the item
    # (since start time is the sort key for our table)
    #if originalStart != modifiedStart:
    delRes = table.delete_item(
        Key={
            'event_id': originalId,
            'start': originalStart,
        }
    )
    print(f"delete response: {delRes}")
    # Ensure the eventId and creator do not change
    modifiedEvent['event_id'] = originalId
    modifiedEvent['creator_id'] = creatorId

    # update last modified time
    modifiedEvent['last_modified'] = get_utc_iso_time()
    response = table.put_item(Item=modifiedEvent)
    print(f"put response: {response}")
    return create_200_response(json.dumps(response))

def addProjectsToEvents(event, context):
    """
    Endpoint to add project ids to calendar events. 
    Args:
        event.body.project_id (str): id of the project to add to the event (project_name#created_timestamp)
        event.body.events (array): contains dicts for each calendar event we 
            want to add the project to. Each dict has keys 'event_id' and 
            'start', which are the partition key and sort key for the event. 
    Returns:
        nothing important. just a list of responses from the item updates.
    """

    event_body = json.loads(event.get("body", ""))
    table = dynamodb.Table(calendar_table_name)

    print("event")
    print(json.dumps(event))

    ## Get the user's roles provided by the lambda authorizer
    #userMakingThisRequest = event["requestContext"]["authorizer"]["principalId"]
    #print(f"userMakingThisRequest: {userMakingThisRequest}")
    #userRoles = json.loads(event["requestContext"]["authorizer"]["userRoles"])
    #print(f"userRoles: {userRoles}")

    ## Check if the requester is an admin
    #requesterIsAdmin="false"
    #if 'admin' in userRoles:
        #requesterIsAdmin="true"
    #print(f"requesterIsAdmin: {requesterIsAdmin}")

    project_id = event_body['project_id']
    events = event_body['events']

    responses = []
    for event in events:
        resp = table.update_item(
            Key={
                "event_id": event["event_id"],
                "start": event["start"],
            },
            UpdateExpression="SET project_id = :id",
            ExpressionAttributeValues={
                ":id": project_id,
            }
        )
        responses.append(resp)

    return create_200_response(json.dumps(responses, indent=4, cls=DecimalEncoder))

def removeProjectFromEvents(event, context):
    '''
    Endpoint to remove project ids to calendar events. 
    Args:
        event.body.events (array): list of 'event_id' to modify

    '''

    request_body = json.loads(event.get("body"))
    table = dynamodb.Table(calendar_table_name)

    events = request_body['events']
    print(request_body)

    for event_id in events:

        # get the start value from the event with given event_id
        # We need both values to do an update_item operation
        query_response = table.query(
            Key={
                "event_id": event_id,
            }
        )
        print(f"query response: {query_response}")
        start = query_response['Items'][0]['start']

        # Update the item, setting the project_id to 'none'
        update_response = table.update_item(
            Key={
                "event_id": event_id,
                "start": start,
            },
            UpdateExpression="SET project_id = :none",
            ExpressionAttributeValues={
                ":none": "none"
            }
        )
        print(f'update response: {update_response}')

    return create_200_response("Success")
    

def deleteEventById(event, context):

    event_body = json.loads(event.get("body", ""))
    table = dynamodb.Table(calendar_table_name)

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

    # Specify the event with our pk (eventToDelete) and sk (startTime)
    eventToDelete = event_body['event_id']
    startTime = event_body['start']

    try:
        response = table.delete_item(
            Key={
                'event_id': eventToDelete,
                'start': startTime
            },
            ConditionExpression=":requesterIsAdmin = :true OR creator_id = :requester_id",
            ExpressionAttributeValues = {
                ":requester_id": userMakingThisRequest, 
                ":requesterIsAdmin": requesterIsAdmin,
                ":true": "true"
            }
        )
    except ClientError as e:
        print(f"error deleting event: {e}")
        if e.response['Error']['Code'] == "ConditionalCheckFailedException":
            print(e.response['Error']['Message'])
            return create_403_response("You may only modify your own events.")
        return create_403_response(e.response['Error']['Message'])
    
    message = json.dumps(response, indent=4, cls=DecimalEncoder)
    print(f"success deleting event, message: {message}")
    return create_200_response(message)


def getSiteEventsInDateRange(event, context):
    ''' 
    Sample python request to this endpoint: 

        import requests, json
        url = "https://calendar.photonranch.org/dev/siteevents"
        body = json.dumps({
            "site": "saf", 
            "start": "2020-06-01T01:00:00Z",
            "end": "2020-06-02T01:00:00Z",
            "full_project_details": True
        })
        response = requests.post(url, body).json()

    '''

    request_body = json.loads(event.get("body", ""))
    print(request_body)
    table = dynamodb.Table(calendar_table_name)

    # Check that all required keys are present.
    required_keys = ['site', 'start', 'end']
    actual_keys = request_body.keys()
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

    start_date = request_body['start']
    end_date = request_body['end']
    site = request_body['site']

    table_response = table.query(
        IndexName="site-end-index",
        KeyConditionExpression=Key('site').eq(site) & Key('end').between(start_date, end_date)
    )

    events = table_response['Items']

    if 'full_project_details' in request_body and request_body['full_project_details']:
        # Get the project details for each event. 
        for e in events: 
            project_id = e['project_id']
            if project_id != "none":
                project_name = project_id.split('#')[-2]
                created_at = project_id.split('#')[-1]
                e['project'] = getProject(project_name, created_at)



    return create_200_response(json.dumps(events, cls=DecimalEncoder))

def getUserEventsEndingAfterTime(event, context):
    event_body = json.loads(event.get("body", ""))
    table = dynamodb.Table(calendar_table_name)
    #table = dynamodb.Table('photonranch-calendar')

    print("event body:")
    print(event_body)

    user_id = event_body["user_id"]
    time = event_body["time"]

    response = table.query(
        IndexName="creatorid-end-index",
        KeyConditionExpression=
                Key('creator_id').eq(user_id)
                & Key('end').gte(time)
    )
    return create_200_response(json.dumps(response['Items'], cls=DecimalEncoder))


def getEventAtTime(event, context):
    '''
    Return events that are happening at a give point in time
    Args: 
        event.body.time: UTC datestring (eg. '2020-05-14T17:30:00Z')
        event.body.site: sitecode (eg. 'wmd')
    Return:
        list of event objects
    '''
    event_body = json.loads(event.get("body", ""))
    table = dynamodb.Table(calendar_table_name)
    print("event body:")
    print(event_body)

    time = event_body["time"]
    site = event_body["site"]
    events = getEventsDuringTime(time, site)
    return create_200_response(json.dumps(events))
        
def isUserScheduled(event, context):
    '''
    Check if a user has a calendar event for a specific site and time.
    Args: 
        event.body.user_id: auth0 user 'sub' (eg. "google-oauth2|xxxxxxxxxxxxx")
        event.body.site: site code (eg. "wmd")
        event.body.time: UTC datestring (eg. '2020-05-14T17:30:00Z')
    '''
    event_body = json.loads(event.get("body", ""))
    print("event body:")
    print(event_body)

    table = dynamodb.Table(calendar_table_name)

    user = event_body["user_id"]
    site = event_body["site"]
    time = event_body["time"]

    events = getEventsDuringTime(time, site)
    allowed_users = [event["creator_id"] for event in events]
    print(f"Allowed users: {allowed_users}")
    return create_200_response(user in allowed_users)

def doesConflictingEventExist(event, context):
    '''
    Calendar reservations should only let the designated user use the observatory.
    If there are no reservations, anyone can use it. 
    Args:
        event.body.user_id: auth0 user 'sub' (eg. "google-oauth2|xxxxxxxxxxxxx")
        event.body.site: site code (eg. "wmd")
        event.body.time: UTC datestring (eg. '2020-05-14T17:30:00Z')
    Returns:
        True if a different user has a reservation at the specified time.
        False otherwise.
    '''

    event_body = json.loads(event.get("body", ""))

    print("event body:")
    print(event_body)

    table = dynamodb.Table(calendar_table_name)

    user = event_body["user_id"]
    site = event_body["site"]
    time = event_body["time"]

    events = getEventsDuringTime(time, site)

    # If any events belong to a different user, return True (indicating conflict)
    for event in events:
        if event["creator_id"] != user:
            return create_200_response(True)

    # Otherwise, report no conflicts (return False)
    return create_200_response(False)
    






if __name__=="__main__":

    calendar_table_name = "photonranch-calendar"

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

    #print(isUserScheduled(event, {}))
    #print(getUserEventsEndingAfterTime(event, {}))




