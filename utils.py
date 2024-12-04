import decimal
import requests
import json
import datetime
import boto3
import os
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

dynamodb = boto3.resource('dynamodb')
calendar_table_name = os.environ['DYNAMODB_CALENDAR']
calendar_table = dynamodb.Table(calendar_table_name)


def create_response(status_code: int, message):
    """Returns a given status code."""

    return { 
        'statusCode': status_code,
        'headers': {
            # Required for CORS support to work
            'Access-Control-Allow-Origin': '*',
            # Required for cookies, authorization headers with HTTPS
            'Access-Control-Allow-Credentials': 'true',
        },
        'body': message
    }


class DecimalEncoder(json.JSONEncoder):
    """Helper class to convert a DynamoDB item to JSON."""

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
    """Returns formatted UTC datetime string of current time."""
    return datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")


def create_calendar_event(event):
    return calendar_table.put_item(Item=event)
    

def get_event_by_id(eventId, eventStart):
    """Returns details of a requested event from the calendar database."""

    print(f'eventId: {eventId}')
    print(f'eventStart: {eventStart}')
    try: 
        response = calendar_table.get_item(
            Key={
                'event_id': eventId,
                'start': eventStart,
            }
        )
        print(f"get_event_by_id response: {response}")
        return response['Item']
    except Exception as e:
        print(f"error with get_event_by_id")
        print(e)
    return ''
      

def get_events_during_time(time, site):
    """Gets calendar events at a site that are active during a given time.
    
    Args:
        time (str): UTC datestring (eg. '2022-05-14T17:30:00Z').
        site (str): sitecode (eg. 'saf').

    Returns:
        A list of event objects matching time and site criteria.
    """

    response = calendar_table.query(
        IndexName="site-end-index",
        KeyConditionExpression=
                Key('site').eq(site)
                & Key('end').gte(time),
        FilterExpression=Key('start').lte(time)
    )
    print(f"Items during {time}: {response['Items']}")
    return response['Items']


def get_projects_url(path):
    # Use the same projects deployment as the one running the calendar.
    # E.g. The dev calendar backend will call the dev projects backend
    stage = os.getenv('STAGE', 'dev')
    # The production projects url replaces 'prod' with 'projects' in the url
    if stage == 'prod':
        stage = 'projects'

    url = f"https://projects.photonranch.org/{stage}/{path}"
    return url
    

def get_project(project_name, created_at):
    """Get project details from the projects backend.

    Args:
        project_name (str):
            Name of the project in the projects-{stage} database.
        created_at (str):
            UTC datestring at creation (eg. '2022-05-14T17:30:00Z').

    Returns:
        Requested project details JSON, if response code 200.
    """
    url = get_projects_url('get-project')
    body = json.dumps({
        "project_name": project_name,
        "created_at": created_at,
    })
    response = requests.post(url, body)
    if response.status_code == 200:
        return response.json()
    else:
        return "Project not found."


def delete_calendar_event(event_id, start_time, user_making_request=None, requester_is_admin=True):
    """Deletes an event from the DynamoDB table with optional authorization check."""
    try:
        # Perform the delete operation
        response = calendar_table.delete_item(
            Key={
                'event_id': event_id,
                'start': start_time
            },
            ConditionExpression=":requesterIsAdmin = :true OR creator_id = :requester_id",
            ExpressionAttributeValues = {
                ":requester_id": user_making_request, 
                ":requesterIsAdmin": requester_is_admin,
                ":true": True
            }
        )
        return response  # Return the raw response data (typically including `Item` if success)
    except ClientError as e:
        # Return None to indicate failure 
        # In the future, handle different error codes as needed here
        print(f"error deleting event: {e}")
        return None
