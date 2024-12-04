import json
from boto3.dynamodb.conditions import Key

from utils import DecimalEncoder
from utils import calendar_table
from utils import create_response
from utils import get_utc_iso_time
from utils import create_calendar_event
from utils import get_event_by_id
from utils import get_events_during_time
from utils import get_project
from utils import delete_calendar_event


#=========================================#
#=======       API Endpoints      ========#
#=========================================#

def addNewEvent(event, context):
    """Endpoint to add a new event (reservation) to the calendar.

    Args:
        event.body.event_id (str): 
            Unique id generated for each new event (eg. '999xx09b-xxxx-...').
        event.body.start (str): 
            UTC datestring of starting time (eg. '2022-05-14T17:30:00Z').
        event.body.site (str):
            sitecode (eg. 'saf').

    Returns:
        200 status code with new calendar event if successful.
        400 status code if missing required keys or otherwise unsuccessful.
    """

    try:
        event_body = json.loads(event.get("body", ""))

        print("event_body:")
        print(event_body)

        # Check that all required keys are present.
        required_keys = ['event_id', 'start', 'site']
        actual_keys = event_body.keys()
        for key in required_keys:
            if key not in actual_keys:
                msg = f"Error: missing required key {key}"
                print(msg)
                return create_response(400, msg)

        # Add creation date
        event_body["last_modified"] = get_utc_iso_time()

        result = create_calendar_event(event_body)

        message = json.dumps({
            'table_response': result,
            'new_calendar_event': event_body,
        })
        return create_response(200, message)

    # Something else went wrong, return a Bad Request status code.
    except Exception as e: 
        print(f"Exception: {e}")
        return create_response(400, json.dumps(e))


def modifyEvent(event, context):
    """Endpoint to update an existing calendar events with changes.

    A user may only modify their own events in the calendar,
    unless they are an admin.

    Args:
        event.body.event_id (str): 
            Unique id generated for each new event (eg. '999xx09b-xxxx-...').
        event.body.start (str): 
            UTC datestring of starting time (eg. '2022-05-14T17:30:00Z').
        context.requestContext.authorizer.principalID (str):
            Auth0 user 'sub' token (eg. 'google-oauth2|xxxxxxxxxxxxx').
        context.requestContext.authorizer.userRoles (str):
            Global user account type (eg. 'admin').

    Returns:
        200 status code with modified project body if successful.
        403 status code if user is unauthorized.
    """

    event_body = json.loads(event.get("body", ""))

    originalEvent = event_body['originalEvent']
    modifiedEvent = event_body['modifiedEvent']

    originalId =  originalEvent['event_id']
    originalStart = originalEvent['start']

    # Make sure the user is admin, or modifying their own event
    creatorId = get_event_by_id(originalId, originalStart)['creator_id']
    userMakingThisRequest = event["requestContext"]["authorizer"]["principalId"]
    userRoles = json.loads(event["requestContext"]["authorizer"]["userRoles"])
    if creatorId != userMakingThisRequest and 'admin' not in userRoles:
        return create_response(403, "You may only modify your own events.")

    # Delete and recreate the item since start time is the sort key for our table
    delete_res = delete_calendar_event(originalId, originalStart)
    print(f"delete response: {delete_res}")
    # Ensure the eventId and creator do not change
    modifiedEvent['event_id'] = originalId
    modifiedEvent['creator_id'] = creatorId

    # Update last modified time
    modifiedEvent['last_modified'] = get_utc_iso_time()
    response = create_calendar_event(modifiedEvent)
    print(f"create response: {response}")
    return create_response(200, json.dumps(response))


def addProjectsToEvents(event, context):
    """Endpoint to add project ids to calendar events.
    
    Args:
        event.body.project_id (str): 
            Id of the project to add to the event
            (eg. 'Orion Assignment#2022-02-14T17:30:00Z').
        event.body.events (arr):
            Contains dicts for each calendar event we want to add the 
            project to. Each dict has keys 'event_id' and 'start', 
            which are the partition key and sort key for the event.

    Returns:
        200 status code with list of items updated in the calendar database.
    """

    event_body = json.loads(event.get("body", ""))

    print("event")
    print(json.dumps(event))

    project_id = event_body['project_id']
    events = event_body['events']

    responses = []
    for event in events:
        resp = calendar_table.update_item(
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

    return create_response(200, json.dumps(responses, indent=4, cls=DecimalEncoder))


def removeProjectFromEvents(event, context):
    """Endpoint to remove projects from calendar events. 
    
    Args:
        event.body.events (arr): 
            Contains dicts for each calendar event we want to add the 
            project to. Each dict has keys 'event_id' and 'start', 
            which are the partition key and sort key for the event.

    Returns:
        200 status code with success message.
    """

    request_body = json.loads(event.get("body"))

    events = request_body['events']
    print(request_body)

    for event_id in events:

        # Get the start value from the event with given event_id
        # We need both values to do an update_item operation
        query_response = calendar_table.query(
            Key={
                "event_id": event_id,
            }
        )
        print(f"query response: {query_response}")
        start = query_response['Items'][0]['start']

        # Update the item, setting the project_id to 'none'
        update_response = calendar_table.update_item(
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

    return create_response(200, "Success")
    

def deleteEventById(event, context):
    """Endpoint to delete calendar events with an event_id.

    Args:
        event.body.event_id (str):
            Unique id for events to delete (eg. '999xx09b-xxxx-...').
        event.body.start (str): 
            UTC datestring of starting time (eg. '2022-05-14T17:30:00Z').
        context.requestContext.authorizer.principalID (str):
            Auth0 user 'sub' token (eg. 'google-oauth2|xxxxxxxxxxxxx').
        context.requestContext.authorizer.userRoles (str):
            Global user account type (eg. 'admin').

    Returns:
        200 status code with response.

    Raises:
        ClientError: ConditionalCheckFailedException with
        status code 403 if the requesting user is unauthorized.
    """

    event_body = json.loads(event.get("body", ""))

    # Get the user's roles provided by the lambda authorizer
    userMakingThisRequest = event["requestContext"]["authorizer"]["principalId"]
    userRoles = json.loads(event["requestContext"]["authorizer"]["userRoles"])
    print(f"userMakingThisRequest: {userMakingThisRequest}")
    print(f"userRoles: {userRoles}")

    # Check if the requester is an admin
    requesterIsAdmin="false"
    if 'admin' in userRoles:
        requesterIsAdmin="true"
    print(f"requesterIsAdmin: {requesterIsAdmin}")

    # Specify the event with our pk (eventId) and sk (startTime)
    eventId = event_body['event_id']
    startTime = event_body['start']

    result = delete_calendar_event(eventId, startTime, userMakingThisRequest, requesterIsAdmin)
    if result is None:
        return create_response(403, "You may only modify your own events.")
    
    message = json.dumps(result, indent=4, cls=DecimalEncoder)
    return create_response(200, message)


def getSiteEventsInDateRange(event, context):
    """Return calendar events within a specified date range at a given site.

    Args:
        event.body.event_id (str):
            Unique id for event (eg. '999xx09b-xxxx-...').
        event.body.start (str):
            UTC datestring of starting time (eg. '2022-05-14T17:30:00Z').
        event.body.end (str):
            UTC datestring of ending time (eg. '2022-05-14T18:00:00Z').

    Returns:
        200 status code with list of matching events objects.
        400 status code if a required key is missing.

    Sample Python request to this endpoint: 

        import requests, json
        url = "https://calendar.photonranch.org/dev/siteevents"
        body = json.dumps({
            "site": "saf", 
            "start": "2022-06-01T01:00:00Z",
            "end": "2022-06-02T01:00:00Z",
            "full_project_details": True
        })
        response = requests.post(url, body).json()
    """

    request_body = json.loads(event.get("body", ""))
    print(request_body)

    # Check that all required keys are present.
    required_keys = ['site', 'start', 'end']
    actual_keys = request_body.keys()
    for key in required_keys:
        if key not in actual_keys:  
            msg = f"Error: missing required key {key}"
            print(msg)
            return create_response(400, msg)

    start_date = request_body['start']
    end_date = request_body['end']
    site = request_body['site']

    table_response = calendar_table.query(
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
                e['project'] = get_project(project_name, created_at)

    return create_response(200, json.dumps(events, cls=DecimalEncoder))


def getUserEventsEndingAfterTime(event, context):
    """Return a list of user events that are ending after a specified time.

    Args:
        event.body.time (str):
            UTC datestring (eg. '2022-05-14T17:30:00Z').
        event.body.user_id (str):
            Auth0 user 'sub' (eg. 'google-oauth2|xxxxxxxxxxxxx').

    Returns:
        200 status code with list of matching event objects.
    """

    event_body = json.loads(event.get("body", ""))

    print("event body:")
    print(event_body)

    user_id = event_body["user_id"]
    time = event_body["time"]

    response = calendar_table.query(
        IndexName="creatorid-end-index",
        KeyConditionExpression=
                Key('creator_id').eq(user_id)
                & Key('end').gte(time)
    )
    return create_response(200, json.dumps(response['Items'], cls=DecimalEncoder))


def getEventAtTime(event, context):
    """Return events that are happening at a given point in time.

    Args: 
        event.body.time (str): UTC datestring (eg. '2022-05-14T17:30:00Z').
        event.body.site (str): Sitecode (eg. 'saf').
    
    Returns:
        200 status code with list of matching event objects.
    """

    event_body = json.loads(event.get("body", ""))
    print("event body:")
    print(event_body)

    time = event_body["time"]
    site = event_body["site"]
    events = get_events_during_time(time, site)
    return create_response(200, json.dumps(events))
      

def isUserScheduled(event, context):
    """Check if a user is scheduled for an event at a specific site and time.

    Args:
        event.body.user_id (str):
            Auth0 user 'sub' (eg. 'google-oauth2|xxxxxxxxxxxxx').
        event.body.site (str):
            Sitecode (eg. 'saf').
        event.body.time (str):
            UTC datestring (eg. '2022-05-14T17:30:00Z').

    Returns:
        A 200 status code with a list of allowed users for an event.
    """
   
    event_body = json.loads(event.get("body", ""))
    print("event body:")
    print(event_body)

    user = event_body["user_id"]
    site = event_body["site"]
    time = event_body["time"]

    events = get_events_during_time(time, site)
    allowed_users = [event["creator_id"] for event in events]
    print(f"Allowed users: {allowed_users}")
    return create_response(200, user in allowed_users)


def doesConflictingEventExist(event, context):
    """Checks for existing calendar events at a given site and time.
    
    Calendar events should only let the designated user use the observatory.
    If there are no reservations, anyone can use it. 

    Args:
        event.body.user_id (str):
            Auth0 user 'sub' (eg. 'google-oauth2|xxxxxxxxxxxxx').
        event.body.site (str):
            Sitecode (eg. 'saf').
        event.body.time (str):
            UTC datestring (eg. '2022-05-14T17:30:00Z').

    Returns:
        200 status code. bool: True if a different user has a reservation
        at the specified time. False otherwise.
    """

    event_body = json.loads(event.get("body", ""))

    print("event body:")
    print(event_body)

    user = event_body["user_id"]
    site = event_body["site"]
    time = event_body["time"]

    events = get_events_during_time(time, site)

    # If any events belong to a different user, return True (indicating conflict)
    for event in events:
        if event["creator_id"] != user:
            return create_response(200, True)

    # Otherwise, report no conflicts (return False)
    return create_response(200, False)
 