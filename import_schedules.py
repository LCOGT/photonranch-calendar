import requests
import json
from datetime import datetime, timezone, timedelta
import uuid
import boto3
from boto3.dynamodb.conditions import Key, Attr

from utils import calendar_table
from utils import get_projects_url
from utils import create_calendar_event
from utils import create_response

ssm = boto3.client('ssm')


class Observation:
    def __init__(self, site, observation):
        self.site = site
        self.observation = observation

    def create_ptr_resources(self):
        self.create_calendar()
        self.create_project()

    def _translate_to_calendar(self):
        # we need the project_id, so do the project translation if it doesn't exist
        if not hasattr(self, 'project'):
            self._translate_to_project()

        observation = self.observation # just for readability
        event = {
            "event_id": str(uuid.uuid4()),
            "start": observation["start"],
            "end": observation["end"],
            "creator": observation["submitter"],
            "creator_id": f'{observation["submitter"]}#LCO',
            "last_modified": observation["modified"],
            "project_id": self.project["project_id"],
            "project_priority": "standard",
            "reservation_note": "This event was created and scheduled by the LCO Scheduler",
            "reservation_type": "project",
            "origin": "LCO",
            "resourceId": observation["site"],
            "site": observation["site"],
            "title": f"{observation['name']} (via LCO)"
        }
        if observation["observation_type"] in ["RAPID_RESPONSE", "TIME_CRITICAL"]:
            event["project_prioirty"] = "time_critical"

        self.calendar_event = event

    def _translate_to_project(self):
        def truncate_microseconds(date_str):
            return date_str.split('.')[0] + 'Z' if len(date_str.split('.')) > 1 else date_str

        observation = self.observation # just for readability
        config_idx = 0 # short term: only use the first configuration

        configuration = observation["request"]["configurations"][config_idx]

        project_name = observation["name"]
        created_at = truncate_microseconds(observation["created"])
        start_date = truncate_microseconds(observation["start"])
        expiry_date = truncate_microseconds(observation["end"])
        user_name = observation["submitter"] + "(via LCO)"
        user_id = observation["submitter"] + "#LCO"
        project = {
            # required keys first
            "user_id": user_id,
            "project_name": project_name,
            "created_at": created_at,
            "origin": "LCO",

            # not part of ptr project spec, but might be useful to include
            "full_lco_observation": json.dumps(observation),

            "project_id": f"{project_name}#{created_at}",

            # with PTR these are usually used for scheduling, but from the observation they specify
            # the start and end times for the already-created schedule. 
            # Including them here doesn't hurt and doesn't deviate from their meaning
            "start_date": start_date,
            "expiry_date": expiry_date,

            # Most of these are used by PTR for scheduling, but we'll include them here
            # if they are provided by the observation anyways.
            "project_constraints": {

            "ra_offset": configuration["instrument_configs"][0]["extra_params"]["offset_ra"],
            "ra_offset_units": "deg",
            "dec_offset": configuration["instrument_configs"][0]["extra_params"]["offset_dec"],
            "dec_offset_units": "deg",
            "defocus": configuration["instrument_configs"][0]["extra_params"].get("defocus", 0),

            # convert phase from [0,1] to %
            "lunar_phase_max": float(configuration["constraints"]["max_lunar_phase"]) * 100,
            "lunar_dist_min": float(configuration["constraints"]["min_lunar_distance"]),
            "max_airmass": float(configuration["constraints"]["max_airmass"]),

            "project_is_active": True,
            "start_date": start_date,
            "expiry_date": expiry_date

            # "sub_stack": True,
            # "smart_stack": True,
            },
            "project_creator": {
            "username": user_name,
            "user_id": user_id
            },
            "project_priority": "standard",
            "project_sites": [ observation["site"] ],
            "project_note": "Created automatically with the LCO scheduler",

            # not sure if we should use this
            "scheduled_with_events": [],
        }

        project["project_targets"] = [ configuration["target"] ]
        project["project_targets"][0]["ra"] /= 15 # convert from degrees to hours to fit PTR defaults

        # Initialize and populate the requested exposures, along with progress and completed data.
        project["exposures"] = []
        project["remaining"] = []
        project["project_data"] = []
        for inst_config in configuration["instrument_configs"]:
            exposure_set = {
            "exposure": float(inst_config["exposure_time"]),
            "count": int(inst_config["exposure_count"]),
            "filter": inst_config["optical_elements"]["filter"], # note: need to ensure matching filter names
            "imtype": configuration["type"], # note: need a mapping from configuration_type to what ptr is used to: ["light", "dark", "bias", "focus"]
            "zoom": inst_config["mode"],
            "angle": inst_config["extra_params"].get("rotator_angle", 0),
            "width": "0.0",  # unspecified and not unused
            "height": "0.0", # unspecified and unused

            # PTR doesn't expect these here, but this is where they are configured from LCO
            "offset_ra": inst_config["extra_params"]["offset_ra"],
            "offset_dec": inst_config["extra_params"]["offset_dec"],
            "defocus": inst_config["extra_params"].get("defocus", 0),

            # "repeat": 0,
            # "bin": 
            }
            exposure_set
            project["exposures"].append(exposure_set)
            # The following deep track of completed and remaining exposures. 
            # They have not been implemented/used by the tcs but we'll initialize them here anyways
            project["project_data"].append([])
            project["remaining"].append(exposure_set["count"])
        self.project = project

    def create_calendar(self):
        if not hasattr(self, 'calendar_event'):
            self._translate_to_calendar()
        response = create_calendar_event(self.calendar_event) # this is an imported function
        print("Created new calendar event")
        return response

    def create_project(self):
        if not hasattr(self, 'project'):
            self._translate_to_project()
        url = get_projects_url('new-project')
        response = requests.post(url, json.dumps(self.project))
        print("Created new project")
        return response.json()


def get_site_proxy_url(site, path):
    return f"https://{site}-proxy.lco.global/{path}"


def get_site_proxy_header(site):
    response = ssm.get_parameter(
        Name=f"/site-proxy-secret/{site}",
        WithDecryption=True  # Ensures SecureString parameters are decrypted
    )
    return { "Authorization": response['Parameter']['Value'] }


def get_schedule(site, start=None, end=None, limit=1000):
    
    # By default, initialize the range to start now and end in 3 weeks.
    now = datetime.now(timezone.utc)
    if start == None:
        start = now.strftime("%Y-%m-%dT%H:%M:%S")
    if end == None:
        end = (now + timedelta(days=21)).strftime("%Y-%m-%dT%H:%M:%S")

    # Call the site proxy to get the latest schedule
    url_path = f"observation-portal/api/schedule?start={start}&end={end}&limit={limit}"
    url = get_site_proxy_url(site, url_path)
    header = get_site_proxy_header(site)
    response = requests.get(url, headers=header)

    # We're only interested in pending observations, ignore others
    sched = [x for x in response.json().get("results") if x["state"] == "PENDING"]
    print(f"getting schedule from {start} to {end}")
    return sched


def remove_projects(project_ids: list):
    url = get_projects_url('delete-scheduler-projects')
    cleaned_ids = [p_id for p_id in project_ids if p_id not in ["none#", "none"]]
    request_body = json.dumps({"project_ids": cleaned_ids})
    response = requests.post(url, request_body)
    return response


def clear_old_schedule(site, cutoff_time=None):
    """ Method for deleting calendar events created in response to the LCO scheduler.
    
    This method takes a site and a cutoff time, and deletes all events that satisfy the following conditions:
        - the event belongs to the given site
        - the event starts after the cutoff_time (specifically, the event start is greater than the cutoff_time)
        - the event origin is 'lco'
    Then it gathers a list of project IDs that were associated with the deleted events, and delete them too. 

    Args:
        cutoff_time (str): 
            Formatted yyyy-MM-ddTHH:mmZ (UTC, 24-hour format)
            Any events that start before this time are not deleted.
        site (str): 
            Only delete events from the given site (e.g. 'mrc')

    Returns:
        (array of str) project IDs for any projects that were connected to deleted events. 
    """
    index_name = "site-end-index"

    if cutoff_time is None:
        cutoff_time = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
        
    
    # Query items from the secondary index with 'site' as the partition key and 'end' greater than the specified end_date
    # We're using 'end' time for the query because it's part of a pre-existing GSI that allows for efficient queries. 
    # But ultimately we want this to apply to events that start after the cutoff, so add that as a filter condition too.
    query = calendar_table.query(
        IndexName=index_name,
        KeyConditionExpression=Key('site').eq(site) & Key('end').gt(cutoff_time),
        FilterExpression=Attr('origin').eq('LCO') & Attr('start').gt(cutoff_time)
    )
    items = query.get('Items', [])
    print(f"Removing expired scheduled events: {items}")
    
    # Extract key attributes for deletion (use the primary key attributes, not the index keys)
    key_names = [k['AttributeName'] for k in calendar_table.key_schema]
    
    with calendar_table.batch_writer() as batch:
        for item in items:
            batch.delete_item(Key={k: item[k] for k in key_names if k in item})
    
    # Handle pagination if results exceed 1MB
    while 'LastEvaluatedKey' in query:
        query = calendar_table.query(
            IndexName=index_name,
            KeyConditionExpression=Key('site').eq(site) & Key('end').gt(cutoff_time),
            FilterExpression=Attr('origin').eq('lco') & Attr('start').gt(cutoff_time),
            ExclusiveStartKey=query['LastEvaluatedKey']
        )
        items = query.get('Items', [])
        
        with calendar_table.batch_writer() as batch:
            for item in items:
                batch.delete_item(Key={k: item[k] for k in key_names if k in item})

    # Delete any projects that were associated with the deleted calendar events
    associated_projects = [x["project_id"] for x in items]
    print(f"{len(associated_projects)} projects slated for removal: ", associated_projects)
    remove_projects(associated_projects) # delete projects
    

def create_latest_schedule(site):
    sched = get_schedule(site)
    print(f"Number of observations to schedule: {len(sched)}")
    for obs in sched:
        observation = Observation(site, obs) 
        observation.create_ptr_resources()
    

# This is the function that is run on a cron timer
def import_all_schedules(event={}, context={}):
    sites = ['mrc']
    for site in sites:
        clear_old_schedule(site)
        create_latest_schedule(site)
    if "httpMethod" in event:
        return create_response(200, "Import schedules routine finished")
