import requests
import json
import boto3
from datetime import datetime, timezone, timedelta

# List of all valid observation states
ALL_OBSERVATION_STATES = [
    "PENDING",
    "IN_PROGRESS",
    "NOT_ATTEMPTED",
    "COMPLETED",
    "CANCELED", # this happens when the observation is overwritten by a later schedule
    "ABORTED",
    "FAILED",
]

# Only query scheduler for observations at these sites
SITES_TO_USE_WITH_SCHEDULER = [
    "mrc",
    "aro",
    "eco"
]

# This allows us to identify a site using configdb telescope ids
# WEMA → telescope ID → PTR site
SITE_FROM_WEMA_AND_TELESCOPE_ID = {
    "mrc": {
        "0m31": "mrc1",
        "0m61": "mrc2"
    },
    "aro": {
        "0m3": "aro1"
    },
    "eco": {
        "0m43": "eco1",
        "0m28": "eco2"
    }
}

# Mapping from PTR site to WEMA and telescope ID
# This is the reverse of the above mapping for easier lookup
PTR_SITE_TO_WEMA_TELESCOPE = {}
for wema, telescopes in SITE_FROM_WEMA_AND_TELESCOPE_ID.items():
    for tel_id, ptr_site in telescopes.items():
        PTR_SITE_TO_WEMA_TELESCOPE[ptr_site] = (wema, tel_id)

# SSM client for accessing secrets
ssm = boto3.client('ssm')

def get_site_proxy_url(site, path):
    """Build the URL for accessing the site proxy API."""
    return f"https://{site}-proxy.lco.global/{path}"

def get_site_proxy_header(site):
    """Get the authorization header for site proxy API access."""
    response = ssm.get_parameter(
        Name=f"/site-proxy-secret/{site}",
        WithDecryption=True  # Ensures SecureString parameters are decrypted
    )
    return { "Authorization": response['Parameter']['Value'] }

def get_full_schedule(wema, telescope_id=None, start=None, end=None, limit=1000, observation_states=None):
    """Return a list of scheduled observations for a given PTR site

    Args:
        wema (str): The WEMA site code (e.g., "mrc")
        telescope_id (str, optional): Specific telescope to filter by
        start (str, optional): Start time in ISO format
        end (str, optional): End time in ISO format
        limit (int, optional): Maximum number of results to return
        filtered_states (list, optional): List of states to filter results by

    Returns:
        list: Schedule entries matching the criteria
    """
    # By default, initialize the range to start now and end in 3 weeks.
    now = datetime.now(timezone.utc)
    if start == None:
        start = now.strftime("%Y-%m-%dT%H:%M:%S")
    if end == None:
        end = (now + timedelta(days=21)).strftime("%Y-%m-%dT%H:%M:%S")

    # Build the URL path with query parameters
    url_path = f"observation-portal/api/schedule?start={start}&end={end}&limit={limit}"
    if telescope_id:
        url_path += f"&telescope={telescope_id}"

    url = get_site_proxy_url(wema, url_path)
    header = get_site_proxy_header(wema)
    response = requests.get(url, headers=header)

    # Filter results by observation state if requested
    if response.status_code == 200:
        sched = response.json().get("results")
        if observation_states is not None:
            sched = [o for o in sched if o["state"] in observation_states]
        return sched

    print(f"Error fetching schedule: {response.status_code}")
    return []

def get_formatted_observations(ptr_site, start, end):
    """Get observations for a site formatted in calendar-like structure.

    This function fetches the latest schedule from the site proxy
    and formats it to match calendar event structure.

    Args:
        ptr_site (str): PTR site code (e.g., 'mrc1')
        start (str): Start time in ISO format
        end (str): End time in ISO format

    Returns:
        list: Array of observations formatted like calendar events
    """
    # Check if site proxy is available for this site
    if ptr_site not in PTR_SITE_TO_WEMA_TELESCOPE:
        return []

    wema, telescope_id = PTR_SITE_TO_WEMA_TELESCOPE[ptr_site]

    try:
        # Get schedule from site proxy
        print(f'get_schedule args: {wema}, {telescope_id}, {start}, {end}')
        schedule = get_full_schedule(wema, telescope_id, start, end, observation_states=ALL_OBSERVATION_STATES)
        print(f'returned schedule: {schedule}')

        # Format observations to match calendar event structure
        formatted_observations = []
        for observation in schedule:
            formatted_obs = {
                "event_id": str(observation["id"]),
                "start": observation["start"],
                "end": observation["end"],
                "creator": observation["submitter"],
                "creator_id": f'{observation["submitter"]}#LCO',
                "last_modified": observation["modified"],
                "reservation_type": "observation",
                "origin": "LCO",
                "resourceId": ptr_site,
                "site": ptr_site,
                "title": f"{observation['name']} (via LCO)",
                "observation_type": observation["observation_type"],
                "observation_state": observation["state"],
                "request_state": observation["request"]["state"],
                "observation_data": observation  # Include full observation data
            }

            formatted_observations.append(formatted_obs)

        return formatted_observations
    except Exception as e:
        print(f"Error fetching observations: {e}")
        return []
