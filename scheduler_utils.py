import requests
import json
import boto3
from datetime import datetime, timezone, timedelta

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

def get_full_schedule(wema, telescope_id=None, start=None, end=None, limit=1000, filtered_states=None):
    """Get the schedule from the site proxy with optional telescope filter.

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

    # Filter results by state if requested
    if response.status_code == 200:
        sched = response.json().get("results")
        if filtered_states is not None:
            sched = [x for x in sched if x["state"] in filtered_states]
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

    filtered_states = [
        "PENDING",
        "IN_PROGRESS",
        "NOT_ATTEMPTED",
        "COMPLETED",
        "CANCELED", # this happens when the observation is overwritten by a later schedule
        "ABORTED",
        "FAILED",
    ]

    try:
        # Get schedule from site proxy
        print(f'get_schedule args: {wema}, {telescope_id}, {start}, {end}')
        sched = get_full_schedule(wema, telescope_id, start, end, filtered_states=filtered_states)
        print(f'returned schedule: {sched}')

        # Format observations to match calendar event structure
        formatted_observations = []
        for obs in sched:
            formatted_obs = {
                "event_id": str(obs["id"]),
                "start": obs["start"],
                "end": obs["end"],
                "creator": obs["submitter"],
                "creator_id": f'{obs["submitter"]}#LCO',
                "last_modified": obs["modified"],
                "reservation_type": "observation",
                "origin": "LCO",
                "resourceId": ptr_site,
                "site": ptr_site,
                "title": f"{obs['name']} (via LCO)",
                "observation_type": obs["observation_type"],
                "observation_state": obs["state"],
                "observation_data": obs  # Include full observation data
            }

            formatted_observations.append(formatted_obs)

        return formatted_observations
    except Exception as e:
        print(f"Error fetching observations: {e}")
        return []
