import requests, json

# Demonstrate add-project-data endpoint

url = "https://projects.photonranch.org/dev/add-project-data"
request_body = json.dumps({

    # A project is uniquely specified by the pair of values: project_name and created_at. 
    "project_name": "m101",
    "created_at": "2020-06-24T16:53:56Z",

    # A project definition will have one or more targets listed in an array. 
    # Specify which target was captured in the data we are adding.
    "target_index": 0,

    # Similarly, we specify which exposure request is being added.
    "exposure_index": 0,

    "base_filename": "filename_abc"
})

response = requests.post(url, request_body)
print(response.json()) 
