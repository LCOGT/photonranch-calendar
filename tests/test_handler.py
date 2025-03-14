import json
import os
import boto3
from unittest.mock import patch, MagicMock

# Mock environment setup
os.environ['DYNAMODB_CALENDAR'] = 'calendar-test'
os.environ['STAGE'] = 'test'

# Patch the boto3 client and the SSM client that would be used to fetch secrets
with patch('scheduler_utils.ssm.get_parameter') as mock_ssm:
    # Mock the SSM response
    mock_ssm.return_value = {'Parameter': {'Value': 'mock-token'}}

    # Patch requests.get for the site proxy API calls
    with patch('scheduler_utils.requests.get') as mock_get:
        # Mock an empty schedule response
        mock_get.return_value = MagicMock(
            status_code=200,
            json=lambda: {"results": []}
        )

        from handler import getSchedulerObservations

        # Set up test event
        test_event = {
            "body": json.dumps({
                "site": "mrc1",
                "start": "2025-02-20T00:00:00",
                "end": "2025-02-22T00:00:00"
            })
        }

        # Mock context
        test_context = {}

        # Call the function
        try:
            response = getSchedulerObservations(test_event, test_context)
            print("Response status code:", response.get("statusCode"))
            print("Response body:", response.get("body"))
            print("Test passed: Successfully using scheduler_utils.py")
        except Exception as e:
            print(f"Test failed: {str(e)}")
