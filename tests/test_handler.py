import json
import os
import pytest
from unittest.mock import patch, MagicMock

# Mock environment setup for all tests
@pytest.fixture(autouse=True)
def setup_environment():
    with patch.dict('os.environ', {
        'DYNAMODB_CALENDAR': 'calendar-test',
        'STAGE': 'test'
    }):
        yield

# Fixture to import handler functions after mocking dependencies
@pytest.fixture
def handler_module():
    with patch('scheduler_utils.ssm.get_parameter') as mock_ssm:
        # Mock the SSM response
        mock_ssm.return_value = {'Parameter': {'Value': 'mock-token'}}

        # Import handler only after mocking dependencies
        from handler import getSchedulerObservations, create_response

        return {
            'getSchedulerObservations': getSchedulerObservations,
            'create_response': create_response
        }

# Sample observation data
with open('./test_data/sample_observation.json', 'r') as file:
    SAMPLE_OBSERVATION = json.load(file)

# Create a modified version with MRC1 telescope
MRC1_OBSERVATION = {**SAMPLE_OBSERVATION}
MRC1_OBSERVATION["site"] = "mrc"
MRC1_OBSERVATION["telescope"] = "0m31"

# Test successful retrieval of empty scheduler observations
def test_get_scheduler_observations_empty_success(handler_module):
    with patch('scheduler_utils.requests.get') as mock_get:
        # Mock an empty schedule response
        mock_get.return_value = MagicMock(
            status_code=200,
            json=lambda: {"results": []}
        )

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
        response = handler_module['getSchedulerObservations'](test_event, test_context)

        # Assert successful response
        assert response['statusCode'] == 200
        assert json.loads(response['body']) == []

# Test successful retrieval with populated scheduler observations
def test_get_scheduler_observations_populated_success(handler_module):
    with patch('scheduler_utils.requests.get') as mock_get:
        # Mock a populated schedule response
        mock_get.return_value = MagicMock(
            status_code=200,
            json=lambda: {"results": [MRC1_OBSERVATION]}
        )

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
        response = handler_module['getSchedulerObservations'](test_event, test_context)

        # Assert successful response
        assert response['statusCode'] == 200
        result = json.loads(response['body'])
        assert len(result) == 1
        assert result[0]['event_id'] == str(MRC1_OBSERVATION['id'])
        assert result[0]['site'] == 'mrc1'  # Should be mapped from mrc/0m31
        assert result[0]['creator_id'] == f"{MRC1_OBSERVATION['submitter']}#LCO"

# Test missing required parameters
def test_get_scheduler_observations_missing_params(handler_module):
    # Missing 'site' parameter
    test_event = {
        "body": json.dumps({
            "start": "2025-02-20T00:00:00",
            "end": "2025-02-22T00:00:00"
        })
    }

    # Mock context
    test_context = {}

    # Call the function
    response = handler_module['getSchedulerObservations'](test_event, test_context)

    # Assert error response
    assert response['statusCode'] == 400
    assert "missing required key site" in response['body']

    # Missing 'start' parameter
    test_event = {
        "body": json.dumps({
            "site": "mrc1",
            "end": "2025-02-22T00:00:00"
        })
    }

    # Call the function
    response = handler_module['getSchedulerObservations'](test_event, test_context)

    # Assert error response
    assert response['statusCode'] == 400
    assert "missing required key start" in response['body']

# Test invalid site parameter
def test_get_scheduler_observations_invalid_site(handler_module):
    # Using a site that doesn't have a site proxy mapping
    test_event = {
        "body": json.dumps({
            "site": "invalid-site",
            "start": "2025-02-20T00:00:00",
            "end": "2025-02-22T00:00:00"
        })
    }

    # Mock context
    test_context = {}

    # We should get a successful response with empty observations
    # since invalid sites just return an empty list
    response = handler_module['getSchedulerObservations'](test_event, test_context)

    # Assert response
    assert response['statusCode'] == 200
    assert json.loads(response['body']) == []

# Test API error handling
def test_get_scheduler_observations_api_error(handler_module):
    with patch('scheduler_utils.requests.get') as mock_get:
        # Mock a failed API response
        mock_get.side_effect = Exception("API Connection Error")

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
        response = handler_module['getSchedulerObservations'](test_event, test_context)

        # Assert error is handled gracefully
        assert response['statusCode'] == 200
        assert json.loads(response['body']) == []  # Should return empty list on error
