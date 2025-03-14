import json
import pytest
import boto3
import os
import datetime
import responses
from unittest.mock import patch, MagicMock, ANY
from moto import mock_dynamodb

import import_schedules
from import_schedules import (
    Observation,
    get_last_schedule_time,
    get_schedule,
    clear_old_schedule,
    update_last_schedule_time,
    get_last_tracked_schedule_time,
    create_latest_schedule_for_subsite,
    import_all_schedules,
    get_formatted_observations
)

# Sample observation data for testing
with open('./test_data/sample_observation.json', 'r') as file:
    SAMPLE_OBSERVATION = json.load(file)

# Create a modified version with MRC1 telescope
MRC1_OBSERVATION = {**SAMPLE_OBSERVATION}
MRC1_OBSERVATION["site"] = "mrc"
MRC1_OBSERVATION["telescope"] = "0m31"

# Create a modified version with MRC2 telescope
MRC2_OBSERVATION = {**SAMPLE_OBSERVATION}
MRC2_OBSERVATION["site"] = "mrc"
MRC2_OBSERVATION["telescope"] = "0m61"

# Mock schedule response from site proxy
MOCK_SCHEDULE_RESPONSE = {
    "results": [MRC1_OBSERVATION, MRC2_OBSERVATION]
}

# Mock last scheduled time response
MOCK_LAST_SCHEDULED = {
    "last_schedule_time": "2025-02-21T16:36:44.534003Z"
}


# Unit Tests

def test_observation_gets_ptr_site():
    """Test that observation correctly identifies the PTR site from WEMA and telescope"""
    # Test MRC1
    obs_mrc1 = Observation(MRC1_OBSERVATION)
    assert obs_mrc1.ptr_site == "mrc1"

    # Test MRC2
    obs_mrc2 = Observation(MRC2_OBSERVATION)
    assert obs_mrc2.ptr_site == "mrc2"

@responses.activate
def test_get_last_schedule_time():
    """Test retrieving the last schedule time from site proxy"""
    # Mock SSM parameter
    with patch('import_schedules.ssm.get_parameter') as mock_ssm:
        mock_ssm.return_value = {'Parameter': {'Value': 'mock-token'}}

        # Mock site-proxy response
        responses.add(
            responses.GET,
            'https://mrc-proxy.lco.global/observation-portal/api/last_scheduled',
            json=MOCK_LAST_SCHEDULED,
            status=200
        )

        # Test function
        result = get_last_schedule_time('mrc')
        assert result == MOCK_LAST_SCHEDULED['last_schedule_time']

@responses.activate
def test_get_schedule():
    """Test retrieving schedules from site proxy"""
    # Mock SSM parameter
    with patch('import_schedules.ssm.get_parameter') as mock_ssm:
        mock_ssm.return_value = {'Parameter': {'Value': 'mock-token'}}

        # Mock site-proxy response for all telescopes
        responses.add(
            responses.GET,
            'https://mrc-proxy.lco.global/observation-portal/api/schedule',
            json=MOCK_SCHEDULE_RESPONSE,
            status=200,
            match_querystring=False
        )

        # Test getting all schedules for site
        result = get_schedule('mrc')
        assert len(result) == 2

        # Mock site-proxy response for specific telescope
        filtered_response = {"results": [MRC1_OBSERVATION]}
        responses.add(
            responses.GET,
            'https://mrc-proxy.lco.global/observation-portal/api/schedule?telescope=0m31',
            json=filtered_response,
            status=200,
            match_querystring=False
        )

        # Test getting schedule filtered by telescope
        result = get_schedule('mrc', telescope_id='0m31')
        assert len(result) == 1
        assert result[0]['telescope'] == '0m31'

@responses.activate
def test_get_full_schedule():
    """Test retrieving schedules from site proxy"""
    # Mock SSM parameter
    with patch('import_schedules.ssm.get_parameter') as mock_ssm:
        mock_ssm.return_value = {'Parameter': {'Value': 'mock-token'}}

        # Mock site-proxy response for all telescopes
        responses.add(
            responses.GET,
            'https://mrc-proxy.lco.global/observation-portal/api/schedule',
            json=MOCK_SCHEDULE_RESPONSE,
            status=200,
            match_querystring=False
        )

        # Test getting all schedules for site
        result = get_schedule('mrc')
        assert len(result) == 2

        # Mock site-proxy response for specific telescope
        filtered_response = {"results": [MRC1_OBSERVATION]}
        responses.add(
            responses.GET,
            'https://mrc-proxy.lco.global/observation-portal/api/schedule?telescope=0m31',
            json=filtered_response,
            status=200,
            match_querystring=False
        )

        # Test getting schedule filtered by telescope
        result = get_schedule('mrc', telescope_id='0m31')
        assert len(result) == 1
        assert result[0]['telescope'] == '0m31'

def test_update_and_get_tracking_time(mock_tables):
    """Test updating and retrieving tracking times"""
    # Test updating tracking time
    update_last_schedule_time('mrc1', MOCK_LAST_SCHEDULED['last_schedule_time'])

    # Test retrieving tracking time
    result = get_last_tracked_schedule_time('mrc1')
    assert result == MOCK_LAST_SCHEDULED['last_schedule_time']

    # Test retrieving non-existent tracking time
    result = get_last_tracked_schedule_time('non-existent-site')
    assert result is None

def test_clear_old_schedule(mock_tables):
    """Test clearing old schedules for a specific site"""
    # Get references to the mock tables
    dynamodb = boto3.resource('dynamodb')
    mock_calendar_table = dynamodb.Table('calendar-dev')

    # Add test events to the mock table
    mock_calendar_table.put_item(Item={
        'event_id': 'test-event-2',
        'start': '2025-02-16T10:00:00Z',
        'end': '2025-02-16T11:00:00Z',
        'site': 'mrc1',
        'creator_id': 'testuser#LCO',
        'origin': 'LCO',
        'project_id': 'test-project-2'
    })

    # IMPORTANT: Patch the imported calendar_table to use our mock
    with patch('import_schedules.calendar_table', mock_calendar_table):
        # Also mock remove_projects
        with patch('import_schedules.remove_projects') as mock_remove:
            mock_remove.return_value = MagicMock()

            # Clear schedules with cutoff time that should remove only the second event
            cutoff_time = '2025-02-15T23:00:00Z'
            removed_projects = clear_old_schedule('mrc1', cutoff_time)

            # Verify only one project was removed
            assert len(removed_projects) == 1
            assert removed_projects[0] == 'test-project-2'

            # Verify remove_projects was called with correct arguments
            mock_remove.assert_called_once_with(['test-project-2'])

            # Check that only one event remains in the table
            response = mock_calendar_table.scan()
            assert len(response['Items']) == 1
            assert response['Items'][0]['event_id'] == 'test-event-1'

# Integration Tests

@patch('import_schedules.get_last_schedule_time')
@patch('import_schedules.get_schedule')
@patch('import_schedules.clear_old_schedule')
@patch('import_schedules.update_last_schedule_time')
@patch('import_schedules.Observation')
def test_create_latest_schedule_for_subsite(
    mock_observation, mock_update, mock_clear, mock_get_schedule, mock_last_time, mock_tables
):
    """Test creating the latest schedule for a subsite"""
    # Setup mocks
    mock_last_time.return_value = "2025-02-21T16:36:44.534003Z"
    mock_get_schedule.return_value = [MRC1_OBSERVATION]
    mock_clear.return_value = []

    mock_obs_instance = MagicMock()
    mock_observation.return_value = mock_obs_instance

    # Test creating schedule for a subsite that needs updating
    with patch('import_schedules.get_last_tracked_schedule_time') as mock_tracked_time:
        # Case 1: No previous tracking time - should update
        mock_tracked_time.return_value = None

        result = create_latest_schedule_for_subsite("mrc1")

        # Verify functions were called
        mock_last_time.assert_called_with("mrc")
        mock_get_schedule.assert_called_with("mrc", "0m31", "PENDING")
        mock_clear.assert_called_with("mrc1")
        mock_observation.assert_called_with(MRC1_OBSERVATION)
        mock_obs_instance.create_ptr_resources.assert_called_once()
        mock_update.assert_called_with("mrc1", "2025-02-21T16:36:44.534003Z")

        # Case 2: Older tracking time - should update
        mock_tracked_time.return_value = "2025-02-21T16:30:00.000000Z"

        result = create_latest_schedule_for_subsite("mrc1")

        # Verify all the same functions were called again
        assert mock_observation.call_count == 2
        assert mock_obs_instance.create_ptr_resources.call_count == 2

        # Case 3: Same tracking time - should not update
        mock_tracked_time.return_value = "2025-02-21T16:36:44.534003Z"

        result = create_latest_schedule_for_subsite("mrc1")

        # Verify schedule was not recreated
        assert mock_observation.call_count == 2  # No change from before
        assert mock_obs_instance.create_ptr_resources.call_count == 2  # No change from before

@responses.activate
@patch('import_schedules.PTR_SITE_TO_WEMA_TELESCOPE')
@patch('import_schedules.get_full_schedule')
def test_get_formatted_observations(mock_get_full_schedule, mock_ptr_map):
    """Test the get_formatted_observations function for retrieving calendar-like observations"""
    # Mock the telescope mapping
    mock_ptr_map.__contains__.side_effect = lambda site: site == "mrc1"
    mock_ptr_map.__getitem__.return_value = ("mrc", "0m31")

    # Set up mock schedule response
    mock_observations = [MRC1_OBSERVATION]
    mock_get_full_schedule.return_value = mock_observations

    # Test 1: Unknown site should return empty list
    result = get_formatted_observations("unknown-site", "2025-02-20T00:00:00Z", "2025-02-22T00:00:00Z")
    assert result == []

    # Test 2: Known site should return formatted observations
    result = get_formatted_observations("mrc1", "2025-02-20T00:00:00Z", "2025-02-22T00:00:00Z")

    # Verify results
    assert len(result) == 1
    formatted_obs = result[0]
    assert formatted_obs["event_id"] == str(MRC1_OBSERVATION["id"])
    assert formatted_obs["start"] == MRC1_OBSERVATION["start"]
    assert formatted_obs["end"] == MRC1_OBSERVATION["end"]
    assert formatted_obs["site"] == "mrc1"
    assert formatted_obs["creator"] == MRC1_OBSERVATION["submitter"]
    assert formatted_obs["creator_id"] == f'{MRC1_OBSERVATION["submitter"]}#LCO'
    assert formatted_obs["reservation_type"] == "observation"
    assert formatted_obs["origin"] == "LCO"
    assert "observation_data" in formatted_obs

    # Verify the get_schedule function was called with correct parameters
    filtered_states = [
        "PENDING",
        "IN_PROGRESS",
        "NOT_ATTEMPTED",
        "COMPLETED",
        "CANCELED",
        "ABORTED",
        "FAILED",
    ]
    mock_get_full_schedule.assert_called_with("mrc", "0m31", "2025-02-20T00:00:00Z", "2025-02-22T00:00:00Z", filtered_states=filtered_states)

@patch('import_schedules.create_latest_schedule_for_subsite')
def test_import_all_schedules(mock_create_schedule):
    """Test the main import_all_schedules function"""
    # Setup mock
    mock_create_schedule.return_value = "Updated schedule"

    # Test importing all schedules
    result = import_all_schedules()

    # Should call create_latest_schedule_for_subsite for each PTR site
    expected_calls = sum(len(sites) for sites in import_schedules.PTR_SITES_PER_WEMA.values())
    assert mock_create_schedule.call_count == expected_calls

    # Test importing schedule for specific subsite via HTTP request
    event = {
        "httpMethod": "GET",
        "pathParameters": {
            "subsite": "mrc1"
        }
    }

    result = import_all_schedules(event)

    # Should call create_latest_schedule_for_subsite only for the specified site
    mock_create_schedule.assert_called_with("mrc1")
    assert "statusCode" in result
    assert result["statusCode"] == 200

if __name__ == "__main__":
    pytest.main()
