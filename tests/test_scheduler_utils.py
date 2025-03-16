import json
import pytest
import boto3
import responses
from unittest.mock import patch, MagicMock
from datetime import datetime, timezone

from scheduler_utils import (
    ALL_OBSERVATION_STATES,
    PTR_SITE_TO_WEMA_TELESCOPE,
    get_full_schedule,
    get_formatted_observations
)
# Import the mock_tables fixture from conftest.py
from conftest import mock_tables

# Load sample observation data for testing
with open('./test_data/sample_observation.json', 'r') as file:
    SAMPLE_OBSERVATION = json.load(file)

# Create a modified version with MRC1 telescope
MRC1_OBSERVATION = {**SAMPLE_OBSERVATION}
MRC1_OBSERVATION["site"] = "mrc"
MRC1_OBSERVATION["telescope"] = "0m31"

# Mock schedule response from site proxy
MOCK_SCHEDULE_RESPONSE = {
    "results": [MRC1_OBSERVATION]
}

@responses.activate
@patch('scheduler_utils.PTR_SITE_TO_WEMA_TELESCOPE')
@patch('scheduler_utils.get_full_schedule')
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

    # Verify the get_full_schedule function was called with correct parameters
    mock_get_full_schedule.assert_called_with("mrc", "0m31", "2025-02-20T00:00:00Z", "2025-02-22T00:00:00Z", observation_states=ALL_OBSERVATION_STATES)
