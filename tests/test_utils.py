import pytest
from utils import create_calendar_event
from utils import get_event_by_id
from utils import get_events_during_time
from utils import get_utc_iso_time

import datetime
import re
import boto3
from unittest.mock import patch
from moto import mock_dynamodb

@pytest.fixture
def mock_calendar_table():
    with mock_dynamodb():
        dynamodb = boto3.resource('dynamodb')
        
        # Create the calendar table with primary key and GSIs
        table = dynamodb.create_table(
            TableName='CalendarTable',
            KeySchema=[
                {'AttributeName': 'event_id', 'KeyType': 'HASH'},
                {'AttributeName': 'start', 'KeyType': 'RANGE'}
            ],
            AttributeDefinitions=[
                {'AttributeName': 'event_id', 'AttributeType': 'S'},
                {'AttributeName': 'start', 'AttributeType': 'S'},
                {'AttributeName': 'end', 'AttributeType': 'S'},
                {'AttributeName': 'site', 'AttributeType': 'S'},
                {'AttributeName': 'creator_id', 'AttributeType': 'S'}
            ],
            GlobalSecondaryIndexes=[
                {
                    'IndexName': 'creatorid-end-index',
                    'KeySchema': [
                        {'AttributeName': 'creator_id', 'KeyType': 'HASH'},
                        {'AttributeName': 'end', 'KeyType': 'RANGE'}
                    ],
                    'Projection': {'ProjectionType': 'ALL'},
                    'ProvisionedThroughput': {'ReadCapacityUnits': 1, 'WriteCapacityUnits': 1}
                },
                {
                    'IndexName': 'site-end-index',
                    'KeySchema': [
                        {'AttributeName': 'site', 'KeyType': 'HASH'},
                        {'AttributeName': 'end', 'KeyType': 'RANGE'}
                    ],
                    'Projection': {'ProjectionType': 'ALL'},
                    'ProvisionedThroughput': {'ReadCapacityUnits': 1, 'WriteCapacityUnits': 1}
                }
            ],
            ProvisionedThroughput={'ReadCapacityUnits': 1, 'WriteCapacityUnits': 1}
        )
        
        # Insert mock data
        table.put_item(Item={
            'event_id': '1',
            'start': '2024-12-01T09:00:00',
            'end': '2024-12-01T10:00:00',
            'site': 'tst',
            'creator_id': 'testuser'
        })        
        yield table

def test_get_utc_iso_time():
    fixed_time = datetime.datetime(2023, 12, 10, 15, 30, 45, tzinfo=datetime.timezone.utc)
    
    with patch('datetime.datetime') as mock_datetime:
        mock_datetime.now.return_value = fixed_time
        mock_datetime.timezone = datetime.timezone

        result = get_utc_iso_time()

        # Check the format
        assert re.match(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$', result), "Output format is incorrect"

        # Verify the output matches the mocked time
        expected = fixed_time.strftime("%Y-%m-%dT%H:%M:%SZ")
        assert result == expected, f"Expected {expected}, got {result}"


def test_create_calendar_event_success(mock_calendar_table):
    # Arrange
    event = {
        'event_id': '2',
        'start': '2024-12-02T09:00:00',
        'end': '2024-12-02T10:00:00',
        'site': 'tst',
        'creator_id': 'testuser2'
    }
    with patch('utils.calendar_table', mock_calendar_table):
        # Act
        result = create_calendar_event(event)

        # Assert
        response = mock_calendar_table.get_item(Key={'event_id': '2', 'start': '2024-12-02T09:00:00'})
        assert 'Item' in response
        assert response['Item'] == event
        assert result['ResponseMetadata']['HTTPStatusCode'] == 200

def test_create_calendar_event_bad_event(mock_calendar_table):
    # Arrange
    event_missing_id = {
        'start': '2024-12-02T09:00:00',
        'end': '2024-12-02T10:00:00',
        'site': 'tst',
        'creator_id': 'testuser2'
    }
    with patch('utils.calendar_table', mock_calendar_table):
        # Act & Assert
        with pytest.raises(Exception, match="ValidationException"):
            create_calendar_event(event_missing_id)
