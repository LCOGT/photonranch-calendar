"""
Common fixtures for tests.
Place reusable fixtures here, so they can be imported by multiple test files.
"""

import pytest
import boto3
from unittest.mock import patch
from moto import mock_dynamodb

@pytest.fixture
def mock_tables():
    """Setup mock DynamoDB tables for testing"""
    with mock_dynamodb():
        # Create dynamodb resource
        dynamodb = boto3.resource('dynamodb')

        # Create the tracking table
        tracking_table = dynamodb.create_table(
            TableName='dev-schedule-tracking',
            KeySchema=[
                {'AttributeName': 'ptr_site', 'KeyType': 'HASH'}
            ],
            AttributeDefinitions=[
                {'AttributeName': 'ptr_site', 'AttributeType': 'S'}
            ],
            ProvisionedThroughput={'ReadCapacityUnits': 1, 'WriteCapacityUnits': 1}
        )

        # Use the table name used in the code
        table_name = 'calendar-dev'

        # Create the calendar table with required indexes
        calendar_table = dynamodb.create_table(
            TableName=table_name,
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

        # Add some sample calendar events
        calendar_table.put_item(Item={
            'event_id': 'test-event-1',
            'start': '2025-02-15T10:00:00Z',
            'end': '2025-02-15T11:00:00Z',
            'site': 'mrc1',
            'creator_id': 'testuser#LCO',
            'origin': 'LCO',
            'project_id': 'test-project-1'
        })

        # This approach allows us to mock both:
        # 1. Environment variables
        # 2. The imported calendar_table object
        from unittest.mock import patch

        class TableContext:
            def __init__(self):
                self.dynamodb = dynamodb
                self.tracking_table = tracking_table
                self.calendar_table = calendar_table
                self.table_name = table_name

        table_context = TableContext()

        # Create a series of nested context managers
        with patch.dict('os.environ', {
            'DYNAMODB_CALENDAR': table_name,
            'STAGE': 'dev'
        }):
            # Patch imported modules - this requires careful order of imports in test files
            try:
                with patch('import_schedules.calendar_table', calendar_table):
                    with patch('utils.calendar_table', calendar_table):
                        yield table_context
            except ImportError:
                # If modules aren't imported yet, just yield the context
                yield table_context