import json
from import_schedules import Observation

with open('./test_data/sample_observation.json', 'r') as file:
    sample_observation = json.load(file)

def test_observation_validation():
    o = Observation(sample_observation)
    assert Observation.validate_observation_format(sample_observation)
    assert Observation.validate_observation_format(o.observation)

def test_observation_create_calendar():
    o = Observation(sample_observation)
    assert not hasattr(o, "calendar_event")
    o._translate_to_calendar()
    assert hasattr(o, "calendar_event")

def test_observation_create_project():
    o = Observation(sample_observation)
    assert not hasattr(o, "project")
    o._translate_to_project()
    assert hasattr(o, "project")


