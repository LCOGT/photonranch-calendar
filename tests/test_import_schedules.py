import json
import pytest
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
    print(o.calendar_event)
    assert hasattr(o, "calendar_event")

def test_observation_create_project():
    o = Observation(sample_observation)
    assert not hasattr(o, "project")
    o._translate_to_project()
    print(o.project)
    assert hasattr(o, "project")

def test_observation_mismatched_site_telescope_fails():
    s = sample_observation
    s['site'] = "mrc"
    s['telescope'] = "not a real telescope"

    with pytest.raises(KeyError):
        Observation(s)

def test_observation_gets_correct_ptr_site():
    s = sample_observation
    s['site'] = "mrc"
    s['telescope'] = "0m31" # valid telescope for mrc, based on configdb
    o = Observation(s)
    assert o.ptr_site




