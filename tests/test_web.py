"""
Smoke test to ensure:
- the service can be started properly
- the config and job are loaded
"""

import json
import os

import clamav_service.main as main

os.environ["JOB_CONFIG"] = os.path.join(os.path.dirname(__file__), "settings_test.py")

app = main.serve_test()


def test_status():
    resp = app.get("/status")
    result_dict = json.loads(resp.data)
    assert 200 == resp.status_code
    assert ["scan"] == result_dict["job_types"]
    assert "clamav" == result_dict["name"]
