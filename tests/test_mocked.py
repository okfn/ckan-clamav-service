import json
from unittest import mock

import pytest
import responses
from ckanserviceprovider import util

from clamav_service import jobs

test_payload = {
    "api_key": "fake-key",
    "metadata": {
        "ckan_url": "http://ckan.example.com",
        "resource_id": "fake-resource",
    },
}


@responses.activate
def test_completed_scan():
    responses.add(
        responses.POST,
        "http://ckan.example.com/api/3/action/resource_show",
        status=200,
        json={
            "result": {
                "url_type": "upload",
                "url": "http://ckan.example.com/foobar.csv",
            }
        },
    )
    responses.add(
        responses.GET,
        "http://ckan.example.com/foobar.csv",
        status=200,
        body="a,b,c",
    )

    jobs.scan_file = mock.Mock(
        return_value=mock.MagicMock(
            returncode=0,
            stdout=b"/tmp/tmp37q_kv9u: OK...",
            elapsed_time=0.123,
        )
    )

    response = jobs.scan("fake-id", test_payload)

    jobs.scan_file.assert_called_once()

    assert response['status_code'] == 0
    assert response['stdout'] == "/tmp/tmp37q_kv9u: OK..."
    assert response["status_text"] == "SUCCESSFUL SCAN, FILE CLEAN"
    assert response["description"] == "/tmp/tmp37q_kv9u: OK..."
    assert response['elapsed_time'] == 0.123
    assert response['file_size'] == 5  # this is the size of "a,b,c"


@responses.activate
def test_invalid_payload():
    with pytest.raises(util.JobError) as excinfo:
        jobs.scan("fake-id", {})
    assert str(excinfo.value) == "Metadata missing"

    with pytest.raises(util.JobError) as excinfo:
        jobs.scan("fake-id", {"metadata": {}})
    assert str(excinfo.value) == "No id provided."

    with pytest.raises(util.JobError) as excinfo:
        jobs.scan("fake-id", {"metadata": {"ckan_url": "http://ckan.example.com"}})
    assert str(excinfo.value) == "No id provided."

    with pytest.raises(util.JobError) as excinfo:
        jobs.scan(
            "fake-id",
            {
                "metadata": {
                    "resource_id": "fake-resource",
                }
            },
        )
    assert str(excinfo.value) == "No ckan_url provided."

    with pytest.raises(util.JobError) as excinfo:
        jobs.scan(
            "fake-id",
            {
                "metadata": {
                    "ckan_url": "http://ckan.example.com",
                    "resource_id": "fake-resource",
                },
            },
        )
    assert str(excinfo.value) == "No CKAN API key provided"


@responses.activate
def test_failed_resource_show():
    responses.add(
        responses.POST,
        "http://ckan.example.com/api/3/action/resource_show",
        status=500,
    )

    with pytest.raises(util.JobError) as excinfo:
        jobs.scan("fake-id", test_payload)
    assert "500 Server Error" in str(excinfo.value)


@responses.activate
def test_resource_is_not_upload():
    responses.add(
        responses.POST,
        "http://ckan.example.com/api/3/action/resource_show",
        status=200,
        json={
            "result": {
                "url_type": "datastore",
            }
        },
    )

    with pytest.raises(util.JobError) as excinfo:
        jobs.scan("fake-id", test_payload)
    expected_error = "Only resources of type 'upload' can be scanned. Received 'datastore'"
    assert expected_error in str(excinfo.value)


@responses.activate
def test_invalid_scheme():
    responses.add(
        responses.POST,
        "http://ckan.example.com/api/3/action/resource_show",
        status=200,
        json={
            "result": {
                "url_type": "upload",
                "url": "git://github.com/okfn/ckan-clamav-service.git",
            }
        },
    )

    with pytest.raises(util.JobError) as excinfo:
        jobs.scan("fake-id", test_payload)
    expected_error = "Only http, https, and ftp resources may be fetched."
    assert expected_error in str(excinfo.value)


@responses.activate
def test_failed_resource_download():
    responses.add(
        responses.POST,
        "http://ckan.example.com/api/3/action/resource_show",
        status=200,
        json={
            "result": {
                "url_type": "upload",
                "url": "http://ckan.example.com/foobar.csv",
            }
        },
    )
    responses.add(
        responses.GET,
        "http://ckan.example.com/foobar.csv",
        status=500,
    )

    with pytest.raises(util.JobError) as excinfo:
        jobs.scan("fake-id", test_payload)
    assert "500 Server Error" in str(excinfo.value)


@responses.activate
def test_clamav_error():
    responses.add(
        responses.POST,
        "http://ckan.example.com/api/3/action/resource_show",
        status=200,
        json={
            "result": {
                "url_type": "upload",
                "url": "http://ckan.example.com/foobar.csv",
            }
        },
    )
    responses.add(
        responses.GET,
        "http://ckan.example.com/foobar.csv",
        status=200,
        body="a,b,c",
    )

    jobs.update_clamav_definitions = mock.Mock()
    jobs.scan_file = mock.Mock(
        return_value=mock.MagicMock(
            returncode=2,
            stdout=b"oh no",
            file_size=1234,
            elapsed_time=0.123,
        )
    )

    with pytest.raises(util.JobError) as excinfo:
        jobs.scan("fake-id", test_payload)

    val_dict = json.loads(str(excinfo.value))
    assert val_dict['status_code'] == 2
    assert val_dict['description'] == "oh no"
    assert val_dict['status_text'] == "SCAN FAILED"
    assert val_dict['elapsed_time'] == 0.123
    assert val_dict['returncode'] == 2
    assert val_dict['file_size'] == 5  # this is the size of "a,b,c"
