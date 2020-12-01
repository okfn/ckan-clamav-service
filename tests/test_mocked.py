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

    jobs.update_clamav_definitions = mock.Mock()
    jobs.scan_file = mock.Mock(
        return_value=mock.MagicMock(returncode=0, stdout=b"/tmp/tmp37q_kv9u: OK...")
    )

    response = jobs.scan("fake-id", test_payload)

    jobs.update_clamav_definitions.assert_called_once()
    jobs.scan_file.assert_called_once()

    assert {
        "status_code": 0,
        "status_text": "SUCCESSFUL SCAN, FILE CLEAN",
        "description": "/tmp/tmp37q_kv9u: OK...",
    } == response


@responses.activate
def test_invalid_payload():
    with pytest.raises(util.JobError):
        jobs.scan("fake-id", {})

    with pytest.raises(util.JobError):
        jobs.scan("fake-id", {"metadata": {}})

    with pytest.raises(util.JobError):
        jobs.scan("fake-id", {"metadata": {}, "ckan_url": "http://ckan.example.com"})


@responses.activate
def test_failed_resource_show():
    responses.add(
        responses.POST,
        "http://ckan.example.com/api/3/action/resource_show",
        status=500,
    )

    with pytest.raises(util.JobError):
        jobs.scan("fake-id", test_payload)


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

    with pytest.raises(util.JobError):
        jobs.scan("fake-id", test_payload)


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

    with pytest.raises(util.JobError):
        jobs.scan("fake-id", test_payload)


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

    with pytest.raises(util.JobError):
        jobs.scan("fake-id", test_payload)


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
        return_value=mock.MagicMock(returncode=2, stdout=b"oh no")
    )

    with pytest.raises(util.JobError):
        jobs.scan("fake-id", test_payload)


"""
TODO: at the moment we can't make assertions about the exception messages
in the error cases because

```
with pytest.raises(util.JobError) as excinfo:
    assert something about excinfo.value
```

throws `TypeError: __str__ returned non-string (type bytes)` in python3.

Hopefully https://github.com/ckan/ckan-service-provider/pull/54 will be
merged and we can upgrade and add assertions about the exception messages
"""
