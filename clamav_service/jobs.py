import json
import logging
import subprocess
import tempfile
import time
from urllib.parse import urlsplit

import ckanserviceprovider.job as job
import ckanserviceprovider.util as util
import requests
from ckanserviceprovider import web
from requests.exceptions import RequestException

MAX_CONTENT_LENGTH = web.app.config.get("MAX_CONTENT_LENGTH") or 10485760
CHUNK_SIZE = web.app.config.get("CHUNK_SIZE") or 16384
DOWNLOAD_TIMEOUT = web.app.config.get("DOWNLOAD_TIMEOUT") or 30
SUBPROCESS_TIMEOUT = web.app.config.get("DOWNLOAD_TIMEOUT") or 300

if web.app.config.get("SSL_VERIFY") in ["False", "FALSE", "0", False, 0]:
    SSL_VERIFY = False
else:
    SSL_VERIFY = True


if not SSL_VERIFY:
    requests.packages.urllib3.disable_warnings()

STATUSES = {
    0: "SUCCESSFUL SCAN, FILE CLEAN",
    1: "SUCCESSFUL SCAN, FILE INFECTED",
    2: "SCAN FAILED",
    3: "JOB FAILED",
}


def init_logger(task_id, payload):
    handler = util.StoringHandler(task_id, payload)
    logger = logging.getLogger(task_id)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    return logger


def get_url(action, ckan_url):
    """
    Get url for ckan action
    """
    if not urlsplit(ckan_url).scheme:
        ckan_url = "http://" + ckan_url.lstrip("/")
    ckan_url = ckan_url.rstrip("/")
    return "{ckan_url}/api/3/action/{action}".format(ckan_url=ckan_url, action=action)


def ckan_action(action, ckan_url, api_key, payload):
    url = get_url(action, ckan_url)
    try:
        r = requests.post(
            url,
            verify=SSL_VERIFY,
            data=json.dumps(payload),
            headers={"Content-Type": "application/json", "Authorization": api_key},
        )
        r.raise_for_status()
    except RequestException as e:
        raise util.JobError(str(e))

    return r.json()["result"]


def validate_payload(payload):
    if "metadata" not in payload:
        raise util.JobError("Metadata missing")

    metadata = payload["metadata"]

    if "resource_id" not in metadata:
        raise util.JobError("No id provided.")
    if "ckan_url" not in metadata:
        raise util.JobError("No ckan_url provided.")
    if not payload.get("api_key"):
        raise util.JobError("No CKAN API key provided")


def fetch_resource(url, tmpfile, api_key):
    response = requests.get(
        url,
        headers={"Authorization": api_key},
        timeout=DOWNLOAD_TIMEOUT,
        verify=SSL_VERIFY,
        stream=True,  # just gets the headers for now
    )
    response.raise_for_status()

    cl = response.headers.get("content-length")
    try:
        if cl and int(cl) > MAX_CONTENT_LENGTH:
            raise util.JobError(
                "Resource too large to download: {cl} > max ({max_cl}).".format(
                    cl=cl, max_cl=MAX_CONTENT_LENGTH
                )
            )
    except ValueError:
        pass

    length = 0
    for chunk in response.iter_content(CHUNK_SIZE):
        length += len(chunk)
        if length > MAX_CONTENT_LENGTH:
            raise util.JobError(
                "Resource too large to process: {cl} > max ({max_cl}).".format(
                    cl=length, max_cl=MAX_CONTENT_LENGTH
                )
            )
        tmpfile.write(chunk)

    tmpfile.seek(0)


def update_clamav_definitions():
    return subprocess.run(
        ["freshclam"], stdout=subprocess.PIPE, timeout=SUBPROCESS_TIMEOUT
    )


def scan_file(filename):
    return subprocess.run(
        ["clamscan", filename], stdout=subprocess.PIPE, timeout=SUBPROCESS_TIMEOUT
    )


def scan_resource(logger, ckan_url, api_key, resource_id):
    try:
        resource = ckan_action("resource_show", ckan_url, api_key, {"id": resource_id})
    except util.JobError:
        # try again in 5 seconds just incase CKAN is slow at adding resource
        time.sleep(5)
        resource = ckan_action("resource_show", ckan_url, api_key, {"id": resource_id})

    if resource.get("url_type") != "upload":
        raise util.JobError("Only resources of type 'upload' can be scanned")

    url = resource.get("url")
    scheme = urlsplit(url).scheme
    if scheme not in ("http", "https", "ftp"):
        raise util.JobError("Only http, https, and ftp resources may be fetched.")

    logger.info("Updating ClamAV Definitions")
    try:
        # TODO: move this to a yacron job instead of doing it inline
        update_clamav_definitions()
    except (subprocess.SubprocessError, subprocess.TimeoutExpired):
        # just steamroller errors here on the basis it is better
        # to scan with old definitions than fail the job completely
        logger.error("Failed to update definitions")

    logger.info(f"Fetching from {url}")
    with tempfile.NamedTemporaryFile() as tmp:
        try:
            fetch_resource(url, tmp, api_key)
        except RequestException as e:
            raise util.JobError(str(e))
        logger.info(f"Scanning {tmp.name}")
        try:
            scan_result = scan_file(tmp.name)
        except (subprocess.SubprocessError, subprocess.TimeoutExpired) as e:
            raise util.JobError(str(e))

    return scan_result


@job.asynchronous
def scan(task_id, payload):
    logger = init_logger(task_id, payload)
    logger.info(f"Starting job {task_id}")

    try:
        validate_payload(payload)

        data = payload["metadata"]
        ckan_url = data["ckan_url"]
        resource_id = data["resource_id"]
        api_key = payload.get("api_key")

        scan_result = scan_resource(logger, ckan_url, api_key, resource_id)

        ckan_action(
            "scanstatus_create",
            ckan_url,
            api_key,
            {
                "status_code": scan_result.returncode,
                "status_text": STATUSES[scan_result.returncode],
                "description": scan_result.stdout.decode("utf-8"),
            },
        )
    except util.JobError as e:
        ckan_action(
            "scanstatus_create",
            ckan_url,
            api_key,
            {
                "status_code": 3,
                "status_text": STATUSES[3],
                "description": e.message,
            },
        )
