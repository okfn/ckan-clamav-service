import json
import logging
import os
import subprocess
import tempfile
import time
from urllib.parse import urlsplit

import requests
from ckanserviceprovider import job, util, web
from requests.exceptions import RequestException

MAX_CONTENT_LENGTH = web.app.config.get("MAX_CONTENT_LENGTH") or 10485760
CHUNK_SIZE = web.app.config.get("CHUNK_SIZE") or 16384
DOWNLOAD_TIMEOUT = web.app.config.get("DOWNLOAD_TIMEOUT") or 30
SUBPROCESS_TIMEOUT = web.app.config.get("SUBPROCESS_TIMEOUT") or 300

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
        raise util.JobError(f"{str(e)} with payload {json.dumps(payload)}")

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


def scan_file(filename):
    # Get file size before scanning
    file_size = os.path.getsize(filename)

    # Record start time
    start_time = time.time()

    try:
        result = subprocess.run(
            ["clamscan", filename], stdout=subprocess.PIPE, timeout=SUBPROCESS_TIMEOUT
        )
        elapsed_time = time.time() - start_time
        # Add size and time info to result object for later logging
        result.file_size = file_size
        result.elapsed_time = elapsed_time
        return result
    except (subprocess.TimeoutExpired, subprocess.SubprocessError) as e:
        elapsed_time = time.time() - start_time
        # Enhance the error with file size and elapsed time info
        e.file_size = file_size
        e.elapsed_time = elapsed_time
        raise e


def scan_resource(logger, ckan_url, api_key, resource_id):
    response = {
        'error': None,
        'file_size': -1,
        'elapsed_time': -1,
        'returncode': 2,  # Default to 2 (scan failed)
        'stdout': '',
    }
    # try again in 5 seconds just incase CKAN is slow at adding resource
    time.sleep(5)
    try:
        resource = ckan_action("resource_show", ckan_url, api_key, {"id": resource_id})
    except util.JobError as e:
        response["error"] = f'Error showing resource: {e}'
        return response

    url_type = resource.get("url_type")
    if url_type != "upload":
        response["error"] = f"Only resources of type 'upload' can be scanned. Received '{str(url_type)}'"
        return response

    url = resource.get("url")
    scheme = urlsplit(url).scheme
    if scheme not in ("http", "https", "ftp"):
        response["error"] = "Only http, https, and ftp resources may be fetched."
        return response

    logger.info(f"Fetching from {url}")
    with tempfile.NamedTemporaryFile() as tmp:
        try:
            fetch_resource(url, tmp, api_key)
        except RequestException as e:
            response["error"] = f"Error fetching resource: {e}"
            return response

        # Add file size info to log message
        file_size = os.path.getsize(tmp.name)
        response["file_size"] = file_size
        logger.info(f"Scanning {tmp.name} (size: {file_size / 1024:.2f} KB)")

        try:
            scan_result = scan_file(tmp.name)
            response["returncode"] = scan_result.returncode
            response["stdout"] = scan_result.stdout.decode("utf-8")
            response["elapsed_time"] = scan_result.elapsed_time
            logger.info(
                f"Scan completed in {scan_result.elapsed_time:.2f} seconds "
                f"for file of size {file_size / 1024:.2f} KB"
            )
        except (subprocess.TimeoutExpired, subprocess.SubprocessError) as e:
            if isinstance(e, subprocess.TimeoutExpired):
                response["error"] = f"Scan timed out: {e}"
            else:
                response["error"] = f"Scan failed: {e}"
            response["file_size"] = file_size
            response["elapsed_time"] = e.elapsed_time
            logger.error(
                f"Scan timed out after {e.elapsed_time:.2f} seconds "
                f"for file of size {e.file_size / 1024:.2f} KB"
            )

    return response


@job.asynchronous
def scan(task_id, payload):
    logger = init_logger(task_id, payload)
    logger.info(f"Starting ClamAV job {task_id}")

    validate_payload(payload)

    data = payload["metadata"]
    ckan_url = data["ckan_url"]
    resource_id = data["resource_id"]
    api_key = payload.get("api_key")

    try:
        response = scan_resource(logger, ckan_url, api_key, resource_id)
        if response.get("error"):
            raise util.JobError(json.dumps(response))
    except Exception as e:
        response = {
            "status_code": 2,
            "description": f"Unexpected error: {e}",
        }
        raise util.JobError(json.dumps(response))
    else:
        response["status_code"] = response['returncode']
        response["description"] = response["stdout"]

    returncode = response["status_code"]
    if returncode not in STATUSES:
        file_size = response.get("file_size", 0)
        elapsed_time = response.get("elapsed_time", 0)
        logger.error(
            f"Unknown return code {returncode} (not in statuses) "
            f"scanning resource {resource_id} "
            f"File size: {file_size / 1024:.2f} KB, "
            f"Scan time: {elapsed_time:.2f} seconds, "
            f"Stdout: {response['description']}"
        )
        raise util.JobError(json.dumps(response))

    response["status_text"] = STATUSES[returncode]
    if returncode == 2:
        logger.error(
            f"Scan failed for resource {resource_id}: {response['description']}"
        )
        raise util.JobError(json.dumps(response))

    final_status_text = response["status_text"]
    logger.info(
        f"Completed scanning {final_status_text}. Resource {resource_id}. Submitting result"
    )
    return response
