import json
import logging
import time

import google.auth
import requests

DATAPLEX_ENDPOINT = 'https://dataplex.googleapis.com'

logging.basicConfig(level=logging.INFO)
log = logging.getLogger()


def __get_session_headers() -> dict:
    """
    This method is to get the session and headers object for authenticating the api requests using credentials.
    Args:
    Returns: dict
    """
    # getting the credentials and project details for gcp project
    credentials, your_project_id = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])

    # getting request object
    auth_req = google.auth.transport.requests.Request()

    credentials.refresh(auth_req)  # refresh token
    auth_token = credentials.token

    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'Authorization': 'Bearer ' + auth_token
    }

    return headers


def __handle_dataplex_job_state(res: requests.Response) -> str:
    log.info(f"Dataplex submit job response: HTTP {res.status_code} {res.text}")
    resp_obj = json.loads(res.text)
    if res.status_code == 200:

        if (
                "jobs" in resp_obj
                and len(resp_obj["jobs"]) > 0
                and "state" in resp_obj["jobs"][0]
        ):
            task_status = resp_obj["jobs"][0]["state"]
            return task_status
    else:
        raise Exception()


def get_clouddq_task_status(project_id: str, region: str, lake_id: str, task_id: str) -> str:
    """
    This method will return the job status for the task.
    Args:
    Returns: str
    """
    headers = __get_session_headers()
    res = requests.get(
        f"{DATAPLEX_ENDPOINT}/v1/projects/{project_id}/locations/{region}/lakes/{lake_id}/tasks/{task_id}/jobs",
        headers=headers)
    return __handle_dataplex_job_state(res)


def submit_dataplex_task(project_id: str, region: str, lake_id: str, task_id: str) -> None:
    """
    This method will submit the job for the task.
    Args:
    Returns: str
    """
    res = requests.post(
        f"{DATAPLEX_ENDPOINT}/v1/projects/{project_id}/locations/{region}/lakes/{lake_id}/tasks/{task_id}:run",
        headers=__get_session_headers())
    log.info(f"Dataplex submit job response: HTTP {res.status_code} {res.text}")
    if res.status_code != 200:
        log.error(f"Dataplex job submission failed")
        raise Exception()


def get_dataplex_job_state(project_id: str, region: str, lake_id: str, task_id: str) -> str:
    """
    This method will try to get the status of the job till it is in either 'SUCCEEDED' or 'FAILED' state.
    Args:
    Returns: str
    """
    task_status = get_clouddq_task_status(project_id, region, lake_id, task_id)
    while (task_status != 'SUCCEEDED' and task_status != 'FAILED' and task_status != 'CANCELLED'
           and task_status != 'ABORTED'):
        log.info(time.ctime())
        time.sleep(30)
        task_status = get_clouddq_task_status(project_id, region, lake_id, task_id)
        log.info(f"CloudDQ task status is {task_status}")
    return task_status


def get_dataplex_task(project_id: str, region: str, lake_id: str, task_id: str) -> str:
    """
    This method will return the status for the task.
    Args:
    Returns: str
    """
    headers = __get_session_headers()
    res = requests.get(
        f"{DATAPLEX_ENDPOINT}/v1/projects/{project_id}/locations/{region}/lakes/{lake_id}/tasks/{task_id}",
        headers=headers)
    if res.status_code == 404:
        return "task_not_exist"
    elif res.status_code == 200:
        return "task_exist"
    else:
        return "task_error"
