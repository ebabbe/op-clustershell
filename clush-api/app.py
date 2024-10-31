import json
import os
import re
import time
from typing import Optional

from chalice import Chalice, Response, IAMAuthorizer

from chalice.app import BadRequestError

# from chalice_spec import PydanticPlugin, ChalicePlugin, Docs, Operation
# from apispec import APISpec
from pydantic import BaseModel

from optparse import Values

from chalicelib.CLI.Clush import (
    fetch_output_from_s3,
    run_command,
    HeliumClient,
)
from chalicelib.Task import Task

from chalicelib.CLI.Display import Display

from chalicelib.MsgTree import MsgTree


app = Chalice(app_name="clush-api")
# spec = APISpec(
#     chalice_app=app,
#     title="clush-api",
#     version="1.0.0",
#     openapi_version="3.0.2",
#     plugins=[PydanticPlugin(), ChalicePlugin()],
# )
authorizer = IAMAuthorizer()

MAX_DEVICES = int(os.environ["MAX_DEVICES"])
ENVIRONMENT = os.environ["ENVIRONMENT"]


class PublishRequest(BaseModel):
    devices: Optional[set] = []
    command: str
    orgs: Optional[set] = []
    timeout: int = 60
    username: str = None
    password: str = None
    namespaceId: int = 1000


class ResultsRequest(BaseModel):
    devices: Optional[list] = []
    requestId: str
    timeout: int = 60


class RequestResponse(BaseModel):
    results: dict
    error: Optional[str] = None
    message: str = None


options = Values(
    defaults={
        "diff": None,
        "gather": None,
        "gatherall": False,
        "line_mode": None,
        "label": True,
        "regroup": False,
        "groupsource": None,
        "groupbase": False,
        "errdir": None,
        "outdir": None,
        "whencolor": None,
    }
)


def create_task():
    task = Task()
    task.set_default("stderr", True)
    task.set_default("stdout_msgtree", True)
    task.set_default("stderr_msgtree", True)
    task.set_default("USER_handle_SIGUSR1", False)
    task.set_default("USER_stdin_worker", False)
    task.set_default("USER_interactive", False)
    task.set_default("USER_running", False)
    task.set_info("App", True)
    return task


def get_output(task, max_time=8 * 60 * 60):
    devices = task.nodes

    results_dict = {}
    response_count = 0
    emp_tree = MsgTree()

    start_time = time.time()
    try:
        while (
            response_count < len(devices) or len(devices) == 0
        ) and time.time() < max_time + start_time:

            with task.msgtree_lock:

                for key, message in task._msgtrees.get("stdout", {}).items():
                    try:
                        stringMessage = json.loads(message.message().decode())

                    except (json.JSONDecodeError, TypeError) as e:

                        stringMessage = message.message().decode()
                    if key[1] not in results_dict:
                        results_dict[key[1]] = {"output": stringMessage}
                    else:
                        results_dict[key[1]]["output"] = stringMessage

                    if "requestID" in stringMessage:
                        result = re.search("requestID (.*) published", stringMessage)
                        results_dict[key[1]]["requestId"] = result.group(1)
                    results_dict[key[1]]["error"] = ""

                    response_count += 1

                task._msgtrees.get("stdout", emp_tree).remove(
                    lambda k: k[1] in results_dict
                )

            with task.msgtree_lock:

                for key, message in task._msgtrees.get("stderr", {}).items():
                    try:
                        stringMessage = json.loads(message.message().decode())

                    except (json.JSONDecodeError, TypeError) as e:

                        stringMessage = message.message().decode()

                    if key[1] not in results_dict:
                        results_dict[key[1]] = {"error": stringMessage}
                    else:
                        results_dict[key[1]]["error"] = stringMessage

                    results_dict[key[1]]["output"] = ""

                    response_count += 1

                task._msgtrees.get("stderr", emp_tree).remove(
                    lambda k: k[1] in results_dict
                )

    except Exception as e:

        return e
    task.abort()

    return results_dict


@app.route(
    "/publish",
    authorizer=authorizer,
    methods=["POST"],
    # docs=Docs(post=Operation(request=PublishRequest, response=RequestResponse)),
)
def publish():
    try:
        request = PublishRequest(**app.current_request.json_body)
    except ValueError as e:
        # If validation fails, return a 400 Bad Request response
        raise BadRequestError(f"Invalid request: {str(e)}")
    task = create_task()

    command = request.command
    list_devices = request.devices
    timeout = request.timeout

    if len(request.orgs) != 0:
        username = request.username
        password = request.password
        if username is None or password is None:
            response = RequestResponse(
                results={},
                message="Error",
                error="You must provide a Helium username and password if you want to aggregate devices by org.",
            )
            return Response(
                body=response.model_dump_json(),
                status_code=400,
                headers={"Content-Type": "application/json"},
            )

        try:
            helium_client = HeliumClient(
                ENVIRONMENT,
                username=username,
                password=password,
                namespaceId=request.namespaceId,
            )
            for org in request.orgs:
                org_info = helium_client.client.listAcus(org)
                for device in org_info:
                    # device = json.loads(device)
                    hostname = device.get("hostname", None)
                    if hostname not in list_devices and hostname is not None:
                        list_devices.add(hostname)

        except Exception as e:
            response = RequestResponse(
                results={},
                message="Error",
                error=str(e),
            )
            return Response(
                body=response.model_dump_json(),
                status_code=400,
                headers={"Content-Type": "application/json"},
            )

    if len(list_devices) > MAX_DEVICES:
        response = RequestResponse(
            results={},
            message="Error",
            error=f"Request is for too many devices. Max allowed: {str(MAX_DEVICES)}",
        )
        return Response(
            body=response.model_dump_json(),
            status_code=400,
            headers={"Content-Type": "application/json"},
        )
    devices = ",".join(list_devices)
    if len(devices) == 0:
        response = RequestResponse(
            results={},
            error="No devices provided",
            message="Error",
        )
        return Response(
            body=response.model_dump_json(),
            status_code=418,
            headers={"Content-Type": "application/json"},
        )

    run_command(
        task,
        command,
        devices,
        -1,
        Display(options),
        True,
        True,
        True,
        {},
        False,
        ENVIRONMENT,
    )
    results_dict = get_output(task, timeout)
    if not isinstance(results_dict, dict):
        response = RequestResponse(results={}, error=str(results_dict), message="Error")
        return Response(
            body=response.model_dump_json(),
            status_code=400,
            headers={"Content-Type": "application/json"},
        )

    if len(results_dict) == 0:
        response = RequestResponse(
            results={},
            message="No results found. Either the requestId is incorrect, or wait longer for responses.",
        )
        return Response(
            body=response.model_dump_json(),
            status_code=200,
            headers={"Content-Type": "application/json"},
        )

    response = RequestResponse(results=results_dict, message="OK")

    return Response(
        body=response.model_dump_json(),
        status_code=200,
        headers={"Content-Type": "application/json"},
    )


@app.route(
    "/results",
    authorizer=authorizer,
    methods=["POST"],
    # docs=Docs(post=Operation(request=ResultsRequest, response=RequestResponse)),
)
def retrieve_results():
    try:
        request = ResultsRequest(**app.current_request.json_body)
    except ValueError as e:
        # If validation fails, return a 400 Bad Request response
        raise BadRequestError(f"Invalid request: {str(e)}")

    requestId = request.requestId
    list_devices = request.devices
    timeout = request.timeout
    # inject error code
    if len(list_devices) > MAX_DEVICES:
        print(
            f"Too many devices supplied. {len(list_devices)} is greater than max allowed: {MAX_DEVICES} "
        )
        response = RequestResponse(
            results={},
            message="Error",
            error=f"Request is for too many devices. Max allowed: {str(MAX_DEVICES)}",
        )
        return Response(
            body=response.model_dump_json(),
            status_code=400,
            headers={"Content-Type": "application/json"},
        )
    devices = ",".join(list_devices)
    task = create_task()
    fetch_output_from_s3(
        task,
        requestId,
        devices,
        -1,
        Display(options),
        True,
        True,
        ENVIRONMENT,
        {},
    )
    results_dict = get_output(task, timeout)

    if len(results_dict) == 0:
        response = RequestResponse(
            results={},
            message="No results found. Either the requestId is incorrect, or wait longer for responses.",
        )
        return Response(
            body=response.model_dump_json(),
            status_code=200,
            headers={"Content-Type": "application/json"},
        )
    if not isinstance(results_dict, dict):
        response = RequestResponse(results={}, error=results_dict, message="Error")
        return Response(
            body=response.model_dump_json(),
            status_code=400,
            headers={"Content-Type": "application/json"},
        )
    response = RequestResponse(results=results_dict, message="OK")

    return Response(
        body=response.model_dump_json(),
        status_code=200,
        headers={"Content-Type": "application/json"},
    )


# @app.route("/openapi.json")
# def openapi():
#     return spec.to_dict()


# @app.route("/docs")
# def docs():
#     html = """
#         <!DOCTYPE html>
#         <html lang="en">
#         <head>
#           <meta charset="utf-8" />
#           <meta name="viewport" content="width=device-width, initial-scale=1" />
#           <meta
#             name="description"
#             content="SwaggerUI"
#           />
#           <title>SwaggerUI</title>
#           <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@4.5.0/swagger-ui.css" />
#         </head>
#         <body>
#         <div id="swagger-ui"></div>
#         <script src="https://unpkg.com/swagger-ui-dist@4.5.0/swagger-ui-bundle.js" crossorigin></script>
#         <script>
#           window.onload = () => {
#             window.ui = SwaggerUIBundle({
#               url: '/openapi.json',
#               dom_id: '#swagger-ui',
#             });
#           };
#         </script>
#         </body>
#         </html>
#     """
#     return Response(body=html, status_code=200, headers={"Content-Type": "text/html"})


# @app.route("/stats")
# def request_stats():
#     pass


# @app.route("/device_list")
# def get_device_list():
#     pass


@app.route("/health")
def get_health():
    return {"Hello": "World"}
