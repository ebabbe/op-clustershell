#
# Copyright (C) 2008-2015 CEA/DAM
#
# This file is part of ClusterShell.
#
# ClusterShell is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation; either
# version 2.1 of the License, or (at your option) any later version.
#
# ClusterShell is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public
# License along with ClusterShell; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA

"""
ClusterShell Mqtt support

"""
import logging
import os
from ClusterShell.NodeSet import NodeSet, expand
import uuid
import json
import threading

import boto3
import botocore


from ClusterShell.Worker.Exec import ExecClient, ExecWorker

from ClusterShell.Engine.Engine import E_READ


class MqttClient(ExecClient):
    """
    Mqtt EngineClient.
    """

    QOS_ONE = 1

    def __init__(
        self,
        node,
        command,
        worker,
        stderr,
        timeout,
        client,
        opal,
        autoclose=False,
        rank=None,
    ):
        self.client = client
        self.topic = opal + "command" if "ERROR" not in opal else opal
        self.msg = command
        self.started = False
        super(MqttClient, self).__init__(
            node, command, worker, stderr, timeout, autoclose, rank
        )

    def _start(self):
        self.started = True
        json_msg = json.dumps(self.msg)
        self._on_nodeset_start(self.key)
        args = ()
        self.stdout_output_pipe, self.stdout_input_pipe = os.pipe()
        self.err_output_pipe, self.err_input_pipe = os.pipe()
        if self._stderr:
            self.streams.set_stream(
                self.worker.SNAME_STDERR, self.err_output_pipe, E_READ, retain=False
            )
        self.streams.set_stream(
            self.worker.SNAME_STDOUT, self.stdout_output_pipe, E_READ, retain=False
        )
        self._engine.evlooprefcnt += 2
        try:
            if self.topic.startswith("ERROR: "):
                raise Exception(self.topic)

        except Exception as e:

            target = self._log_error_msg
            args = (repr(e) + "\n",)
        else:
            target = self._send_message
            args = (json_msg,)
        self.thread = threading.Thread(target=target, args=args)
        self.thread.start()
        self._on_nodeset_start(self.key)
        return self

    def _log_error_msg(self, msg):
        os.write(self.err_input_pipe, str.encode(msg))

        os.close(self.stdout_input_pipe)
        os.close(self.err_input_pipe)
        if self._engine is not None:
            self._engine.evlooprefcnt -= 2

    def _send_message(self, json_msg):
        try:
            self.client.publish(topic=self.topic, payload=json_msg, qos=self.QOS_ONE)
            os.write(
                self.stdout_input_pipe,
                str.encode(
                    f"MQTT message with requestID {self.msg['requestId']} published successfully!\n"
                ),
            )
            os.close(self.stdout_input_pipe)
            os.close(self.err_input_pipe)
            if self._engine is not None:
                self._engine.evlooprefcnt -= 2

        except Exception as e:
            self._log_error_msg((repr(e) + "\n"))

    def _kill_client(self):
        while self._engine is not None:
            if self.registered:
                self._engine.remove(self)
                return

    def _close(self, abort, timeout):
        if self.thread.name != threading.current_thread().name:
            self.thread.join()
        self.streams.clear()
        self.invalidate()
        self._on_nodeset_close(self.key, 0)
        self.worker._check_fini()


class WorkerMqttPub(ExecWorker):

    MQTT_CLASS = MqttClient
    OPAL_TEMPLATE = "opal/{env}/helium/alpha/{org}/acu/{acu}/"

    def __init__(self, nodes, handler, timeout=None, **kwargs):
        logging.getLogger("AWSIoTPythonSDK.core").setLevel(logging.ERROR)
        self.botocore = __import__("botocore.utils")
        errors = kwargs.get("errors", {})
        requestId = kwargs.get("requestId", None)
        true_nodes = expand(NodeSet.fromlist(nodes))
        client_config = botocore.config.Config(max_pool_connections=len(true_nodes))
        self.getOpals(true_nodes, errors)
        self.mqttcli = boto3.client(
            "iot-data",
            config=client_config,
            region_name=self.botocore.utils.InstanceMetadataRegionFetcher().retrieve_region(),
        )
        self.msg = {
            "command": "runCommand",
            "data": {"command": f"{kwargs.get('command')}"},
        }
        requestId = str(uuid.uuid4()) if requestId is None else requestId[0]
        self.msg["requestId"] = requestId
        super(WorkerMqttPub, self).__init__(nodes, handler, timeout, **kwargs)

    def _add_client(self, nodes, **kwargs):
        """Create one mqtt publish client."""
        autoclose = kwargs.get("autoclose", False)
        stderr = kwargs.get("stderr", False)
        rank = kwargs.get("rank")
        timeout = kwargs.get("timeout")
        opal = self.acu_opal_map[nodes]
        if self.command is not None:
            cls = self.__class__.MQTT_CLASS
            self._clients.append(
                cls(
                    nodes,
                    self.msg,
                    self,
                    stderr,
                    timeout,
                    self.mqttcli,
                    opal,
                    autoclose,
                    rank,
                )
            )
        else:
            raise ValueError(
                "missing command or source parameter in " "worker constructor"
            )

    def getOpals(self, nodes, errors):
        self.acu_opal_map = {}
        for node in nodes:
            if errors.get(node):
                self.acu_opal_map[node] = "ERROR: " + errors[node]
                continue
            acu, org, env, _, _ = node.split(".")

            opal = self.OPAL_TEMPLATE.format(
                env=env, org=org.split("org")[1], acu=acu.split("acu")[1]
            )
            self.acu_opal_map[node] = opal


WORKER_CLASS = WorkerMqttPub
