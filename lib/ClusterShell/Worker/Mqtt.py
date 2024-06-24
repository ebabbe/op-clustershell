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
from ClusterShell.NodeSet import NodeSet, expand
import uuid
import json
import threading

import boto3
import botocore


from ClusterShell.Worker.Exec import ExecClient, ExecWorker


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
        try:
            if self.topic.startswith("ERROR: "):
                raise Exception(self.topic)

        except Exception as e:
            self._on_nodeset_msgline(
                self.key, str.encode(repr(e)), self.worker.SNAME_STDERR
            )
            target = self._kill_client
        else:
            target = self._send_message
            args = (
                self._pub_callback,
                json_msg,
            )
        thread = threading.Thread(target=target, args=args)
        thread.start()
        return self

    def _send_message(self, callback, json_msg):
        try:
            self.client.publish(topic=self.topic, payload=json_msg, qos=self.QOS_ONE)
            callback()
        except Exception as e:
            self._on_nodeset_msgline(
                self.key, str.encode(repr(e)), self.worker.SNAME_STDERR
            )
            self._kill_client()

    def _kill_client(self):
        while self._engine is not None:
            if self.registered:
                self._engine.remove(self)
                return

    def _close(self, abort, timeout):
        self.streams.clear()
        self.invalidate()
        self._on_nodeset_close(self.key, 0)
        self.worker._check_fini()

    def _pub_callback(self):
        self._on_nodeset_msgline(
            self.key,
            str.encode(
                f"MQTT message with requestID {self.msg['requestId']} published successfully!"
            ),
            self.worker.SNAME_STDOUT,
        )
        self._kill_client()


class WorkerMqttPub(ExecWorker):

    MQTT_CLASS = MqttClient
    OPAL_TEMPLATE = "opal/{env}/helium/alpha/{org}/acu/{acu}/"

    def __init__(self, nodes, handler, timeout=None, **kwargs):
        logging.getLogger("AWSIoTPythonSDK.core").setLevel(logging.ERROR)
        self.botocore = __import__("botocore.utils")
        errors = kwargs.get("errors", {})
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
        requestId = str(uuid.uuid4())
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
