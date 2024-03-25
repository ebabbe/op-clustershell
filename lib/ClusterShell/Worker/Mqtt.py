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

This module implements OpenSSH engine client and task's worker.
"""
import inspect
import logging
import time
import hashlib
import json
import threading

from ClusterShell.Worker.Exec import ExecClient, ExecWorker


class MqttClient(ExecClient):
    """
    Mqtt EngineClient.
    """
    QOS_ONE = 1
    def __init__(self,  node, command, worker, stderr, timeout, client, opal, autoclose=False,
                 rank=None):
        self.client = client
        self.topic = opal + "command"
        self.msg = command
        self.started = False
        super(MqttClient, self).__init__(node, command, worker, stderr, timeout, autoclose,
                 rank)

    def _start(self):
        self.started = True
        json_msg = json.dumps(self.msg)
        self._on_nodeset_start(self.key)
        try:
            if self.topic.startswith("ERROR: "):
                raise Exception(self.topic)
            self.client.publishAsync(self.topic, json_msg, self.QOS_ONE, ackCallback=self._pub_callback)
        except Exception as e:
            self._on_nodeset_msgline(self.key, str.encode(repr(e)), self.worker.SNAME_STDERR)
            thread = threading.Thread(target=self._kill_client)
            thread.start()
        return self

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

    def _pub_callback(self, mid):
        self._on_nodeset_msgline(self.key, str.encode("MQTT message published successfully!"), self.worker.SNAME_STDOUT)
        self._kill_client()

class WorkerMqttPub(ExecWorker):

    MQTT_CLASS = MqttClient

    def __init__(self, nodes, handler, timeout=None, **kwargs):
        logging.getLogger("AWSIoTPythonSDK.core").setLevel(logging.ERROR)
        logging.getLogger("opus.helium").setLevel(logging.ERROR)
        self.opbok = __import__("opbok.util")
        self.botocore = __import__("botocore.utils")
        self.opus = __import__("opus")
        self.proton = __import__("proton.tools.client")
        errors = kwargs.get("errors", {})
        self.authenticate()
        self.getOpals(nodes, errors)
        self.mqttcli = self.proton.tools.client.OPMqttClient("publisher")
        self.msg = {"command" : "runCommand", "data": { "command" : f"{kwargs.get('command')}"}}
        requestId = int(str(hash(str(self.msg) + str(time.time())))[1:9])
        self.msg["requestId"] = requestId
        super(WorkerMqttPub, self).__init__(nodes, handler, timeout, **kwargs)

    def _add_client(self, nodes, **kwargs):
        """Create one mqtt publish client."""
        autoclose = kwargs.get('autoclose', False)
        stderr = kwargs.get('stderr', False)
        rank = kwargs.get('rank')
        timeout = kwargs.get('timeout')
        opal = self.acu_opal_map[nodes]
        if self.command is not None:
            cls = self.__class__.MQTT_CLASS
            self._clients.append(cls(nodes, self.msg, self, stderr,
                                     timeout, self.mqttcli, opal, autoclose, rank))
        else:
            raise ValueError("missing command or source parameter in "
                             "worker constructor")
    def getOpals(self, nodes, errors):
        self.acu_opal_map = {}
        for node in nodes:
            if errors.get(node):
                self.acu_opal_map[node] = "ERROR: "  + errors[node]
                continue
            acu, org, _, _, _ = node.split(".")
            acu_info = (self.HELIUM_CLIENT.describeAcu(int(org.split("org")[1]), int(acu.split("acu")[1])))
            self.acu_opal_map[node]  = acu_info["opal"].replace(":","/") + "/"

    def authenticate(self):
        AWS_REGION = self.botocore.utils.InstanceMetadataRegionFetcher().retrieve_region()
        self.HELIUM_CLIENT = self.opus.Helium(self.opbok.util.HELIUM_API, username=self.opbok.util.HELIUM_USERNAME, password=self.opbok.util.HELIUM_PASSWORD, namespaceId=self.opbok.util.HELIUM_NAMESPACE_ID)

WORKER_CLASS=WorkerMqttPub
