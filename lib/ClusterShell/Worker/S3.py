#
# Copyright (C) 2014-2015 CEA/DAM
# Copyright (C) 2014-2015 Aurelien Degremont <aurelien.degremont@cea.fr>
# Copyright (C) 2014-2017 Stephane Thiell <sthiell@stanford.edu>
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
ClusterShell S3 Client and Worker classes.

This module manages the worker class to spawn S3 result fetches
on a per ACU basis.

"""

import opconf
from io import BytesIO
import socket
import boto3
import threading
from ClusterShell.Worker.Exec import ExecClient, ExecWorker


class S3Client(ExecClient):
    """
    Fetch S3 object, where the object is an ACU's result from
    running a run-script, given by an MQTT message.

    First will check for output.json. If not found, will check error.json.
    If that isn't found, there is no response yet.
    """

    OUTPUT_SUFFIX = "output.json"
    ERROR_SUFFIX = "error.json"

    def __init__(
        self,
        node,
        responseId,
        s3_conn,
        worker,
        stderr,
        timeout,
        autoclose=False,
        rank=None,
    ):
        self.started = False
        self.s3_conn = s3_conn
        super(S3Client, self).__init__(
            node, responseId, worker, stderr, timeout, autoclose, rank
        )

    def _start(self):
        self.started = True
        self._on_nodeset_start(self.key)
        target = self._grab_response
        if self.key == "localhost":
            self._on_nodeset_msgline(
                self.key,
                str.encode(
                    f"No ACUs have replied with responseId {self.worker.responseId}. Either wait longer for a response, or check that the value provided is correct."
                ),
                self.worker.SNAME_STDERR,
            )
            target = self._kill_client

        thread = threading.Thread(target=target)
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

    def _grab_response(self):
        f = BytesIO()
        base_output_path = f"{self.worker.responseId}/{self.key}/"
        try:
            file_path = base_output_path + self.OUTPUT_SUFFIX
            self.s3_conn.download_fileobj(
                self.worker.LOG_SCRIPT_BUCKET_NAME, file_path, f
            )
            self._on_nodeset_msgline(
                self.key,
                str.encode(f.getvalue().decode()),
                self.worker.SNAME_STDOUT,
            )
        except Exception:
            try:
                file_path = base_output_path + self.ERROR_SUFFIX
                self.s3_conn.download_fileobj(
                    self.worker.LOG_SCRIPT_BUCKET_NAME, file_path, f
                )
                self._on_nodeset_msgline(
                    self.key,
                    str.encode(f.getvalue().decode()),
                    self.worker.SNAME_STDERR,
                )
            except Exception:

                self._on_nodeset_msgline(
                    self.key,
                    str.encode(f"Cannot find reponse for {self.key}. Try again later."),
                    self.worker.SNAME_STDERR,
                )

        self._kill_client()


class S3Worker(ExecWorker):
    """
    ClusterShell S3 worker Class.

    It creates an S3 connection that the clients share. If no nodes will given, it will populate the nodelist
    based on the results found in S3.

    """

    S3_CLASS = S3Client
    HOSTNAME = opconf.main.get("acu_hostname", default=socket.gethostname())
    ENVIRONMENT = opconf.main.get("env", default=HOSTNAME.split(".")[1])
    LOG_SCRIPT_BUCKET_NAME = f"openpath.{ENVIRONMENT}.acu.run-logs"

    def __init__(self, responseId, nodes, handler, timeout=None, **kwargs):
        self.session = boto3.Session()
        self.s3 = self.session.client("s3")
        self.responseId = responseId[0]
        nodes = self._get_nodes() if len(nodes) == 0 else nodes
        if len(nodes) == 0:
            nodes = "localhost"
        super(S3Worker, self).__init__(nodes, handler, timeout, **kwargs)

    def _get_nodes(self):
        nodes = []
        result = self.s3.list_objects(
            Bucket=self.LOG_SCRIPT_BUCKET_NAME,
            Prefix=f"{self.responseId}/",
            Delimiter="/",
        )
        for o in result.get("CommonPrefixes", []):
            nodes.append(o.get("Prefix").split("/")[1])
        return ",".join(nodes)

    def _add_client(self, nodes, **kwargs):
        """Create one s3 client object."""
        autoclose = kwargs.get("autoclose", False)
        stderr = kwargs.get("stderr", False)
        rank = kwargs.get("rank")
        timeout = kwargs.get("timeout")
        if self.responseId is not None:
            cls = self.__class__.S3_CLASS
            self._clients.append(
                cls(
                    nodes,
                    self.responseId,
                    self.s3,
                    self,
                    stderr,
                    timeout,
                    autoclose,
                    rank,
                )
            )
        else:
            raise ValueError(
                "missing command or source parameter in " "worker constructor"
            )


WORKER_CLASS = S3Worker
