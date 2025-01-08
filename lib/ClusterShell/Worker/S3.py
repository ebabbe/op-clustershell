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

import os
from io import BytesIO
import boto3
import threading
from ClusterShell.Worker.Exec import ExecClient, ExecWorker
from ClusterShell.CLI.Clush import DirectOutputDirHandler
from ClusterShell.NodeSet import NodeSet
from ClusterShell.Engine.Engine import E_READ


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
        requestId,
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
            node, requestId, worker, stderr, timeout, autoclose, rank
        )

    def _start(self):
        self.started = True
        self._on_nodeset_start(self.key)
        target = self._grab_response
        self.stdout_output_pipe, self.stdout_input_pipe = os.pipe()
        self.err_output_pipe, self.err_input_pipe = os.pipe()
        args = ()
        if self._stderr:
            self.streams.set_stream(
                self.worker.SNAME_STDERR, self.err_output_pipe, E_READ, retain=False
            )
        self.streams.set_stream(
            self.worker.SNAME_STDOUT, self.stdout_output_pipe, E_READ, retain=False
        )
        self._engine.evlooprefcnt += 2
        if self.key == "localhost":
            target = self._log_error_msg
            args = (
                f"No ACUs have replied with requestId {self.worker.requestId}. Either wait longer for a response, or check that the value provided is correct.\n",
            )

        self.thread = threading.Thread(target=target, args=args)
        self.thread.start()
        return self

    def _log_error_msg(self, msg):
        os.write(self.err_input_pipe, str.encode(msg))

        os.close(self.stdout_input_pipe)
        os.close(self.err_input_pipe)
        if self._engine is not None:
            self._engine.evlooprefcnt -= 2

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

    def _grab_response(self):
        f = BytesIO()
        base_output_path = f"{self.worker.requestId}/{self.key}/"
        try:
            file_path = base_output_path + self.OUTPUT_SUFFIX
            self.s3_conn.download_fileobj(
                self.worker.LOG_SCRIPT_BUCKET_NAME, file_path, f
            )

            os.write(self.stdout_input_pipe, str.encode(f"{f.getvalue().decode()}\n"))

            os.close(self.err_input_pipe)
            os.close(self.stdout_input_pipe)

        except Exception:

            try:
                file_path = base_output_path + self.ERROR_SUFFIX
                self.s3_conn.download_fileobj(
                    self.worker.LOG_SCRIPT_BUCKET_NAME, file_path, f
                )
                self._log_error_msg(f"{f.getvalue().decode()}\n")

            except Exception:
                self._log_error_msg(
                    f"Cannot find reponse for {self.key}. Try again later.\n"
                )

        else:
            if self._engine is not None:
                self._engine.evlooprefcnt -= 2


class S3Worker(ExecWorker):
    """
    ClusterShell S3 worker Class.

    It creates an S3 connection that the clients share. If no nodes will given, it will populate the nodelist
    based on the results found in S3.

    """

    S3_CLASS = S3Client

    def __init__(self, requestId, nodes, handler, display, timeout=None, **kwargs):
        environment = kwargs.get("environment", None)
        self.LOG_SCRIPT_BUCKET_NAME = f"openpath.{environment}.acu.run-logs"
        self.session = boto3.Session()
        self.s3 = self.session.client("s3")
        self.requestId = requestId[0]
        if len(nodes) == 0:
            nodes = self._get_nodes()
            if len(nodes) == 0:
                nodes = NodeSet.fromlist(["localhost"])
            if display.outdir and not os.path.exists(display.outdir):
                os.makedirs(display.outdir)
            if display.errdir and not os.path.exists(display.errdir):
                os.makedirs(display.errdir)
            handler = DirectOutputDirHandler(display, nodes)

        super(S3Worker, self).__init__(nodes, handler, timeout, **kwargs)

    def _get_nodes(self):
        nodes = []
        paginator = self.s3.get_paginator("list_objects_v2")
        pages = paginator.paginate(
            Bucket=self.LOG_SCRIPT_BUCKET_NAME,
            Prefix=f"{self.requestId}/",
            Delimiter="/",
        )
        for page in pages:
            for o in page.get("CommonPrefixes", []):
                nodes.append(o.get("Prefix").split("/")[1])
        return NodeSet.fromlist(nodes)

    def _add_client(self, nodes, **kwargs):
        """Create one s3 client object."""
        autoclose = kwargs.get("autoclose", False)
        stderr = kwargs.get("stderr", False)
        rank = kwargs.get("rank")
        timeout = kwargs.get("timeout")
        if self.requestId is not None:
            cls = self.__class__.S3_CLASS
            self._clients.append(
                cls(
                    nodes,
                    self.requestId,
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
