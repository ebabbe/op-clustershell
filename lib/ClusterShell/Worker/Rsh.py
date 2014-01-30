#
# Copyright CEA/DAM/DIF (2013, 2014)
#
# This file is part of the ClusterShell library.
#
# This software is governed by the CeCILL-C license under French law and
# abiding by the rules of distribution of free software.  You can  use,
# modify and/ or redistribute the software under the terms of the CeCILL-C
# license as circulated by CEA, CNRS and INRIA at the following URL
# "http://www.cecill.info".
#
# As a counterpart to the access to the source code and  rights to copy,
# modify and redistribute granted by the license, users are provided only
# with a limited warranty  and the software's author,  the holder of the
# economic rights,  and the successive licensors  have only  limited
# liability.
#
# In this respect, the user's attention is drawn to the risks associated
# with loading,  using,  modifying and/or developing or reproducing the
# software by the user in light of its specific status of free software,
# that may mean  that it is complicated to manipulate,  and  that  also
# therefore means  that it is reserved for developers  and  experienced
# professionals having in-depth computer knowledge. Users are therefore
# encouraged to load and test the software's suitability as regards their
# requirements in conditions enabling the security of their systems and/or
# data to be ensured and,  more generally, to use and operate it in the
# same conditions as regards security.
#
# The fact that you are presently reading this means that you have had
# knowledge of the CeCILL-C license and that you accept its terms.

"""
ClusterShell RSH support

It could also handles rsh forks, like krsh or mrsh.
This is also the base class for rsh evolutions, like Ssh worker.
"""

import copy
import os

from ClusterShell.NodeSet import NodeSet
from ClusterShell.Worker.EngineClient import EngineClient
from ClusterShell.Worker.Worker import DistantWorker


class Rsh(EngineClient):
    """
    Rsh EngineClient.
    """

    def __init__(self, node, command, worker, stderr, timeout, autoclose=False):
        """
        Initialize Rsh EngineClient instance.
        """
        EngineClient.__init__(self, worker, stderr, timeout, autoclose)

        self.key = copy.copy(node)
        self.command = command
        self.popen = None

    def _build_cmd(self):
        """
        Build the shell command line to start the rsh commmand.
        Return an array of command and arguments.
        """
        # Does not support 'connect_timeout'
        task = self.worker.task
        path = task.info("rsh_path") or "rsh"
        user = task.info("rsh_user")
        options = task.info("rsh_options")

        cmd_l = [ path ]

        if user:
            cmd_l.append("-l")
            cmd_l.append(user)

        # Add custom options
        if options:
            cmd_l += options.split()

        cmd_l.append("%s" % self.key)  # key is the node
        cmd_l.append("%s" % self.command)

        return cmd_l

    def _start(self):
        """
        Start worker, initialize buffers, prepare command.
        """

        # Build command
        cmd_l = self._build_cmd()

        task = self.worker.task
        if task.info("debug", False):
            name = str(self.__class__).upper().split('.')[-1]
            task.info("print_debug")(task, "%s: %s" % (name, ' '.join(cmd_l)))

        self.popen = self._exec_nonblock(cmd_l)
        self.worker._on_start()
        return self

    def _close(self, abort, flush, timeout):
        """
        Close client. See EngineClient._close().
        """
        if flush and self._rbuf:
            # We still have some read data available in buffer, but no
            # EOL. Generate a final message before closing.
            self.worker._on_node_msgline(self.key, self._rbuf)

        rc = -1
        if abort:
            prc = self.popen.poll()
            if prc is None:
                # process is still running, kill it
                self.popen.kill()
        prc = self.popen.wait()
        if prc >= 0:
            rc = prc

        os.close(self.fd_reader)
        self.fd_reader = None
        if self.fd_error:
            os.close(self.fd_error)
            self.fd_error = None
        if self.fd_writer:
            os.close(self.fd_writer)
            self.fd_writer = None

        if rc >= 0:
            self.worker._on_node_rc(self.key, rc)
        elif timeout:
            assert abort, "abort flag not set on timeout"
            self.worker._on_node_timeout(self.key)

        self.worker._check_fini()

    def _handle_read(self):
        """
        Handle a read notification. Called by the engine as the result of an
        event indicating that a read is available.
        """
        # Local variables optimization
        worker = self.worker
        task = worker.task
        key = self.key
        node_msgline = worker._on_node_msgline
        debug = task.info("debug", False)
        if debug:
            print_debug = task.info("print_debug")
        for msg in self._readlines():
            if debug:
                print_debug(task, "%s: %s" % (key, msg))
            node_msgline(key, msg)  # handle full msg line

    def _handle_error(self):
        """
        Handle a read error (stderr) notification.
        """
        # Local variables optimization
        worker = self.worker
        task = worker.task
        key = self.key
        node_errline = worker._on_node_errline
        debug = task.info("debug", False)
        if debug:
            print_debug = task.info("print_debug")
        for msg in self._readerrlines():
            if debug:
                print_debug(task, "%s@STDERR: %s" % (key, msg))
            node_errline(key, msg)  # handle full stderr line



class Rcp(Rsh):
    """
    Rcp EngineClient.
    """

    def __init__(self, node, source, dest, worker, stderr, timeout, preserve,
        reverse):
        """
        Initialize Rcp instance.
        """
        Rsh.__init__(self, node, None, worker, stderr, timeout)
        self.source = source
        self.dest = dest
        self.popen = None

        # Preserve modification times and modes?
        self.preserve = preserve

        # Reverse copy?
        self.reverse = reverse

        # Directory?
        if self.reverse:
            self.isdir = os.path.isdir(self.dest)
            if not self.isdir:
                raise ValueError("reverse copy dest must be a directory")
        else:
            self.isdir = os.path.isdir(self.source)

        # FIXME: file sanity checks could be moved to Rcp._start() as we
        # should now be able to handle error when starting (#215).

    def _build_cmd(self):
        """
        Build the shell command line to start the rcp commmand.
        Return an array of command and arguments.
        """

        # Does not support 'connect_timeout'
        task = self.worker.task
        path = task.info("rcp_path") or "rcp"
        user = task.info("rsh_user")
        options = [ task.info("rsh_options"), task.info("rcp_options") ]

        cmd_l = [ path ]

        if self.isdir:
            cmd_l.append("-r")

        if self.preserve:
            cmd_l.append("-p")


        # Add custom rcp options
        for opts in options:
            if opts:
                cmd_l += opts.split()

        if self.reverse:
            if user:
                cmd_l.append("%s@%s:%s" % (user, self.key, self.source))
            else:
                cmd_l.append("%s:%s" % (self.key, self.source))

            cmd_l.append(os.path.join(self.dest, "%s.%s" % \
                         (os.path.basename(self.source), self.key)))
        else:
            cmd_l.append(self.source)
            if user:
                cmd_l.append("%s@%s:%s" % (user, self.key, self.dest))
            else:
                cmd_l.append("%s:%s" % (self.key, self.dest))

        return cmd_l


class WorkerRsh(DistantWorker):
    """
    ClusterShell rsh-based worker Class.

    Remote Shell (rsh) usage example:
       >>> worker = WorkerRsh(nodeset, handler=MyEventHandler(),
       ...                    timeout=30, command="/bin/hostname")
       >>> task.schedule(worker)      # schedule worker for execution
       >>> task.resume()              # run

    Remote Copy (rcp) usage example:
       >>> worker = WorkerRsh(nodeset, handler=MyEventHandler(),
       ...                     source="/etc/my.conf",
       ...                     dest="/etc/my.conf")
       >>> task.schedule(worker)      # schedule worker for execution
       >>> task.resume()              # run

    connect_timeout option is ignored by this worker.
    """

    SHELL_CLASS = Rsh
    COPY_CLASS = Rcp

    def __init__(self, nodes, handler, timeout, **kwargs):
        """
        Initialize Rsh worker instance.
        """
        DistantWorker.__init__(self, handler)

        self.clients = []
        self.nodes = NodeSet(nodes)
        self.command = kwargs.get('command')
        self.source = kwargs.get('source')
        self.dest = kwargs.get('dest')
        autoclose = kwargs.get('autoclose', False)
        stderr = kwargs.get('stderr', False)
        self._close_count = 0
        self._has_timeout = False

        # Prepare underlying engine clients (mrsh/mrcp processes)
        if self.command is not None:
            # secure remote shell
            for node in self.nodes:
                rsh = self.__class__.SHELL_CLASS
                self.clients.append(rsh(node, self.command, self, stderr,
                                       timeout, autoclose))
        elif self.source:
            # secure copy
            for node in self.nodes:
                rcp = self.__class__.COPY_CLASS
                self.clients.append(rcp(node, self.source, self.dest,
                    self, stderr, timeout, kwargs.get('preserve', False),
                    kwargs.get('reverse', False)))
        else:
            raise ValueError("missing command or source parameter in " \
			     "WorkerRsh constructor")

    def _engine_clients(self):
        """
        Access underlying engine clients.
        """
        return self.clients

    def _on_node_rc(self, node, rc):
        DistantWorker._on_node_rc(self, node, rc)
        self._close_count += 1

    def _on_node_timeout(self, node):
        DistantWorker._on_node_timeout(self, node)
        self._close_count += 1
        self._has_timeout = True

    def _check_fini(self):
        if self._close_count >= len(self.clients):
            handler = self.eh
            if handler:
                if self._has_timeout:
                    handler.ev_timeout(self)
                handler.ev_close(self)

    def write(self, buf):
        """
        Write to worker clients.
        """
        for client in self.clients:
            client._write(buf)

    def set_write_eof(self):
        """
        Tell worker to close its writer file descriptor once flushed. Do not
        perform writes after this call.
        """
        for client in self.clients:
            client._set_write_eof()

    def abort(self):
        """
        Abort processing any action by this worker.
        """
        for client in self.clients:
            client.abort()

WORKER_CLASS=WorkerRsh
