#
# Copyright (C) 2007-2016 CEA/DAM
# Copyright (C) 2015-2018 Stephane Thiell <sthiell@stanford.edu>
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
Execute cluster commands in parallel

clush is an utility program to run commands on a cluster which benefits
from the ClusterShell library and its Ssh worker. It features an
integrated output results gathering system (dshbak-like), can get node
groups by running predefined external commands and can redirect lines
read on its standard input to the remote commands.

When no command are specified, clush runs interactively.

"""

from __future__ import print_function
from inspect import trace

import boto3
import configparser
import datetime
import json
import opconf
import logging
import optparse
import os
from os.path import abspath, dirname, exists, isdir, join
import random
import resource
import socket
import signal
import subprocess
import sys
import time
import threading
from urllib import response

# Python 3 compatibility
try:
    raw_input
except NameError:
    raw_input = input

from ClusterShell.Defaults import DEFAULTS, _load_workerclass
from ClusterShell.CLI.Config import ClushConfig, ClushConfigError
from ClusterShell.CLI.Display import Display, sys_stdin
from ClusterShell.CLI.Display import VERB_QUIET, VERB_STD, VERB_VERB, VERB_DEBUG
from ClusterShell.CLI.OptionParser import OptionParser
from ClusterShell.CLI.Error import GENERIC_ERRORS, handle_generic_error
from ClusterShell.CLI.Utils import bufnodeset_cmpkey, human_bi_bytes_unit

from ClusterShell.Event import EventHandler
from ClusterShell.MsgTree import MsgTree
from ClusterShell.NodeSet import RESOLVER_NOGROUP, set_std_group_resolver_config
from ClusterShell.NodeSet import NodeSet, NodeSetParseError, std_group_resolver
from ClusterShell.NodeSet import expand
from ClusterShell.Task import Task, task_self


class UpdatePromptException(Exception):
    """Exception used by the signal handler"""


class StdInputHandler(EventHandler):
    """Standard input event handler class."""

    def __init__(self, worker):
        EventHandler.__init__(self)
        self.master_worker = worker

    def ev_msg(self, port, msg):
        """invoked when a message is received from port object"""
        if not msg:
            self.master_worker.set_write_eof()
            return
        # Forward messages to master worker
        self.master_worker.write(msg)


class OutputHandler(EventHandler):
    """Base class for generic output handlers."""

    def __init__(self, prog=None):
        EventHandler.__init__(self)
        self._runtimer = None
        self._prog = prog if prog else os.path.basename(sys.argv[0])

    def runtimer_init(self, task, ntotal=0):
        """Init timer for live command-completed progressmeter."""
        thandler = RunTimer(task, ntotal, prog=self._prog)
        self._runtimer = task.timer(1.33, thandler, interval=1.0 / 3.0, autoclose=True)

    def _runtimer_clean(self):
        """Hide runtimer counter"""
        if self._runtimer:
            self._runtimer.eh.erase_line()

    def _runtimer_set_dirty(self):
        """Force redisplay of counter"""
        if self._runtimer:
            self._runtimer.eh.set_dirty()

    def _runtimer_finalize(self, worker):
        """Finalize display of runtimer counter"""
        if self._runtimer:
            self._runtimer.eh.finalize(worker.task.default("USER_interactive"))
            self._runtimer.invalidate()
            self._runtimer = None

    def update_prompt(self, worker):
        """
        If needed, notify main thread to update its prompt by sending
        a SIGUSR1 signal. We use task-specific user-defined variable
        to record current states (prefixed by USER_).
        """
        worker.task.set_default("USER_running", False)
        if worker.task.default("USER_handle_SIGUSR1"):
            os.kill(os.getpid(), signal.SIGUSR1)

    def ev_start(self, worker):
        """Worker is starting."""
        if self._runtimer:
            self._runtimer.eh.start_time = time.time()

    def ev_written(self, worker, node, sname, size):
        """Bytes written on worker"""
        if self._runtimer:
            self._runtimer.eh.bytes_written += size


class DirectOutputHandler(OutputHandler):
    """Direct output event handler class."""

    def __init__(self, display, prog=None):
        OutputHandler.__init__(self, prog=prog)
        self._display = display

    def ev_read(self, worker, node, sname, msg):
        if sname == worker.SNAME_STDOUT:
            self._display.print_line(node, msg)
        elif sname == worker.SNAME_STDERR:
            self._display.print_line_error(node, msg)

    def ev_hup(self, worker, node, rc):
        if rc > 0:
            verb = VERB_QUIET
            if self._display.maxrc:
                verb = VERB_STD
            self._display.vprint_err(
                verb, "%s: %s: exited with exit code %d" % (self._prog, node, rc)
            )

    def ev_close(self, worker, timedout):
        if timedout:
            nodeset = NodeSet._fromlist1(worker.iter_keys_timeout())
            self._display.vprint_err(
                VERB_QUIET, "%s: %s: command timeout" % (self._prog, nodeset)
            )
        self.update_prompt(worker)


class DirectOutputDirHandler(DirectOutputHandler):
    """Direct output files event handler class. pssh style"""

    def __init__(self, display, ns, prog=None):
        DirectOutputHandler.__init__(self, display, prog)
        self._ns = ns
        self._outfiles = {}
        self._errfiles = {}
        self.display = display
        if display.outdir:
            for n in self._ns:
                # wipe output file
                open(join(display.outdir, n), mode="w").close()
                self._outfiles[n] = join(display.outdir, n)
        if display.errdir:
            for n in self._ns:
                # wipe err file
                open(join(display.errdir, n), mode="w").close()
                self._errfiles[n] = join(display.errdir, n)

    def ev_read(self, worker, node, sname, msg):
        DirectOutputHandler.ev_read(self, worker, node, sname, msg)
        if sname == worker.SNAME_STDOUT:
            if self._display.outdir:
                file_name = self._outfiles[node]
                with open(file_name, mode="a") as f:
                    f.write("{}\n".format(msg.decode()))

        elif sname == worker.SNAME_STDERR:
            if self._display.errdir:
                file_name = self._errfiles[node]
                with open(file_name, mode="a") as f:
                    f.write("{}\n".format(msg.decode()))

    def ev_close(self, worker, timedout):
        DirectOutputHandler.ev_close(self, worker, timedout)


class DirectProgressOutputHandler(DirectOutputHandler):
    """Direct output event handler class with progress support."""

    # NOTE: This class is very similar to DirectOutputHandler, thus it could
    #       first look overkill, but merging both is slightly impacting ev_read
    #       performance of current DirectOutputHandler.

    def ev_read(self, worker, node, sname, msg):
        self._runtimer_clean()
        # it is ~10% faster to avoid calling super here
        if sname == worker.SNAME_STDOUT:
            self._display.print_line(node, msg)
        elif sname == worker.SNAME_STDERR:
            self._display.print_line_error(node, msg)

    def ev_close(self, worker, timedout):
        self._runtimer_clean()
        DirectOutputHandler.ev_close(self, worker, timedout)


class CopyOutputHandler(DirectProgressOutputHandler):
    """Copy output event handler."""

    def __init__(self, display, reverse=False, prog=None):
        DirectOutputHandler.__init__(self, display, prog=prog)
        self.reverse = reverse

    def ev_close(self, worker, timedout):
        """A copy worker has finished."""
        for rc, nodes in worker.iter_retcodes():
            if rc == 0:
                if self.reverse:
                    self._display.vprint(
                        VERB_VERB,
                        "%s:`%s' -> `%s'" % (nodes, worker.source, worker.dest),
                    )
                else:
                    self._display.vprint(
                        VERB_VERB,
                        "`%s' -> %s:`%s'" % (worker.source, nodes, worker.dest),
                    )
                break
        # multiple copy workers may be running (handled by this task's thread)
        copies = worker.task.default("USER_copies") - 1
        worker.task.set_default("USER_copies", copies)
        if copies == 0:
            self._runtimer_finalize(worker)
            # handle timeout
            DirectOutputHandler.ev_close(self, worker, timedout)


class GatherOutputHandler(OutputHandler):
    """Gathered output event handler class (e.g. clush -b)."""

    def __init__(self, display, prog=None):
        OutputHandler.__init__(self, prog=prog)
        self._display = display

    def ev_read(self, worker, node, sname, msg):
        if sname == worker.SNAME_STDOUT:
            if self._display.verbosity == VERB_VERB:
                self._display.print_line(node, worker.current_msg)
        elif sname == worker.SNAME_STDERR:
            self._runtimer_clean()
            self._display.print_line_error(node, msg)
            self._runtimer_set_dirty()

    def ev_close(self, worker, timedout):
        # Worker is closing -- it's time to gather results...
        self._runtimer_finalize(worker)
        # Display command output, try to order buffers by rc
        nodesetify = lambda v: (v[0], NodeSet._fromlist1(v[1]))
        cleaned = False
        for _rc, nodelist in sorted(worker.iter_retcodes()):
            ns_remain = NodeSet._fromlist1(nodelist)
            # Then order by node/nodeset (see nodeset_cmpkey)
            for buf, nodeset in sorted(
                map(nodesetify, worker.iter_buffers(nodelist)), key=bufnodeset_cmpkey
            ):
                if not cleaned:
                    # clean runtimer line before printing first result
                    self._runtimer_clean()
                    cleaned = True
                self._display.print_gather(nodeset, buf)
                ns_remain.difference_update(nodeset)
            if ns_remain:
                self._display.print_gather_finalize(ns_remain)
        self._display.flush()

        self._close_common(worker)

        # Notify main thread to update its prompt
        self.update_prompt(worker)

    def _close_common(self, worker):
        verbexit = VERB_QUIET
        if self._display.maxrc:
            verbexit = VERB_STD
        # Display return code if not ok ( != 0)
        for rc, nodelist in worker.iter_retcodes():
            if rc != 0:
                nsdisp = ns = NodeSet._fromlist1(nodelist)
                if self._display.verbosity > VERB_QUIET and len(ns) > 1:
                    nsdisp = "%s (%d)" % (ns, len(ns))
                msgrc = "%s: %s: exited with exit code %d" % (self._prog, nsdisp, rc)
                self._display.vprint_err(verbexit, msgrc)

        # Display nodes that didn't answer within command timeout delay
        if worker.num_timeout() > 0:
            self._display.vprint_err(
                verbexit,
                "%s: %s: command timeout"
                % (self._prog, NodeSet._fromlist1(worker.iter_keys_timeout())),
            )


class SortedOutputHandler(GatherOutputHandler):
    """Sorted by node output event handler class (e.g. clush -L)."""

    def ev_close(self, worker, timedout):
        # Overrides GatherOutputHandler.ev_close()
        self._runtimer_finalize(worker)

        # Display command output, try to order buffers by rc
        for _rc, nodelist in sorted(worker.iter_retcodes()):
            for node in nodelist:
                # NOTE: msg should be a MsgTreeElem as Display will iterate
                # over it to display multiple lines. As worker.node_buffer()
                # returns either a string or None if there is no output, it
                # cannot be used here. We use worker.iter_node_buffers() with
                # a single node as match_keys instead.
                for node, msg in worker.iter_node_buffers(match_keys=(node,)):
                    self._display.print_gather(node, msg)

        self._close_common(worker)

        # Notify main thread to update its prompt
        self.update_prompt(worker)


class LiveGatherOutputHandler(GatherOutputHandler):
    """Live line-gathered output event handler class (-bL)."""

    def __init__(self, display, nodes, prog=None):
        assert nodes is not None, "cannot gather local command"
        GatherOutputHandler.__init__(self, display, prog=prog)
        self._nodes = NodeSet(nodes)
        self._nodecnt = dict.fromkeys(self._nodes, 0)
        self._mtreeq = []
        self._offload = 0

    def ev_read(self, worker, node, sname, msg):
        if sname != worker.SNAME_STDOUT:
            GatherOutputHandler.ev_read(self, worker, node, sname, msg)
            return
        # Read new line from node
        self._nodecnt[node] += 1
        cnt = self._nodecnt[node]
        if len(self._mtreeq) < cnt:
            self._mtreeq.append(MsgTree())
        self._mtreeq[cnt - self._offload - 1].add(node, msg)
        self._live_line(worker)

    def ev_hup(self, worker, node, rc):
        if self._mtreeq and node not in self._mtreeq[0]:
            # forget a node that doesn't answer to continue live line
            # gathering anyway
            self._nodes.remove(node)
            self._live_line(worker)

    def _live_line(self, worker):
        # if all nodes have replied, display gathered line
        while self._mtreeq and len(self._mtreeq[0]) == len(self._nodes):
            mtree = self._mtreeq.pop(0)
            self._offload += 1
            self._runtimer_clean()
            nodesetify = lambda v: (v[0], NodeSet.fromlist(v[1]))
            for buf, nodeset in sorted(
                map(nodesetify, mtree.walk()), key=bufnodeset_cmpkey
            ):
                self._display.print_gather(nodeset, buf)
            self._runtimer_set_dirty()

    def ev_close(self, worker, timedout):
        # Worker is closing -- it's time to gather results...
        self._runtimer_finalize(worker)

        for mtree in self._mtreeq:
            nodesetify = lambda v: (v[0], NodeSet.fromlist(v[1]))
            for buf, nodeset in sorted(
                map(nodesetify, mtree.walk()), key=bufnodeset_cmpkey
            ):
                self._display.print_gather(nodeset, buf)

        self._close_common(worker)

        # Notify main thread to update its prompt
        self.update_prompt(worker)


class RunTimer(EventHandler):
    """Running progress timer event handler"""

    def __init__(self, task, total, prog=None):
        EventHandler.__init__(self)
        self.task = task
        self.total = total
        self.cnt_last = -1
        self.tslen = len(str(self.total))
        self.wholelen = 0
        self.started = False
        # updated by worker handler for progress
        self.start_time = 0
        self.bytes_written = 0
        self._prog = prog if prog else os.path.basename(sys.argv[0])

    def ev_timer(self, timer):
        self.update()

    def set_dirty(self):
        self.cnt_last = -1

    def erase_line(self):
        if self.wholelen:
            sys.stderr.write(" " * self.wholelen + "\r")
            self.wholelen = 0

    def update(self):
        """Update runtime progress info"""
        wrbwinfo = ""
        if self.bytes_written > 0:
            bandwidth = self.bytes_written / (time.time() - self.start_time)
            wrbwinfo = " write: %s/s" % human_bi_bytes_unit(bandwidth)

        gwcnt = len(self.task.gateways)
        if gwcnt:
            # tree mode
            act_targets = NodeSet()
            for gw, (chan, metaworkers) in self.task.gateways.items():
                act_targets.updaten(mw.gwtargets[gw] for mw in metaworkers)
            cnt = len(act_targets) + len(self.task._engine.clients()) - gwcnt
            gwinfo = " gw %d" % gwcnt
        else:
            cnt = len(self.task._engine.clients())
            gwinfo = ""
        if self.bytes_written > 0 or cnt != self.cnt_last:
            self.cnt_last = cnt
            # display completed/total clients
            towrite = "%s: %*d/%*d%s%s\r" % (
                self._prog,
                self.tslen,
                self.total - cnt,
                self.tslen,
                self.total,
                gwinfo,
                wrbwinfo,
            )
            self.wholelen = len(towrite)
            sys.stderr.write(towrite)
            self.started = True

    def finalize(self, force_cr):
        """finalize display of runtimer"""
        if not self.started:
            return
        self.erase_line()
        # display completed/total clients
        fmt = "%s: %*d/%*d"
        if force_cr:
            fmt += "\n"
        else:
            fmt += "\r"
        sys.stderr.write(
            fmt % (self._prog, self.tslen, self.total, self.tslen, self.total)
        )


class HeliumClient:
    def __init__(self, env):
        self.opus = __import__("opus")
        self.botocore = __import__("botocore.utils")

        api = "https://helium.{}.openpath.com".format(env)

        self.region = (
            self.botocore.utils.InstanceMetadataRegionFetcher().retrieve_region()
        )
        self.boto_client = boto3.client("ssm", region_name=self.region)
        try:
            nebula_username = self.ssm_get_parameter("nebula.helium.username")
            nebula_password = self.ssm_get_parameter("nebula.helium.password")
            nebula_namespaceId = self.ssm_get_parameter("nebula.helium.namespaceId")
        except Exception:
            logging.critical("You are not authorized to login to Helium.")
            sys.exit(1)

        self.client = self.opus.Helium(
            api,
            username=nebula_username,
            password=nebula_password,
            namespaceId=nebula_namespaceId,
        )

    def ssm_get_parameter(self, parameter):
        """Get a parameter from AWS"""

        result = self.boto_client.get_parameter(Name=parameter, WithDecryption=True)

        parameter = result.get("Parameter")

        if parameter:
            return parameter.get("Value")

        return None

    def format_nodes(self, nodes):
        new_node_list = []
        errors = {}
        tmp_list = []
        for node in nodes:
            if "openpath.local" not in node:
                if node.isnumeric():
                    tmp_list.append(node)
                else:
                    node = node[3 : len(node)]
                    tmp_list.append(node)
            else:
                new_node_list.append(node)
        filter_string = ",".join(tmp_list)
        results = self.client.listAcus(filter=f"id:({filter_string})")
        for result in results:
            tmp_list.remove(str(result.get("id")))
            new_node_list.append(result.get("hostname"))
        if len(tmp_list) > 0:
            for node in tmp_list:
                errors[node] = f"Cannot find ACU in Helium."
                new_node_list.append(node)

        return new_node_list, errors


def signal_handler(signum, frame):
    """Signal handler used for main thread notification"""
    if signum == signal.SIGUSR1:
        signal.signal(signal.SIGUSR1, signal.SIG_IGN)
        raise UpdatePromptException()


def get_history_file():
    """Turn the history file path"""
    return join(os.environ["HOME"], ".clush_history")


def readline_setup():
    """
    Configure readline to automatically load and save a history file
    named .clush_history
    """
    import readline

    readline.parse_and_bind("tab: complete")
    readline.set_completer_delims("")
    try:
        readline.read_history_file(get_history_file())
    except IOError:
        pass


def ttyloop(task, nodeset, timeout, display, remote, trytree, is_nebula, environment):
    """Manage the interactive prompt to run command"""
    readline_avail = False
    interactive = task.default("USER_interactive")
    if interactive:
        try:
            import readline

            readline_setup()
            readline_avail = True
        except ImportError:
            pass
        display.vprint(VERB_STD, "Enter 'quit' to leave this interactive mode")

    rc = 0
    ns = NodeSet(nodeset)
    ns_info = True
    cmd = ""
    while task.default("USER_running") or (interactive and cmd.lower() != "quit"):
        try:
            # Set SIGUSR1 handler if needed
            if task.default("USER_handle_SIGUSR1"):
                signal.signal(signal.SIGUSR1, signal_handler)

            if task.default("USER_interactive") and not task.default("USER_running"):
                if ns_info:
                    display.vprint(VERB_QUIET, "Working with nodes: %s" % ns)
                    ns_info = False
                prompt = "clush> "
            else:
                prompt = ""
            try:
                cmd = raw_input(prompt)
                assert cmd is not None, "Result of raw_input() is None!"
            finally:
                signal.signal(signal.SIGUSR1, signal.SIG_IGN)
        except EOFError:
            print()
            return
        except UpdatePromptException:
            if task.default("USER_interactive"):
                continue
            return
        except KeyboardInterrupt as kbe:
            # Caught SIGINT here (main thread) but the signal will also reach
            # subprocesses (that will most likely kill them)
            if display.gather:
                # Suspend task, so we can safely access its data from here
                task.suspend()

                # If USER_running is not set, the task had time to finish,
                # that could mean all subprocesses have been killed and all
                # handlers have been processed.
                if not task.default("USER_running"):
                    # let's clush_excepthook handle the rest
                    raise kbe

                # If USER_running is set, the task didn't have time to finish
                # its work, so we must print something for the user...
                print_warn = False

                # Display command output, but cannot order buffers by rc
                nodesetify = lambda v: (v[0], NodeSet._fromlist1(v[1]))
                for buf, nodeset in sorted(
                    map(nodesetify, task.iter_buffers()), key=bufnodeset_cmpkey
                ):
                    if not print_warn:
                        print_warn = True
                        display.vprint_err(
                            VERB_STD, "Warning: Caught keyboard interrupt!"
                        )
                    display.print_gather(nodeset, buf)

                # Return code handling
                verbexit = VERB_QUIET
                if display.maxrc:
                    verbexit = VERB_STD
                ns_ok = NodeSet()
                for rc, nodelist in task.iter_retcodes():
                    ns_ok.add(NodeSet._fromlist1(nodelist))
                    if rc != 0:
                        # Display return code if not ok ( != 0)
                        nsdisp = ns = NodeSet._fromlist1(nodelist)
                        if display.verbosity >= VERB_QUIET and len(ns) > 1:
                            nsdisp = "%s (%d)" % (ns, len(ns))
                        msgrc = "clush: %s: exited with exit code %d" % (nsdisp, rc)
                        display.vprint_err(verbexit, msgrc)

                # Add uncompleted nodeset to exception object
                kbe.uncompleted_nodes = ns - ns_ok

                # Display nodes that didn't answer within command timeout delay
                if task.num_timeout() > 0:
                    display.vprint_err(
                        verbexit,
                        "clush: %s: command timeout"
                        % NodeSet._fromlist1(task.iter_keys_timeout()),
                    )
            raise kbe

        if task.default("USER_running"):
            ns_reg, ns_unreg = NodeSet(), NodeSet()
            for client in task._engine.clients():
                if client.registered:
                    ns_reg.add(client.key)
                else:
                    ns_unreg.add(client.key)
            if ns_unreg:
                pending = "\nclush: pending(%d): %s" % (len(ns_unreg), ns_unreg)
            else:
                pending = ""
            display.vprint_err(VERB_QUIET, "clush: interrupt (^C to abort task)")
            gws = list(task.gateways)
            if not gws:
                display.vprint_err(
                    VERB_QUIET,
                    "clush: in progress(%d): %s%s" % (len(ns_reg), ns_reg, pending),
                )
            else:
                display.vprint_err(
                    VERB_QUIET,
                    "clush: in progress(%d): %s%s\n"
                    "clush: [tree] open gateways(%d): %s"
                    % (len(ns_reg), ns_reg, pending, len(gws), NodeSet._fromlist1(gws)),
                )
            for gw, (chan, metaworkers) in task.gateways.items():
                act_targets = NodeSet.fromlist(mw.gwtargets[gw] for mw in metaworkers)
                if act_targets:
                    display.vprint_err(
                        VERB_QUIET,
                        "clush: [tree] in progress(%d) on %s: %s"
                        % (len(act_targets), gw, act_targets),
                    )
        else:
            cmdl = cmd.lower()
            try:
                ns_info = True
                if cmdl.startswith("+"):
                    ns.update(cmdl[1:])
                elif cmdl.startswith("-"):
                    ns.difference_update(cmdl[1:])
                elif cmdl.startswith("@"):
                    ns = NodeSet(cmdl[1:])
                elif cmdl == "=":
                    display.gather = not display.gather
                    if display.gather:
                        display.vprint(VERB_STD, "Switching to gathered output format")
                    else:
                        display.vprint(VERB_STD, "Switching to standard output format")
                    task.set_default(
                        "stdout_msgtree", display.gather or display.line_mode
                    )
                    ns_info = False
                    continue
                elif not cmdl.startswith("?"):  # if ?, just print ns_info
                    ns_info = False
            except NodeSetParseError:
                display.vprint_err(VERB_QUIET, "clush: nodeset parse error (ignoring)")

            if ns_info:
                continue

            if cmdl.startswith("!") and len(cmd.strip()) > 0:
                run_command(task, cmd[1:], None, timeout, display, remote, trytree)
            elif cmdl != "quit":
                if not cmd:
                    continue
                if readline_avail:
                    readline.write_history_file(get_history_file())
                run_command(
                    task,
                    cmd,
                    ns,
                    timeout,
                    display,
                    remote,
                    trytree,
                    is_nebula=is_nebula,
                    environment=environment,
                )
    return rc


def _stdin_thread_start(stdin_port, display):
    """Standard input reader thread entry point."""
    try:
        # Note: read length should be as large as possible for performance
        # yet not too large to not introduce artificial latency.
        # 64k seems to be perfect with an openssh backend (they issue 64k
        # reads) ; could consider making it an option for e.g. gsissh.
        bufsize = 64 * 1024
        # thread loop: read stdin + send messages to specified port object
        # use os.read() to work around https://bugs.python.org/issue42717
        while True:
            buf = os.read(sys_stdin().fileno(), bufsize)
            if not buf:
                break
            # send message to specified port object (with ack)
            stdin_port.msg(buf)
    except IOError as ex:
        display.vprint(VERB_VERB, "stdin: %s" % ex)
    # send a None message to indicate EOF
    stdin_port.msg(None)


def bind_stdin(worker, display):
    """Create a stdin->port->worker binding: connect specified worker
    to stdin with the help of a reader thread and a ClusterShell Port
    object."""
    assert sys.stdin is not None and not sys.stdin.isatty()
    # Create a ClusterShell Port object bound to worker's task. This object
    # is able to receive messages in a thread-safe manner and then will safely
    # trigger ev_msg() on a specified event handler.
    port = worker.task.port(handler=StdInputHandler(worker), autoclose=True)
    # Launch a dedicated thread to read stdin in blocking mode. Indeed stdin
    # can be a file, so we cannot use a WorkerSimple here as polling on file
    # may result in different behaviors depending on selected engine.
    stdin_thread = threading.Thread(None, _stdin_thread_start, args=(port, display))
    # setDaemon because we're sometimes left with data that has been read and
    # ssh connection already closed.
    # Syntax for compat with Python < 2.6
    stdin_thread.setDaemon(True)
    stdin_thread.start()


def run_command(
    task,
    cmd,
    ns,
    timeout,
    display,
    remote,
    trytree,
    publish=None,
    requestId=None,
    errors={},
    is_nebula=None,
    environment=None,
):
    """
    For SSH command:
        Create and run the specified command line, displaying
        results in a dshbak way when gathering is used.
    For MQTT Command:
        Create and publish the specified command line to each
        ACU's command topic.
    """
    task.set_default("USER_running", True)

    if (display.gather or display.line_mode) and ns is not None:
        if display.gather and display.line_mode:
            handler = LiveGatherOutputHandler(display, ns)
        elif not display.gather and display.line_mode:
            handler = SortedOutputHandler(display)
        else:
            handler = GatherOutputHandler(display)

        if display.verbosity in (VERB_STD, VERB_VERB) or (
            display.progress and display.verbosity > VERB_QUIET
        ):
            handler.runtimer_init(task, len(ns))
    elif display.progress and display.verbosity > VERB_QUIET:
        handler = DirectProgressOutputHandler(display)
        handler.runtimer_init(task, len(ns))
    elif (display.outdir or display.errdir) and ns is not None:
        if display.outdir and not exists(display.outdir):
            os.makedirs(display.outdir)
        if display.errdir and not exists(display.errdir):
            os.makedirs(display.errdir)
        handler = DirectOutputDirHandler(display, ns)
    else:
        # this is the simpler but faster output handler
        handler = DirectOutputHandler(display)
    if publish is not None:
        worker = task.mqtt_pub(
            cmd,
            nodes=ns,
            handler=handler,
            timeout=timeout,
            remote=remote,
            tree=trytree,
            errors=errors,
            requestId=requestId,
        )
        task.resume()
    else:
        worker = task.shell(
            cmd,
            nodes=ns,
            handler=handler,
            timeout=timeout,
            remote=remote,
            tree=trytree,
            is_nebula=is_nebula,
            environment=environment,
        )
        if ns is None:
            worker.set_key("LOCAL")
        if task.default("USER_stdin_worker"):
            bind_stdin(worker, display)
        task.resume()


def fetch_output_from_s3(
    task, requestId, ns, timeout, display, remote, trytree, environment=None, errors={}
):
    """
    Fetch the output of a request from S3
    """
    task.set_default("USER_running", True)

    if (display.gather or display.line_mode) and ns is not None:
        if display.gather and display.line_mode:
            handler = LiveGatherOutputHandler(display, ns)
        elif not display.gather and display.line_mode:
            handler = SortedOutputHandler(display)
        else:
            handler = GatherOutputHandler(display)

        if display.verbosity in (VERB_STD, VERB_VERB) or (
            display.progress and display.verbosity > VERB_QUIET
        ):
            handler.runtimer_init(task, len(ns))
    elif display.progress and display.verbosity > VERB_QUIET:
        handler = DirectProgressOutputHandler(display)
        handler.runtimer_init(task, len(ns))
    elif (display.outdir or display.errdir) and ns is not None:
        if display.outdir and not exists(display.outdir):
            os.makedirs(display.outdir)
        if display.errdir and not exists(display.errdir):
            os.makedirs(display.errdir)
        handler = DirectOutputDirHandler(display, ns)
    else:
        # this is the simpler but faster output handler
        handler = DirectOutputHandler(display)
    worker = task.s3_fetch(
        requestId,
        nodes=ns,
        handler=handler,
        display=display,
        timeout=timeout,
        remote=remote,
        tree=trytree,
        environment=environment,
        errors=errors,
    )
    task.resume()


def run_copy(task, sources, dest, ns, timeout, preserve_flag, display):
    """run copy command"""
    task.set_default("USER_running", True)
    task.set_default("USER_copies", len(sources))

    copyhandler = CopyOutputHandler(display)
    if display.verbosity in (VERB_STD, VERB_VERB):
        copyhandler.runtimer_init(task, len(ns) * len(sources))

    # Sources check
    for source in sources:
        if not exists(source):
            display.vprint_err(VERB_QUIET, 'ERROR: file "%s" not found' % source)
            clush_exit(1, task)
        task.copy(
            source,
            dest,
            ns,
            handler=copyhandler,
            timeout=timeout,
            preserve=preserve_flag,
        )
    task.resume()


def run_rcopy(task, sources, dest, ns, timeout, preserve_flag, display):
    """run reverse copy command"""
    task.set_default("USER_running", True)
    task.set_default("USER_copies", len(sources))

    # Sanity checks
    if not exists(dest):
        display.vprint_err(VERB_QUIET, 'ERROR: directory "%s" not found' % dest)
        clush_exit(1, task)
    if not isdir(dest):
        display.vprint_err(
            VERB_QUIET, 'ERROR: destination "%s" is not a directory' % dest
        )
        clush_exit(1, task)

    copyhandler = CopyOutputHandler(display, True)
    if display.verbosity == VERB_STD or display.verbosity == VERB_VERB:
        copyhandler.runtimer_init(task, len(ns) * len(sources))
    for source in sources:
        task.rcopy(
            source,
            dest,
            ns,
            handler=copyhandler,
            timeout=timeout,
            stderr=True,
            preserve=preserve_flag,
        )
    task.resume()


def set_fdlimit(fd_max, display):
    """Make open file descriptors soft limit the max."""
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    if hard < fd_max:
        msgfmt = "Warning: fd_max set to %d but max open files hard limit is %d"
        display.vprint_err(VERB_VERB, msgfmt % (fd_max, hard))
    rlim_max = max(hard, fd_max)
    if soft != rlim_max:
        msgfmt = "Changing max open files soft limit from %d to %d"
        display.vprint(VERB_DEBUG, msgfmt % (soft, rlim_max))
        try:
            resource.setrlimit(resource.RLIMIT_NOFILE, (rlim_max, hard))
        except (ValueError, resource.error) as exc:
            # Most probably the requested limit exceeds the system imposed limit
            msgfmt = "Warning: Failed to set max open files limit to %d (%s)"
            display.vprint_err(VERB_VERB, msgfmt % (rlim_max, exc))


def clush_exit(status, task=None):
    """Exit script, flushing stdio buffers and stopping ClusterShell task."""
    if task:
        # Clean, usual termination
        task.abort()
        task.join()
        sys.exit(status)
    else:
        # Best effort cleanup if no task is set
        for stream in [sys.stdout, sys.stderr]:
            try:
                stream.flush()
            except IOError:
                pass
        # Use os._exit to avoid threads cleanup
        os._exit(status)


def clush_excepthook(extype, exp, traceback):
    """Exceptions hook for clush: this method centralizes exception
    handling from main thread and from (possible) separate task thread.
    This hook has to be previously installed on startup by overriding
    sys.excepthook and task.excepthook."""

    try:
        raise exp
    except ClushConfigError as econf:
        print("ERROR: %s" % econf, file=sys.stderr)
        clush_exit(1)
    except KeyboardInterrupt as kbe:
        uncomp_nodes = getattr(kbe, "uncompleted_nodes", None)
        if uncomp_nodes:
            print(
                "Keyboard interrupt (%s did not complete)." % uncomp_nodes,
                file=sys.stderr,
            )
        else:
            print("Keyboard interrupt.", file=sys.stderr)
        clush_exit(128 + signal.SIGINT)
    except GENERIC_ERRORS as exc:
        clush_exit(handle_generic_error(exc))

    # Error not handled
    task_self().default_excepthook(extype, exp, traceback)


def format_nodes(nodes):
    new_node_list = []
    errors = {}
    for node in nodes:
        if node.isnumeric():
            node = "acu" + str(node)
        if "openpath.local" not in node:
            try:
                node = socket.gethostbyname_ex(node)[0]
            except Exception as e:
                errors[node] = str(e)
            finally:
                new_node_list.append(node)
        else:
            new_node_list.append(node)
    return new_node_list, errors


def load_profile_credentials_static(op_env: str) -> None:
    """
    Calls `aws --profile={env} sts get-caller-identity` to trigger a credential cache update, then searches the
    ~/.aws/cli/cache folder for the matching cache entry. This was separated from load_profile_credentials to allow
    other scripts to setup the credentials without having to instantiate a full EpoxyGlueOperation object.

    With set_env true, this function will set the appropriate AWS env credential variables.

    Returns None on failure; success returns { AwsAccountId: '...', AwsCliCacheFile: aws_cli_cache_file,
    AccessKeyId: '...', SecretAccessKey: '...', SessionToken: '...'].

    """
    # 2021-03-31: updated this code to no longer assume
    # deterministic cache filenames (due to botocore changes);
    # instead, following the suggestion at
    # https://github.com/aws/aws-sdk-js/issues/1543#issuecomment-353574613
    # we scan the whole ~/.aws/cli/cache directory looking for
    # matching non-expired cache files

    # 2020-10-05: Now that pull request
    # https://github.com/boto/botocore/pull/1157 has merged, we can update
    # this code. However, we cannot update simply to the sample code
    # provided at the end of the pull request thread. That code assumes
    # AWS_PROFILE env var is set prior to the import completions. We often
    # set AWS_PROFILE after processing command line arguments. As such,
    # https://github.com/OpenPathSec/Palladium/blob/master/src/palladium_lib/palladium_lib/boto3_session_cache.py
    # provides a better model for our command line tools. It finalizes the
    # session setup on first boto3 use rather than at import time. This
    # allows time for CLI arg processing to setup AWS_PROFILE.
    #
    # We could update this cf.py module to use the new boto3_client and
    # boto3_resource calls as suggested by the pull request. However, that
    # could potentially break existing EpoxyGlueOperation clients because
    # they would also need to switch to the newer boto3_client and
    # boto3_resource calls. Without this client code change, their code
    # would always prompt for 2nd factor authentication. That is the
    # remaining value to the below code. By setting the AWS_* environmental
    # variables, any client directly using boto3 keeps the benefit of the
    # skipped need for always prompting for 2nd factor authbenefits.

    # see https://github.com/boto/botocore/pull/1157 for how this
    # whole function is almost moot if someone would just accept the
    # PR into botocore upstream

    logging.info("loading AWS credentials...")

    # super-hacky: just call some dummy aws command to either verify
    # that we have fresh MFA credentials, or else prompt the user to
    # enter an MFA token to create/refresh the cached credentials
    try:
        subprocess.check_output(
            ["aws", f"--profile={op_env}", "sts", "get-caller-identity"]
        )
    except subprocess.CalledProcessError:
        logging.critical("Incorrect AWS Credentials")
        sys.exit(1)

    conf_ini = configparser.ConfigParser()
    conf_ini.read(f'{os.environ["HOME"]}/.aws/config')
    conf = conf_ini[f"profile {op_env}"]
    sts_assume_role_arn_prefix = (
        conf["role_arn"].replace("iam", "sts").replace(":role/", ":assumed-role/")
    )
    # sys.stderr.write(f'prefix {sts_assume_role_arn_prefix}\n')

    # get the current time in a format that we can compare vs. the Expiration timestamps in the AWS credential cache
    # files.
    now = datetime.datetime.now(datetime.timezone.utc)
    now_str = (
        now.isoformat()
    )  # e.g. 2021-04-01T00:18:02.975586+00:00, but we want it as 2021-04-01T00:18:02Z
    now_str = now_str.split(".")[0] + "Z"
    # sys.stderr.write(f'now_str {now_str}\n')

    cache_dir = f'{os.environ["HOME"]}/.aws/cli/cache'
    aws_cli_cache = None
    aws_cli_cache_file = None

    with os.scandir(cache_dir) as iter:
        # check the cache directory for a credentials cache file that:
        #   1. matches the desired role name,
        #   2. is not expired,
        #   3. has the latest expiration date if more than one file matches
        for dir_entry in iter:
            try:
                if dir_entry.name.startswith(".") or not dir_entry.is_file():
                    continue

                fname = f"{cache_dir}/{dir_entry.name}"
                with open(fname) as f:
                    # cache contents will look like:
                    # {
                    #   'Credentials': {
                    #     'AccessKeyId': '...',
                    #     'SecretAccessKey': '...',
                    #     'SessionToken': '...',
                    #     'Expiration': '2021-04-01T07:42:58Z'
                    #   },
                    #   'AssumedRoleUser': {
                    #     'AssumedRoleId': 'AROAJ3WDHZ6T3YVXT7LR2:botocore-session-1617219775',
                    #     'Arn': 'arn:aws:sts::77...714:assumed-role/Organization...Role/botocore-session-16...75'
                    #   },
                    #   'ResponseMetadata': {
                    #     'RequestId': '15275a1a-d441-471b-bc7a-6302c5aee3e0',
                    #     'HTTPStatusCode': 200,
                    #     'HTTPHeaders': {
                    #       'x-amzn-requestid': '15275a1a-d441-471b-bc7a-6302c5aee3e0',
                    #       'content-type': 'text/xml',
                    #       'content-length': '1114',
                    #       'date': 'Wed, 31 Mar 2021 19:42:58 GMT'
                    #     },
                    #     'RetryAttempts': 0
                    #   }
                    # }
                    contents = json.loads(f.read())
                    # sys.stderr.write(f'dir_entry {dir_entry.name} -> {contents}\n')

                    if not contents["AssumedRoleUser"]["Arn"].startswith(
                        sts_assume_role_arn_prefix
                    ):
                        # sys.stderr.write('nomatch\n')
                        continue

                    if contents["Credentials"]["Expiration"] < now_str:
                        # sys.stderr.write('expired\n')
                        continue

                    # sys.stderr.write('match\n')
                    # sys.stderr.write(f'expiration {contents["Credentials"]["Expiration"]}\n')
                    # NOTE python doesn't make it easy to parse dates from ISO strings, but fortunately
                    # lexicographical comparison of the date strings is also the correct date comparison (at
                    # least until Y10K)
                    if (not aws_cli_cache) or (
                        contents["Credentials"]["Expiration"]
                        > aws_cli_cache["Credentials"]["Expiration"]
                    ):
                        aws_cli_cache = contents
                        aws_cli_cache_file = fname

            except Exception as e:
                sys.stderr.write(str(e))

    if aws_cli_cache and aws_cli_cache_file:
        logging.info(f"loaded session credentials from {aws_cli_cache_file}\n")

        os.environ["AWS_ACCESS_KEY_ID"] = aws_cli_cache["Credentials"]["AccessKeyId"]
        os.environ["AWS_SECRET_ACCESS_KEY"] = aws_cli_cache["Credentials"][
            "SecretAccessKey"
        ]
        os.environ["AWS_SESSION_TOKEN"] = aws_cli_cache["Credentials"]["SessionToken"]
    else:
        logging.info(f"no valid session credentials found in {cache_dir}\n")


def get_hosts_from_ansible(filter_string, limit):
    limit_arg = " --limit " + limit if limit != "" else ""
    filter_string = "all" if (limit != "" and filter_string == "") else filter_string
    command_to_run = "ansible " + filter_string + limit_arg + " --list-hosts"
    try:
        hosts = (
            subprocess.check_output(command_to_run.split(), stderr=subprocess.DEVNULL)
            .decode("utf-8")
            .split("\n")
        )
    except Exception:
        logging.warn("No ACUs discovered with given filter string")
        return []
    final_node_list = []
    for line in hosts:
        if "openpath.local" not in line:
            continue
        final_node_list.append(line)

    return final_node_list


def main():
    """clush script entry point"""
    sys.excepthook = clush_excepthook

    #
    # Argument management
    #
    usage = "%prog [options] command"

    parser = OptionParser(usage)

    parser.add_option(
        "-n",
        "--nostdin",
        action="store_true",
        dest="nostdin",
        help="don't watch for possible input from stdin",
    )

    parser.add_option(
        "--ssh",
        action="store_true",
        dest="ssh",
        help=optparse.SUPPRESS_HELP,
    )

    parser.install_clush_config_options()
    parser.install_groupsconf_option()
    parser.install_nodes_options()
    parser.install_display_options(verbose_options=True)
    parser.install_mqtt_options()
    parser.install_s3_options()

    is_nebula = "nebula" in socket.gethostname()
    if is_nebula:
        parser.install_ansible_node_selection()

    if "--ssh" in sys.argv:
        parser.install_connector_options()
        if is_nebula:
            parser.install_filecopy_options()
    else:
        parser.install_filecopy_options(optparse.SUPPRESS_HELP)
        parser.install_connector_options(optparse.SUPPRESS_HELP)

    (options, args) = parser.parse_args()
    #
    # Load config file and apply overrides
    #
    config = ClushConfig(options, options.conf)

    # Initialize logging
    if config.verbosity >= VERB_DEBUG:
        logging.basicConfig(level=logging.DEBUG)
        logging.debug("clush: STARTING DEBUG")
    else:
        logging.basicConfig(level=logging.CRITICAL)

    if not (
        options.ssh
        or options.requestId
        or options.publish
        or options.copy
        or options.rcopy
    ):
        parser.error(
            "You must specify whether you want clush to publish an mqtt message or fetch a command result from S3."
        )
    if not is_nebula and not options.env:
        parser.error("You must specify which environment you are running against.")

    if not is_nebula:
        load_profile_credentials_static(options.env)
        helium_client = HeliumClient(options.env)

    if options.ssh and not is_nebula:
        if options.fanout > 10 and options.fanout != 500:
            parser.info(
                f"Changing fanout from {options.fanout} to 20. This will slow down results, but ensure nebula does not freeze up."
            )
            options.fanout = 10

        if options.fanout > 10:
            options.fanout = 10
    else:
        if options.fanout > 500:
            parser.error(
                f"The maximum fanout value is 500 and you supplied a value of {options.fanout}. Rerun with a lesser value or let the default of 500 take over."
            )

    set_std_group_resolver_config(options.groupsconf)

    # Should we use ANSI colors for nodes?
    if config.color == "auto":
        color = sys.stdout.isatty() and (options.gatherall or sys.stderr.isatty())
    else:
        color = config.color == "always"

    try:
        # Create and configure display object.
        display = Display(options, config, color)
    except ValueError as exc:
        parser.error("option mismatch (%s)" % exc)

    if options.groupsource:
        # Be sure -a/g -s source work as espected.
        std_group_resolver().default_source_name = options.groupsource

    # Compute the nodeset and warn for possible use of shell pathname
    # expansion (#225)
    wnodelist = []
    xnodelist = []
    if options.nodes:
        wnodelist = [NodeSet(nodes) for nodes in options.nodes]

    if options.exclude:
        xnodelist = [NodeSet(nodes) for nodes in options.exclude]

    for opt, nodelist in (("w", wnodelist), ("x", xnodelist)):
        for nodes in nodelist:
            if len(nodes) == 1 and exists(str(nodes)):
                display.vprint_err(
                    VERB_STD,
                    "Warning: using '-%s %s' and "
                    "local path '%s' exists, was it expanded "
                    "by the shell?" % (opt, nodes, nodes),
                )

    # --hostfile support (#235)
    for opt_hostfile in options.hostfile:
        try:
            fnodeset = NodeSet()
            with open(opt_hostfile) as hostfile:
                for line in hostfile.read().splitlines():
                    fnodeset.updaten(nodes for nodes in line.split())
            display.vprint_err(
                VERB_DEBUG,
                "Using nodeset %s from hostfile %s" % (fnodeset, opt_hostfile),
            )
            wnodelist.append(fnodeset)
        except IOError as exc:
            # re-raise as OSError to be properly handled
            errno, strerror = exc.args
            raise OSError(errno, strerror, exc.filename)

    # Retrieve hosts from ansible inventory with limit or filter string
    if options.limit or options.filter_string:
        discovered_nodes = get_hosts_from_ansible(options.filter_string, options.limit)
        wnodelist.append(NodeSet.fromlist(discovered_nodes))

    # Instantiate target nodeset from command line and hostfile
    nodeset_base = NodeSet.fromlist(wnodelist)
    # Instantiate filter nodeset (command line only)
    nodeset_exclude = NodeSet.fromlist(xnodelist)

    # Specified engine prevails over default engine
    DEFAULTS.engine = options.engine

    # Do we have nodes group?
    task = task_self()
    task.set_info("debug", config.verbosity >= VERB_DEBUG)
    if config.verbosity == VERB_DEBUG:
        std_group_resolver().set_verbosity(1)
    if options.nodes_all:
        all_nodeset = NodeSet.fromall()
        display.vprint(VERB_DEBUG, "Adding nodes from option -a: %s" % all_nodeset)
        nodeset_base.add(all_nodeset)

    if options.group:
        grp_nodeset = NodeSet.fromlist(options.group, resolver=RESOLVER_NOGROUP)
        for grp in grp_nodeset:
            addingrp = NodeSet("@" + grp)
            display.vprint(
                VERB_DEBUG, "Adding nodes from option -g %s: %s" % (grp, addingrp)
            )
            nodeset_base.update(addingrp)

    if options.exgroup:
        grp_nodeset = NodeSet.fromlist(options.exgroup, resolver=RESOLVER_NOGROUP)
        for grp in grp_nodeset:
            removingrp = NodeSet("@" + grp)
            display.vprint(
                VERB_DEBUG, "Excluding nodes from option -X %s: %s" % (grp, removingrp)
            )
            nodeset_exclude.update(removingrp)

    # Do we have an exclude list? (-x ...)
    nodeset_base.difference_update(nodeset_exclude)
    if len(nodeset_base) < 1 and not options.requestId:
        parser.error(
            "No node to run on. Ensure you provide nodelist before running command on CLI."
        )

    if options.pick and options.pick < len(nodeset_base):
        # convert to string for sample as nsiter() is slower for big
        # nodesets; and we assume options.pick will remain small-ish
        keep = random.sample(list(nodeset_base), options.pick)
        nodeset_base.intersection_update(",".join(keep))
        if config.verbosity >= VERB_VERB:
            msg = "Picked random nodes: %s" % nodeset_base
            print(Display.COLOR_RESULT_FMT % msg)

    # Set open files limit.
    # Let system settings take over
    set_fdlimit(config.fd_max, display)

    #
    # Task management
    #
    # check for clush interactive mode
    interactive = not len(args) and not (
        options.copy or options.rcopy or options.publish or options.requestId
    )
    # check for foreground ttys presence (input)
    stdin_isafgtty = (
        sys.stdin is not None
        and sys.stdin.isatty()
        and os.tcgetpgrp(sys.stdin.fileno()) == os.getpgrp()
    )
    # check for special condition (empty command and stdin not a tty)
    if interactive and not stdin_isafgtty:
        # looks like interactive but stdin is not a tty:
        # switch to non-interactive + disable ssh pseudo-tty
        interactive = False
        # SSH: disable pseudo-tty allocation (-T)
        ssh_options = config.ssh_options or ""
        ssh_options += " -T"
        config._set_main("ssh_options", ssh_options)
    if options.nostdin and interactive:
        parser.error("illegal option `--nostdin' in that case")

    # Force user_interaction if Clush._f_user_interaction for test purposes
    user_interaction = hasattr(sys.modules[__name__], "_f_user_interaction")
    if not options.nostdin:
        # Try user interaction: check for foreground ttys presence (ouput)
        stdout_isafgtty = (
            sys.stdout.isatty() and os.tcgetpgrp(sys.stdout.fileno()) == os.getpgrp()
        )
        user_interaction |= stdin_isafgtty and stdout_isafgtty
    display.vprint(VERB_DEBUG, "User interaction: %s" % user_interaction)
    if user_interaction:
        # Standard input is a terminal and we want to perform some user
        # interactions in the main thread (using blocking calls), so
        # we run cluster commands in a new ClusterShell Task (a new
        # thread is created).
        task = Task()
    # else: perform everything in the main thread

    # Handle special signal only when user_interaction is set
    task.set_default("USER_handle_SIGUSR1", user_interaction)

    task.excepthook = sys.excepthook
    task.set_default(
        "USER_stdin_worker",
        not (
            sys.stdin is None
            or sys.stdin.isatty()
            or options.nostdin
            or user_interaction
        ),
    )
    display.vprint(
        VERB_DEBUG, "Create STDIN worker: %s" % task.default("USER_stdin_worker")
    )

    task.set_info("debug", config.verbosity >= VERB_DEBUG)
    task.set_info("fanout", config.fanout)

    if options.worker:
        try:
            if options.remote == "no":
                task.set_default("local_worker", _load_workerclass(options.worker))
            else:
                task.set_default("distant_worker", _load_workerclass(options.worker))
        except (ImportError, AttributeError):
            msg = "ERROR: Could not load worker '%s'" % options.worker
            display.vprint_err(VERB_QUIET, msg)
            clush_exit(1, task)
    elif options.topofile or task._default_tree_is_enabled():
        if options.topofile:
            task.load_topology(options.topofile)
        if config.verbosity >= VERB_VERB:
            roots = len(task.topology.root.nodeset)
            gws = task.topology.inner_node_count() - roots
            msg = "enabling tree topology (%d gateways)" % gws
            print("clush: %s" % msg, file=sys.stderr)

    if options.grooming_delay:
        if config.verbosity >= VERB_VERB:
            msg = Display.COLOR_RESULT_FMT % (
                "Grooming delay: %f" % options.grooming_delay
            )
            print(msg, file=sys.stderr)
        task.set_info("grooming_delay", options.grooming_delay)
    elif options.rcopy:
        # By default, --rcopy should inhibit grooming
        task.set_info("grooming_delay", 0)

    if config.ssh_user:
        task.set_info("ssh_user", config.ssh_user)
    if config.ssh_path:
        task.set_info("ssh_path", config.ssh_path)
    if config.ssh_options:
        task.set_info("ssh_options", config.ssh_options)
    if config.scp_path:
        task.set_info("scp_path", config.scp_path)
    if config.scp_options:
        task.set_info("scp_options", config.scp_options)
    if config.rsh_path:
        task.set_info("rsh_path", config.rsh_path)
    if config.rcp_path:
        task.set_info("rcp_path", config.rcp_path)
    if config.rsh_options:
        task.set_info("rsh_options", config.rsh_options)

    # Set detailed timeout values
    task.set_info("connect_timeout", config.connect_timeout)
    task.set_info("command_timeout", config.command_timeout)

    # Enable stdout/stderr separation
    task.set_default("stderr", not options.gatherall)

    # Prevent reading from stdin?
    task.set_default("stdin", not options.nostdin)

    # Disable MsgTree buffering if not gathering outputs
    task.set_default("stdout_msgtree", display.gather or display.line_mode)

    # Always disable stderr MsgTree buffering
    task.set_default("stderr_msgtree", False)

    # Set timeout at worker level when command_timeout is defined.
    if config.command_timeout > 0:
        timeout = config.command_timeout
    else:
        timeout = -1

    # Configure task custom status
    task.set_default("USER_interactive", interactive)
    task.set_default("USER_running", False)

    if (options.copy or options.rcopy) and not args:
        parser.error("--[r]copy option requires at least one argument")
    if options.publish and not args:
        parser.error("--publish option requires at least one argument")
    if options.copy:
        if not options.dest_path:
            # append '/' to clearly indicate a directory for tree mode
            options.dest_path = join(dirname(abspath(args[0])), "")
        op = "copy sources=%s dest=%s" % (args, options.dest_path)
    elif options.rcopy:
        if not options.dest_path:
            options.dest_path = dirname(abspath(args[0]))
        op = "rcopy sources=%s dest=%s" % (args, options.dest_path)
    else:
        op = 'command="%s"' % " ".join(args)

    # print debug values (fanout value is get from the config object
    # and not task itself as set_info() is an asynchronous call.
    display.vprint(
        VERB_DEBUG,
        "clush: nodeset=%s fanout=%d [timeout "
        "conn=%.1f cmd=%.1f] %s"
        % (
            nodeset_base,
            config.fanout,
            config.connect_timeout,
            config.command_timeout,
            op,
        ),
    )

    formatted_nodes, errors = (
        format_nodes(expand(nodeset_base))
        if is_nebula
        else helium_client.format_nodes(expand(nodeset_base))
    )
    nodeset_base = NodeSet(",".join(formatted_nodes))
    environment = (
        opconf.main.get("env", default=socket.gethostname().split(".")[1])
        if is_nebula
        else options.env
    )
    if not task.default("USER_interactive"):
        if display.verbosity >= VERB_DEBUG and task.topology:
            print(Display.COLOR_RESULT_FMT % "-" * 15)
            print(Display.COLOR_RESULT_FMT % task.topology, end="")
            print(Display.COLOR_RESULT_FMT % "-" * 15)
        if options.copy:
            run_copy(
                task,
                args,
                options.dest_path,
                nodeset_base,
                timeout,
                options.preserve_flag,
                display,
            )
        elif options.rcopy:
            run_rcopy(
                task,
                args,
                options.dest_path,
                nodeset_base,
                timeout,
                options.preserve_flag,
                display,
            )
        elif options.requestId and options.publish is None:

            fetch_output_from_s3(
                task,
                options.requestId,
                nodeset_base,
                timeout,
                display,
                options.remote != "no",
                options.worker is None,
                environment,
                errors,
            )
        else:
            run_command(
                task,
                " ".join(args),
                nodeset_base,
                timeout,
                display,
                options.remote != "no",
                options.worker is None,
                options.publish,
                options.requestId,
                errors,
                is_nebula,
                environment,
            )

    if user_interaction:
        ttyloop(
            task,
            nodeset_base,
            timeout,
            display,
            options.remote != "no",
            options.worker is None,
            is_nebula,
            environment,
        )
    elif task.default("USER_interactive"):
        display.vprint_err(VERB_QUIET, "ERROR: interactive mode requires a tty")
        clush_exit(1, task)

    rc = 0
    if config.maxrc:
        # Instead of clush return code, return commands retcode
        rc = task.max_retcode()
        if task.num_timeout() > 0:
            rc = 255
    clush_exit(rc, task)


if __name__ == "__main__":
    main()
