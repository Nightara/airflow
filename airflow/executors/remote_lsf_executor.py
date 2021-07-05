import os
import re
import time
from typing import Optional, Any
from urllib.parse import urlparse

from airflow.configuration import conf, log
from airflow.executors.base_executor import BaseExecutor, CommandType
from airflow.models.taskinstance import TaskInstanceKey
from paramiko.rsakey import RSAKey
from paramiko import SSHClient

from airflow.utils.state import State


class RemoteLsfExecutor(BaseExecutor):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.user = None
        self.remote = None
        self.key_file = None
        self.connector = None
        self.private_key = None
        self.bjob_to_task = None
        self.task_to_bjob = None
        self.runnings_tasks = None

    def start(self):
        """Load configuration from config files."""
        self.user = conf.get("lsf", "user")
        self.remote = urlparse(conf.get("lsf", "remote"))
        self.key_file = conf.get("lsf", "key_file")
        self.bjob_to_task = dict()
        self.task_to_bjob = dict()
        self.runnings_tasks = dict()

        # TODO: Mock SSH connection
        # Test SSH connection
        ssh_client = SSHClient()
        ssh_client.load_host_keys(os.path.expanduser('~/.ssh/known_hosts'))
        with open(self.key_file) as f:
            self.private_key = RSAKey.from_private_key(f)
            self.connector = SshConnector(ssh_client, self.remote.geturl(), self.user, self.private_key)
            with self.connector:
                if self.connector.exception:
                    raise self.connector.exception

    def sync(self) -> None:
        """
        Sync will get called periodically by the heartbeat method.
        Executors should override this to perform gather statuses.
        """
        with self.connector as connection:
            _, stdout, stderr = connection.exec_command("source /etc/profile.d/lsf.sh && bjobs -aW")
            for bjob in [RemoteLsfExecutor._parse_bjob(line) for line in stdout if line[0].isdigit()]:
                if bjob.lsf_id in self.bjob_to_task:
                    task_id = self.bjob_to_task[bjob.lsf_id]
                    if bjob.finished:
                        del self.task_to_bjob[task_id]
                        del self.bjob_to_task[bjob.lsf_id]
                        self.success(task_id)
                    elif bjob.failed:
                        del self.task_to_bjob[task_id]
                        del self.bjob_to_task[bjob.lsf_id]
                        self.fail(task_id)
                    elif bjob.running:
                        # TODO: Why is this not propagated?
                        log.info(self.runnings_tasks[task_id])
                        self.runnings_tasks[task_id].check_and_change_state_before_execution()
                        self.change_state(task_id, State.RUNNING)
                    else:
                        # self.change_state(task_id, State.QUEUED, bjob.lsf_id)
                        pass

    def trigger_tasks(self, open_slots: int) -> None:
        """
        Triggers tasks

        :param open_slots: Number of open slots
        """
        sorted_queue = self.order_queued_tasks_by_priority()

        for _ in range(min((open_slots, len(self.queued_tasks)))):
            key, (command, _, queue, ti) = sorted_queue.pop(0)
            self.queued_tasks.pop(key)
            self.running.add(key)
            self.runnings_tasks[key] = ti
            self.execute_async(key=key, command=command, queue=queue, executor_config=ti.executor_config)

    def execute_async(self, key: TaskInstanceKey, command: CommandType, queue: Optional[str] = None,
                      executor_config: Optional[Any] = None) -> None:
        """
        This method will execute the command asynchronously.

        :param key: Unique key for the task instance
        :param command: Command to run
        :param queue: name of the queue
        :param executor_config: Configuration passed to the executor.
        """
        with self.connector as connection:
            # TODO: Determine RAM, cores, job name, job command
            # TODO: Can queue be used for lsf_queue?
            lsf_mem = 1
            lsf_cores = 1
            lsf_queue = "short"
            lsf_name = "Test"
            lsf_command = "sleep 20"
            log.info(command)
            log.info(queue)
            log.info(executor_config)

            bsub = "bsub -R rusage[mem=%dG] -n %d -q %s -J '%s' %s" % (lsf_mem, lsf_cores, lsf_queue, lsf_name,
                                                                       lsf_command)
            log.info(bsub)
            _, stdout, stderr = connection.exec_command("source /etc/profile.d/lsf.sh && " + bsub)
            for line in stdout:
                log.info(line)
                match = re.match("^Job <(\\d+)> is submitted to queue <%s>.\\s*$" % lsf_queue, line)
                if match:
                    log.info("Submitted with LSF ID " + match.group(1))
                    self.bjob_to_task[match.group(1)] = key
                    self.task_to_bjob[key] = match.group(1)
                    self.change_state(key, State.QUEUED, match.group(1))
                    return

            log.error("Submission failed.")
            self.fail(key)

    def end(self, heartbeat_interval=10) -> None:
        """
        This method is called when the caller is done submitting job and
        wants to wait synchronously for the jobs submitted previously to be
        all done.
        """
        while self.bjob_to_task:
            self.sync()
            log.info("Waiting for %d tasks to end." % len(self.bjob_to_task))
            time.sleep(heartbeat_interval)

    def terminate(self):
        """This method is called when the daemon receives a SIGTERM"""
        with self.connector as connection:
            _, stdout, stderr = connection.exec_command("source /etc/profile.d/lsf.sh && bjobs -aW")
            for bjob in [RemoteLsfExecutor._parse_bjob(line) for line in stdout if line[0].isdigit()]:
                if bjob.running and bjob.lsf_id in self.bjob_to_task:
                    connection.exec_command("source /etc/profile.d/lsf.sh && bkill " + bjob.lsf_id)
                    self.fail(self.bjob_to_task[bjob.lsf_id])

    @staticmethod
    def _parse_bjob(line: str):
        data = line.split()
        return LsfTask(data[0], data[6], data[2] == "RUN", data[2] == "DONE", data[2] == "EXIT")


class SshConnector:

    def __init__(self, client: SSHClient, remote: str, user: str, pkey: RSAKey):
        self.client = client
        self.remote = remote
        self.user = user
        self.pkey = pkey
        self.exception = None

    def __enter__(self) -> SSHClient:
        self.exception = None
        self.client.connect(self.remote, username=self.user, pkey=self.pkey)
        return self.client

    def __exit__(self, exception_type, exception_value, exception_traceback):
        self.exception = (exception_type, exception_value, exception_traceback)
        self.client.close()


class LsfTask:

    def __init__(self, lsf_id: str, lsf_name: str, running: bool, finished: bool, failed: bool):
        self.lsf_id = lsf_id
        self.lsf_name = lsf_name
        self.running = running
        self.finished = finished
        self.failed = failed
