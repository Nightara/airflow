import os
import re
import time
from typing import Optional, Any
from urllib.parse import urlparse

from airflow.configuration import conf
from airflow.executors.base_executor import BaseExecutor, CommandType
from airflow.models.taskinstance import TaskInstanceKey
from paramiko.rsakey import RSAKey
from paramiko import SSHClient


class RemoteLsfExecutor(BaseExecutor):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.remote = None
        self.user = None
        self.key_file = None
        self.bjob_to_task = None
        self.task_to_bjob = None

    def start(self):
        """Load configuration from config files."""
        self.remote = urlparse(conf.get("lsf", "remote"))
        self.user = conf.get("lsf", "user")
        self.key_file = conf.get("lsf", "key_file")
        self.bjob_to_task = dict()
        self.task_to_bjob = dict()

        # Test SSH connection
        success = False
        ssh_client = SSHClient()
        ssh_client.load_host_keys(os.path.expanduser('~/.ssh/known_hosts'))
        with open(self.key_file) as f:
            self.private_key = RSAKey.from_private_key(f)
            self.connector = SshConnector(ssh_client, self.remote.geturl(), self.user, RSAKey.from_private_key(f))
            with self.connector:
                success = self.connector.exception is None

        if not success:
            raise ValueError("Invalid SSH configuration.")

    def sync(self) -> int:
        """
        Sync will get called periodically by the heartbeat method.
        Executors should override this to perform gather statuses.
        """
        running_jobs = 0
        with self.connector as connection:
            _, stdout, stderr = connection.exec_command("source /etc/profile.d/lsf.sh && bjobs -aW")
            for bjob in [RemoteLsfExecutor._parse_bjob(line) for line in stdout if line[0].isdigit()]:
                if bjob.lsf_id in self.bjob_to_task:
                    task_id = self.bjob_to_task[bjob.lsf_id]
                    if bjob.finished:
                        self.success(task_id)
                    elif bjob.failed:
                        self.fail(task_id)
                    else:
                        running_jobs += 1
        return running_jobs

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
            lsf_command = "ls -la"

            bsub = "bsub -R rusage[mem=%dG] -n %d -q %s -J '%s' %s" % (lsf_mem, lsf_cores, lsf_queue, lsf_name,
                                                                       lsf_command)
            _, stdout, stderr = connection.exec_command("source /etc/profile.d/lsf.sh && " + bsub)
            for line in stdout:
                match = re.match("^Job <(.*)> is submitted to queue <%s>.$" % queue, line)
                if match:
                    self.bjob_to_task[match.group(1)] = key
                    self.task_to_bjob[key] = match.group(1)

    def end(self, heartbeat_interval=10) -> None:
        """
        This method is called when the caller is done submitting job and
        wants to wait synchronously for the jobs submitted previously to be
        all done. Using this method is not recommended.
        """
        while not self.sync():
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
