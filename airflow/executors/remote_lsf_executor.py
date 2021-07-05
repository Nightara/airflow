import os
from typing import Optional, Any
from urllib.parse import urlparse

from airflow.executors.base_executor import BaseExecutor, CommandType
from airflow.models.taskinstance import TaskInstanceKey
from paramiko.rsakey import RSAKey
from paramiko import SSHClient


class RemoteLsfExecutor(BaseExecutor):

    def __init__(self, *args, remote: str, user: str, key_file: str, **kwargs):
        super().__init__(*args, **kwargs)
        self.remote = urlparse(remote)
        self.user = user
        self.key_file = key_file
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

    def start(self):
        """Executors may need to get things started."""
        # TODO: Setup?
        raise NotImplementedError()

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
                        self.success(task_id)
                    elif bjob.failed:
                        self.fail(task_id)
                    else:
                        # TODO: Handle running / pending states.
                        pass

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
            # TODO: Submit, register LSF job ID in bjob_to_task and task_to_bjob
            pass
        raise NotImplementedError()

    def end(self) -> None:
        """
        This method is called when the caller is done submitting job and
        wants to wait synchronously for the job submitted previously to be
        all done.
        """
        # TODO: Wait for all jobs to finish - call sync in loop.
        raise NotImplementedError()

    def terminate(self):
        """This method is called when the daemon receives a SIGTERM"""
        # TODO: Kill all jobs
        raise NotImplementedError()

    @staticmethod
    def _parse_bjob(line: str):
        data = line.split()
        return LsfTask(int(data[0]), data[6], data[2] == "RUN", data[2] == "DONE", data[2] == "EXIT")


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

    def __init__(self, lsf_id: int, lsf_name: str, running: bool, finished: bool, failed: bool):
        self.lsf_id = lsf_id
        self.lsf_name = lsf_name
        self.running = running
        self.finished = finished
        self.failed = failed
