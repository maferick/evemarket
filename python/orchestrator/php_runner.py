from __future__ import annotations

import os
import queue
import signal
import subprocess
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable

from .logging_utils import LoggerAdapter


@dataclass(slots=True)
class ProcessStatus:
    pid: int | None
    started_at: float | None
    exit_code: int | None
    runtime_seconds: float


@dataclass(slots=True)
class ManagedPhpProcess:
    php_binary: str
    script_path: Path
    cwd: Path
    env: dict[str, str]
    logger: LoggerAdapter
    name: str = "scheduler-daemon"
    process: subprocess.Popen[str] | None = None
    started_at: float | None = None
    _pump_threads: list[threading.Thread] = field(default_factory=list)
    _line_queue: queue.Queue[tuple[str, str]] = field(default_factory=queue.Queue)

    def start(self, extra_args: Iterable[str] = ()) -> None:
        if self.process is not None and self.process.poll() is None:
            raise RuntimeError(f"{self.name} is already running")

        command = [self.php_binary, str(self.script_path), *list(extra_args)]
        child_env = os.environ.copy()
        child_env.update(self.env)
        self.process = subprocess.Popen(
            command,
            cwd=str(self.cwd),
            env=child_env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            start_new_session=True,
        )
        self.started_at = time.time()
        self._pump_threads = [
            threading.Thread(target=self._pump_stream, args=("stdout", self.process.stdout), daemon=True),
            threading.Thread(target=self._pump_stream, args=("stderr", self.process.stderr), daemon=True),
        ]
        for thread in self._pump_threads:
            thread.start()
        self.logger.info(
            "started managed PHP process",
            payload={"event": "worker.started", "pid": self.process.pid, "script": str(self.script_path)},
        )

    def _pump_stream(self, stream_name: str, stream: subprocess.PIPE | None) -> None:
        if stream is None:
            return
        for line in iter(stream.readline, ""):
            self._line_queue.put((stream_name, line.rstrip()))
        stream.close()

    def flush_output(self) -> None:
        while True:
            try:
                stream_name, line = self._line_queue.get_nowait()
            except queue.Empty:
                return
            payload = {"event": f"worker.{stream_name}", "stream": stream_name, "pid": self.pid, "line": line}
            if stream_name == "stderr":
                self.logger.warning("PHP worker emitted stderr", payload=payload)
            else:
                self.logger.info("PHP worker emitted stdout", payload=payload)

    @property
    def pid(self) -> int | None:
        return None if self.process is None else self.process.pid

    def poll(self) -> int | None:
        if self.process is None:
            return None
        return self.process.poll()

    def terminate(self, grace_seconds: int) -> int | None:
        if self.process is None:
            return None
        if self.process.poll() is not None:
            return self.process.returncode

        self.logger.info(
            "terminating managed PHP process",
            payload={"event": "worker.terminate", "pid": self.process.pid, "grace_seconds": grace_seconds},
        )
        try:
            os.killpg(os.getpgid(self.process.pid), signal.SIGTERM)
        except ProcessLookupError:
            return self.process.poll()

        deadline = time.time() + grace_seconds
        while time.time() < deadline:
            self.flush_output()
            code = self.process.poll()
            if code is not None:
                return code
            time.sleep(0.5)

        self.logger.warning(
            "managed PHP process exceeded graceful stop timeout",
            payload={"event": "worker.kill", "pid": self.process.pid},
        )
        try:
            os.killpg(os.getpgid(self.process.pid), signal.SIGKILL)
        except ProcessLookupError:
            return self.process.poll()
        return self.process.wait(timeout=5)

    def status(self) -> ProcessStatus:
        exit_code = self.poll()
        runtime = 0.0 if self.started_at is None else max(0.0, time.time() - self.started_at)
        return ProcessStatus(pid=self.pid, started_at=self.started_at, exit_code=exit_code, runtime_seconds=runtime)
