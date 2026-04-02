"""Test the srun serialize-launch-collect protocol on Docker SLURM.

Run inside the container:
    salloc -n4 --mem=4G python /workspace/_dev/scripts/test_srun_protocol.py
"""

from __future__ import annotations

import os
import pickle
import subprocess
import sys
import time

import cloudpickle

PYTHON = sys.executable
BASE_DIR = f"/tmp/srun_test_{os.environ.get('SLURM_JOB_ID', 'nojob')}"


def compute_task(x: int) -> dict:
    """A sample task that runs inside srun."""
    import os
    import socket

    time.sleep(1)  # simulate work
    return {
        "input": x,
        "output": x**2,
        "step_id": os.environ.get("SLURM_STEP_ID", "unset"),
        "job_id": os.environ.get("SLURM_JOB_ID", "unset"),
        "pid": os.getpid(),
        "hostname": socket.gethostname(),
    }


def failing_task(x: int) -> None:
    """A task that raises."""
    raise ValueError(f"bad input: {x}")


# --- Worker script (inlined as -c arg) ---
WORKER_SCRIPT = r"""
import pickle, os, sys, tempfile

job_folder = sys.argv[1]
with open(os.path.join(job_folder, "job.pkl"), "rb") as f:
    fn, args, kwargs = pickle.load(f)

try:
    result = fn(*args, **kwargs)
    envelope = {"status": "ok", "result": result}
except Exception as e:
    envelope = {"status": "error", "error": str(e), "type": type(e).__name__}

result_path = os.path.join(job_folder, "result.pkl")
fd, tmp_path = tempfile.mkstemp(dir=job_folder, suffix=".tmp")
with os.fdopen(fd, "wb") as f:
    pickle.dump(envelope, f)
os.rename(tmp_path, result_path)
"""


def serialize_task(folder: str, fn, args, kwargs=None):
    """Serialize a callable to a job folder."""
    os.makedirs(folder, exist_ok=True)
    with open(os.path.join(folder, "job.pkl"), "wb") as f:
        cloudpickle.dump((fn, args, kwargs or {}), f)


def launch_srun(folder: str) -> subprocess.Popen:
    """Launch an srun step that runs the worker script."""
    cmd = [
        "srun",
        "--exact",
        "--mpi=none",
        "-n1",
        PYTHON,
        "-u",
        "-c",
        WORKER_SCRIPT,
        folder,
    ]
    return subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)


def collect_result(folder: str) -> dict:
    """Read the result envelope from a completed step."""
    with open(os.path.join(folder, "result.pkl"), "rb") as f:
        return pickle.load(f)


def test_single_task():
    """Test 1: Single task serialize-launch-collect."""
    print("--- Test: single task ---")
    folder = os.path.join(BASE_DIR, "single")
    serialize_task(folder, compute_task, (42,))

    proc = launch_srun(folder)
    stdout, stderr = proc.communicate(timeout=30)
    assert proc.returncode == 0, f"srun failed: {stderr.decode()}"

    envelope = collect_result(folder)
    assert envelope["status"] == "ok"
    assert envelope["result"]["output"] == 1764  # 42^2
    assert envelope["result"]["step_id"] != "unset"
    print(
        f"  PASS: result={envelope['result']['output']}, "
        f"step_id={envelope['result']['step_id']}"
    )


def test_error_envelope():
    """Test 2: Task raises, worker exits 0, error in envelope."""
    print("--- Test: error envelope ---")
    folder = os.path.join(BASE_DIR, "error")
    serialize_task(folder, failing_task, (-1,))

    proc = launch_srun(folder)
    stdout, stderr = proc.communicate(timeout=30)
    assert proc.returncode == 0, f"srun should exit 0: {stderr.decode()}"

    envelope = collect_result(folder)
    assert envelope["status"] == "error"
    assert envelope["type"] == "ValueError"
    assert "bad input: -1" in envelope["error"]
    print(f"  PASS: error type={envelope['type']}, msg={envelope['error']}")


def test_bootstrap_failure():
    """Test 3: No job.pkl — worker exits non-zero, no result.pkl."""
    print("--- Test: bootstrap failure ---")
    folder = os.path.join(BASE_DIR, "bootstrap_fail")
    os.makedirs(folder, exist_ok=True)
    # deliberately don't write job.pkl

    proc = launch_srun(folder)
    stdout, stderr = proc.communicate(timeout=30)
    assert proc.returncode != 0, f"Expected non-zero exit, got {proc.returncode}"
    assert not os.path.exists(os.path.join(folder, "result.pkl"))
    print(f"  PASS: exit_code={proc.returncode}, no result.pkl")


def test_concurrent_tasks():
    """Test 4: 4 concurrent tasks with unique step IDs."""
    print("--- Test: concurrent tasks ---")
    n_tasks = 4
    folders = []
    procs = []

    # Serialize all tasks
    for i in range(n_tasks):
        folder = os.path.join(BASE_DIR, f"concurrent_{i}")
        serialize_task(folder, compute_task, (i,))
        folders.append(folder)

    # Launch all concurrently
    t0 = time.time()
    for folder in folders:
        procs.append(launch_srun(folder))
        time.sleep(0.05)  # 50ms inter-launch interval (per plan)

    # Wait for all
    for proc in procs:
        proc.wait(timeout=30)
    elapsed = time.time() - t0

    # Collect and verify
    step_ids = set()
    for i, folder in enumerate(folders):
        assert procs[i].returncode == 0, f"Task {i} failed"
        envelope = collect_result(folder)
        assert envelope["status"] == "ok"
        assert envelope["result"]["output"] == i**2
        step_ids.add(envelope["result"]["step_id"])
        print(
            f"  task {i}: output={envelope['result']['output']}, "
            f"step_id={envelope['result']['step_id']}, "
            f"pid={envelope['result']['pid']}"
        )

    assert len(step_ids) == n_tasks, (
        f"Expected {n_tasks} unique step IDs, got {step_ids}"
    )
    print(
        f"  PASS: {n_tasks} tasks, {len(step_ids)} unique step IDs, "
        f"elapsed={elapsed:.2f}s (should be ~1s, not ~4s)"
    )


def test_sacct_visibility():
    """Test 5: sacct shows step-level detail for the current job."""
    print("--- Test: sacct visibility ---")
    job_id = os.environ.get("SLURM_JOB_ID")
    if not job_id:
        print("  SKIP: no SLURM_JOB_ID")
        return

    # Give sacct a moment to catch up
    time.sleep(1)
    result = subprocess.run(
        [
            "sacct",
            "-j",
            job_id,
            "--format=JobID,JobName,State,ExitCode,AllocCPUS",
            "--noheader",
            "--parsable2",
        ],
        capture_output=True,
        text=True,
    )
    lines = [l for l in result.stdout.strip().split("\n") if l]
    step_lines = [l for l in lines if "." in l.split("|")[0]]
    print(f"  sacct output ({len(lines)} lines, {len(step_lines)} steps):")
    for line in lines:
        print(f"    {line}")
    assert len(step_lines) >= 4, f"Expected >=4 steps, got {len(step_lines)}"
    print(f"  PASS: {len(step_lines)} steps visible in sacct")


def test_resource_flags():
    """Test 6: srun respects --cpus-per-task and --mem step-level flags."""
    print("--- Test: resource flags ---")
    folder = os.path.join(BASE_DIR, "resources")
    os.makedirs(folder, exist_ok=True)

    # Task that reports its resource environment
    def resource_check():
        import os

        return {
            "SLURM_CPUS_PER_TASK": os.environ.get("SLURM_CPUS_PER_TASK", "unset"),
            "SLURM_MEM_PER_NODE": os.environ.get("SLURM_MEM_PER_NODE", "unset"),
            "SLURM_STEP_ID": os.environ.get("SLURM_STEP_ID", "unset"),
        }

    serialize_task(folder, resource_check, ())

    cmd = [
        "srun",
        "--exact",
        "--mpi=none",
        "-n1",
        "--cpus-per-task=2",
        "--mem=512M",
        PYTHON,
        "-u",
        "-c",
        WORKER_SCRIPT,
        folder,
    ]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = proc.communicate(timeout=30)
    assert proc.returncode == 0, f"srun failed: {stderr.decode()}"

    envelope = collect_result(folder)
    assert envelope["status"] == "ok"
    result = envelope["result"]
    print(f"  SLURM_CPUS_PER_TASK={result['SLURM_CPUS_PER_TASK']}")
    print(f"  SLURM_MEM_PER_NODE={result['SLURM_MEM_PER_NODE']}")
    print(f"  SLURM_STEP_ID={result['SLURM_STEP_ID']}")
    # SLURM should set these env vars for the step
    assert result["SLURM_CPUS_PER_TASK"] != "unset", "SLURM_CPUS_PER_TASK not set"
    print("  PASS: resource flags propagated to step environment")


if __name__ == "__main__":
    print(f"SLURM_JOB_ID={os.environ.get('SLURM_JOB_ID', 'unset')}")
    print(f"Python: {PYTHON}")
    print(f"Working dir: {BASE_DIR}")
    print()

    try:
        test_single_task()
        test_error_envelope()
        test_bootstrap_failure()
        test_concurrent_tasks()
        test_sacct_visibility()
        test_resource_flags()
        print("\n=== ALL TESTS PASSED ===")
    finally:
        # Cleanup
        import shutil

        if os.path.exists(BASE_DIR):
            shutil.rmtree(BASE_DIR)
