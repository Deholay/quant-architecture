"""Independent executable endpoints; no inherited SHM mapping or third-party packages."""
import importlib.util
from pathlib import Path
import re
import struct
import subprocess
import sys
import tempfile
import uuid

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("prepare", ROOT / "examples/prepare_tick_replay.py")
prepare = importlib.util.module_from_spec(spec)
spec.loader.exec_module(prepare)
FIELDS = prepare.field_spec()
RECORD = struct.Struct("<16s" + "".join("q" if t == "INT" else "d" for _, t in FIELDS))
BINARY = sys.argv[1]


def run(*args, ok=True):
    result = subprocess.run([BINARY, *map(str, args)], capture_output=True, text=True, timeout=5)
    if (result.returncode == 0) != ok:
        raise AssertionError((args, result.returncode, result.stdout, result.stderr))
    return result


def fixture(path, count):
    with path.open("wb") as out:
        out.write(struct.pack("<8sQQQ", prepare.MAGIC, prepare.SCHEMA, RECORD.size, count))
        for n in range(count):
            out.write(RECORD.pack(b"0050", *(n if t == "INT" else n + 0.25 for _, t in FIELDS)))


def paired(path, count, delay=0):
    name = "/quant.replay.test." + uuid.uuid4().hex
    run("create", name, 1)
    consumer = None
    try:
        consumer = subprocess.Popen([BINARY, "receive", name, "1", "2000", str(delay)],
                                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        producer = run("send", name, 1, path, 2000)
        stdout, stderr = consumer.communicate(timeout=5)
        assert consumer.returncode == 0, (stdout, stderr)
        assert f"producer rows={count} " in producer.stdout, producer.stdout
        assert f"consumer rows={count} " in stdout and "verified=true" in stdout, stdout
        assert re.search(r"checksum=(\d+)", producer.stdout)[1] == re.search(r"checksum=(\d+)", stdout)[1]
        assert f"enqueue_wait samples={count}" in producer.stdout
        assert f"queue_delivery samples={count}" in stdout
        if count == 0:
            assert "latency_ns=n/a" in stdout
        if delay:
            assert int(re.search(r"full_retries=(\d+)", producer.stdout)[1]) > 0, producer.stdout
        return stdout
    finally:
        if consumer is not None and consumer.poll() is None:
            consumer.kill()
            consumer.wait()
        run("remove", name)
        assert not Path("/dev/shm", name[1:]).exists()


with tempfile.TemporaryDirectory(prefix="quant-replay-test-") as directory:
    directory = Path(directory)
    path = directory / "ticks.bin"
    fixture(path, 0)
    paired(path, 0)
    fixture(path, 4096)
    paired(path, 4096)
    paired(path, 4096, delay=100)

    name = "/quant.replay.test." + uuid.uuid4().hex
    run("create", name, 1)
    try:
        run("receive", name, 2, 20, ok=False)  # wrong generation
        result = run("receive", name, 1, 20, ok=False)
        assert "timeout" in result.stderr
        fixture(path, 1)
        with path.open("r+b") as file:
            file.truncate(path.stat().st_size - 1)
        assert "mismatch" in run("send", name, 1, path, 20, ok=False).stderr
        fixture(path, 1)
        with path.open("r+b") as file:
            file.write(b"BADMAGIC")
        assert "mismatch" in run("send", name, 1, path, 20, ok=False).stderr
        fixture(path, 4096)
        assert "timeout" in run("send", name, 1, path, 20, ok=False).stderr
    finally:
        run("remove", name)

    # Exercise shell orchestration and its failure cleanup, with fast endpoint exits.
    fixture(path, 4)
    runner = ROOT / "examples/run_tick_replay.sh"
    result = subprocess.run(["bash", str(runner), str(path), BINARY, "1000"],
                            capture_output=True, text=True, timeout=5)
    assert result.returncode == 0, (result.stdout, result.stderr)
    result = subprocess.run(["bash", str(runner), str(directory / "missing"), BINARY, "1000"],
                            capture_output=True, text=True, timeout=5)
    assert result.returncode != 0, result

print("Replay integration: FIFO/checksum, empty file, backpressure, format errors, timeout and runner passed")
