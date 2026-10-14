"""Optional PyArrow conversion tests, including complete field preservation."""
import importlib.util
from pathlib import Path
import struct
import sys
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("prepare", ROOT / "examples/prepare_tick_replay.py")
prepare = importlib.util.module_from_spec(spec)
spec.loader.exec_module(prepare)
fields = prepare.field_spec()
record = struct.Struct("<16s" + "".join("q" if t == "INT" else "d" for _, t in fields))


def fails(action):
    try:
        action()
    except (ValueError, FileExistsError):
        return
    raise AssertionError("expected rejection")


with tempfile.TemporaryDirectory(prefix="quant-parquet-test-") as directory:
    source = Path(directory) / "source.parquet"
    output = Path(directory) / "ticks.bin"
    columns = {"symbol": pa.array(["0050", "2330"], type=pa.string())}
    for name, kind in fields:
        columns[name] = pa.array([1, -2] if kind == "INT" else [35.55, 0.25],
                                type=pa.int64() if kind == "INT" else pa.float64())
    table = pa.table(columns)
    pq.write_table(table, source, row_group_size=1)
    prepare.convert(source, output, batch_size=1)
    data = output.read_bytes()
    assert struct.unpack("<8sQQQ", data[:32]) == (prepare.MAGIC, prepare.SCHEMA, record.size, 2)
    for n, symbol in enumerate([b"0050", b"2330"]):
        values = record.unpack_from(data, 32 + n * record.size)
        assert values[0].rstrip(b"\0") == symbol
        assert values[1:] == tuple(([1, -2] if kind == "INT" else [35.55, 0.25])[n] for _, kind in fields)
    fails(lambda: prepare.convert(source, output))
    assert output.read_bytes() == data  # existing output remains intact
    limited = Path(directory) / "limited.bin"
    prepare.convert(source, limited, limit=1)
    assert limited.stat().st_size == 32 + record.size

    for symbol_values in [[None, "2330"], ["1234567890123456", "2330"]]:
        invalid = table.set_column(0, "symbol", pa.array(symbol_values, type=pa.string()))
        pq.write_table(invalid, source)
        rejected = Path(directory) / "rejected.bin"
        fails(lambda: prepare.convert(source, rejected))
        assert not rejected.exists()
        assert not list(Path(directory).glob(".tick-replay-*"))
    pq.write_table(table.drop(["sequence"]), source)
    fails(lambda: prepare.convert(source, Path(directory) / "wrong-schema.bin"))

print("Parquet conversion: all fields, batching, limit, overwrite, null, symbol and schema tests passed")
