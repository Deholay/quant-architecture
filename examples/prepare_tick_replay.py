#!/usr/bin/env python3
"""Batch-convert daily_parquet into the fixed little-endian C++ replay format."""
import argparse
import os
from pathlib import Path
import re
import struct
import tempfile

MAGIC = b"QTICK001"
SCHEMA = 0x52504C4159000001


def field_spec():
    text = Path(__file__).with_name("replay_fields.def").read_text()
    fields = []
    for line in text.splitlines():
        match = re.fullmatch(r"TICK_(INT|DOUBLE)\((\w+)\)", line)
        if not match:
            raise ValueError(f"invalid replay field definition: {line}")
        fields.append((match[2], match[1]))
    return fields


def convert(source, destination, limit=None, batch_size=16384):
    import pyarrow as pa
    import pyarrow.parquet as pq

    fields = field_spec()
    record = struct.Struct("<16s" + "".join("q" if t == "INT" else "d" for _, t in fields))
    parquet = pq.ParquetFile(source)
    expected = [("symbol", pa.string())] + [
        (name, pa.int64() if kind == "INT" else pa.float64()) for name, kind in fields
    ]
    if parquet.schema_arrow.names != [name for name, _ in expected]:
        raise ValueError("Parquet column names/order differ from replay_fields.def")
    for name, dtype in expected:
        if parquet.schema_arrow.field(name).type != dtype:
            raise ValueError(f"unexpected type for {name}; expected {dtype}")
    count = parquet.metadata.num_rows
    if limit is not None:
        count = min(count, limit)
    destination = Path(destination)
    if destination.exists():
        raise FileExistsError(f"refusing to overwrite {destination}")
    completed = 0
    # Publish only complete output; link() also refuses a concurrently created destination.
    with tempfile.NamedTemporaryFile(dir=destination.parent, prefix=".tick-replay-", delete=False) as out:
        temporary = Path(out.name)
        try:
            out.write(struct.pack("<8sQQQ", MAGIC, SCHEMA, record.size, count))
            for batch in parquet.iter_batches(batch_size=batch_size):
                if completed == count:
                    break
                batch = batch.slice(0, min(batch.num_rows, count - completed))
                if any(column.null_count for column in batch.columns):
                    raise ValueError(f"null value in batch starting at row {completed + 1}")
                columns = [column.to_pylist() for column in batch.columns]
                buffer = bytearray(batch.num_rows * record.size)
                for i, row in enumerate(zip(*columns)):
                    symbol = row[0].encode("utf-8")
                    if len(symbol) > 15 or b"\0" in symbol:
                        raise ValueError(f"symbol exceeds 15 UTF-8 bytes or contains NUL at row {completed + i + 1}")
                    record.pack_into(buffer, i * record.size, symbol, *row[1:])
                out.write(buffer)
                completed += batch.num_rows
                if completed // 1000000 != (completed - batch.num_rows) // 1000000:
                    print(f"prepared {completed}/{count} rows", flush=True)
            if completed != count:
                raise ValueError("Parquet row count mismatch")
            out.flush()
            os.fsync(out.fileno())
            os.link(temporary, destination)
        finally:
            temporary.unlink(missing_ok=True)
    print(f"prepared rows={completed} record_bytes={record.size} output={destination}", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path)
    parser.add_argument("destination", type=Path)
    parser.add_argument("--limit", type=int, help="optional first N rows; default is the entire file")
    args = parser.parse_args()
    if args.limit is not None and args.limit < 0:
        parser.error("--limit must be nonnegative")
    try:
        convert(args.source, args.destination, args.limit)
    except (OSError, ValueError, ImportError) as error:
        parser.exit(1, f"prepare failed: {error}\n")


if __name__ == "__main__":
    main()
