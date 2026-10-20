"""Replay producer and strategy consumer attach independently to the same SHM."""
import os
from pathlib import Path
import re
import struct
import subprocess
import sys
import tempfile
import uuid

ROOT = Path(__file__).resolve().parents[1]
REPLAY, STRATEGY = sys.argv[1:]
FIELDS = [line.split('(')[1].split(')')[0]
          for line in (ROOT / 'examples/replay_fields.def').read_text().splitlines()]
TYPES = ['q' if line.startswith('TICK_INT') else 'd'
         for line in (ROOT / 'examples/replay_fields.def').read_text().splitlines()]
RECORD = struct.Struct('<16s' + ''.join(TYPES))


def fixture(path, count):
    with path.open('wb') as out:
        out.write(struct.pack('<8sQQQ', b'QTICK001', 0x52504C4159000001, RECORD.size, count))
        for n in range(count):
            tick = dict.fromkeys(FIELDS, 0)
            tick.update(last_price=101, bid_price1=99, ask_price1=101,
                        bid_volume1=10, ask_volume1=10, sequence=1000 + n)
            case = n % 8
            if case == 1:
                tick['last_price'] = 99
            elif case == 2:
                tick['last_price'] = 100
            elif case == 3:
                tick['last_price'] = float('nan')
            elif case == 4:
                tick['ask_price1'] = 98
            elif case == 5:
                tick['bid_volume1'] = 0
            elif case == 6:
                tick['bid_price1'] = float('inf')
            elif case == 7:
                tick['last_price'] = -1
            out.write(RECORD.pack(b'0050' if n % 2 == 0 else b'2330',
                                  *(tick[field] for field in FIELDS)))


def run(binary, *args, ok=True):
    result = subprocess.run([binary, *map(str, args)], capture_output=True, text=True, timeout=5)
    assert (result.returncode == 0) == ok, (args, result.stdout, result.stderr)
    return result


with tempfile.TemporaryDirectory(prefix='quant-strategy-test-') as directory:
    directory = Path(directory)
    path = directory / 'ticks.bin'
    env = dict(os.environ, REPLAY_LOG_DIR=str(directory / 'logs'))
    for count in (0, 4096):
        fixture(path, count)
        result = subprocess.run(['bash', str(ROOT / 'examples/run_tick_replay.sh'),
                                 str(path), REPLAY, '2000', '0', STRATEGY],
                                capture_output=True, text=True, timeout=8, env=env)
        assert result.returncode == 0, (result.stdout, result.stderr)
        assert f'strategy rows={count} ' in result.stdout, result.stdout
        per_case = count // 8
        assert f'verified=true buy={per_case} sell={per_case} hold={per_case} ignored={5 * per_case}' in result.stdout
        checksums = re.findall(r'checksum=(\d+)', result.stdout)
        assert len(checksums) == 2 and checksums[0] == checksums[1], result.stdout
        assert result.stdout.count('tick replay_sequence=') == min(5, count)
        if count:
            assert 'symbol=0050 source_sequence=1000 last=101 bid=99 ask=101 signal=buy' in result.stdout
            assert 'symbol=2330 source_sequence=1001 last=99 bid=99 ask=101 signal=sell' in result.stdout

    name = '/quant.strategy.test.' + uuid.uuid4().hex
    run(REPLAY, 'create', name, 1)
    try:
        run(STRATEGY, 'receive', name, 2, 20, ok=False)
        assert 'timeout' in run(STRATEGY, 'receive', name, 1, 20, ok=False).stderr
        # Fill the queue; producer exits on timeout before it can publish EOF.
        fixture(path, 1024)
        assert 'timeout' in run(REPLAY, 'send', name, 1, path, 20, ok=False).stderr
        result = run(STRATEGY, 'receive', name, 1, 20, ok=False)
        assert 'timeout' in result.stderr and 'verified=true' not in result.stdout
    finally:
        run(REPLAY, 'remove', name)
        assert not Path('/dev/shm', name[1:]).exists()

print('Strategy integration: direct SHM reads, signals, samples, FIFO/checksum, empty input, generation and missing EOF passed')
