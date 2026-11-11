import json
import subprocess
import sys
from pathlib import Path

import pytest

from src.simulator import cli


def test_cli_outputs_requested_json_samples_with_interval(monkeypatch, capsys):
    sleeps = []
    monkeypatch.setattr(cli.time, "sleep", sleeps.append)

    assert cli.main(["--samples", "3", "--interval", "0.25", "--seed", "42"]) == 0

    samples = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert [sample["message_id"] for sample in samples] == [1, 2, 3]
    assert all(sample["asset_id"] == "P101" for sample in samples)
    assert sleeps == [0.25, 0.25]


def test_single_sample_does_not_wait(monkeypatch, capsys):
    sleeps = []
    monkeypatch.setattr(cli.time, "sleep", sleeps.append)

    assert cli.main(["-n", "1", "--interval", "10"]) == 0
    assert len(capsys.readouterr().out.splitlines()) == 1
    assert sleeps == []


@pytest.mark.parametrize(
    "args",
    [
        ["--samples", "0"],
        ["--samples", "-1"],
        ["--samples", "1.5"],
        ["--interval", "-0.1"],
        ["--interval", "nan"],
        ["--interval", "inf"],
        ["--interval", "invalid"],
        ["--seed", "invalid"],
    ],
)
def test_cli_rejects_invalid_arguments(args, capsys):
    with pytest.raises(SystemExit) as exc_info:
        cli.main(args)

    assert exc_info.value.code == 2
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "error:" in captured.err


def test_module_entry_point_and_seed_reproducibility():
    command = [
        sys.executable, "-m", "src.simulator", "--samples", "3",
        "--interval", "0", "--seed", "42",
    ]
    root = Path(__file__).resolve().parents[3]
    streams = []
    for _ in range(2):
        result = subprocess.run(command, cwd=root, capture_output=True, text=True, timeout=10)
        assert result.returncode == 0, result.stderr
        assert result.stderr == ""
        samples = [json.loads(line) for line in result.stdout.splitlines()]
        assert len(samples) == 3
        assert [sample["message_id"] for sample in samples] == [1, 2, 3]
        for sample in samples:
            sample.pop("timestamp")
        streams.append(samples)

    assert streams[0] == streams[1]
