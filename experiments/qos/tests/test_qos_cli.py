import csv
import subprocess
import sys
from pathlib import Path

import pytest

from experiments.qos.analyze import expand_inputs, main


def test_glob_expansion_and_overlapping_inputs_do_not_duplicate_files(make_csv):
    first = make_csv("qos0_run1.csv", [(1, 1)])
    second = make_csv("qos1_run1.csv", [(1, 2)])
    paths = expand_inputs([str(first.parent / "qos*_run*.csv"), str(first)])
    assert paths == [first.resolve(), second.resolve()]


def test_no_matches_or_directory_inputs_are_rejected(tmp_path):
    with pytest.raises(ValueError, match="No CSV matches"):
        expand_inputs([str(tmp_path / "qos*.csv")])
    with pytest.raises(ValueError, match="Not a file"):
        expand_inputs([str(tmp_path)])


def test_cli_analyzes_multiple_synthetic_runs_and_prints_comparison(make_csv, tmp_path, capsys):
    make_csv("qos0_run1.csv", [(1, 1), (2, 3)])
    make_csv("qos1_run1.csv", [(1, 2)])
    output = tmp_path / "processed"
    assert main([
        str(tmp_path / "qos*_run*.csv"), "--expected-messages", "2", "--output-dir", str(output),
    ]) == 0
    captured = capsys.readouterr()
    assert captured.err == ""
    assert "QoS 0" in captured.out and "QoS 1" in captured.out
    assert "Promedio de medias por corrida" in captured.out
    assert "Resumen por corrida:" in captured.out
    assert "Resumen por QoS:" in captured.out
    assert len(list(output.glob("*.csv"))) == 2


def test_single_qos_input_does_not_invent_data_for_the_other_qos(make_csv, tmp_path, capsys):
    path = make_csv("qos0_run1.csv", [])
    output = tmp_path / "processed"
    assert main([str(path), "--output-dir", str(output)]) == 0
    captured = capsys.readouterr()
    assert "sin datos" in captured.out
    assert "N/A" in captured.out
    with next(output.glob("qos_aggregate_*.csv")).open(encoding="utf-8", newline="") as stream:
        rows = list(csv.DictReader(stream))
    assert len(rows) == 1
    assert rows[0]["qos"] == "0"
    assert rows[0]["run_count"] == "1"
    assert rows[0]["expected_messages"] == "500"


def test_invalid_input_fails_without_writing_partial_results(make_csv, tmp_path, capsys):
    good = make_csv("qos0_run1.csv", [(1, 1)])
    bad = make_csv("qos1_run1.csv", [(1, "invalid")])
    output = tmp_path / "processed"
    assert main([str(good), str(bad), "--output-dir", str(output)]) == 1
    assert "CSV line 2" in capsys.readouterr().err
    assert not output.exists()


def test_unmatched_input_returns_clear_error(tmp_path, capsys):
    assert main([str(tmp_path / "missing*.csv")]) == 1
    assert "No CSV matches" in capsys.readouterr().err


@pytest.mark.parametrize("value", ["0", "-1", "1.5", "invalid"])
def test_expected_message_count_cli_validation(value, tmp_path):
    with pytest.raises(SystemExit) as exc_info:
        main([str(tmp_path / "unused.csv"), "--expected-messages", value])
    assert exc_info.value.code == 2


def test_module_entry_point_accepts_multiple_paths(make_csv, tmp_path):
    first = make_csv("qos0_run1.csv", [(1, 1)])
    second = make_csv("qos1_run1.csv", [(1, 2)])
    output = tmp_path / "processed"
    result = subprocess.run(
        [sys.executable, "-m", "experiments.qos.analyze", str(first), str(second),
         "--expected-messages", "1", "--output-dir", str(output)],
        cwd=Path(__file__).resolve().parents[3], capture_output=True, text=True, timeout=10,
    )
    assert result.returncode == 0, result.stderr
    assert "Comparacion MQTT QoS 0 y QoS 1" in result.stdout
    assert len(list(output.glob("*.csv"))) == 2
