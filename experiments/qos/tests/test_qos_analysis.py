import csv
import json
from pathlib import Path

import pytest

from experiments.qos.analysis import (
    AGGREGATE_FIELDS,
    LATENCY_FIELDS,
    RUN_FIELDS,
    aggregate_by_qos,
    analyze_csv,
    analyze_files,
    detect_run,
    latency_statistics,
    write_summaries,
)


@pytest.mark.parametrize(
    "name,expected",
    [
        ("qos0_run1_500msg_seed42.csv", (0, 1)),
        ("qos0_run2_500msg_seed42.csv", (0, 2)),
        ("qos0_run3_500msg_seed42.csv", (0, 3)),
        ("qos1_run1_500msg_seed42.csv", (1, 1)),
        ("qos1_run2_500msg_seed42.csv", (1, 2)),
        ("qos1_run3_500msg_seed42.csv", (1, 3)),
        ("capture-QoS1-run12.CSV", (1, 12)),
    ],
)
def test_detects_qos_and_run_from_filename(name, expected):
    assert detect_run(Path(name)) == expected


@pytest.mark.parametrize(
    "name",
    [
        "baseline_qos0_500msg_seed42.csv", "qos2_run1.csv", "qos0_run0.csv",
        "qos0_run1.txt", "qos0_qos1_run1.csv", "qos0_run1_run2.csv", "qos0_run1copy.csv",
    ],
)
def test_ambiguous_or_unsupported_filenames_are_rejected(name):
    with pytest.raises(ValueError, match="filename"):
        detect_run(Path(name))


def test_per_run_counts_loss_duplicates_and_all_latency_statistics(make_csv):
    path = make_csv("qos0_run1.csv", [(1, 10), (2, 20), (2, 30), (5, 40)])
    result = analyze_csv(path, expected_messages=5).summary()

    assert result["csv_file"] == str(path.resolve())
    assert (result["qos"], result["run"]) == (0, 1)
    assert result["expected_messages"] == 5
    assert result["received_rows"] == 4
    assert result["unique_ids"] == 3
    assert result["missing_ids"] == [3, 4]
    assert result["missing_count"] == 2
    assert result["loss_pct"] == 40.0
    assert result["duplicates"] == 1
    assert result["duplicate_ids"] == [2]
    assert result["unexpected_ids"] == []
    assert result["latency_min_ms"] == 10
    assert result["latency_max_ms"] == 40
    assert result["latency_mean_ms"] == 25
    assert result["latency_median_ms"] == 25
    assert result["latency_p95_ms"] == pytest.approx(38.5)


def test_out_of_range_ids_and_duplicates_do_not_compensate_for_missing_messages(make_csv):
    path = make_csv("qos1_run1.csv", [(1, 1), (1, 2), (7, 3), (7, 4)])
    result = analyze_csv(path, expected_messages=3).summary()
    assert result["received_rows"] == 4
    assert result["unique_ids"] == 2
    assert result["missing_ids"] == [2, 3]
    assert result["loss_pct"] == pytest.approx(200 / 3)
    assert result["duplicates"] == 2
    assert result["duplicate_ids"] == [1, 7]
    assert result["unexpected_count"] == 1
    assert result["unexpected_ids"] == [7]


def test_header_only_run_has_complete_loss_and_no_latency(make_csv):
    path = make_csv("qos0_run1.csv", [])
    result = analyze_csv(path, expected_messages=3).summary()
    assert result["received_rows"] == result["unique_ids"] == result["duplicates"] == 0
    assert result["missing_ids"] == [1, 2, 3]
    assert result["loss_pct"] == 100
    assert all(result[field] is None for field in LATENCY_FIELDS)


def test_default_expected_range_is_one_through_500(make_csv):
    result = analyze_csv(make_csv("qos0_run1.csv", [(500, 1)])).summary()
    assert result["expected_messages"] == 500
    assert result["missing_ids"] == list(range(1, 500))
    assert result["loss_pct"] == 99.8


@pytest.mark.parametrize("expected", [0, -1, True, 1.5])
def test_invalid_expected_message_count_is_rejected(expected, make_csv):
    path = make_csv("qos0_run1.csv", [])
    with pytest.raises(ValueError, match="positive integer"):
        analyze_csv(path, expected_messages=expected)


@pytest.mark.parametrize(
    "values,expected",
    [
        ([], [None, None, None, None, None]),
        ([7], [7, 7, 7, 7, 7]),
        ([0, 100], [0, 100, 50, 50, 95]),
        ([4, 1, 3, 2], [1, 4, 2.5, 2.5, 3.85]),
        ([3, 1, 2], [1, 3, 2, 2, 2.9]),
        ([4, 4, 4], [4, 4, 4, 4, 4]),
        ([-3, -1], [-3, -1, -2, -2, -1.1]),
    ],
)
def test_latency_statistics_use_linear_interpolation_and_preserve_observations(values, expected):
    result = latency_statistics(values)
    for field, value in zip(LATENCY_FIELDS, expected):
        if value is None:
            assert result[field] is None
        else:
            assert result[field] == pytest.approx(value)


@pytest.mark.parametrize(
    "contents",
    [
        "", "message_id\n1\n", "latency_ms\n1\n",
        "message_id,latency_ms\n1.5,2\n", "message_id,latency_ms\n0,2\n",
        "message_id,latency_ms\n-1,2\n", "message_id,latency_ms\n,2\n",
        "message_id,latency_ms\n1,\n", "message_id,latency_ms\n1,nan\n",
        "message_id,latency_ms\n1,inf\n", "message_id,latency_ms\n1,-inf\n",
        "message_id,latency_ms\n1,invalid\n", "message_id,latency_ms\n1,2,extra\n",
        'message_id,latency_ms\n1,"unclosed\n',
    ],
)
def test_invalid_csv_is_rejected_instead_of_silently_dropping_rows(contents, tmp_path):
    path = tmp_path / "qos0_run1.csv"
    path.write_text(contents, encoding="utf-8")
    with pytest.raises(ValueError, match=path.name):
        analyze_csv(path)


def test_subscriber_style_csv_extra_columns_and_utf8_bom_are_accepted(tmp_path):
    path = tmp_path / "qos0_run1.csv"
    path.write_text(
        "message_id,asset_id,timestamp,received_at,sent_at_unix_ns,latency_ms,pressure_bar,"
        "flow_lpm,temperature_c,vibration_mms,rpm\n"
        "1,P101,2026-01-01T00:00:00+00:00,2026-01-01T00:00:01+00:00,1,2.5,5.2,120,60,2,1450\n",
        encoding="utf-8-sig",
    )
    result = analyze_csv(path, expected_messages=1).summary()
    assert result["received_rows"] == 1
    assert result["loss_pct"] == 0
    assert result["latency_mean_ms"] == 2.5


def test_aggregate_pools_latencies_and_keeps_message_ids_scoped_to_each_run(make_csv):
    paths = [
        make_csv("qos1_run1.csv", [(1, 10), (2, 20), (3, 30), (4, 40)]),
        make_csv("qos0_run2.csv", [(1, 100)]),
        make_csv("qos0_run1.csv", [(1, 0), (2, 0), (2, 0), (3, 0)]),
    ]
    runs = analyze_files(paths, expected_messages=4)
    assert [(run.qos, run.run) for run in runs] == [(0, 1), (0, 2), (1, 1)]
    qos0, qos1 = aggregate_by_qos(runs)
    assert qos0["qos"] == 0
    assert qos0["run_count"] == qos0["runs_with_latency"] == 2
    assert qos0["expected_messages"] == 8
    assert qos0["received_rows"] == 5
    assert qos0["unique_ids"] == 4
    assert qos0["missing_count"] == 4
    assert qos0["loss_pct"] == 50
    assert qos0["duplicates"] == 1
    assert qos0["latency_mean_ms"] == 20
    assert qos0["latency_mean_of_run_means_ms"] == 50
    assert qos0["latency_median_ms"] == 0
    assert qos0["latency_p95_ms"] == pytest.approx(80)
    assert qos0["latency_min_ms"] == 0
    assert qos0["latency_max_ms"] == 100
    assert qos1["expected_messages"] == qos1["received_rows"] == 4
    assert qos1["missing_count"] == qos1["duplicates"] == 0
    assert qos1["latency_mean_ms"] == qos1["latency_mean_of_run_means_ms"] == 25
    assert qos1["latency_p95_ms"] == pytest.approx(38.5)


def test_empty_run_contributes_expected_messages_and_loss_but_no_invented_latency(make_csv):
    runs = analyze_files(
        [make_csv("qos0_run1.csv", []), make_csv("qos0_run2.csv", [(1, 10)])],
        expected_messages=2,
    )
    result = aggregate_by_qos(runs)[0]
    assert result["run_count"] == 2
    assert result["runs_with_latency"] == 1
    assert result["expected_messages"] == 4
    assert result["missing_count"] == 3
    assert result["loss_pct"] == 75
    assert result["latency_mean_ms"] == result["latency_mean_of_run_means_ms"] == 10


def test_all_empty_runs_have_no_aggregate_latency(make_csv):
    runs = analyze_files([make_csv("qos1_run1.csv", [])], expected_messages=2)
    result = aggregate_by_qos(runs)[0]
    assert result["loss_pct"] == 100
    assert result["runs_with_latency"] == 0
    assert all(result[field] is None for field in LATENCY_FIELDS)
    assert result["latency_mean_of_run_means_ms"] is None


def test_repeated_qos_run_pair_is_rejected(make_csv):
    first = make_csv("qos0_run1.csv", [(1, 1)])
    other = make_csv("copy_qos0_run1.csv", [(1, 1)])
    with pytest.raises(ValueError, match="Repeated"):
        analyze_files([first, other])


def test_no_input_runs_are_rejected(tmp_path):
    with pytest.raises(ValueError):
        analyze_files([])
    with pytest.raises(ValueError):
        write_summaries([], tmp_path / "output")
    assert not (tmp_path / "output").exists()


def test_export_writes_per_run_and_aggregate_csv_with_json_id_lists(make_csv, tmp_path):
    runs = analyze_files(
        [make_csv("qos0_run1.csv", [(1, 10), (1, 20)]), make_csv("qos1_run1.csv", [])],
        expected_messages=2,
    )
    output = tmp_path / "processed"
    run_path, aggregate_path = write_summaries(runs, output)
    with run_path.open(encoding="utf-8", newline="") as stream:
        reader = csv.DictReader(stream)
        assert reader.fieldnames == list(RUN_FIELDS)
        rows = list(reader)
    assert len(rows) == 2
    assert json.loads(rows[0]["missing_ids"]) == [2]
    assert json.loads(rows[0]["duplicate_ids"]) == [1]
    assert rows[0]["received_rows"] == "2"
    assert rows[1]["latency_mean_ms"] == ""
    with aggregate_path.open(encoding="utf-8", newline="") as stream:
        reader = csv.DictReader(stream)
        assert reader.fieldnames == list(AGGREGATE_FIELDS)
        aggregates = list(reader)
    assert [row["qos"] for row in aggregates] == ["0", "1"]
    assert aggregates[0]["latency_mean_ms"] == "15.0"
    assert aggregates[1]["latency_mean_ms"] == ""
    assert aggregates[1]["expected_messages"] == "2"
    second_paths = write_summaries(runs, output)
    assert set(second_paths).isdisjoint({run_path, aggregate_path})
    assert len(list(output.glob("*.csv"))) == 4
