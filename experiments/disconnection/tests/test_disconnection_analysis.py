import csv
import json

import pytest

from experiments.disconnection import analysis, analyze
from experiments.disconnection.common import iso_from_ns

BASE_NS = 1_700_000_000_000_000_000


def at(seconds):
    return BASE_NS + int(seconds * 1_000_000_000)


def write_csv(path, rows, fields):
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


@pytest.fixture
def dataset(tmp_path):
    pub = []
    for message_id in range(1, 7):
        attempted, success = message_id not in (3, 4), message_id in (1, 2, 6)
        pub.append({
            "message_id": message_id, "generated_at": iso_from_ns(at(message_id)),
            "publish_attempted": attempted, "publish_success": success,
            "sent_at_unix_ns": at(message_id) if attempted else None, "qos": 1,
            "publish_status": "published" if success else "unconfirmed" if attempted else "not_attempted",
        })
    received = [
        {"message_id": message_id, "received_at": iso_from_ns(at(seconds)),
         "received_at_unix_ns": at(seconds), "latency_ms": latency}
        for message_id, seconds, latency in [(1, 1, 1), (2, 2, 3), (2, 2.2, 5), (6, 15.2, 7)]
    ]
    events = [
        {"timestamp": iso_from_ns(at(seconds)), "unix_ns": at(seconds), "role": "subscriber", "event": event}
        for seconds, event in [(0, "CONNECTED"), (10, "DISCONNECTED"), (11, "RECONNECT_ATTEMPT"),
                               (15, "RECONNECTED"), (15.01, "SUBSCRIBED")]
    ]
    paths = (tmp_path / "publisher.csv", tmp_path / "subscriber.csv", tmp_path / "events.csv")

    def save():
        write_csv(paths[0], pub, list(pub[0]) if pub else [
            "message_id", "generated_at", "publish_attempted", "publish_success", "sent_at_unix_ns", "qos",
        ])
        write_csv(paths[1], received, ["message_id", "received_at", "received_at_unix_ns", "latency_ms"])
        write_csv(paths[2], events, ["timestamp", "unix_ns", "role", "event"])
        return paths

    return pub, received, events, save


def test_analysis_counts_generated_publications_missing_ids_duplicates_and_latencies(dataset):
    _, _, _, save = dataset
    result = analysis.analyze_run(*save())
    assert result["generated_samples"] == 6
    assert result["publications_attempted"] == 4
    assert result["publications_successful"] == 3
    assert result["publications_unsuccessful"] == 3
    assert result["publications_not_attempted"] == 2
    assert result["publications_unconfirmed"] == 1
    assert result["received_messages"] == 4
    assert result["unique_received_ids"] == 3
    assert result["missing_ids"] == [3, 4, 5]
    assert result["missing_count"] == 3
    assert result["loss_pct"] == 50
    assert result["duplicates"] == 1 and result["duplicate_ids"] == [2]
    assert result["unexpected_received_ids"] == []
    assert result["latency_samples"] == 4
    assert result["latency_min_ms"] == 1 and result["latency_max_ms"] == 7
    assert result["latency_mean_ms"] == result["latency_median_ms"] == 4
    assert result["latency_p95_ms"] == pytest.approx(6.7)


def test_interruption_duration_and_time_to_first_message_are_measured_from_events(dataset):
    _, _, _, save = dataset
    result = analysis.analyze_run(*save())
    assert result["interruption_count"] == 1
    assert result["disconnected_at"] == iso_from_ns(at(10))
    assert result["reconnected_at"] == iso_from_ns(at(15))
    assert result["interruption_duration_s"] == 5
    assert result["first_received_after_reconnect_at"] == iso_from_ns(at(15.2))
    assert result["recovery_to_first_message_s"] == pytest.approx(0.2)


def test_loss_uses_received_ids_even_when_publication_was_unconfirmed(dataset):
    _, received, _, save = dataset
    received.append({"message_id": 5, "received_at": iso_from_ns(at(16)),
                     "received_at_unix_ns": at(16), "latency_ms": None})
    result = analysis.analyze_run(*save())
    assert result["publications_unconfirmed"] == 1
    assert result["missing_ids"] == [3, 4]
    assert result["loss_pct"] == pytest.approx(100 / 3)
    assert result["latency_samples"] == 4


def test_missing_latencies_and_missing_recovery_do_not_produce_fabricated_values(dataset):
    _, received, events, save = dataset
    for row in received:
        row["latency_ms"] = None
    events[:] = events[:2]
    result = analysis.analyze_run(*save())
    assert result["interruption_count"] == 1
    assert result["reconnected_at"] is None
    assert result["interruption_duration_s"] is None
    assert result["recovery_to_first_message_s"] is None
    assert result["latency_mean_ms"] is None
    assert result["latency_samples"] == 0
    assert "N/A" in analyze.format_summary(result)


def test_no_recovery_message_remains_unknown(dataset):
    _, received, _, save = dataset
    received[:] = received[:3]
    result = analysis.analyze_run(*save())
    assert result["interruption_duration_s"] == 5
    assert result["recovery_to_first_message_s"] is None


def test_initial_connection_failure_is_not_a_midrun_interruption(dataset):
    _, _, events, save = dataset
    events[:] = [
        {"timestamp": iso_from_ns(at(seconds)), "unix_ns": at(seconds), "role": "subscriber", "event": event}
        for seconds, event in [(0, "DISCONNECTED"), (1, "RECONNECT_ATTEMPT"), (2, "CONNECTED")]
    ]
    result = analysis.analyze_run(*save())
    assert result["interruption_count"] == 0
    assert result["interruption_duration_s"] is None


def test_multiple_outages_preserve_first_detection_and_do_not_borrow_later_recovery_message():
    events = [
        {"event": event, "unix_ns": at(seconds)}
        for event, seconds in [("CONNECTED", 0), ("DISCONNECTED", 10), ("DISCONNECTED", 11),
                               ("RECONNECTED", 15), ("DISCONNECTED", 20), ("RECONNECTED", 25)]
    ]
    episodes = analysis.interruption_metrics(events, [{"received_at_unix_ns": at(25.5)}])
    assert len(episodes) == 2
    assert episodes[0]["interruption_duration_s"] == 5
    assert episodes[0]["recovery_to_first_message_s"] is None
    assert episodes[1]["recovery_to_first_message_s"] == 0.5


def test_unexpected_received_ids_do_not_reduce_loss(dataset):
    _, received, _, save = dataset
    received.append({"message_id": 99, "received_at": iso_from_ns(at(16)),
                     "received_at_unix_ns": at(16), "latency_ms": 1})
    result = analysis.analyze_run(*save())
    assert result["missing_ids"] == [3, 4, 5]
    assert result["loss_pct"] == 50
    assert result["unexpected_received_ids"] == [99]


def test_empty_logs_report_no_samples_or_invented_loss(dataset):
    pub, received, events, save = dataset
    pub.clear()
    received.clear()
    events.clear()
    result = analysis.analyze_run(*save())
    assert result["generated_samples"] == 0
    assert result["loss_pct"] is None
    assert result["latency_mean_ms"] is None


@pytest.mark.parametrize(
    "field,value",
    [("publish_attempted", "invalid"), ("message_id", "1.5"), ("generated_at", "2026-11-11"),
     ("sent_at_unix_ns", ""), ("qos", 3)],
)
def test_invalid_publisher_rows_fail_clearly(field, value, dataset):
    pub, _, _, save = dataset
    pub[0][field] = value
    with pytest.raises(ValueError, match="CSV line"):
        analysis.analyze_run(*save())


def test_duplicate_publisher_ids_are_not_miscounted_as_generated_samples(dataset):
    pub, _, _, save = dataset
    pub.append(dict(pub[0]))
    with pytest.raises(ValueError, match="exactly one row"):
        analysis.analyze_run(*save())


@pytest.mark.parametrize("latency", ["nan", "inf", "invalid"])
def test_invalid_subscriber_latencies_are_not_silently_ignored(latency, dataset):
    _, received, _, save = dataset
    received[0]["latency_ms"] = latency
    with pytest.raises(ValueError, match="CSV line"):
        analysis.analyze_run(*save())


def test_publisher_event_file_cannot_be_used_as_subscriber_timing(dataset):
    _, _, events, save = dataset
    events[0]["role"] = "publisher"
    with pytest.raises(ValueError, match="subscriber events"):
        analysis.analyze_run(*save())


def test_cli_writes_summary_csv_only_from_synthetic_inputs(dataset, tmp_path, capsys):
    pub, sub, events = dataset[3]()
    output = tmp_path / "processed"
    assert analyze.main([
        "--publisher", str(pub), "--subscriber", str(sub), "--events", str(events), "--output-dir", str(output),
    ]) == 0
    captured = capsys.readouterr()
    assert "Resumen del experimento" in captured.out
    assert captured.err == ""
    with next(output.glob("*.csv")).open(encoding="utf-8", newline="") as stream:
        row = next(csv.DictReader(stream))
    assert json.loads(row["missing_ids"]) == [3, 4, 5]
    assert json.loads(row["interruptions"])[0]["interruption_duration_s"] == 5
    assert row["generated_samples"] == "6"
    first = analysis.write_summary(analysis.analyze_run(pub, sub, events), output)
    assert len(list(output.glob("*.csv"))) == 2 and first.exists()


def test_cli_bad_inputs_do_not_create_summary(tmp_path, capsys):
    missing = str(tmp_path / "missing.csv")
    output = tmp_path / "processed"
    assert analyze.main([
        "--publisher", missing, "--subscriber", missing, "--events", missing, "--output-dir", str(output),
    ]) == 1
    assert "Error de analisis" in capsys.readouterr().err
    assert not output.exists()
