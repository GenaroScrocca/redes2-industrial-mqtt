import csv

import pytest


@pytest.fixture
def make_csv(tmp_path):
    """Write only synthetic inputs beneath pytest's temporary directory."""
    def create(name, rows, *, encoding="utf-8"):
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding=encoding, newline="") as stream:
            writer = csv.writer(stream)
            writer.writerow(["message_id", "latency_ms"])
            writer.writerows(rows)
        return path

    return create
