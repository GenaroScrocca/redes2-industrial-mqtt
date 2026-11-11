"""Keep sampling through broker outages; record only observed completions."""

import argparse
import json
import sys
import time
from queue import Empty

import paho.mqtt.client as mqtt

from src.messaging.cli import interval_seconds, mqtt_config, positive_int
from src.simulator import PumpSimulator

from .client import ResilientClient
from .common import (
    EVENT_FIELDS, PUBLISHER_FIELDS, CsvJournal, add_experiment_arguments,
    drain_events, iso_from_ns, run_paths, wait_until,
)


class PublicationLog:
    """Callback queue avoids the on_publish-before-publish-return race.

    Offline samples are not queued. Paho may retry QoS>0 messages already in
    flight; their original transport timestamp is retained in that payload.
    """

    def __init__(self, session: ResilientClient, journal: CsvJournal) -> None:
        self.session, self.journal = session, journal
        self.pending: dict[int, dict] = {}

    def _record(self, row: dict) -> None:
        self.journal.write(row)
        print(
            f"RESULT message_id={row['message_id']} publish_attempted={int(row['publish_attempted'])} "
            f"publish_success={int(row['publish_success'])} status={row['publish_status']}", flush=True,
        )

    def submit(self, sample: dict) -> None:
        row = {
            "message_id": sample["message_id"], "generated_at": sample["timestamp"],
            "publish_attempted": False, "publish_success": False, "sent_at_unix_ns": None,
            "qos": self.session.config.qos, "publish_status": "not_attempted",
            "publish_return_code": None, "completed_at": None, "detail": "broker unavailable",
        }
        print(f"GENERATED message_id={row['message_id']}", flush=True)
        if not self.session.connected.is_set():
            self._record(row)
            return
        row.update(publish_attempted=True, sent_at_unix_ns=time.time_ns(), detail="")
        payload = {**sample, "sent_at_unix_ns": row["sent_at_unix_ns"]}
        try:
            info = self.session.client.publish(
                self.session.config.topic, json.dumps(payload, allow_nan=False),
                qos=self.session.config.qos, retain=False,
            )
        except KeyboardInterrupt:
            row.update(publish_status="unconfirmed", detail="Interrupted during publish; completion unknown")
            self._record(row)
            raise
        except (OSError, RuntimeError) as exc:
            row.update(publish_status="error", detail=str(exc))
            self._record(row)
            return
        row["publish_return_code"] = int(info.rc)
        # A QoS>0 publish racing a disconnect can still be retained by Paho.
        if info.rc == mqtt.MQTT_ERR_SUCCESS or (
            info.rc == mqtt.MQTT_ERR_NO_CONN and self.session.config.qos > 0
        ):
            row["publish_status"] = "pending"
            self.pending[info.mid] = row
            print(f"PUBLISH_ATTEMPT message_id={row['message_id']} awaiting_completion", flush=True)
        else:
            row.update(publish_status="error", detail=mqtt.error_string(info.rc))
            self._record(row)
        self.collect()

    def collect(self) -> None:
        while True:
            try:
                mid, success, completed_ns = self.session.acknowledgements.get_nowait()
            except Empty:
                return
            row = self.pending.pop(mid, None)
            if row is not None:
                row.update(
                    publish_success=success, publish_status="published" if success else "rejected",
                    completed_at=iso_from_ns(completed_ns),
                )
                self._record(row)

    def finish(self) -> None:
        self.collect()
        for row in self.pending.values():
            row.update(publish_status="unconfirmed", detail="No completion observed before shutdown")
            self._record(row)
        self.pending.clear()


def run_publisher(config, *, samples, interval, seed, retry_interval, drain_seconds, paths) -> int:
    session = ResilientClient(config, role="publisher", retry_interval=retry_interval)
    interrupted = False
    with CsvJournal(paths[0], PUBLISHER_FIELDS) as journal, CsvJournal(paths[1], EVENT_FIELDS) as events:
        print(f"Publisher CSV={paths[0]}\nPublisher events CSV={paths[1]}", flush=True)
        log = PublicationLog(session, journal)
        simulator = PumpSimulator(seed=seed)
        try:
            session.start()
            # Avoid an artificial first-sample loss while the initial CONNACK arrives.
            # A failed startup is bounded; sampling then continues offline as well.
            session.ready.wait(timeout=5)
            deadline = time.monotonic()
            for _ in range(samples):
                wait_until(deadline)
                drain_events(session, events)
                log.collect()
                log.submit(simulator.next_sample())
                deadline += interval
            end = time.monotonic() + drain_seconds
            while log.pending and time.monotonic() < end:
                log.collect()
                drain_events(session, events)
                time.sleep(0.02)
        except KeyboardInterrupt:
            interrupted = True
            print("\nPublisher experimental detenido.")
        finally:
            try:
                session.stop()
            finally:
                log.finish()
                drain_events(session, events)
    return 130 if interrupted else 0


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Experimental publisher for a manual broker interruption.")
    add_experiment_arguments(parser)
    parser.add_argument("-n", "--samples", type=positive_int, default=600)
    parser.add_argument("--interval", type=interval_seconds, default=0.1)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--drain-seconds", type=interval_seconds, default=2.0, help="final completion wait")
    args = parser.parse_args(argv)
    config = mqtt_config(parser, args)
    try:
        paths = run_paths(args, "publisher")
        return run_publisher(
            config, samples=args.samples, interval=args.interval, seed=args.seed,
            retry_interval=args.retry_interval, drain_seconds=args.drain_seconds, paths=paths,
        )
    except (OSError, ValueError, RuntimeError) as exc:
        print(f"Error publisher experimental: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
