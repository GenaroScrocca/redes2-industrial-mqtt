import os
from unittest.mock import patch

import pytest
from dotenv import load_dotenv

from src.messaging import config, publisher, subscriber
from src.messaging.config import MqttConfig


def test_defaults_match_env_example():
    assert MqttConfig.from_env() == MqttConfig()


def test_environment_and_explicit_override_precedence(monkeypatch):
    monkeypatch.setenv("MQTT_BROKER_HOST", "broker")
    monkeypatch.setenv("MQTT_BROKER_PORT", "1884")
    monkeypatch.setenv("MQTT_TOPIC_BASE", "test/pump/")
    monkeypatch.setenv("MQTT_QOS", "1")
    assert MqttConfig.from_env() == MqttConfig("broker", 1884, "test/pump/telemetry", 1)
    assert MqttConfig.from_env(host="localhost", port=1885, topic="custom/exact", qos=0) == (
        MqttConfig("localhost", 1885, "custom/exact", 0)
    )


def test_dotenv_is_loaded_from_project_root_without_overriding_environment(tmp_path, monkeypatch):
    (tmp_path / ".env").write_text(
        "MQTT_BROKER_HOST=file-broker\nMQTT_BROKER_PORT=1884\n"
        "MQTT_TOPIC_BASE=test/p101\nMQTT_QOS=1\n", encoding="utf-8"
    )
    monkeypatch.setattr(config, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(config, "load_dotenv", load_dotenv)
    with patch.dict(os.environ, {"MQTT_BROKER_HOST": "environment-broker"}, clear=True):
        assert MqttConfig.from_env() == MqttConfig("environment-broker", 1884, "test/p101/telemetry", 1)


@pytest.mark.parametrize(
    "name,value",
    [
        ("MQTT_BROKER_HOST", " "), ("MQTT_BROKER_PORT", "0"),
        ("MQTT_BROKER_PORT", "65536"), ("MQTT_BROKER_PORT", "invalid"),
        ("MQTT_QOS", "3"), ("MQTT_TOPIC_BASE", ""),
        ("MQTT_TOPIC_BASE", "pump/#"), ("MQTT_TOPIC_BASE", "pump/+"),
    ],
)
def test_invalid_environment_is_rejected(name, value, monkeypatch):
    monkeypatch.setenv(name, value)
    with pytest.raises(ValueError):
        MqttConfig.from_env()


@pytest.mark.parametrize(
    "module,args",
    [
        (publisher, ["--samples", "0"]), (publisher, ["--interval", "-1"]),
        (publisher, ["--interval", "nan"]), (publisher, ["--interval", "inf"]),
        (publisher, ["--port", "70000"]), (publisher, ["--qos", "3"]),
        (subscriber, ["--expected-messages", "0"]), (subscriber, ["--topic", "pump/#"]),
    ],
)
def test_invalid_cli_arguments_fail_before_connecting(module, args, fake_client, capsys):
    with pytest.raises(SystemExit) as exc_info:
        module.main(args)
    assert exc_info.value.code == 2
    assert fake_client.connections == []
    assert "error:" in capsys.readouterr().err
