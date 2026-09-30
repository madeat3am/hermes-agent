"""Constructor must reject malformed security configuration before model setup."""
import pytest
from unittest.mock import patch
from run_agent import AIAgent

@pytest.mark.parametrize("config", [
    {"agent": {"output_release": False}},
    {"agent": {"output_release": {"required_policy": False}}},
    {"agent": {"output_release": {"required_policy": []}}},
    {"agent": {"output_release": {"required_policy": ""}}},
    {"agent": {"output_release": {"required_policy": "bad token"}}},
    {"agent": {"output_release": {"required_policy": "a/b", "typo": True}}},
])
def test_bad_policy_precedes_model_initialization(monkeypatch, config):
    monkeypatch.setattr("hermes_cli.config.load_config_readonly", lambda: config)
    with patch("agent.agent_init._build_client", side_effect=AssertionError("MODEL INIT REACHED")):
        with pytest.raises(ValueError, match="Output release configuration invalid"):
            AIAgent(api_key="test", quiet_mode=True, skip_memory=True)

def test_unreadable_config_cannot_silently_disable_required_policy(monkeypatch):
    def broken():
        raise RuntimeError("private config detail")
    monkeypatch.setattr("hermes_cli.config.load_config_readonly", broken)
    with patch("agent.agent_init._build_client", side_effect=AssertionError("MODEL INIT REACHED")):
        with pytest.raises(ValueError, match="Output release configuration invalid"):
            AIAgent(api_key="test", quiet_mode=True, skip_memory=True)
