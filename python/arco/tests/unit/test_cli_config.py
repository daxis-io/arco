"""Tests for CLI configuration."""
from __future__ import annotations

import os
from unittest.mock import patch


class TestServoConfig:
    """Tests for ServoConfig."""

    def test_default_values(self) -> None:
        """Config has sensible defaults."""
        from servo.cli.config import ServoConfig

        with patch.dict(os.environ, {}, clear=True):
            config = ServoConfig()

        assert config.servo_api_url == "https://api.servo.dev"
        assert config.workspace_id == "default"

    def test_from_environment(self) -> None:
        """Config reads from environment variables."""
        from servo.cli.config import ServoConfig

        env = {
            "SERVO_API_URL": "https://custom.api.dev",
            "SERVO_TENANT_ID": "acme",
            "SERVO_WORKSPACE_ID": "production",
            "SERVO_API_KEY": "secret-key",
        }

        with patch.dict(os.environ, env, clear=True):
            config = ServoConfig()

        assert config.servo_api_url == "https://custom.api.dev"
        assert config.tenant_id == "acme"
        assert config.workspace_id == "production"
        assert config.api_key.get_secret_value() == "secret-key"

    def test_validate_for_deploy_without_api_key(self) -> None:
        """Validation fails without API key for non-dry-run deploy."""
        from servo.cli.config import ServoConfig

        with patch.dict(os.environ, {}, clear=True):
            config = ServoConfig()

        errors = config.validate_for_deploy()
        assert any("API key" in e for e in errors)

    def test_validate_for_deploy_success(self) -> None:
        """Validation passes with required config."""
        from servo.cli.config import ServoConfig

        env = {
            "SERVO_API_KEY": "sk-test-key",
            "SERVO_TENANT_ID": "acme",
        }

        with patch.dict(os.environ, env, clear=True):
            config = ServoConfig()

        errors = config.validate_for_deploy()
        assert errors == []


class TestGetConfig:
    """Tests for get_config function."""

    def test_returns_config(self) -> None:
        """get_config returns a ServoConfig instance."""
        from servo.cli.config import clear_config_cache, get_config

        clear_config_cache()
        config = get_config()
        assert config is not None
        assert hasattr(config, "servo_api_url")
