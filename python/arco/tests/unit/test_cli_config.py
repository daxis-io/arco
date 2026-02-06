"""Tests for CLI configuration."""

from __future__ import annotations

import os
from unittest.mock import patch


class TestArcoFlowConfig:
    """Tests for ArcoFlowConfig."""

    def test_default_values(self) -> None:
        """Config has sensible defaults."""
        from arco_flow.cli.config import ArcoFlowConfig

        with patch.dict(os.environ, {}, clear=True):
            config = ArcoFlowConfig()

        assert config.api_url == "https://api.arco.dev"
        assert config.workspace_id == "default"

    def test_from_environment(self) -> None:
        """Config reads from environment variables."""
        from arco_flow.cli.config import ArcoFlowConfig

        env = {
            "ARCO_FLOW_API_URL": "https://custom.api.dev",
            "ARCO_FLOW_TENANT_ID": "acme",
            "ARCO_FLOW_WORKSPACE_ID": "production",
            "ARCO_FLOW_API_KEY": "secret-key",
        }

        with patch.dict(os.environ, env, clear=True):
            config = ArcoFlowConfig()

        assert config.api_url == "https://custom.api.dev"
        assert config.tenant_id == "acme"
        assert config.workspace_id == "production"
        assert config.api_key.get_secret_value() == "secret-key"

    def test_validate_for_deploy_without_api_key(self) -> None:
        """Validation fails without API key for non-dry-run deploy."""
        from arco_flow.cli.config import ArcoFlowConfig

        with patch.dict(os.environ, {}, clear=True):
            config = ArcoFlowConfig()

        errors = config.validate_for_deploy()
        assert any("API key" in e for e in errors)

    def test_validate_for_deploy_success(self) -> None:
        """Validation passes with required config."""
        from arco_flow.cli.config import ArcoFlowConfig

        env = {
            "ARCO_FLOW_API_KEY": "sk-test-key",
            "ARCO_FLOW_TENANT_ID": "acme",
        }

        with patch.dict(os.environ, env, clear=True):
            config = ArcoFlowConfig()

        errors = config.validate_for_deploy()
        assert errors == []


class TestGetConfig:
    """Tests for get_config function."""

    def test_returns_config(self) -> None:
        """get_config returns a ArcoFlowConfig instance."""
        from arco_flow.cli.config import clear_config_cache, get_config

        clear_config_cache()
        config = get_config()
        assert config is not None
        assert hasattr(config, "api_url")
