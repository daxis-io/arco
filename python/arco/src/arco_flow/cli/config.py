"""CLI configuration using Pydantic settings.

Configuration is loaded from:
1. Environment variables (ARCO_FLOW_* prefix)
2. .env file in current directory
3. Default values
"""

from __future__ import annotations

from functools import lru_cache

from pydantic import SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class ArcoFlowConfig(BaseSettings):
    """Configuration for Arco Flow CLI.

    Environment variables are prefixed with ARCO_FLOW_.
    """

    model_config = SettingsConfigDict(
        env_prefix="ARCO_FLOW_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # API configuration
    api_url: str = "https://api.arco.dev"
    api_key: SecretStr = SecretStr("")
    task_token: SecretStr = SecretStr("")

    # Tenant/workspace scope
    tenant_id: str = ""
    workspace_id: str = "default"
    user_id: str = ""

    # Local development
    debug: bool = False
    log_level: str = "INFO"

    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        """Validate log level is valid."""
        valid_levels = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        upper = v.upper()
        if upper not in valid_levels:
            msg = f"Invalid log level: {v}. Must be one of {valid_levels}"
            raise ValueError(msg)
        return upper

    def validate_for_deploy(self) -> list[str]:
        """Validate configuration for deployment.

        Returns:
            List of validation error messages (empty if valid).
        """
        errors: list[str] = []

        if not self.debug and not self.api_key.get_secret_value():
            errors.append("API key not configured. Set ARCO_FLOW_API_KEY environment variable.")

        if not self.tenant_id:
            errors.append("Tenant ID not configured. Set ARCO_FLOW_TENANT_ID environment variable.")

        return errors

    def validate_for_run(self) -> list[str]:
        """Validate configuration for triggering runs.

        Returns:
            List of validation error messages (empty if valid).
        """
        errors = self.validate_for_deploy()
        return errors


@lru_cache
def get_config() -> ArcoFlowConfig:
    """Get the global configuration.

    Configuration is cached after first load.

    Returns:
        ArcoFlowConfig instance.
    """
    return ArcoFlowConfig()


def clear_config_cache() -> None:
    """Clear the configuration cache (for testing)."""
    get_config.cache_clear()
