from __future__ import annotations

from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict


def _expand(path: str) -> Path:
    return Path(path).expanduser().resolve()


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    callback_store_dir: str = "/var/lib/auth-manager/callbacks"
    codex_profiles_dir: str = "/var/lib/auth-manager/legacy/profiles"
    usage_db_path: str = "/var/lib/auth-manager/legacy/auth-manager.sqlite3"
    database_url: str | None = None
    auth_encryption_key: str | None = None
    login_session_ttl_seconds: int = 600
    web_login_username: str | None = None
    web_login_password: str | None = None
    web_login_session_secret: str | None = None
    web_login_cookie_name: str = "auth_manager_session"
    web_login_session_ttl_seconds: int = 43200
    trusted_proxy_ips: str = ""
    internal_network_cidrs: str = (
        "127.0.0.1/32,10.0.0.0/8,172.16.0.0/12,192.168.0.0/16,::1/128,fc00::/7"
    )
    internal_api_token: str | None = None
    openai_organization: str | None = None
    openai_project: str | None = None
    openai_auth_url: str = "https://auth.openai.com/oauth/authorize"
    openai_token_url: str | None = None
    openai_client_id: str | None = None
    openai_client_secret: str | None = None
    openai_redirect_uri: str | None = None
    openai_scope: str = "openid profile email offline_access"
    openai_originator: str = "codex_cli_rs"
    openai_id_token_add_organizations: bool = True
    openai_codex_cli_simplified_flow: bool = True
    openai_rate_limits_url: str | None = None
    openai_account_url: str | None = None
    chatgpt_backend_rate_limits_url: str = "https://chatgpt.com/backend-api/codex/responses"
    chatgpt_backend_rate_limits_model: str = "gpt-5.1-codex-mini"
    chatgpt_backend_rate_limits_instructions: str = "You are Codex. Be concise."
    chatgpt_backend_rate_limits_prompt: str = "Reply with exactly OK."
    codex_bin: str = "codex"
    rate_limit_sync_interval_seconds: int = 300
    rate_limit_worker_timeout_seconds: int = 20
    live_rate_limit_worker_timeout_seconds: int = 8
    live_refresh_concurrency: int = 12
    analytics_timezone: str = "UTC"
    analytics_snapshot_interval_seconds: int = 600
    max_assignable_utilization_percent: float = 99.0
    rotation_request_threshold_percent: float = 90.0
    exhausted_utilization_percent: float = 100.0
    min_quota_remaining: int = 0
    allow_client_initiated_rotation: bool = True
    lease_default_ttl_seconds: int = 3600
    lease_renewal_min_remaining_seconds: int = 300
    lease_stale_after_seconds: int = 60
    lease_reclaim_after_seconds: int = 180
    weekly_reset_confirmation_required: bool = True
    auth_keepalive_in_app: bool = True

    def callback_dir(self) -> Path:
        return _expand(self.callback_store_dir)

    def profiles_dir(self) -> Path:
        return _expand(self.codex_profiles_dir)

    def usage_db_file(self) -> Path:
        return _expand(self.usage_db_path)


settings = Settings()
