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

    codex_cli_bin: str = "codex"
    codex_auth_path: str = "/root/.codex/auth.json"
    callback_store_dir: str = "/root/.codex-switch/callbacks"
    codex_profiles_dir: str = "/root/.codex-switch/profiles"
    usage_db_path: str = "/root/.codex-switch/auth-manager.sqlite3"
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
    openai_token_url: str | None = None
    openai_client_id: str | None = None
    openai_client_secret: str | None = None
    openai_redirect_uri: str | None = None

    def codex_auth_file(self) -> Path:
        return _expand(self.codex_auth_path)

    def callback_dir(self) -> Path:
        return _expand(self.callback_store_dir)

    def profiles_dir(self) -> Path:
        return _expand(self.codex_profiles_dir)

    def usage_db_file(self) -> Path:
        return _expand(self.usage_db_path)


settings = Settings()
