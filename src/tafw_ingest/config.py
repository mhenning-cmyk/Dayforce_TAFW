"""Runtime configuration for the ingestion service.

One :class:`Settings` model, three constructors by environment:

* :meth:`Settings.from_env`        - local dev: optional YAML defaults, then
  environment / ``.env`` (``TAFW_`` / ``DAYFORCE_`` prefixes), then kwargs.
* :meth:`Settings.from_databricks` - the notebook: job widgets for the knobs,
  a Databricks **secret scope** for the Dayforce credentials.
* ``Settings(...)`` directly       - tests.

Nothing else in the package reads ``os.environ`` or ``dbutils`` - inject a
``Settings`` instance instead.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Annotated, Any

import yaml
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict

# List fields accept a plain comma-separated env var (TAFW_STATUSES=APPROVED,DENIED).
# NoDecode stops pydantic-settings from trying to JSON-parse the value first;
# _coerce_csv below turns the raw string into a list.
CsvList = Annotated[list[str], NoDecode]

__all__ = ["Settings"]


#: OS keyring service name for local-dev Dayforce credentials. Populate with:
#:   keyring set dayforce-tafw username
#:   keyring set dayforce-tafw password
KEYRING_SERVICE = "dayforce-tafw"


def _keyring_creds(service: str = KEYRING_SERVICE) -> dict[str, str]:
    """Best-effort read of ``username`` / ``password`` from the OS keyring.

    Returns ``{}`` if ``keyring`` is not installed or has no usable backend
    (e.g. a bare WSL shell) - callers treat missing creds as "not configured".
    """
    try:
        import keyring
    except ModuleNotFoundError:
        return {}
    found: dict[str, str] = {}
    for key in ("username", "password"):
        try:
            value = keyring.get_password(service, key)
        except Exception:  # noqa: BLE001 - locked/unavailable backend
            value = None
        if value:
            found[key] = value
    return found


def _split_csv(value: Any) -> list[str]:
    if value in (None, "", []):
        return []
    if isinstance(value, list | tuple):
        return [str(v).strip() for v in value if str(v).strip()]
    return [part.strip() for part in str(value).split(",") if part.strip()]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    # --- Dayforce connection ---
    # base_uri + company + api_version compose the endpoint root:
    #   {base_uri}/{company}/{api_version}/Employees
    #   https://cantest261-services.dayforcehcm.com/api / bluedrop / v1 / Employees
    dayforce_base_uri: str = "https://dayforcehcm.com/Api"
    dayforce_company: str = "bluedrop"
    dayforce_api_version: str = "v1"
    dayforce_username: str = ""
    dayforce_password: str = ""
    dayforce_auth_mode: str = "basic"
    dayforce_test_mode: bool = False

    # --- pacing / windows ---
    # Dayforce's documented ceiling is 10 req/sec and 100 req/min, applied at
    # the whole-client level (see tafw_ingest.dayforce_client's module
    # docstring) - the le= bounds here keep a misconfigured value from
    # exceeding that ceiling outright. Turn these down further if another
    # integration shares this Dayforce client/service account.
    dayforce_max_rps: int = Field(8, ge=1, le=10)
    dayforce_max_rpm: int = Field(90, ge=1, le=100)
    dayforce_throttle_seconds: float = Field(0.2, ge=0)
    tafw_lookback_days: int = Field(30, ge=0)
    tafw_horizon_days: int = Field(90, ge=1)
    tafw_statuses: CsvList = ["APPROVED"]
    tafw_employee_xrefs: CsvList = []
    tafw_expand_multi_day: bool = False

    # --- staging target ---
    writer: str = Field("memory", pattern="^(memory|local_delta|databricks)$")
    staging_table: str = "staging.tafw_day_record"
    local_delta_path: str = "./spark-warehouse/tafw_day_record"

    @field_validator("tafw_statuses", "tafw_employee_xrefs", mode="before")
    @classmethod
    def _coerce_csv(cls, v: Any) -> list[str]:
        return _split_csv(v)

    @field_validator("dayforce_auth_mode")
    @classmethod
    def _check_auth(cls, v: str) -> str:
        v = v.lower().strip()
        if v not in {"basic", "oauth2"}:
            raise ValueError("dayforce_auth_mode must be 'basic' or 'oauth2'")
        if v == "oauth2":
            raise ValueError("oauth2 auth is not implemented yet; use 'basic'")
        return v

    # ------------------------------------------------------------------ #
    @classmethod
    def from_env(
        cls, config_path: str | os.PathLike[str] | None = None, **overrides: Any
    ) -> Settings:
        """Resolve settings for local / CLI use.

        Precedence, low to high: YAML defaults file -> OS keyring (Dayforce
        credentials only) -> environment / ``.env`` -> explicit ``overrides``.
        The keyring is only consulted for ``dayforce_username`` /
        ``dayforce_password`` and only when they are not already set, so a CI
        env var or an explicit override always wins.
        """
        base: dict[str, Any] = {}
        if config_path:
            path = Path(config_path)
            if path.is_file():
                loaded = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
                base.update({k.lower(): v for k, v in loaded.items()})
        base.update(overrides)

        settings = cls(**base)
        if not (settings.dayforce_username and settings.dayforce_password):
            creds = _keyring_creds()
            patch: dict[str, str] = {}
            if not settings.dayforce_username and creds.get("username"):
                patch["dayforce_username"] = creds["username"]
            if not settings.dayforce_password and creds.get("password"):
                patch["dayforce_password"] = creds["password"]
            if patch:
                settings = settings.model_copy(update=patch)
        return settings

    @classmethod
    def from_databricks(
        cls,
        dbutils: Any,
        *,
        secret_scope: str = "dayforce",
        widget_prefix: str = "",
        **overrides: Any,
    ) -> Settings:
        """Build from notebook widgets + a Databricks secret scope.

        Expected widgets (all optional, sensible defaults): ``run_mode``,
        ``lookback_days``, ``horizon_days``, ``statuses``, ``employee_xrefs``,
        ``staging_table``, ``max_rps``, ``max_rpm``, ``expand_multi_day``.

        Expected secrets in ``secret_scope``: ``username``, ``password``,
        optionally ``company``, ``base_uri``.
        """

        def widget(name: str, default: str = "") -> str:
            try:
                return dbutils.widgets.get(widget_prefix + name)
            except Exception:  # noqa: BLE001 - widget not defined
                return default

        def secret(name: str, default: str = "") -> str:
            try:
                return dbutils.secrets.get(scope=secret_scope, key=name)
            except Exception:  # noqa: BLE001 - secret absent
                return default

        values: dict[str, Any] = {
            "writer": "databricks",
            "dayforce_username": secret("username"),
            "dayforce_password": secret("password"),
            "dayforce_company": secret("company", "bluedrop"),
            "dayforce_base_uri": secret(
                "base_uri", cls.model_fields["dayforce_base_uri"].default
            ),
            "dayforce_test_mode": widget("test_mode", "false").lower() == "true",
        }
        for widget_name, field_name in {
            "lookback_days": "tafw_lookback_days",
            "horizon_days": "tafw_horizon_days",
            "max_rps": "dayforce_max_rps",
            "max_rpm": "dayforce_max_rpm",
            "statuses": "tafw_statuses",
            "employee_xrefs": "tafw_employee_xrefs",
            "staging_table": "staging_table",
            "expand_multi_day": "tafw_expand_multi_day",
        }.items():
            raw = widget(widget_name)
            if raw != "":
                values[field_name] = raw
        values.update(overrides)
        return cls(**values)
