"""Dayforce HCM REST API client (TAFW ingestion).

Adapted from ``bluedrop-mavenlink-sync/services/dayforce.py``. Same transport
behaviour, retargeted at feeding the Databricks staging table:

* HTTP Basic auth on a session that **keeps the ``Authorization`` header across
  Dayforce's discovery-host redirect** (``requests`` drops it by default).
* urllib3 ``Retry`` with exponential backoff on 429 / 5xx / connection errors,
  honouring ``Retry-After``.
* A process-wide token-bucket rate limiter (``pyrate_limiter``) capping request
  rate under Dayforce's published ~100 req/min, plus a small floor sleep.
* The ``TimeAwayFromWork`` endpoint accepts at most a 31-day window, so
  :meth:`iter_tafw_records` fans a sync out across consecutive windows spanning
  ``lookback_days`` back to ``horizon_days`` forward.

Endpoints used:
    GET  {base}/{company}/V1/Employees                     - roster
    GET  {base}/{company}/V1/Employees/{xref}/TimeAwayFromWork
"""

from __future__ import annotations

import logging
import time
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

import requests
from pyrate_limiter import Duration, Limiter, Rate
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

if TYPE_CHECKING:
    from tafw_ingest.config import Settings

logger = logging.getLogger(__name__)

__all__ = ["DayforceClient", "DayforceApiError"]

_RETRYABLE_STATUS = [*range(100, 200), 429, *range(500, 600)]


class DayforceApiError(RuntimeError):
    """A Dayforce request failed after retries (non-2xx or transport error)."""

    def __init__(self, message: str, *, status: int | None = None, body: Any = None):
        super().__init__(message)
        self.status = status
        self.body = body


class _PreserveAuthOnRedirectSession(requests.Session):
    """Keep ``Authorization`` across the ``dayforcehcm.com`` -> regional redirect.

    ``requests.Session.rebuild_auth`` strips the header on a cross-host
    redirect; Dayforce's entry host 302s to e.g. ``us-west-2.dayforcehcm.com``,
    which would then 401. Overriding to a no-op mirrors the axios
    ``beforeRedirect`` hook in the original JS client.
    """

    def rebuild_auth(self, prepared_request: Any, response: Any) -> None:  # noqa: D102
        return


def _iso8601(dt: datetime) -> str:
    """UTC, millisecond precision, trailing ``Z`` - Dayforce's ``filterTAFW*`` shape."""
    dt = dt.astimezone(UTC)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


def _start_of_day(dt: datetime) -> datetime:
    return dt.astimezone(UTC).replace(hour=0, minute=0, second=0, microsecond=0)


def _end_of_day(dt: datetime) -> datetime:
    return dt.astimezone(UTC).replace(hour=23, minute=59, second=59, microsecond=999_000)


class DayforceClient:
    """Thin REST wrapper around the Dayforce HCM Web Services API."""

    STATUS_APPROVED = "APPROVED"
    STATUS_PENDING = "PENDING"
    STATUS_CANCELED = "CANCELED"
    STATUS_DENIED = "DENIED"
    STATUS_CANCELPENDING = "CANCELPENDING"

    BASE_URI = "https://dayforcehcm.com/Api"
    TEST_BASE_URI = "https://canconfig63.dayforcehcm.com/Api"
    API_VERSION = "v1"

    def __init__(
        self,
        username: str,
        password: str,
        company_name: str,
        *,
        base_uri: str | None = None,
        api_version: str | None = None,
        test_mode: bool = False,
        timeout: float = 30.0,
        max_rpm: int = 90,
        throttle_seconds: float = 0.2,
        lookback_days: int = 30,
        horizon_days: int = 90,
        window_days: int = 30,
        session: requests.Session | None = None,
    ) -> None:
        self.username = username
        self.password = password
        self.company_name = company_name
        self.timeout = timeout
        self.throttle_seconds = throttle_seconds
        self.lookback_days = lookback_days
        self.horizon_days = horizon_days
        self.window_days = min(window_days, 31)

        base = base_uri or (self.TEST_BASE_URI if test_mode else self.BASE_URI)
        version = (api_version or self.API_VERSION).strip("/")
        # e.g. https://cantest261-services.dayforcehcm.com/api/bluedrop/v1/
        self.api_base = f"{base.rstrip('/')}/{company_name}/{version}/"

        # Token bucket shared by every call this client makes. Databricks runs
        # one job at a time, so a per-process limiter is enough to stay under
        # Dayforce's tenant ceiling.
        self._limiter = Limiter(Rate(max(1, max_rpm), Duration.MINUTE))

        self._http = session or self._build_session()
        self._http.auth = (username, password)

    @classmethod
    def from_settings(
        cls, settings: Settings, *, session: requests.Session | None = None
    ) -> DayforceClient:
        return cls(
            settings.dayforce_username,
            settings.dayforce_password,
            settings.dayforce_company,
            base_uri=settings.dayforce_base_uri,
            api_version=settings.dayforce_api_version,
            test_mode=settings.dayforce_test_mode,
            max_rpm=settings.dayforce_max_rpm,
            throttle_seconds=settings.dayforce_throttle_seconds,
            lookback_days=settings.tafw_lookback_days,
            horizon_days=settings.tafw_horizon_days,
            window_days=settings.tafw_window_days,
            session=session,
        )

    # ------------------------------------------------------------------ #
    @staticmethod
    def _build_session() -> requests.Session:
        retry = Retry(
            total=4,
            connect=2,
            read=2,
            status=4,
            backoff_factor=1.0,
            status_forcelist=_RETRYABLE_STATUS,
            allowed_methods=frozenset({"GET", "HEAD", "OPTIONS"}),
            respect_retry_after_header=True,
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry)
        session = _PreserveAuthOnRedirectSession()
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def _url(self, endpoint: str) -> str:
        # `Paging.Next` from a Dayforce list response is an absolute URL - pass
        # it through untouched; otherwise resolve against the tenant api base.
        if endpoint.startswith(("http://", "https://")):
            return endpoint
        return self.api_base + endpoint.lstrip("/")

    def _get(self, endpoint: str, params: dict[str, Any] | None = None) -> Any:
        """Rate-limited GET -> parsed JSON, or raise :class:`DayforceApiError`."""
        self._limiter.try_acquire("dayforce")
        if self.throttle_seconds > 0:
            time.sleep(self.throttle_seconds)
        try:
            response = self._http.get(
                self._url(endpoint), params=params or None, timeout=self.timeout
            )
        except requests.RequestException as err:  # transport failure after retries
            raise DayforceApiError(f"GET {endpoint} failed: {err}") from err

        if not response.ok:
            body: Any
            try:
                body = response.json()
            except ValueError:
                body = response.text
            raise DayforceApiError(
                f"GET {endpoint} -> HTTP {response.status_code}",
                status=response.status_code,
                body=body,
            )
        return response.json()

    # ------------------------------------------------------------------ #
    # Roster                                                            #
    # ------------------------------------------------------------------ #
    #: Safety valve so a malformed ``Paging.Next`` chain can't loop forever.
    MAX_PAGES = 10_000

    @staticmethod
    def _next_page_url(payload: Any) -> str | None:
        """Extract the follow-on URL from a Dayforce list response, if any.

        Dayforce returns ``{"Data": [...], "Paging": {"Next": "<url>"}}``. The
        ``Next`` value is normally a plain URL string; tolerate an empty string
        (last page) and a ``{"href": ...}`` wrapper just in case.
        """
        paging = payload.get("Paging") if isinstance(payload, dict) else None
        if not isinstance(paging, dict):
            return None
        nxt = paging.get("Next")
        if isinstance(nxt, dict):
            nxt = nxt.get("href")
        nxt = (nxt or "").strip() if isinstance(nxt, str) else None
        return nxt or None

    def _iter_pages(self, endpoint: str, params: dict[str, Any]) -> Iterator[Any]:
        """Yield each page's parsed body, following ``Paging.Next`` to the end."""
        url: str | None = endpoint
        first = True
        for page_num in range(1, self.MAX_PAGES + 1):
            if url is None:
                return
            # `params` only apply to the first request; `Next` already encodes
            # the page size, filters, and cursor.
            payload = self._get(url, params=params if first else None)
            yield payload
            first = False
            url = self._next_page_url(payload)
            if url is not None:
                logger.debug("Employees: following page %d", page_num + 1)
        logger.warning("Employees pagination hit MAX_PAGES=%d; stopping", self.MAX_PAGES)

    def iter_employees(
        self, *, page_size: int | None = None, **query_params: Any
    ) -> Iterator[dict[str, Any]]:
        """Yield every employee object from ``GET /Employees`` across all pages.

        The bare endpoint returns objects carrying ``XRefCode`` (and whatever
        else the tenant includes); pass Dayforce filter params such as
        ``filterHireStartDate`` / ``contextDate`` via ``**query_params``.
        """
        params: dict[str, Any] = dict(query_params)
        if page_size is not None:
            params["pageSize"] = page_size

        total = 0
        for payload in self._iter_pages("Employees", params):
            data = payload.get("Data") if isinstance(payload, dict) else payload
            for row in data or []:
                if isinstance(row, dict):
                    total += 1
                    yield row
        logger.info("Employees endpoint returned %d record(s)", total)

    def get_all_employees(self, **query_params: Any) -> list[dict[str, Any]]:
        """List form of :meth:`iter_employees` - every employee, all pages."""
        return list(self.iter_employees(**query_params))

    def list_employee_xrefs(self, **query_params: Any) -> list[str]:
        """Return every ``XRefCode`` from ``GET /Employees`` (paginated)."""
        return [
            str(row["XRefCode"])
            for row in self.iter_employees(**query_params)
            if row.get("XRefCode")
        ]

    def get_employee(
        self, xref_code: str, *, expand: str | None = None, **query_params: Any
    ) -> dict[str, Any]:
        """Return one employee's detail record from ``GET /Employees/{xref}``.

        Pass ``expand`` (comma-separated) to pull sub-collections, e.g.
        ``expand="WorkAssignments,EmploymentStatuses,Locations"`` - needed to see
        the org/department assignment.
        """
        params = dict(query_params)
        if expand:
            params["expand"] = expand
        payload = self._get(f"Employees/{xref_code}", params=params)
        data = payload.get("Data") if isinstance(payload, dict) else None
        return data if isinstance(data, dict) else (payload or {})

    # ------------------------------------------------------------------ #
    # TAFW                                                              #
    # ------------------------------------------------------------------ #
    def _windows(self, now: datetime | None = None) -> list[tuple[datetime, datetime]]:
        now = now or datetime.now(UTC)
        cursor = _start_of_day(now - timedelta(days=self.lookback_days))
        far_end = _end_of_day(now + timedelta(days=self.horizon_days))
        windows: list[tuple[datetime, datetime]] = []
        while cursor <= far_end:
            end = min(_end_of_day(cursor + timedelta(days=self.window_days - 1)), far_end)
            windows.append((cursor, end))
            cursor = _start_of_day(end + timedelta(days=1))
        return windows

    def get_tafw_records_for_employee(
        self,
        xref_code: str,
        status_type: str,
        start_date: datetime,
        end_date: datetime,
    ) -> list[dict[str, Any]]:
        """One employee, one status, one <=31-day window -> list of raw entries."""
        params = {
            "filterTAFWStartDate": _iso8601(start_date),
            "filterTAFWEndDate": _iso8601(end_date),
            "status": status_type,
        }
        payload = self._get(f"Employees/{xref_code}/TimeAwayFromWork", params=params)
        data = payload.get("Data") if isinstance(payload, dict) else None
        return list(data) if isinstance(data, list) else []

    def iter_tafw_records(
        self, xref_code: str, status_type: str, *, now: datetime | None = None
    ) -> Iterator[dict[str, Any]]:
        """Yield every raw TAFW entry for one employee/status across the full span."""
        for start, end in self._windows(now):
            yield from self.get_tafw_records_for_employee(
                xref_code, status_type, start, end
            )

    def get_tafw_records_for_employee_in_timeframe(
        self, xref_code: str, status_type: str, *, now: datetime | None = None
    ) -> list[dict[str, Any]]:
        """List form of :meth:`iter_tafw_records` (name kept from the old client)."""
        return list(self.iter_tafw_records(xref_code, status_type, now=now))
