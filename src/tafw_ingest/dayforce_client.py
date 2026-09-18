"""Dayforce HCM REST API client (TAFW ingestion).

Adapted from ``bluedrop-mavenlink-sync/services/dayforce.py``. Same transport
behaviour, retargeted at feeding the Databricks staging table:

* HTTP Basic auth on a session that **keeps the ``Authorization`` header across
  Dayforce's discovery-host redirect** (``requests`` drops it by default).
* urllib3 ``Retry`` with exponential backoff on 429 / 5xx / connection errors,
  honouring ``Retry-After``.
* :meth:`iter_tafw_records` queries the whole ``lookback_days`` back to
  ``horizon_days`` forward span in one request per employee/status; the
  endpoint was assumed to cap windows at 31 days (inherited, uncited, from
  the original ``bluedrop-mavenlink-sync`` client) but live testing against
  a full year showed that limit doesn't hold, so this client no longer
  chunks the date range itself. ``get_tafw_records_for_employee`` still
  follows ``Paging.Next`` for large result sets within one query.

Rate limiting
-------------
Per Dayforce's "RESTful Rate Limiter" guide (help.dayforce.com, under
Dayforce-Web-Services-Introduction-Guide/RESTful-Rate-Limiter):

* The published ceiling is **10 requests/second and 100 requests/minute**,
  described in terms of "employee XRefCodes" - i.e. each employee-scoped call
  (``Employees/{xref}/...``) counts as one unit. Heavier operations (the doc
  names ``Get Reports``) have a lower, unspecified threshold.
* The limit applies **at the client level** (the whole service-account/tenant
  pairing), not per user or per endpoint - if another integration shares this
  Dayforce client, this service is not entitled to the full budget alone.
  ``dayforce_max_rps`` / ``dayforce_max_rpm`` should be turned down to leave
  that other integration room.
* Dayforce does **not** document a guaranteed HTTP 429 or ``Retry-After``
  header for a denial - only that an over-limit request "will be denied ...
  with a message noting that the limit has been reached." The guide asks
  consuming applications to (a) track their own request count and stay under
  the limit rather than relying on being told off, and (b) queue/back off
  when denied rather than failing outright.

This client follows both halves of that:

* A dual-tier token-bucket limiter (``pyrate_limiter``, 10/sec AND 100/min,
  configurable and clamped below those ceilings - see ``max_rps``/``max_rpm``)
  gates every request *before* it is sent, so we typically never hit the
  server-side limit at all. When the bucket is full it sleeps until a slot
  frees up (bounded by ``_RATE_LIMIT_MAX_QUEUE_DELAY_MS``) rather than
  raising, i.e. it queues by default.
* Because a denial's shape isn't guaranteed, ``_get`` also inspects the body
  of an *ok* (2xx) response for rate-limit language and treats that the same
  as a 429 would be: back off and retry (bounded by
  ``MAX_RATE_LIMIT_RETRIES``), rather than trusting it as real data.
* A large batch (many employees) is expected to take a while - Dayforce's own
  example is ~60 minutes for 6,000 employees at 100/min - so this is paced by
  simple sequential blocking, not by a single request. Callers should not add
  concurrency (threads/async) around this client without routing it through
  the same shared limiter, or the rate guarantee no longer holds.

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
from pyrate_limiter import (
    BucketFullException,
    Duration,
    Limiter,
    LimiterDelayException,
    Rate,
)
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

if TYPE_CHECKING:
    from tafw_ingest.config import Settings

logger = logging.getLogger(__name__)

__all__ = ["DayforceClient", "DayforceApiError"]

_RETRYABLE_STATUS = [*range(100, 200), 429, *range(500, 600)]

#: Dayforce's documented ceiling (see module docstring); constructor args are
#: clamped to these so a misconfigured setting can't exceed the real limit.
_DAYFORCE_MAX_RPS_CEILING = 10
_DAYFORCE_MAX_RPM_CEILING = 100

#: How long the limiter may sleep queuing a single request before giving up
#: and raising loudly. A rolling per-minute bucket never needs more than
#: ~60s for a slot to free up, so this is a generous multiple of that, not a
#: cap on how long a whole multi-employee batch may run (that's just many
#: sequential waits, per Dayforce's own ~60-minutes-for-6000-employees example).
_RATE_LIMIT_MAX_QUEUE_DELAY_MS = 120_000

#: Phrases that show up if Dayforce denies a request "with a message" instead
#: of (or alongside) a conventional error status - see module docstring.
_RATE_LIMIT_DENIAL_MARKERS = (
    "rate limit",
    "too many requests",
    "threshold",
    "limit has been reached",
    "limit reached",
)


def _rate_limit_denial_text(payload: Any) -> str | None:
    """Return the offending message if ``payload`` looks like a rate-limit
    denial dressed up as a normal response, else ``None``."""
    if not isinstance(payload, dict):
        return None
    values: list[str] = []
    for key in ("Message", "Messages", "Error", "error", "errorMessage"):
        value = payload.get(key)
        if isinstance(value, str):
            values.append(value)
        elif isinstance(value, list):
            values.extend(v for v in value if isinstance(v, str))
    for text in values:
        if any(marker in text.lower() for marker in _RATE_LIMIT_DENIAL_MARKERS):
            return text
    return None


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
        max_rps: int = 8,
        max_rpm: int = 90,
        throttle_seconds: float = 0.2,
        lookback_days: int = 30,
        horizon_days: int = 90,
        session: requests.Session | None = None,
    ) -> None:
        self.username = username
        self.password = password
        self.company_name = company_name
        self.timeout = timeout
        self.throttle_seconds = throttle_seconds
        self.lookback_days = lookback_days
        self.horizon_days = horizon_days
        self.max_rps = min(max(1, max_rps), _DAYFORCE_MAX_RPS_CEILING)
        self.max_rpm = min(max(1, max_rpm), _DAYFORCE_MAX_RPM_CEILING)

        base = base_uri or (self.TEST_BASE_URI if test_mode else self.BASE_URI)
        version = (api_version or self.API_VERSION).strip("/")
        # e.g. https://cantest261-services.dayforcehcm.com/api/bluedrop/v1/
        self.api_base = f"{base.rstrip('/')}/{company_name}/{version}/"

        # Token bucket shared by every call this client makes. Databricks runs
        # one job at a time, so a per-process limiter is enough to stay under
        # Dayforce's tenant ceiling. Dual-tier (per-second AND per-minute) to
        # match Dayforce's documented "10/sec, 100/min" shape rather than
        # approximating it with a per-minute-only average that could still
        # burst past the per-second cap. max_delay makes it queue (sleep)
        # instead of failing when a tier is momentarily full - see module
        # docstring for why 120s is a safe, generous bound.
        self._limiter = Limiter(
            [Rate(self.max_rps, Duration.SECOND), Rate(self.max_rpm, Duration.MINUTE)],
            max_delay=_RATE_LIMIT_MAX_QUEUE_DELAY_MS,
        )

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
            max_rps=settings.dayforce_max_rps,
            max_rpm=settings.dayforce_max_rpm,
            throttle_seconds=settings.dayforce_throttle_seconds,
            lookback_days=settings.tafw_lookback_days,
            horizon_days=settings.tafw_horizon_days,
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

    #: Bound on retries when Dayforce denies a request "with a message"
    #: instead of a conventional 429 (see module docstring / _rate_limit_denial_text).
    MAX_RATE_LIMIT_RETRIES = 5

    def _acquire_rate_limit_slot(self) -> None:
        """Block (if needed) until the shared limiter has room for one more
        request. Only raises if the wait would exceed
        ``_RATE_LIMIT_MAX_QUEUE_DELAY_MS`` - i.e. something is wrong, not just
        "the bucket is momentarily full"."""
        try:
            self._limiter.try_acquire("dayforce")
        except (BucketFullException, LimiterDelayException) as err:
            raise DayforceApiError(
                f"Dayforce client-side rate limit could not be satisfied: {err}"
            ) from err

    def _get(self, endpoint: str, params: dict[str, Any] | None = None) -> Any:
        """Rate-limited GET -> parsed JSON, or raise :class:`DayforceApiError`.

        Backs off and retries if Dayforce's response looks like a rate-limit
        denial, even when it arrives as an ok-looking response rather than a
        429 (Dayforce's guide doesn't promise either shape - see module
        docstring).
        """
        last_denial = ""
        for attempt in range(1, self.MAX_RATE_LIMIT_RETRIES + 1):
            self._acquire_rate_limit_slot()
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

            payload = response.json()
            last_denial = _rate_limit_denial_text(payload) or ""
            if not last_denial:
                return payload

            if attempt < self.MAX_RATE_LIMIT_RETRIES:
                backoff = min(2**attempt, 30)
                logger.warning(
                    "Dayforce reported rate limiting on GET %s (attempt %d/%d): "
                    "%r - backing off %ds",
                    endpoint,
                    attempt,
                    self.MAX_RATE_LIMIT_RETRIES,
                    last_denial,
                    backoff,
                )
                time.sleep(backoff)

        raise DayforceApiError(
            f"GET {endpoint} denied as rate-limited {self.MAX_RATE_LIMIT_RETRIES} "
            f"times in a row: {last_denial!r}",
            status=response.status_code,
            body=payload,
        )

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
                logger.debug("%s: following page %d", endpoint, page_num + 1)
        logger.warning(
            "%s pagination hit MAX_PAGES=%d; stopping", endpoint, self.MAX_PAGES
        )

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

    def get_employee_work_assignments(self, xref_code: str) -> dict[str, Any]:
        """One employee's detail record, expanded with ``WorkAssignments``.

        Carries ``WorkAssignments.Items[].Position.Department`` (XRefCode /
        ShortName / LongName) - see :func:`tafw_ingest.employees.get_employee_department`.
        """
        return self.get_employee(xref_code, expand="WorkAssignments")

    # ------------------------------------------------------------------ #
    # TAFW                                                              #
    # ------------------------------------------------------------------ #
    def _lookback_horizon_span(
        self, now: datetime | None = None
    ) -> tuple[datetime, datetime]:
        now = now or datetime.now(UTC)
        start = _start_of_day(now - timedelta(days=self.lookback_days))
        end = _end_of_day(now + timedelta(days=self.horizon_days))
        return start, end

    def get_tafw_records_for_employee(
        self,
        xref_code: str,
        status_type: str,
        start_date: datetime,
        end_date: datetime,
    ) -> list[dict[str, Any]]:
        """One employee, one status, one date range -> list of raw entries.

        Follows ``Paging.Next`` to collect every page, not just the first.
        """
        params = {
            "filterTAFWStartDate": _iso8601(start_date),
            "filterTAFWEndDate": _iso8601(end_date),
            "status": status_type,
        }
        records: list[dict[str, Any]] = []
        for payload in self._iter_pages(
            f"Employees/{xref_code}/TimeAwayFromWork", params
        ):
            data = payload.get("Data") if isinstance(payload, dict) else None
            records.extend(row for row in data or [] if isinstance(row, dict))
        return records

    def iter_tafw_records(
        self, xref_code: str, status_type: str, *, now: datetime | None = None
    ) -> Iterator[dict[str, Any]]:
        """Yield every raw TAFW entry for one employee/status across the full span."""
        start, end = self._lookback_horizon_span(now)
        yield from self.get_tafw_records_for_employee(xref_code, status_type, start, end)

    def get_tafw_records_for_employee_in_timeframe(
        self, xref_code: str, status_type: str, *, now: datetime | None = None
    ) -> list[dict[str, Any]]:
        """List form of :meth:`iter_tafw_records` (name kept from the old client)."""
        return list(self.iter_tafw_records(xref_code, status_type, now=now))
