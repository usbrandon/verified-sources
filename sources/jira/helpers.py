"""Shared HTTP + pagination helpers for the JIRA source.

JIRA has three pagination shapes:

1. Offset-paginated with values[] (or aliased):
   - Most Platform endpoints (/project/search, /workflowscheme, ...).
   - Software API (/board, /board/{id}/sprint, /board/{id}/epic).
   - Stop when len(page) < maxResults OR isLast==True.

2. Offset-paginated array body (no envelope):
   - /users/search returns a bare JSON array; pagination is via
     startAt + maxResults. Stop when the page is empty or short.

3. Cursor-paginated with nextPageToken:
   - The new /search/jql endpoint (replaces /search since 2024).
   - Stop when nextPageToken is absent.

Plus a small zoo of "one-of-a-kind" shapes (top-level array, nested
array under a named key) that we model via the `array_key` parameter.

The `paginate()` function below covers all three shapes via a `pagination`
discriminator. Higher-level resources (in resources.py) call this once
per endpoint they expose.

Designed to keep the original `get_paginated_data` signature working for
back-compat — it's a thin shim over `paginate()`. Existing tests against
the 4-endpoint dlthub source continue to pass.
"""
from __future__ import annotations

from typing import Any, Iterable, Iterator, Optional

from dlt.common.typing import DictStrAny, TDataItem
from dlt.sources.helpers import requests
from dlt.sources.helpers.requests import HTTPError


# Pagination kinds — keep as bare strings rather than an Enum so the
# settings.py dicts remain plain data.
OFFSET = "offset"
CURSOR = "cursor"
NONE = "none"  # endpoint returns one array, no paging at all


def _url(subdomain: str, api_path: str) -> str:
    return f"https://{subdomain}.atlassian.net/{api_path.lstrip('/')}"


def _extract_rows(
    body: dict | list,
    *,
    array_key: Optional[str],
) -> list[Any]:
    """Pull the page array out of a JSON body.

    array_key=None     → body itself is the array (e.g. /field, /issuetype).
    array_key="values" → standard JIRA shape: {startAt, maxResults, total, values: [...]}
    array_key="issues" → /search/jql cursor shape: {issues: [...], nextPageToken: "..."}
    array_key=other    → custom array under a named key, e.g. "issueLinkTypes"
                          for /issueLinkType; "permissionSchemes" for
                          /permissionscheme.
    """
    if array_key is None:
        return body if isinstance(body, list) else []
    if isinstance(body, dict):
        return body.get(array_key) or []
    return []


def _get(url, *, auth, params, skip_on_status: tuple[int, ...]):
    """Wrap dlt's requests.get with a skip_on_status escape hatch.

    dlt's session calls raise_for_status() automatically on 4xx/5xx
    BEFORE returning, so a naive `if resp.status_code in skip_on_status`
    check never runs. Catch HTTPError and inspect the response — for
    skippable statuses (e.g. 400 for boards without sprint support,
    404 for archived projects, 403 for paid-only endpoints) we return
    None instead of propagating. Anything else re-raises so dlt's
    retry layer still does its job.

    Returns None to signal "skip this resource silently" or the parsed
    JSON body on success.
    """
    try:
        resp = requests.get(
            url, auth=auth, headers={"Accept": "application/json"}, params=params,
        )
        return resp.json()
    except HTTPError as e:
        status = getattr(e.response, "status_code", None)
        if status in skip_on_status:
            return None
        raise


def paginate(
    subdomain: str,
    api_path: str,
    *,
    email: str,
    api_token: str,
    pagination: str,
    array_key: Optional[str] = "values",
    page_size: int = 50,
    params: Optional[DictStrAny] = None,
    skip_on_status: tuple[int, ...] = (),
    is_last_key: Optional[str] = "isLast",
) -> Iterator[TDataItem]:
    """Generic paginator for all three JIRA pagination shapes.

    Args:
        subdomain:      e.g. "your-org" for your-org.atlassian.net
        api_path:       URL path after the host root, e.g. "rest/api/3/project/search"
        email, api_token: HTTP Basic auth pair (Atlassian's "API token" auth)
        pagination:     "offset" | "cursor" | "none"
        array_key:      JSON key the page array lives under. None means
                        the body itself is the array. "values" is the JIRA
                        default; pass "issues" for /search/jql; pass
                        "issueLinkTypes" / "permissionSchemes" / etc. for
                        named-array endpoints.
        page_size:      maxResults / limit / page size per request.
        params:         additional query-string params (expand, fields,
                        jql, etc.).
        skip_on_status: HTTP statuses to treat as "this resource isn't
                        available here", silently. Used for agile
                        endpoints that 400 on boards without sprint
                        support, and project children that 404 on
                        archived projects. Caught from dlt's HTTPError
                        (the session raises before returning, so
                        checking resp.status_code post-hoc doesn't work
                        — see _get()).
        is_last_key:    Some endpoints return isLast=true on the final
                        page even when len(page)==page_size; honoring it
                        avoids one extra request. Set None to disable.

    Yields:
        One JSON object per row (NOT one list per page). dlt resources
        get flat row streams.
    """
    base_params: dict[str, Any] = dict(params or {})
    auth = (email, api_token)
    url = _url(subdomain, api_path)

    if pagination == NONE:
        body = _get(url, auth=auth, params=base_params, skip_on_status=skip_on_status)
        if body is None:
            return
        rows = _extract_rows(body, array_key=array_key)
        yield from rows
        return

    base_params["maxResults"] = page_size

    if pagination == OFFSET:
        start_at = 0
        base_params["startAt"] = start_at
        while True:
            body = _get(url, auth=auth, params=base_params, skip_on_status=skip_on_status)
            if body is None:
                return
            rows = _extract_rows(body, array_key=array_key)
            if not rows:
                return
            yield from rows
            # Stop conditions: explicit isLast, or a short page.
            if is_last_key and isinstance(body, dict) and body.get(is_last_key) is True:
                return
            if len(rows) < page_size:
                return
            start_at += len(rows)
            base_params["startAt"] = start_at
        return

    if pagination == CURSOR:
        # /search/jql + similar — cursor is in body, not a header.
        while True:
            body = _get(url, auth=auth, params=base_params, skip_on_status=skip_on_status)
            if body is None:
                return
            rows = _extract_rows(body, array_key=array_key)
            if not rows:
                return
            yield from rows
            next_token = body.get("nextPageToken") if isinstance(body, dict) else None
            if not next_token:
                return
            base_params["nextPageToken"] = next_token
        return

    raise ValueError(f"unknown pagination kind: {pagination!r}")


# ---------------------------------------------------------------------------
# Back-compat shim — keep upstream's `get_paginated_data` symbol exported.
# Yields one PAGE (list) at a time, matching the original semantics.
# ---------------------------------------------------------------------------
def get_paginated_data(
    subdomain: str,
    email: str,
    api_token: str,
    page_size: int,
    api_path: str = "rest/api/3/search/jql",
    data_path: Optional[str] = None,
    params: Optional[DictStrAny] = None,
    use_cursor_pagination: bool = False,
) -> Iterable[TDataItem]:
    """Original dlthub paginator — preserved so external callers don't break.

    Differences vs paginate():
      - Yields one PAGE (list) at a time. Most callers `yield from` it
        into a dlt resource, so the difference washes out, but anyone
        consuming it manually keeps working.
      - `data_path` ↔ paginate()'s `array_key`.
      - `use_cursor_pagination` ↔ paginate()'s `pagination=CURSOR`.
    """
    pagination = CURSOR if use_cursor_pagination else OFFSET
    rows: list[Any] = []
    for row in paginate(
        subdomain=subdomain,
        api_path=api_path,
        email=email,
        api_token=api_token,
        pagination=pagination,
        array_key=data_path,
        page_size=page_size,
        params=params,
    ):
        rows.append(row)
        if len(rows) >= page_size:
            yield rows
            rows = []
    if rows:
        yield rows
