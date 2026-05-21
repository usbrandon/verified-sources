"""JIRA dlt source — issues, users, workflows, projects + extended streams.

This module exposes two dlt sources:

- `jira()`             — the full source. Returns every top-level + child
                          resource (Phase A + B + C streams; see
                          settings.py for the full registry). Backward
                          compatible: passing only the original auth
                          params still works.
- `jira_search()`      — the original JQL-driven issues search. Kept
                          unchanged for backward compatibility with the
                          upstream dlt-hub source's API.

The `get_paginated_data` symbol is also re-exported from `helpers.py`
for any existing user code that imported it directly from this
module.
"""
from __future__ import annotations

from typing import Iterable, List

import dlt
from dlt.common.typing import TDataItem
from dlt.sources import DltResource

from .helpers import get_paginated_data, paginate
from .resources import (
    epics_resource,
    filter_sharing_resource,
    issue_changelogs_resource,
    issue_comments_resource,
    issue_custom_field_contexts_resource,
    issue_transitions_resource,
    issue_worklogs_resource,
    issues_resource,
    project_avatars_resource,
    project_components_resource,
    project_emails_resource,
    project_versions_resource,
    sprint_issues_resource,
    sprints_resource,
)
from .settings import (
    DEFAULT_ENDPOINTS,
    DEFAULT_PAGE_SIZE,
    EXTENDED_ENDPOINTS,
)

__all__ = [
    "jira",
    "jira_search",
    "get_paginated_data",
    "paginate",
    "DEFAULT_ENDPOINTS",
    "DEFAULT_PAGE_SIZE",
    "EXTENDED_ENDPOINTS",
]


@dlt.source(max_table_nesting=2, name="jira")
def jira(
    subdomain: str = dlt.secrets.value,
    email: str = dlt.secrets.value,
    api_token: str = dlt.secrets.value,
    page_size: int = DEFAULT_PAGE_SIZE,
    start_date: str = "2020-01-01T00:00:00.000+0000",
) -> Iterable[DltResource]:
    """Full JIRA source — top-level + child resources.

    Args:
        subdomain:      e.g. "your-org" for your-org.atlassian.net.
        email:          API token owner's email.
        api_token:      Atlassian API token (NOT a password).
        page_size:      maxResults per request. JIRA's /search/jql is
                        hard-capped at 100; this is passed as-is to
                        every endpoint and they truncate to their own
                        cap as needed.
        start_date:     ISO timestamp; floor for issues' first
                        incremental seed.

    Yields each resource individually so users can select a subset
    via `.with_resources("issues", "sprints", ...)` on the returned
    source.
    """
    auth = dict(subdomain=subdomain, email=email, api_token=api_token,
                page_size=page_size)

    # Phase A + B: top-level full-refresh resources
    top_resources: dict[str, DltResource] = {}
    for name, cfg in EXTENDED_ENDPOINTS.items():
        def _make_loader(_name: str, _cfg: dict):
            def _load() -> Iterable[TDataItem]:
                yield from paginate(
                    subdomain=subdomain,
                    api_path=_cfg["api_path"],
                    email=email,
                    api_token=api_token,
                    pagination=_cfg["pagination"],
                    array_key=_cfg.get("array_key"),
                    page_size=page_size,
                    params=dict(_cfg.get("params") or {}),
                )
            _load.__name__ = _name
            return _load

        res = dlt.resource(
            _make_loader(name, cfg),
            name=name,
            write_disposition="replace",
        )
        top_resources[name] = res
        yield res

    # Issues (cursor-paginated, incremental)
    issues = issues_resource(
        subdomain=subdomain, email=email, api_token=api_token,
        start_date=start_date, page_size=page_size,
    )
    yield issues

    # Issue children
    yield issue_comments_resource(issues, **auth)
    yield issue_changelogs_resource(issues, **auth)
    yield issue_worklogs_resource(issues, **auth)
    yield issue_transitions_resource(issues, **auth)

    # Board / sprint children
    boards = top_resources["boards"]
    sprints = sprints_resource(boards, **auth)
    epics = epics_resource(boards, **auth)
    yield sprints
    yield epics
    yield sprint_issues_resource(sprints, **auth)

    # Project children (Phase C)
    projects = top_resources["projects"]
    yield project_components_resource(projects, **auth)
    yield project_versions_resource(projects, **auth)
    yield project_emails_resource(projects, **auth)
    yield project_avatars_resource(projects, **auth)

    # Field / filter children (Phase C)
    yield issue_custom_field_contexts_resource(
        top_resources["issue_fields"], **auth,
    )
    yield filter_sharing_resource(top_resources["filters"], **auth)


@dlt.source(max_table_nesting=2)
def jira_search(
    subdomain: str = dlt.secrets.value,
    email: str = dlt.secrets.value,
    api_token: str = dlt.secrets.value,
    page_size: int = DEFAULT_PAGE_SIZE,
) -> Iterable[DltResource]:
    """JQL-driven issues search.

    Returns a single `issues` resource that the caller binds to a list
    of JQL queries via `.bind(jql_queries=[...])`. Unchanged from the
    original dlt-hub source — same signature, same write_disposition,
    same data_path. Preserved verbatim so users importing `jira_search`
    don't have to migrate.
    """

    @dlt.resource(write_disposition="replace")
    def issues(jql_queries: List[str]) -> Iterable[TDataItem]:
        api_path = "rest/api/3/search/jql"
        for jql in jql_queries:
            params = {
                "fields": "*all",
                "expand": "fields,changelog,operations,transitions,names",
                "validateQuery": "strict",
                "jql": jql,
            }
            yield from paginate(
                subdomain=subdomain,
                api_path=api_path,
                email=email,
                api_token=api_token,
                pagination="cursor",
                array_key="issues",
                page_size=page_size,
                params=params,
            )

    return issues
