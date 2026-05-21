"""Child-fanout resources for the JIRA source.

Top-level (parent) resources live in __init__.py + settings.py because
each has a fixed api_path. The resources in this module are templated
on a parent row's id (e.g. /rest/api/3/issue/{id}/comment) — they're
implemented as `@dlt.transformer`s that take the parent stream as
input.

Each factory returns a fully-decorated dlt.resource. The source
function in __init__.py composes them into one source.
"""
from __future__ import annotations

from typing import Any, Iterator

import dlt
from dlt.common.typing import TDataItem
from dlt.sources import DltResource

from .helpers import paginate
from .settings import (
    CHILD_ENDPOINTS,
    DEFAULT_PAGE_SIZE,
    ISSUES_ENDPOINT,
)


def issues_resource(
    subdomain: str,
    email: str,
    api_token: str,
    *,
    start_date: str = "2020-01-01T00:00:00.000+0000",
    page_size: int = DEFAULT_PAGE_SIZE,
) -> DltResource:
    """The /search/jql issues endpoint, cursor-paginated, incremental on
    fields.updated.

    JQL gets the watermark prepended: `updated >= '<last>' order by
    updated DESC`. dlt's incremental decorator stores `last_value` in
    pipeline state — empty result on a no-change run yields no rows and
    therefore no destination writes.
    """
    cfg = ISSUES_ENDPOINT
    effective_page_size = min(page_size, int(cfg["max_page_size"]))

    @dlt.resource(
        name="issues",
        write_disposition="merge",
        primary_key="id",
    )
    def issues(
        updated: dlt.sources.incremental[str] = dlt.sources.incremental(
            "fields.updated",
            initial_value=start_date,
        ),
    ) -> Iterator[TDataItem]:
        since = updated.last_value
        jql = cfg["default_jql_suffix"]
        if since:
            jql = f"updated >= '{since}' {jql}"
        params = {
            "jql": jql,
            "fields": cfg["fields"],
            "expand": cfg["expand"],
            "validateQuery": "warn",
        }
        yield from paginate(
            subdomain=subdomain,
            api_path=cfg["api_path"],
            email=email,
            api_token=api_token,
            pagination=cfg["pagination"],
            array_key=cfg["array_key"],
            page_size=effective_page_size,
            params=params,
        )

    return issues


def _issue_child(
    child_name: str,
    parent_resource: DltResource,
    *,
    subdomain: str,
    email: str,
    api_token: str,
    page_size: int,
) -> DltResource:
    cfg = CHILD_ENDPOINTS[child_name]
    disposition = cfg.get("write_disposition", "merge")
    primary_key = None if disposition == "append" else "id"

    @dlt.transformer(
        name=child_name,
        data_from=parent_resource,
        write_disposition=disposition,
        primary_key=primary_key,
    )
    def _child(parent_issue: TDataItem) -> Iterator[TDataItem]:
        issue_id = parent_issue["id"]
        api_path = cfg["api_path_template"].format(id=issue_id)
        for row in paginate(
            subdomain=subdomain,
            api_path=api_path,
            email=email,
            api_token=api_token,
            pagination=cfg["pagination"],
            array_key=cfg["array_key"],
            page_size=page_size,
            params=dict(cfg.get("params") or {}),
            skip_on_status=tuple(cfg.get("skip_on_status") or ()),
        ):
            # Stamp the natural FK alongside whatever surrogate dlt adds.
            row["issue_id"] = issue_id
            row["issue_key"] = parent_issue.get("key")
            yield row

    return _child


def issue_comments_resource(parent, *, subdomain, email, api_token,
                            page_size=DEFAULT_PAGE_SIZE) -> DltResource:
    return _issue_child("issue_comments", parent,
                        subdomain=subdomain, email=email, api_token=api_token,
                        page_size=page_size)


def issue_changelogs_resource(parent, *, subdomain, email, api_token,
                              page_size=DEFAULT_PAGE_SIZE) -> DltResource:
    return _issue_child("issue_changelogs", parent,
                        subdomain=subdomain, email=email, api_token=api_token,
                        page_size=page_size)


def issue_worklogs_resource(parent, *, subdomain, email, api_token,
                            page_size=DEFAULT_PAGE_SIZE) -> DltResource:
    return _issue_child("issue_worklogs", parent,
                        subdomain=subdomain, email=email, api_token=api_token,
                        page_size=page_size)


def issue_transitions_resource(parent, *, subdomain, email, api_token,
                               page_size=DEFAULT_PAGE_SIZE) -> DltResource:
    return _issue_child("issue_transitions", parent,
                        subdomain=subdomain, email=email, api_token=api_token,
                        page_size=page_size)


def _board_child(
    child_name: str,
    parent_resource: DltResource,
    *,
    subdomain: str,
    email: str,
    api_token: str,
    page_size: int,
) -> DltResource:
    cfg = CHILD_ENDPOINTS[child_name]

    @dlt.transformer(
        name=child_name,
        data_from=parent_resource,
        write_disposition="replace",
    )
    def _child(parent_board: TDataItem) -> Iterator[TDataItem]:
        board_id = parent_board["id"]
        api_path = cfg["api_path_template"].format(id=board_id)
        for row in paginate(
            subdomain=subdomain,
            api_path=api_path,
            email=email,
            api_token=api_token,
            pagination=cfg["pagination"],
            array_key=cfg["array_key"],
            page_size=page_size,
            params=dict(cfg.get("params") or {}),
            skip_on_status=tuple(cfg.get("skip_on_status") or ()),
        ):
            row["board_id"] = board_id
            yield row

    return _child


def sprints_resource(parent, *, subdomain, email, api_token,
                     page_size=DEFAULT_PAGE_SIZE) -> DltResource:
    return _board_child("sprints", parent,
                        subdomain=subdomain, email=email, api_token=api_token,
                        page_size=page_size)


def epics_resource(parent, *, subdomain, email, api_token,
                   page_size=DEFAULT_PAGE_SIZE) -> DltResource:
    return _board_child("epics", parent,
                        subdomain=subdomain, email=email, api_token=api_token,
                        page_size=page_size)


def sprint_issues_resource(
    parent_sprints: DltResource,
    *,
    subdomain: str,
    email: str,
    api_token: str,
    page_size: int = DEFAULT_PAGE_SIZE,
) -> DltResource:
    cfg = CHILD_ENDPOINTS["sprint_issues"]

    @dlt.transformer(
        name="sprint_issues",
        data_from=parent_sprints,
        write_disposition="replace",
    )
    def sprint_issues(parent_sprint: TDataItem) -> Iterator[TDataItem]:
        sprint_id = parent_sprint["id"]
        api_path = cfg["api_path_template"].format(id=sprint_id)
        for row in paginate(
            subdomain=subdomain,
            api_path=api_path,
            email=email,
            api_token=api_token,
            pagination=cfg["pagination"],
            array_key=cfg["array_key"],
            page_size=page_size,
            params=dict(cfg.get("params") or {}),
        ):
            row["sprint_id"] = sprint_id
            yield row

    return sprint_issues


# ---------------------------------------------------------------------------
# Phase C — project / field / filter fan-outs
# ---------------------------------------------------------------------------

def _project_child(
    child_name: str,
    parent_resource: DltResource,
    *,
    subdomain: str,
    email: str,
    api_token: str,
    page_size: int,
    project_status: str = "live",
) -> DltResource:
    """Project children fan-out. `project_status` filters parents so we
    don't waste calls on archived projects that 404 most child endpoints.
    """
    cfg = CHILD_ENDPOINTS[child_name]

    @dlt.transformer(
        name=child_name,
        data_from=parent_resource,
        write_disposition="replace",
    )
    def _child(parent_project: TDataItem) -> Iterator[TDataItem]:
        # Some Atlassian instances surface project status under
        # `archived`/`deleted` booleans rather than a status field.
        # Respect both — default-include if neither is present.
        statuses_wanted = set(project_status.split(","))
        if "archived" not in statuses_wanted and parent_project.get("archived"):
            return
        if "deleted" not in statuses_wanted and parent_project.get("deleted"):
            return
        project_id = parent_project["id"]
        api_path = cfg["api_path_template"].format(id=project_id)
        for row in paginate(
            subdomain=subdomain,
            api_path=api_path,
            email=email,
            api_token=api_token,
            pagination=cfg["pagination"],
            array_key=cfg["array_key"],
            page_size=page_size,
            params=dict(cfg.get("params") or {}),
            skip_on_status=tuple(cfg.get("skip_on_status") or ()),
        ):
            # Stamp project FK explicitly — `add_parent_id_to_documents`
            # in Estuary's idiom but in dlt we just write the column.
            row["project_id"] = project_id
            row["project_key"] = parent_project.get("key")
            yield row

    return _child


def project_components_resource(parent, *, subdomain, email, api_token,
                                page_size=DEFAULT_PAGE_SIZE) -> DltResource:
    return _project_child("project_components", parent,
                          subdomain=subdomain, email=email, api_token=api_token,
                          page_size=page_size)


def project_versions_resource(parent, *, subdomain, email, api_token,
                              page_size=DEFAULT_PAGE_SIZE) -> DltResource:
    return _project_child("project_versions", parent,
                          subdomain=subdomain, email=email, api_token=api_token,
                          page_size=page_size)


def project_emails_resource(parent, *, subdomain, email, api_token,
                            page_size=DEFAULT_PAGE_SIZE) -> DltResource:
    return _project_child("project_emails", parent,
                          subdomain=subdomain, email=email, api_token=api_token,
                          page_size=page_size)


def project_avatars_resource(parent, *, subdomain, email, api_token,
                             page_size=DEFAULT_PAGE_SIZE) -> DltResource:
    return _project_child("project_avatars", parent,
                          subdomain=subdomain, email=email, api_token=api_token,
                          page_size=page_size)


def issue_custom_field_contexts_resource(
    parent_issue_fields: DltResource,
    *,
    subdomain: str,
    email: str,
    api_token: str,
    page_size: int = DEFAULT_PAGE_SIZE,
) -> DltResource:
    """Only custom fields have contexts. Filter parents whose key starts
    with `customfield_` to avoid wasting requests on system fields.
    """
    cfg = CHILD_ENDPOINTS["issue_custom_field_contexts"]

    @dlt.transformer(
        name="issue_custom_field_contexts",
        data_from=parent_issue_fields,
        write_disposition="replace",
    )
    def _ctx(parent_field: TDataItem) -> Iterator[TDataItem]:
        if not str(parent_field.get("key", "")).startswith("customfield_"):
            return
        field_id = parent_field["id"]
        api_path = cfg["api_path_template"].format(id=field_id)
        for row in paginate(
            subdomain=subdomain,
            api_path=api_path,
            email=email,
            api_token=api_token,
            pagination=cfg["pagination"],
            array_key=cfg["array_key"],
            page_size=page_size,
            skip_on_status=tuple(cfg.get("skip_on_status") or ()),
        ):
            row["field_id"] = field_id
            yield row

    return _ctx


def screen_tabs_resource(
    parent_screens: DltResource,
    *,
    subdomain: str,
    email: str,
    api_token: str,
    page_size: int = DEFAULT_PAGE_SIZE,
) -> DltResource:
    """Tabs of each screen. Atlassian requires the screen id in the path
    (/rest/api/3/screens/{screenId}/tabs)."""
    cfg = CHILD_ENDPOINTS["screen_tabs"]

    @dlt.transformer(
        name="screen_tabs",
        data_from=parent_screens,
        write_disposition="replace",
    )
    def _tabs(parent_screen: TDataItem) -> Iterator[TDataItem]:
        screen_id = parent_screen["id"]
        api_path = cfg["api_path_template"].format(id=screen_id)
        for row in paginate(
            subdomain=subdomain,
            api_path=api_path,
            email=email,
            api_token=api_token,
            pagination=cfg["pagination"],
            array_key=cfg["array_key"],
            page_size=page_size,
            skip_on_status=tuple(cfg.get("skip_on_status") or ()),
        ):
            row["screen_id"] = screen_id
            yield row

    return _tabs


def filter_sharing_resource(
    parent_filters: DltResource,
    *,
    subdomain: str,
    email: str,
    api_token: str,
    page_size: int = DEFAULT_PAGE_SIZE,
) -> DltResource:
    cfg = CHILD_ENDPOINTS["filter_sharing"]

    @dlt.transformer(
        name="filter_sharing",
        data_from=parent_filters,
        write_disposition="replace",
    )
    def _share(parent_filter: TDataItem) -> Iterator[TDataItem]:
        filter_id = parent_filter["id"]
        api_path = cfg["api_path_template"].format(id=filter_id)
        for row in paginate(
            subdomain=subdomain,
            api_path=api_path,
            email=email,
            api_token=api_token,
            pagination=cfg["pagination"],
            array_key=cfg["array_key"],
            page_size=page_size,
            skip_on_status=tuple(cfg.get("skip_on_status") or ()),
        ):
            row["filter_id"] = filter_id
            yield row

    return _share
