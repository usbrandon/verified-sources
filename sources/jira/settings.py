"""Endpoint registry for the JIRA source.

This file declares every JIRA endpoint we know how to call. Each entry
is enough metadata for helpers.paginate() to fetch the data; the
__init__.py source function loops over the registry and turns each
entry into a dlt resource.

`DEFAULT_ENDPOINTS` is kept for backward compatibility with the
original dlthub 4-endpoint source. Anyone importing it gets the same
four streams they had before (projects/users/workflows/issues).

`EXTENDED_ENDPOINTS` adds the broader Tier-1/2 coverage that mirrors
Estuary's source-jira-native taxonomy. The `jira()` source function
iterates over the union (DEFAULT + EXTENDED), so adding to EXTENDED is
the right home for new top-level full-refresh streams.

Streams that need parent fan-out (issue children, board children,
project children, field children) are NOT in this registry — they
live in resources.py because their api_path is templated on a parent
row's id.
"""
from __future__ import annotations

from typing import Any

DEFAULT_PAGE_SIZE = 50

# ---------------------------------------------------------------------------
# Back-compat: dlthub's original 4 streams. Same names + behavior. Kept
# verbatim so anyone running tests against the upstream tenant still
# passes.
# ---------------------------------------------------------------------------
DEFAULT_ENDPOINTS: dict[str, dict[str, Any]] = {
    "issues": {
        "data_path": "issues",
        "api_path": "rest/api/3/search/jql",
        "use_cursor_pagination": True,
        "params": {
            "fields": "*all",
            "expand": "fields,changelog,operations,transitions,names",
            "validateQuery": "strict",
            "jql": "created >= '2000-01-01' order by created DESC",
        },
    },
    "users": {
        "api_path": "rest/api/3/users",
        "params": {"includeInactiveUsers": True},
    },
    "workflows": {
        "data_path": "values",
        "api_path": "/rest/api/3/workflow/search",
        "params": {},
    },
    "projects": {
        "data_path": "values",
        "api_path": "rest/api/3/project/search",
        "params": {
            "expand": "description,lead,issueTypes,url,projectKeys,permissions,insight"
        },
    },
}


# ---------------------------------------------------------------------------
# EXTENDED — new endpoint registry consumed by the richer jira() source.
#
# Each entry declares:
#   api_path:    URL after the atlassian.net host root.
#   pagination:  "offset" | "cursor" | "none"
#   array_key:   JSON key the page array lives under. None = body is array.
#                "values" = standard JIRA shape.
#                custom = "issueLinkTypes", "permissionSchemes", etc.
#   params:      Fixed query-string params (expand, etc.).
# ---------------------------------------------------------------------------

# Phase A — full-refresh top-level endpoints (issues handled separately
# because it's incremental + cursor-paginated).
PHASE_A_TOP_LEVEL: dict[str, dict[str, Any]] = {
    "projects": {
        "api_path": "rest/api/3/project/search",
        "pagination": "offset",
        "array_key": "values",
        "params": {
            "expand": "description,lead,issueTypes,url,projectKeys,permissions,insight",
        },
    },
    "users": {
        "api_path": "rest/api/3/users/search",
        "pagination": "offset",
        "array_key": None,
        "params": {},
    },
    "issue_fields": {
        "api_path": "rest/api/3/field",
        "pagination": "none",
        "array_key": None,
    },
    "issue_types": {
        "api_path": "rest/api/3/issuetype",
        "pagination": "none",
        "array_key": None,
    },
    "statuses": {
        "api_path": "rest/api/3/status",
        "pagination": "none",
        "array_key": None,
    },
    "boards": {
        "api_path": "rest/agile/1.0/board",
        "pagination": "offset",
        "array_key": "values",
        "params": {"includePrivate": "true", "orderBy": "name"},
    },
}

# Phase B — simple full-refresh adds. All single-endpoint, no parent
# fan-out, no incremental cursor needed.
PHASE_B_TOP_LEVEL: dict[str, dict[str, Any]] = {
    # Platform: top-level array endpoints
    "application_roles": {
        "api_path": "rest/api/3/applicationrole",
        "pagination": "none",
        "array_key": None,
    },
    "issue_priorities": {
        "api_path": "rest/api/3/priority",
        "pagination": "none",
        "array_key": None,
    },
    "project_categories": {
        "api_path": "rest/api/3/projectCategory",
        "pagination": "none",
        "array_key": None,
    },
    "project_types": {
        "api_path": "rest/api/3/project/type",
        "pagination": "none",
        "array_key": None,
    },
    "project_roles": {
        "api_path": "rest/api/3/role",
        "pagination": "none",
        "array_key": None,
    },
    "workflow_status_categories": {
        "api_path": "rest/api/3/statuscategory",
        "pagination": "none",
        "array_key": None,
    },
    # Platform: paginated under "values"
    "dashboards": {
        "api_path": "rest/api/3/dashboard",
        "pagination": "offset",
        "array_key": "dashboards",
    },
    "issue_resolutions": {
        "api_path": "rest/api/3/resolution/search",
        "pagination": "offset",
        "array_key": "values",
    },
    "issue_field_configurations": {
        "api_path": "rest/api/3/fieldconfiguration",
        "pagination": "offset",
        "array_key": "values",
    },
    "issue_type_schemes": {
        "api_path": "rest/api/3/issuetypescheme",
        "pagination": "offset",
        "array_key": "values",
    },
    "issue_type_screen_schemes": {
        "api_path": "rest/api/3/issuetypescreenscheme",
        "pagination": "offset",
        "array_key": "values",
    },
    "filters": {
        "api_path": "rest/api/3/filter/search",
        "pagination": "offset",
        "array_key": "values",
    },
    "screens": {
        "api_path": "rest/api/3/screens",
        "pagination": "offset",
        "array_key": "values",
    },
    # NOTE: screen_tabs is NOT a top-level endpoint — Atlassian's API
    # requires the screen id (/rest/api/3/screens/{screenId}/tabs).
    # It's declared in CHILD_ENDPOINTS below and fans out from `screens`.
    "screen_schemes": {
        "api_path": "rest/api/3/screenscheme",
        "pagination": "offset",
        "array_key": "values",
    },
    "groups": {
        "api_path": "rest/api/3/group/bulk",
        "pagination": "offset",
        "array_key": "values",
    },
    "workflow_schemes": {
        "api_path": "rest/api/3/workflowscheme",
        "pagination": "offset",
        "array_key": "values",
    },
    "workflows": {
        "api_path": "rest/api/3/workflows/search",
        "pagination": "offset",
        "array_key": "values",
        "params": {"orderBy": "created"},
    },
    # Platform: nested arrays under a named key
    "issue_link_types": {
        "api_path": "rest/api/3/issueLinkType",
        "pagination": "none",
        "array_key": "issueLinkTypes",
    },
    "permission_schemes": {
        "api_path": "rest/api/3/permissionscheme",
        "pagination": "none",
        "array_key": "permissionSchemes",
    },
    "issue_security_schemes": {
        "api_path": "rest/api/3/issuesecurityschemes",
        "pagination": "none",
        "array_key": "issueSecuritySchemes",
    },
}

EXTENDED_ENDPOINTS: dict[str, dict[str, Any]] = {
    **PHASE_A_TOP_LEVEL,
    **PHASE_B_TOP_LEVEL,
}


# Issues endpoint — separate from the registry because of incremental +
# cursor pagination + JQL mutation per run.
ISSUES_ENDPOINT: dict[str, Any] = {
    "api_path": "rest/api/3/search/jql",
    "pagination": "cursor",
    "array_key": "issues",
    "default_jql_suffix": "order by updated DESC",
    "fields": "*all",
    "expand": "names,renderedFields",
    "max_page_size": 100,
}


# Child endpoints (templated on a parent row's id). Each declares its
# parent resource name + path template. The actual resource is wired in
# resources.py.
CHILD_ENDPOINTS: dict[str, dict[str, Any]] = {
    # Issue children
    "issue_comments": {
        "parent": "issues",
        "api_path_template": "rest/api/3/issue/{id}/comment",
        "pagination": "offset",
        "array_key": "comments",
    },
    "issue_changelogs": {
        "parent": "issues",
        "api_path_template": "rest/api/3/issue/{id}/changelog",
        "pagination": "offset",
        "array_key": "values",
        "write_disposition": "append",
    },
    "issue_worklogs": {
        "parent": "issues",
        "api_path_template": "rest/api/3/issue/{id}/worklog",
        "pagination": "offset",
        "array_key": "worklogs",
    },
    "issue_transitions": {
        "parent": "issues",
        "api_path_template": "rest/api/3/issue/{id}/transitions",
        "pagination": "none",
        "array_key": "transitions",
        "skip_on_status": (401, 404),
    },
    # Board children. 400 = board doesn't support sprints/epics (kanban,
    # next-gen without agile features). 404 = board was deleted between
    # the /board listing and the child fetch (race), OR the agile API
    # doesn't recognize the board (some board types).
    "sprints": {
        "parent": "boards",
        "api_path_template": "rest/agile/1.0/board/{id}/sprint",
        "pagination": "offset",
        "array_key": "values",
        "skip_on_status": (400, 404),
    },
    "epics": {
        "parent": "boards",
        "api_path_template": "rest/agile/1.0/board/{id}/epic",
        "pagination": "offset",
        "array_key": "values",
        "skip_on_status": (400, 404),
    },
    # Sprint children. 400 = sprint closed and can't be queried (rare).
    # 404 = sprint deleted between the /sprint listing and the issue
    # fetch.
    "sprint_issues": {
        "parent": "sprints",
        "api_path_template": "rest/agile/1.0/sprint/{id}/issue",
        "pagination": "offset",
        "array_key": "issues",
        "params": {"fields": "summary,status,assignee"},
        "skip_on_status": (400, 404),
    },
    # Project children (Phase C)
    "project_components": {
        "parent": "projects",
        "api_path_template": "rest/api/3/project/{id}/component",
        "pagination": "none",
        "array_key": None,
        "skip_on_status": (404,),
    },
    "project_versions": {
        "parent": "projects",
        # The paginated form (/project/{id}/version, singular) scales
        # to projects with many versions; the non-paginated /versions
        # endpoint returns the whole list in one request and is the
        # simpler option for small projects but unsafe if any project
        # accumulates hundreds of versions. Matches Estuary's choice.
        "api_path_template": "rest/api/3/project/{id}/version",
        "pagination": "offset",
        "array_key": "values",
        "skip_on_status": (404,),
    },
    "project_emails": {
        "parent": "projects",
        "api_path_template": "rest/api/3/project/{id}/email",
        "pagination": "none",
        "array_key": None,
        "skip_on_status": (403, 404),
    },
    "project_avatars": {
        "parent": "projects",
        "api_path_template": "rest/api/3/project/{id}/avatars",
        "pagination": "none",
        "array_key": None,
        "skip_on_status": (404,),
    },
    # Field children (Phase C)
    "issue_custom_field_contexts": {
        "parent": "issue_fields",
        "api_path_template": "rest/api/3/field/{id}/context",
        "pagination": "offset",
        "array_key": "values",
        "skip_on_status": (400, 404),
    },
    # Screen children
    # /rest/api/3/screens/{screenId}/tabs returns a bare JSON array (no
    # values envelope, no pagination).
    "screen_tabs": {
        "parent": "screens",
        "api_path_template": "rest/api/3/screens/{id}/tabs",
        "pagination": "none",
        "array_key": None,
        "skip_on_status": (404,),
    },
    # Filter children (Phase C)
    "filter_sharing": {
        "parent": "filters",
        "api_path_template": "rest/api/3/filter/{id}/permission",
        "pagination": "none",
        "array_key": None,
        "skip_on_status": (403, 404),
    },
}
