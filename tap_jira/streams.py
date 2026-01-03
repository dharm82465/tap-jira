"""Stream type classes for tap-jira."""

from __future__ import annotations

import json
import sys
from datetime import datetime
from typing import TYPE_CHECKING, Any
from urllib.parse import parse_qsl

from docling.document_converter import DocumentConverter
from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_jira.attachment import AttachmentFetcher
from tap_jira.client import JiraStartAtPaginatedStream, JiraStream
from tap_jira.paginator import IssuesPaginator

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if TYPE_CHECKING:
    import requests
    from singer_sdk.helpers.types import Context, Record
    from singer_sdk.pagination import BaseHATEOASPaginator
    from singer_sdk.tap_base import Tap
    from singer_sdk.typing import Schema

PropertiesList = th.PropertiesList
Property = th.Property
ObjectType = th.ObjectType
DateTimeType = th.DateTimeType
DateType = th.DateType
StringType = th.StringType
ArrayType = th.ArrayType
BooleanType = th.BooleanType
IntegerType = th.IntegerType
NumberType = th.NumberType


ADFRootBlockNode = ObjectType(
    Property("type", StringType),
    Property("version", IntegerType),
    Property(
        "content",
        ArrayType(ObjectType(additional_properties=True)),
    ),
)


class UsersStream(JiraStartAtPaginatedStream):
    """Users stream.

    https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-users/#api-rest-api-3-user-get
    """

    """
    name: stream name
    path: path which will be added to api url in client.py
    schema: instream schema
    primary_keys = primary keys for the table
    replication_key = datetime keys for replication
    """

    name = "users"
    path = "/user/search?username=."
    primary_keys = ("key",)
    replication_key = "key"
    replication_method = "INCREMENTAL"
    records_jsonpath = "$[*]"

    @override
    def get_next_page_token(
        self,
        response: requests.Response,
        previous_token: int | None,
    ) -> int | None:
        """Return a token for identifying next page or None if no more pages."""
        # If pagination is required, return a token which can be used to get the
        #       next page. If this is the final page, return "None" to end the
        #       pagination loop.
        resp_json = response.json()
        if previous_token is None:
            previous_token = 0

        page = resp_json
        if len(page) == 0:
            return None

        return previous_token + len(page)


class ProjectStream(JiraStartAtPaginatedStream):
    """Project stream.

    https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-projects/#api-rest-api-3-project-get
    """

    """
    name: stream name
    path: path which will be added to api url in client.py
    schema: instream schema
    primary_keys = primary keys for the table
    replication_key = datetime keys for replication
    records_jsonpath = json response body
    """

    name = "projects"
    path = "/project"
    primary_keys = ("id",)
    replication_key = "id"
    replication_method = "INCREMENTAL"
    records_jsonpath = "$[*]"  # Or override `parse_response`.
    instance_name = "values"


class IssueStream(JiraStream[str]):
    """Issue stream.

    https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issue-search/#api-rest-api-3-search-jql-get
    """

    """
    name: stream name
    path: path which will be added to api url in client.py
    schema: instream schema
    primary_keys = primary keys for the table
    replication_key = datetime keys for replication
    records_jsonpath = json response body
    """

    name = "issues"
    path = "/search"
    primary_keys = ("id",)
    replication_key = "updated"
    replication_method = "INCREMENTAL"
    records_jsonpath = "$[issues][*]"  # Or override `parse_response`.
    instance_name = "issues"

    def __init__(
        self,
        tap: Tap,
        name: str | None = None,
        schema: dict[str, Any] | Schema | None = None,
        path: str | None = None,
        *,
        http_method: str | None = None,
    ) -> None:
        """Initialize the Issue stream with document converter and attachment fetcher."""
        super().__init__(tap, name, schema, path, http_method=http_method)
        self.converter = DocumentConverter()
        self.attachment_fetcher = AttachmentFetcher(
            converter=self.converter,
            email=self.config.get("email"),
            token=self.config.get("api_token"),
        )

    @override
    def is_timestamp_replication_key(self) -> bool:
        """Return True if the replication key is a timestamp field."""
        return True

    @override
    def get_new_paginator(self) -> BaseHATEOASPaginator | None:
        """Return a new paginator for this stream."""
        return IssuesPaginator()

    @override
    def get_url_params(  # ty: ignore[invalid-method-override]
        self,
        context: Context | None,
        next_page_token: str | None,
    ) -> dict[str, Any]:
        """Return a dictionary of query parameters."""
        params: dict[str, Any] = {}

        params["maxResults"] = self.config.get("page_size", {}).get("issues", 10)
        params["fields"] = (
            self.config.get("stream_options", {})
            .get("issues", {})
            .get("fields", "*all")
        )

        jql: list[str] = []

        if "end_date" in self.config:
            end_date = self.config["end_date"]
            jql.append(f"(created<'{end_date}' or updated<'{end_date}')")

        if self.get_starting_replication_key_value(context):
            updated = self.get_starting_replication_key_value(context)
            jql.append(f"(updated>'{updated}')")
        elif "start_date" in self.config:
            start_date = self.config["start_date"]
            jql.append(f"(created>='{start_date}' or updated>='{start_date}')")

        base_jql = (self.config.get("stream_options", {}).get("issues", {})).get(
            "jql",
            "id != null",
        )

        jql.append(f"({base_jql})")

        params["jql"] = " and ".join(jql) + " order by updated asc"

        if next_page_token:
            params.update(parse_qsl(next_page_token.query))
        return params

    @override
    def post_process(self, row: Record, context: Context | None = None) -> Record:
        """Post-process the record.

        - Add top-level `created` field.
        """
        created = row.get("fields", {}).get("created", None)
        row["created"] = created

        updated = row.get("fields", {}).get("updated", None)
        dt = datetime.fromisoformat(updated)
        row["updated"] = dt.strftime("%Y-%m-%d %H:%M")

        attachments = row.get("fields", {}).get("attachment", [])
        contents: list[str] = []
        for attachment in attachments:
            url = attachment.get("content")
            title = attachment.get("filename")
            if url and title:
                markdown_content = self.attachment_fetcher.fetch_attachment(
                    url=url,
                    title=title,
                )
                if markdown_content:
                    contents.append(
                        f"## File: [{title}]({url})\n### Content\n{markdown_content}",
                    )

        row["attachments"] = "\n---\n".join(contents)
        return row

    @override
    def get_child_context(self, record: Record, context: Context | None) -> Context:
        """Return a context dictionary for child streams."""
        return {"issue_id": record["id"]}


class Resolutions(JiraStartAtPaginatedStream):
    """Resolution stream.

    https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issue-resolutions/#api-rest-api-3-resolution-get
    """

    """
    name: stream name
    path: path which will be added to api url in client.py
    schema: instream schema
    primary_keys = primary keys for the table
    replication_key = datetime keys for replication
    records_jsonpath = json response body
    """

    name = "resolutions"

    path = "/resolution"

    records_jsonpath = "$[*]"

    primary_keys = ("id",)

    instance_name = "values"


# Child Streams


class IssueChangeLogStream(JiraStartAtPaginatedStream):
    """Issue Change Log.

    https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-workflows/#api-rest-api-3-workflow-get
    """

    """
    name: stream name
    path: path which will be added to api url in client.py
    schema: instream schema
    primary_keys = primary keys for the table
    replication_key = datetime keys for replication
    records_jsonpath = json response body
    """

    name = "changelogs"

    parent_stream_type = IssueStream

    ignore_parent_replication_keys = True

    path = "/issue/{issue_id}?expand=changelog"

    replication_key = "created"

    primary_keys = ("id",)

    records_jsonpath = "$[changelog][histories][*]"

    instance_name = "values"

    @override
    def post_process(self, row: Record, context: Context | None = None) -> Record:
        """Post-process the record before it is returned."""
        assert context is not None  # noqa: S101
        row["issueId"] = context["issue_id"]
        return row


class IssueComments(JiraStartAtPaginatedStream):
    """Issue Comments.

    https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issue-comments/#api-rest-api-3-issue-issueidorkey-comment-get
    """

    """
    name: stream name
    path: path which will be added to api url in client.py
    schema: instream schema
    primary_keys = primary keys for the table
    replication_key = datetime keys for replication
    records_jsonpath = json response body
    """

    name = "issue_comments"

    parent_stream_type = IssueStream

    ignore_parent_replication_keys = True

    path = "/issue/{issue_id}/comment"

    primary_keys = ("id",)

    records_jsonpath = "$[comments][*]"

    instance_name = "comments"

    @override
    def post_process(self, row: Record, context: Context | None = None) -> Record:
        """Post-process the record before it is returned."""
        assert context is not None  # noqa: S101
        row["issueId"] = context["issue_id"]
        return row


class IssueWorklogs(JiraStartAtPaginatedStream):
    """Issue Worklogs.

    https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issue-comments/#api-rest-api-3-issue-issueidorkey-comment-get
    """

    """
    name: stream name
    path: path which will be added to api url in client.py
    schema: instream schema
    primary_keys = primary keys for the table
    replication_key = datetime keys for replication
    records_jsonpath = json response body
    """

    name = "worklogs"

    parent_stream_type = IssueStream

    ignore_parent_replication_keys = True

    path = "/issue/{issue_id}/worklog"

    primary_keys = ("id",)

    records_jsonpath = "$[worklogs][*]"

    instance_name = "worklogs"
