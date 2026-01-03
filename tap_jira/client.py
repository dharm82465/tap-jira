"""REST client handling, including tap-jiraStream base class."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any, TypeVar

import requests.auth
from singer_sdk import SchemaDirectory, StreamSchema
from singer_sdk.streams import RESTStream

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if TYPE_CHECKING:
    from collections.abc import Callable, ClassVar

    from requests import Response
    from singer_sdk.helpers.types import Context

    _Auth = Callable[[requests.PreparedRequest], requests.PreparedRequest]

SCHEMAS_DIR = SchemaDirectory(Path(__file__).parent / "schemas")

_TNextPageToken = TypeVar("_TNextPageToken")


class JiraStream(RESTStream[_TNextPageToken]):
    """tap-jira stream class."""

    records_jsonpath = "$[*]"  # Or override `parse_response`.
    instance_name: str

    schema: ClassVar[StreamSchema] = StreamSchema(SCHEMAS_DIR)

    @override
    @property
    def url_base(self) -> str:
        """Returns base url."""
        domain = self.config["domain"]
        return f"https://{domain}:443/rest/api/2"

    @override
    @property
    def authenticator(self) -> _Auth:
        """Stream authenticator."""
        return requests.auth.HTTPBasicAuth(
            password=self.config.get("api_token") or os.getenv("TAP_JIRA_API_TOKEN"),
            username=self.config.get("email"),
        )


class JiraStartAtPaginatedStream(JiraStream[int]):
    """Jira stream that uses the startAt pagination parameter."""

    @override
    def get_url_params(
        self,
        context: Context | None,
        next_page_token: int | None,
    ) -> dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict[str, Any] = {}
        if self.replication_key:
            params["sort"] = "asc"
            params["order_by"] = self.replication_key

        return params
