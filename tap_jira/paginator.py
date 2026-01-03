"""Pagination classes for the Jira tap.

This module provides:
- IssuesPaginator: paginator that generate next page URL from response links.
"""

import string

import requests
from singer_sdk.pagination import BaseHATEOASPaginator


class IssuesPaginator(BaseHATEOASPaginator):
    """Paginator for Jira Issues API that handles pagination using startAt parameter."""

    def get_next_url(self, response: requests.Response) -> str | None:
        """Return the next page URL from the response, or None if no more pages."""
        resp_json = response.json()
        if "startAt" in resp_json:
            start_at = resp_json["startAt"]
            max_results = resp_json["maxResults"]
            total = resp_json["total"]
            next_start_at = start_at + max_results
            if next_start_at < total:
                url = response.request.url
                if "startAt=" in url:
                    next_url = url.split("startAt=")[0] + f"startAt={next_start_at}"
                else:
                    separator = "&" if "?" in url else "?"
                    next_url = f"{url}{separator}startAt={next_start_at}"
                return next_url
        return None
