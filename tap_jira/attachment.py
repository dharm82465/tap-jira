"""Module for fetching and converting Jira attachments.

This module provides:
- AttachmentFetcher: class to fetch and convert attachments to Markdown.
"""

import logging
import tempfile
from pathlib import Path

import requests
from docling.document_converter import DocumentConverter


class AttachmentFetcher:
    """Class to fetch and convert attachments from Jira."""

    def __init__(self, converter: DocumentConverter, email: str, token: str) -> None:
        """Initialize the AttachmentFetcher with a converter and authentication token."""
        self.converter = converter
        self.email = email
        self.token = token
        self.logger = logging.getLogger(__name__)

    @staticmethod
    def __suffix_from_filename(filename: str) -> str:
        """Extract the file suffix from a filename.

        Parameters
        ----------
        filename : str
            The name of the file.

        Returns
        -------
        str
            The file suffix, including the dot (e.g., '.pdf').
        """
        path = Path(filename)
        return path.suffix

    def fetch_attachment(self, url: str, title: str) -> str | None:
        """Fetch and convert an attachment from Confluence to Markdown.

        Parameters
        ----------
        url : str
            The URL of the attachment to fetch.
        username : str
            The username for authentication.
        password : str
            The password for authentication.

        Returns
        -------
        str | None
            The converted Markdown content, or None if an error occurs.
        """
        # Fetch the attachment content with OAuth2 authentication
        try:
            response = requests.get(url, stream=True, auth=(self.email, self.token))
            response.raise_for_status()  # Raise an exception for bad status codes

            # Save the content to a temporary local file
            with tempfile.NamedTemporaryFile(
                delete=False,
                suffix=f"{self.__suffix_from_filename(title)}",
            ) as temp_file:
                for chunk in response.iter_content(chunk_size=8192):
                    temp_file.write(chunk)
                temp_file_path = temp_file.name

            # Convert the local file using Docling
            result = self.converter.convert(temp_file_path)
            Path(temp_file_path).unlink()
            return result.document.export_to_markdown()

        except requests.exceptions.RequestException:
            self.logger.exception("Error fetching the file")
            return None
        except Exception:
            self.logger.exception("An error occurred during Docling conversion")
            return None
