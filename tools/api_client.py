"""API integration tool — makes HTTP requests to external APIs."""

import json
from typing import Optional

import requests


def api_request(
    url: str,
    method: str = "GET",
    headers: Optional[dict] = None,
    body: Optional[dict] = None,
    timeout: int = 15,
) -> str:
    """Make an HTTP request to an external API.

    Args:
        url: The API endpoint URL.
        method: HTTP method — GET, POST, PUT, DELETE (default GET).
        headers: Optional dict of HTTP headers.
        body: Optional dict to send as JSON body.
        timeout: Request timeout in seconds (default 15).

    Returns:
        A formatted string with the status code and response body.
    """
    try:
        response = requests.request(
            method=method.upper(),
            url=url,
            headers=headers or {},
            json=body,
            timeout=timeout,
        )
        # Try to pretty-print JSON responses
        try:
            data = response.json()
            body_text = json.dumps(data, indent=2)
        except ValueError:
            body_text = response.text[:2000]

        return f"Status: {response.status_code}\n\n{body_text}"
    except requests.RequestException as e:
        return f"API request failed: {e}"
