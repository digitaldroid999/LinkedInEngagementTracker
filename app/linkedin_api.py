"""RapidAPI Fresh LinkedIn Scraper client with retries and pagination."""

from __future__ import annotations

import logging
import time
from typing import Any, Callable, Iterator

import requests

from app.config import RAPIDAPI_BASE, RAPIDAPI_HOST
from app.scrape_log import LOGGER_NAME

_log = logging.getLogger(f"{LOGGER_NAME}.api")


class LinkedInAPIError(Exception):
    pass


def _extract_pagination_token(payload: dict[str, Any]) -> str | None:
    for k in ("pagination_token", "paginationToken", "next_page_token", "nextPageToken"):
        v = payload.get(k)
        if v:
            return str(v)
    data = payload.get("data")
    if isinstance(data, dict):
        for k in ("pagination_token", "paginationToken"):
            v = data.get(k)
            if v:
                return str(v)
    return None


def _extract_data_list(payload: dict[str, Any]) -> list[dict[str, Any]]:
    # Some endpoints return arrays at top-level keys.
    for key in ("data", "items", "comments", "reactions"):
        top = payload.get(key)
        if isinstance(top, list):
            return [x for x in top if isinstance(x, dict)]
    data = payload.get("data")
    if isinstance(data, dict):
        inner = data.get("data")
        if isinstance(inner, list):
            return [x for x in inner if isinstance(x, dict)]
        for key in ("items", "comments", "reactions"):
            items = data.get(key)
            if isinstance(items, list):
                return [x for x in items if isinstance(x, dict)]
        # Last fallback: first list value inside data object.
        for v in data.values():
            if isinstance(v, list):
                return [x for x in v if isinstance(x, dict)]
    return []


class LinkedInAPIClient:
    def __init__(self, rapidapi_key: str, max_retries: int = 15, timeout: int = 10) -> None:
        self._key = rapidapi_key
        self._max_retries = max_retries
        self._timeout = timeout
        self._on_api_call: Callable[[str, str], None] | None = None
        self._session = requests.Session()
        self._session.headers.update(
            {
                "x-rapidapi-key": self._key,
                "x-rapidapi-host": RAPIDAPI_HOST,
                "Content-Type": "application/json",
            }
        )

    def _request_json(self, method: str, url: str, **kwargs) -> dict[str, Any]:
        last_err: Exception | None = None
        for attempt in range(self._max_retries):
            try:
                resp = self._session.request(method, url, timeout=self._timeout, **kwargs)
                if resp.status_code in (429, 500, 502, 503, 504):
                    raise LinkedInAPIError(f"HTTP {resp.status_code}: {resp.text[:500]}")
                resp.raise_for_status()
                js = resp.json()
                if not isinstance(js, dict):
                    raise LinkedInAPIError("Invalid JSON (not an object)")
                if js.get("success") is False and "message" in js:
                    raise LinkedInAPIError(str(js.get("message")))
                # Count and log only after a successful response; failed attempts retry without hook.
                if self._on_api_call is not None:
                    self._on_api_call(method, resp.url)
                return js
            except (requests.RequestException, LinkedInAPIError, ValueError) as e:
                last_err = e
                if attempt >= self._max_retries - 1:
                    _log.error(
                        "API request failed after %d attempt(s): %s %s — %s",
                        self._max_retries,
                        method,
                        url,
                        e,
                    )
                    break
                _log.warning(
                    "API request error (attempt %d/%d), retrying in 10s: %s %s — %s",
                    attempt + 1,
                    self._max_retries,
                    method,
                    url,
                    e,
                )
                time.sleep(10)
        assert last_err is not None
        raise last_err

    def set_api_call_hook(self, hook: Callable[[str, str], None] | None) -> None:
        """Invoked once per successful API response with (method, url); retries do not invoke the hook."""
        self._on_api_call = hook

    def get_profile_by_username(self, username: str) -> dict[str, Any]:
        url = f"{RAPIDAPI_BASE}/user/profile"
        return self._request_json("GET", url, params={"username": username})

    def iter_comment_pages(self, urn: str) -> Iterator[list[dict[str, Any]]]:
        yield from self._iter_pages(f"{RAPIDAPI_BASE}/user/comments", {"urn": urn})

    def iter_reaction_pages(self, urn: str) -> Iterator[list[dict[str, Any]]]:
        yield from self._iter_pages(f"{RAPIDAPI_BASE}/user/reactions", {"urn": urn})

    def _iter_pages(self, url: str, base_params: dict[str, str]) -> Iterator[list[dict[str, Any]]]:
        token: str | None = None
        page = 1
        while True:
            if token is None:
                params = {**base_params, "page": page}
            else:
                params = {**base_params, "page": page, "pagination_token": token}
            payload = self._request_json("GET", url, params=params)
            items = _extract_data_list(payload)
            if items:
                yield items
            token = _extract_pagination_token(payload)
            
            if not items:
                break
            
            # Prefer token-based pagination; fallback to page increments.
            if token:
                page += 1
                continue
            break
        

    def iter_experience_pages(self, urn: str) -> Iterator[list[dict[str, Any]]]:
        url = f"{RAPIDAPI_BASE}/user/experience"
        token: str | None = None
        page = 1
        while True:
            params = {"urn": urn, "page": page, "pagination_token": token}
            payload = self._request_json("GET", url, params=params)
            chunk: list[dict[str, Any]] = []
            data = payload.get("data")
            if isinstance(data, list):
                chunk = [x for x in data if isinstance(x, dict)]
            elif isinstance(data, dict) and isinstance(data.get("data"), list):
                chunk = [x for x in data["data"] if isinstance(x, dict)]
            if chunk:
                yield chunk
            page += 1
            if not chunk:
                break
