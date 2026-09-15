# Copyright 2026 S&P Global Energy (previously S&P Global Commodity Insights)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Module to handle API requests."""

import threading
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from time import sleep
from typing import Any, Callable, Dict, NamedTuple, Union
from urllib.parse import parse_qsl, quote, urlencode, urlparse

import pandas as pd
import requests
import spgci.config
from pandas import DataFrame
from spgci.auth import get_token
from spgci.exceptions import AuthError, DailyLimitError, PerSecondLimitError
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
    wait_fixed,
)
from tqdm import tqdm


class TransientServerError(Exception):
    """Raised when an API request fails with a retryable server error."""


_auth_retry = retry(
    retry=retry_if_exception_type(AuthError),
    reraise=True,
    stop=stop_after_attempt(2),
)

_throttle_retry = retry(
    retry=retry_if_exception_type(PerSecondLimitError),
    reraise=True,
    wait=wait_fixed(1),
    stop=stop_after_attempt(5),
)

_timeout_retry = retry(
    retry=retry_if_exception_type(requests.exceptions.Timeout),
    reraise=True,
    wait=wait_fixed(1),
    stop=stop_after_attempt(3),
)

_server_retry = retry(
    retry=retry_if_exception_type(TransientServerError),
    reraise=True,
    wait=wait_exponential(multiplier=1, min=1, max=4),
    stop=stop_after_attempt(3),
)


class Paginator(NamedTuple):
    has_more_pages: bool
    key: str
    total_pages: int
    next_link: str = ""
    pg_type: str = ""


def _to_df(resp: requests.Response) -> DataFrame:
    j = resp.json()
    return DataFrame(j["results"])


def _convert_to_df(resp: requests.Response) -> DataFrame:
    j = resp.json()

    if isinstance(j, dict) and "results" in j:
        return pd.json_normalize(j["results"])  # type: ignore

    if isinstance(j, list):
        return pd.json_normalize(j)

    return pd.json_normalize([j])


def _nop_paginate(resp: requests.Response) -> Paginator:
    return Paginator(False, "page", 1)


def _paginate(resp: requests.Response) -> Paginator:
    j = resp.json()
    total_pages = j["metadata"]["totalPages"]

    if total_pages <= 1:
        return Paginator(False, "page", 1)

    return Paginator(True, "page", total_pages=total_pages)


_session = requests.Session()
_token_lock = threading.Lock()


def _clear_config_token_best_effort() -> None:
    """
    Best-effort clearing of any token stored in spgci.config if present.

    This is intentionally defensive because different versions of the config
    module may store the token differently.
    """
    try:
        if hasattr(spgci.config, "set_token") and callable(
            getattr(spgci.config, "set_token")
        ):
            spgci.config.set_token(None)  # type: ignore[attr-defined]
            return

        if hasattr(spgci.config, "_token"):
            setattr(spgci.config, "_token", None)
            return

        if hasattr(spgci.config, "token"):
            setattr(spgci.config, "token", None)
            return
    except Exception:
        # If config does not allow mutation, ignore it. The auth cache is still
        # cleared separately.
        return


def _get_token_threadsafe(force_refresh: bool = False) -> str:
    """
    Acquire an authentication token with single-flight refresh behavior.

    If a token is already available in config and a refresh was not requested,
    it is returned immediately. Otherwise, token refresh is protected by a
    lock so concurrent requests do not stampede the authentication service.
    """
    if not force_refresh:
        token = spgci.config.get_token()
        if token is not None:
            return token

    with _token_lock:
        if not force_refresh:
            token = spgci.config.get_token()
            if token is not None:
                return token

        get_token.cache_clear()
        _clear_config_token_best_effort()

        return spgci.auth.get_token(
            spgci.config.username,
            spgci.config.password,
            spgci.config.base_url,
            getattr(spgci.config, "auth_path", "/auth/api"),
        )



def _request_timeout_seconds() -> float:
    """Return the configured request timeout, defaulting to 60 seconds."""
    return float(getattr(spgci.config, "timeout", 60))


def _raise_for_transient_server_error(response: requests.Response) -> None:
    """Raise a retryable exception for transient gateway or server errors."""
    if response.status_code in (500, 502, 503, 504):
        raise TransientServerError(
            f"Transient server error: HTTP {response.status_code} "
            f"for {response.request.method} {response.url}"
        )


@_auth_retry
@_throttle_retry
@_timeout_retry
@_server_retry
def _get(
    url: str,
    params: Dict[Any, Any],
    session: requests.Session,
) -> requests.Response:
    headers = {"User-Agent": f"spgci-py/{spgci.config.version}"}

    token = _get_token_threadsafe(force_refresh=False)
    headers["Authorization"] = f"Bearer {token}"

    # Retained for compatibility with the existing request throttling behavior.
    sleep(spgci.config.sleep_time)

    response: requests.Response = session.get(
        url=url,
        params=params,
        headers=headers,
        verify=spgci.config.verify_ssl,
        proxies=spgci.config.proxies,
        auth=spgci.config.auth,
        timeout=_request_timeout_seconds(),
    )

    if response.status_code in (401, 403):
        _get_token_threadsafe(force_refresh=True)
        raise AuthError("Unauthorized (token refreshed); retrying request")

    if response.status_code == 429:
        remaining_daily_requests = int(
            response.headers.get("x-ratelimit-remaining-day", 0)
        )

        if remaining_daily_requests > 0:
            raise PerSecondLimitError("Per Second Rate Limit Reached")

        raise DailyLimitError("Daily Rate Limit Reached")

    _raise_for_transient_server_error(response)

    if response.status_code != 200:
        print(response.text)
        response.raise_for_status()

    return response


@_auth_retry
@_throttle_retry
@_timeout_retry
@_server_retry
def _post(
    url: str,
    body: Dict[Any, Any],
    session: requests.Session,
) -> requests.Response:
    headers = {
        "User-Agent": f"spgci-py/{spgci.config.version}",
        "Content-Type": "application/json",
        "accept": "application/json",
    }

    token = _get_token_threadsafe(force_refresh=False)
    headers["Authorization"] = f"Bearer {token}"

    # Retained for compatibility with the existing request throttling behavior.
    sleep(spgci.config.sleep_time)

    response: requests.Response = session.post(
        url=url,
        json=body,
        headers=headers,
        verify=spgci.config.verify_ssl,
        proxies=spgci.config.proxies,
        auth=spgci.config.auth,
        timeout=_request_timeout_seconds(),
    )

    if response.status_code in (401, 403):
        _get_token_threadsafe(force_refresh=True)
        raise AuthError("Unauthorized (token refreshed); retrying request")

    if response.status_code == 429:
        remaining_daily_requests = int(
            response.headers.get("x-ratelimit-remaining-day", 0)
        )

        if remaining_daily_requests > 0:
            raise PerSecondLimitError("Per Second Rate Limit Reached")

        raise DailyLimitError("Daily Rate Limit Reached")

    _raise_for_transient_server_error(response)

    if response.status_code not in (200, 201):
        print(response.text)
        response.raise_for_status()

    return response


def _fetch_page_worker(
    page_num: int,
    url: str,
    params: Dict[Any, Any],
    pagination: Paginator,
    df_fn: Callable[[requests.Response], DataFrame],
) -> tuple[int, DataFrame]:
    """Fetch and convert a single page from a pagination worker thread."""
    local_params = params.copy()

    if pagination.pg_type == "odata":
        parsed = urlparse(url)
        query_params = dict(parse_qsl(parsed.query))
        query_params[pagination.key] = str(
            (page_num - 1) * int(query_params["pageSize"])
        )
        parsed = parsed._replace(
            query=urlencode(query_params, quote_via=quote)
        )
        response = _get(
            url=parsed.geturl(),
            params={},
            session=_session,
        )
    else:
        local_params[pagination.key] = page_num
        response = _get(
            url=url,
            params=local_params,
            session=_session,
        )

    return page_num, df_fn(response)


def get_data(
    path: str,
    params: Dict[Any, Any],
    df_fn: Callable[[requests.Response], DataFrame] = _to_df,
    paginate_fn: Callable[[requests.Response], Paginator] = _paginate,
    raw: bool = False,
    paginate: bool = False,
) -> Union[DataFrame, requests.Response]:
    url = f"{spgci.config.base_url}/{path}"

    # Fetch the first page synchronously to determine whether more pages exist.
    response = _get(
        url=url,
        params=params,
        session=_session,
    )

    if raw:
        if paginate:
            warnings.warn(
                "\nCannot set `paginate=True` along with `raw=True`. "
                "Returning only the page requested."
            )

        return response

    content_type = response.headers.get("content-type", "").lower()

    if (
        "application/json" not in content_type
        and not content_type.startswith("text/")
    ):
        return response

    df: DataFrame = df_fn(response)
    pagination = paginate_fn(response)

    if not pagination.has_more_pages:
        return df

    if not paginate:
        warnings.warn(
            f"\nFetched page [1] of [{pagination.total_pages}]. "
            "Set `paginate=True` to fetch all pages."
        )
        return df

    total_pages = pagination.total_pages

    if total_pages > 500:
        if spgci.config.is_agent:
            raise ValueError(
                "Agent mode is enabled. Cannot paginate when total pages "
                f"({total_pages}) > 500. Set `paginate=False` to fetch only "
                "the first page."
            )

        warnings.warn(
            f"\nWith `paginate=True` this will fetch {total_pages} pages. "
            "Set `paginate=False` to disable."
        )

    page_results: Dict[int, DataFrame] = {}
    pages_to_fetch = list(range(2, total_pages + 1))
    workers = spgci.config.parallelism

    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_to_page = {
            executor.submit(
                _fetch_page_worker,
                page,
                url,
                params,
                pagination,
                df_fn,
            ): page
            for page in pages_to_fetch
        }

        for future in tqdm(
            as_completed(future_to_page),
            desc="Fetching pages...",
            initial=1,
            total=total_pages,
        ):
            page_num, page_df = future.result()
            page_results[page_num] = page_df

    sorted_dfs = [df] + [
        page_results[page_num]
        for page_num in sorted(page_results)
    ]

    final_df = pd.concat(
        objs=sorted_dfs,
        ignore_index=True,
    )

    preview_rows = getattr(df, "_preview_rows", None)

    if preview_rows is not None:
        final_df._preview_rows = preview_rows

    return final_df


def post_data(
    path: str,
    body: Dict[Any, Any],
    df_fn: Callable[[requests.Response], DataFrame] = _convert_to_df,
    raw: bool = False,
) -> Union[DataFrame, requests.Response]:
    url = f"{spgci.config.base_url}/{path}"

    response = _post(
        url=url,
        body=body,
        session=_session,
    )

    if raw:
        return response

    content_type = response.headers.get("content-type", "").lower()

    if (
        "application/json" not in content_type
        and not content_type.startswith("text/")
    ):
        return response

    return df_fn(response)