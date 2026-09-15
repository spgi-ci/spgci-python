# Copyright 2026 S&P Global Energy (previously S&P Global Commodity Insights)

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#       http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from functools import lru_cache
import spgci.config as config
import requests
from requests.exceptions import HTTPError, SSLError
import warnings
from tenacity import retry, retry_if_exception_type, wait_fixed
from spgci.exceptions import AuthError, PerSecondLimitError, DailyLimitError

_throttle_retry = retry(
    retry=retry_if_exception_type(PerSecondLimitError), reraise=True, wait=wait_fixed(1)
)


@lru_cache()
@_throttle_retry
def get_token(
    username: str = config.username,
    password: str = config.password,
    url: str = config.base_url,
    auth_path: str = "/auth/api",
) -> str:
    """
    Get an access token for API calls.

    Automatically caches the token based on the supplied arguments.
    """

    body = {
        "username": username,
        "password": password,
    }
    headers = {
        "User-Agent": f"spgci-py/{config.version}",
    }

    token_url = f"{url.rstrip('/')}/{auth_path.lstrip('/')}"

    try:
        r = requests.post(
            token_url,
            data=body,
            headers=headers,
            verify=config.verify_ssl,
            proxies=config.proxies,
            auth=config.auth,
        )
        r.raise_for_status()
        return r.json()["access_token"]
    except SSLError:
        warnings.warn(
            "You can likely avoid this issue by setting "
            "`spgci.config.verify_ssl = False`"
        )
        raise
    except HTTPError as err:
        resp = err.response

        if resp.status_code in [400, 401, 403]:
            raise AuthError(
                "Invalid username or password. Try calling "
                "`set_credentials(username, password)`.\n"
                f"{resp.json()}"
            ) from None

        if resp.status_code == 429:
            rl = int(resp.headers.get("x-ratelimit-remaining-day", 0))
            if rl > 0:
                raise PerSecondLimitError("Per Second Rate Limit Reached")
            raise DailyLimitError("Daily Rate Limit Reached")

        raise