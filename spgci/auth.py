# Copyright 2023 S&P Global Commodity Insights

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
) -> str:
    """
    Get an Access Token for API calls.\n

    *Does not need to be invoked in user code. Instead see ``config.set_credentials()``*

    Can be called without arguments if environment variables are set.

    Automatically caches token based on the arguments supplied.\n

    Parameters
    ----------
    username : str, optional
        username for calling APIs, by default config.username or `SPGCI_USERNAME`
    password : str, optional
        password for calling APIs, by default config.password or ``SPGCI_PASSWORD``
    appkey : str, optional
        appkey for calling APIs, by default config.appkey or ``SPGCI_APPKEY``
    url : str, optional
        base url, by default config.base_url

    Returns
    -------
    str
        Access Token
    """

    body = {
        "username": username,
        "password": password,
    }
    headers = {
        "User-Agent": f"spgci-py/{config.version}",
    }

    url = f"{url}/auth/api"

    try:
        r = requests.post(
            url,
            data=body,
            headers=headers,
            verify=config.verify_ssl,
            proxies=config.proxies,
            auth=config.auth,
        )
        r.raise_for_status()
        return r.json()["access_token"]
    except SSLError as err:
        resp = err.response
        warnings.warn(
            "You can likely avoid this issue by setting `ci.config.verify_ssl = False`"
        )

        raise
    except HTTPError as err:
        resp = err.response
        # if 400, 401, 401 throw an auth error
        if resp.status_code in [400, 401, 403]:
            raise AuthError(
                f"Invalid Username, Password or Appkey. Try calling `set_credentials(username, password, appkey)`\n{resp.json()}"
            ) from None

        # if 429 check if more requests can be made today.
        if resp.status_code == 429:
            rl = int(resp.headers.get("x-ratelimit-remaining-day", 0))
            if rl > 0:
                raise PerSecondLimitError("Per Second Rate Limit Reached")
            else:
                raise DailyLimitError("Daily Rate Limit Reached")
        raise
