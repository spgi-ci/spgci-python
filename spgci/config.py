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

"""
Configure SPGCI settings
"""
import os
from typing import Dict, Union
from requests.auth import AuthBase

# from requests import _Auth

#: Username to use with the SPGCI API
username: str = os.getenv("SPGCI_USERNAME", "")
#: Password to use with the SPGCI API
password: str = os.getenv("SPGCI_PASSWORD", "")
#: Appkey to use with the SPGCI API
appkey: str = os.getenv("SPGCI_APPKEY", "")

#: Set the base url used when making HTTP calls
base_url = "https://api.platts.com"
#: Set `verify` to `False` when making HTTP calls
verify_ssl = True
#: Add proxy to HTTP calls
proxies: Dict[str, str] = {
    "HTTP_PROXY": os.getenv("HTTP_PROXY", ""),
    "HTTPS_PROXY": os.getenv("HTTPS_PROXY", ""),
}

#: Add special auth mechanism such as requests-kerberos
auth: Union[AuthBase, None] = None

#: Version of the SPGCI Pkg
version = "0.0.35"

#: time to sleep between api calls
sleep_time = 0


def set_credentials(un: str, pw: str, apikey: str) -> None:
    """
    Set credentials to use when calling the SPGCI API.

    You can avoid calling `set_credentials` by setting environment variables or creating a ``.env`` file with the following structure:\n
    SPGCI_USERNAME=``username``\n
    SPGCI_PASSWORD=``password``\n
    SPGCI_APPKEY=``appkey``\n

    Parameters
    ----------
    un : str
        username
    pw : str
        password
    apikey : str
        appkey
    """
    global username, password, appkey
    username = un
    password = pw
    appkey = apikey
