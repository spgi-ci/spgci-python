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

import unittest
import os
from spgci.api_client import get_token
from spgci.exceptions import AuthError
from spgci import set_credentials, MarketData
import pytest
from typing import cast
from pandas import DataFrame


class AuthTest(unittest.TestCase):
    un = os.getenv("SPGCI_USERNAME", "")
    pw = os.getenv("SPGCI_PASSWORD", "")
    key = os.getenv("SPGCI_APPKEY", "")

    @pytest.mark.integtest
    def test_token(self):
        t = get_token(self.un, self.pw, self.key)
        self.assertGreater(len(t), 100)

    @pytest.mark.integtest
    def test_token_cache(self):
        t = get_token(self.un, self.pw, self.key)
        t2 = get_token(self.un, self.pw, self.key)

        self.assertEqual(t, t2)

    def test_wrong_pw(self):
        set_credentials("un", "pw", "key")
        with pytest.raises(AuthError):
            cast(DataFrame, MarketData().get_mdcs())
