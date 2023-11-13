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
