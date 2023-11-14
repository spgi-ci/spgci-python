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


class AuthError(Exception):
    """
    [401, 403] - Invalid Username, Password or Appkey
    """

    pass


class PerSecondLimitError(Exception):
    """
    [429] - Too Many Requests Per Second
    """

    pass


class DailyLimitError(Exception):
    """
    [429] - Too Many Requests Per Day
    """

    pass
