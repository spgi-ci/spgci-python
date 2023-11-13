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
