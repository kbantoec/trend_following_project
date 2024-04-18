from collections import namedtuple
import pandas as pd

Period = namedtuple('Period', ['start', 'end'])


def __cumreturn(ts):
    return (ts + 1.0).prod() - 1.0


def periods(today=None):
    """
    Construct a series of Period objects

    :param today: If not specified use today's date. Specifying a today is quite useful in unit tests.
    :return:
    """
    today = today or pd.Timestamp("today")

    def __f(offset, today):
        return Period(start=today - offset, end=today)

    offset = pd.Series()
    offset["Two weeks"] = pd.DateOffset(weeks=2)
    offset["Month-to-Date"] = pd.offsets.MonthBegin()
    offset["Year-to-Date"] = pd.offsets.YearBegin()
    offset["One Month"] = pd.DateOffset(months=1)
    offset["Three Months"] = pd.DateOffset(months=3)
    offset["One Year"] = pd.DateOffset(years=1)
    offset["Three Years"] = pd.DateOffset(years=3)
    offset["Five Years"] = pd.DateOffset(years=5)
    offset["Ten Years"] = pd.DateOffset(years=10)

    return offset.apply(__f, today=today)


def period_returns(returns, offset=None, today=None):
    # check series is indeed a series
    assert isinstance(returns, pd.Series)
    # check that all indices are increasing
    assert returns.index.is_monotonic_increasing

    if not isinstance(offset, pd.Series):
        offset = periods(today=today)

    assert isinstance(returns.index[0], pd.Timestamp)
    p_returns = {key: __cumreturn(returns.truncate(before=period.start, after=period.end)) for key, period in offset.iteritems()}

    # preserve the order of the elements in the offset series
    return pd.Series(p_returns).loc[offset.index]


def period_prices(prices, offset=None, today=None):
    """
    Compute the returns achieve over certain periods

    :param prices: time series of prices
    :param offset: periods given as a Series, if not specified use standard set of periods
    :return: Series of periods returns, same order as in the period Series
    """
    # check series is indeed a series
    assert isinstance(prices, pd.Series)
    # check that all indices are increasing
    assert prices.index.is_monotonic_increasing
    # make sure all entries non-negative
    assert not (prices < 0).any()

    return period_returns(returns=prices.pct_change().dropna(), offset=offset, today=today)






