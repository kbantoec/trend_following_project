import numpy as np
import pandas as pd


def volatility_adjust(prices, vola=32, min_periods=50, winsor=4.2, n=1) -> pd.Series:
    assert winsor > 0
    # check that all indices are increasing
    assert prices.index.is_monotonic_increasing
    # make sure all entries non-negative
    assert not (prices <= 0).any()

    # go into log space, now returns are just simple differences
    prices = np.log(prices)

    for i in range(n):
        # compute the returns
        returns = prices.diff()
        # estimate the volatility
        volatility = returns.ewm(com=vola, min_periods=min_periods).std(bias=False)
        # compute new log prices
        prices = (returns / volatility).clip(lower=-winsor, upper=winsor).cumsum()

    return prices


def oscillator(price, a=32, b=96, min_periods=100) -> pd.Series:
    def __geom(q):
        return 1.0 / (1 - q)

    osc = price.ewm(span=2 * a - 1, min_periods=min_periods).mean() - price.ewm(span=2 * b - 1, min_periods=min_periods).mean()
    l_fast = 1.0 - 1.0 / a
    l_slow = 1.0 - 1.0 / b
    return osc / np.sqrt(__geom(l_fast**2) - 2.0 * __geom(l_slow * l_fast) + __geom(l_slow**2))


def trend_new(price, a=32, b=96, vola=32, winsor=4.2, min_periods=50, f=np.tanh, n=1) -> pd.Series:
    return f(oscillator(volatility_adjust(price,vola,min_periods,winsor,n),a,b,2*min_periods))
