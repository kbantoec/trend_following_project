import pandas as pd


def drawdown(series) -> pd.Series:
    return Drawdown(series).drawdown


class Drawdown(object):
    def __init__(self, series: pd.Series, eps: float = 0) -> object:
        """
        Drawdown for a given series
        :param series: pandas Series
        :param eps: a day is down day if the drawdown (positive) is larger than eps
        """
        # check series is indeed a series
        assert isinstance(series, pd.Series)
        # check that all indices are increasing
        assert series.index.is_monotonic_increasing
        # make sure all entries non-negative
        assert not (series < 0).any()

        self.__series = series
        self.__eps = eps

    @property
    def eps(self):
        return self.__eps

    @property
    def price_series(self) -> pd.Series:
        return self.__series

    @property
    def highwatermark(self) -> pd.Series:
        return self.__series.expanding(min_periods=1).max()

    @property
    def drawdown(self) -> pd.Series:
        return 1 - self.__series / self.highwatermark

    @property
    def periods(self):
        d = self.drawdown

        # the first price can not be in drawdown
        assert d.iloc[0] == 0

        # Drawdown days
        is_down = d > self.__eps

        s = pd.Series(index=is_down.index[1:], data=[r for r in zip(is_down[:-1], is_down[1:])])

        # move from no-drawdown to drawdown
        start = list(s[s == (False, True)].index)

        # move from drawdown to drawdown
        end = list(s[s == (True, False)].index)

        # eventually append the very last day...
        if len(end) < len(start):
            # add a point to the series... value doesn't matter
            end.append(s.index[-1])

        return pd.Series({s: e - s for s, e in zip(start, end)})
