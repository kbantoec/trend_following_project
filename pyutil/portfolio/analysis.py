import pandas as pd

from pyutil.performance.summary import NavSeries


def __attempt(f, argument):
    try:
        return f(argument)
    except:
        return None


def __last(frame, datefmt=None):
    frame = frame.sort_index(axis=1, ascending=False)
    if datefmt:
        frame = frame.rename(columns=lambda x: x.strftime(datefmt))
    frame = frame.dropna(axis=0, how="all")
    frame["total"] = (frame + 1).prod(axis=1) - 1
    frame.index.name = "Portfolio"
    return frame


def nav(portfolios: dict) -> pd.DataFrame:
    """
    :param portfolios: A dictionary of Portfolios
    :return: A dictionary of NAV curves
    """
    assert isinstance(portfolios, dict)
    frame = pd.DataFrame({name: p.nav for name, p in portfolios.items() if hasattr(p, "nav")})
    frame.columns.name = "Portfolio"
    return frame


def mtd(frame) -> pd.DataFrame:
    """

    :param frame: A DataFrame of Nav curves?

    :return:
    """
    d = {name: __attempt(f=lambda x: NavSeries(x).mtd_series, argument=series) for name, series in frame.items()}
    x = __last(pd.DataFrame(d).transpose(), datefmt="%b %d")
    return x.dropna(axis=0, how="all")


def ytd(frame) -> pd.DataFrame:
    """
    :param frame: 
    :return: 
    """
    d = {name: __attempt(f=lambda x: NavSeries(x).ytd_series, argument=series) for name, series in frame.items()}
    return __last(pd.DataFrame(d).transpose(), datefmt="%m").dropna(axis=0, how="all")


def recent(frame, n=15) -> pd.DataFrame:
    d = {name: __attempt(f=lambda x: NavSeries(x).recent(n=n), argument=series) for name, series in frame.items()}
    return __last(pd.DataFrame(d).tail(n).transpose(), datefmt="%b %d").dropna(axis=0, how="all")


def sector(portfolios, symbolmap, total=False) -> pd.DataFrame:
    assert isinstance(portfolios, dict)
    d = {name: __attempt(f=lambda x: x.sector(symbolmap=symbolmap, total=total).iloc[-1], argument=portfolio) for name, portfolio in portfolios.items()}
    return pd.DataFrame(d).dropna(axis=1, how="all")


def performance(frame, **kwargs) -> pd.DataFrame:
    d = {name: __attempt(f=lambda x: NavSeries(x).summary_format(**kwargs), argument=series) for name, series in frame.items()}
    return pd.DataFrame(d).dropna(axis=1, how="all")


def drawdown(frame) -> pd.DataFrame:
    d = {name: __attempt(f=lambda x: NavSeries(x).drawdown, argument=series) for name, series in frame.items()}
    return pd.DataFrame(d).dropna(axis=1, how="all")


def ewm_volatility(frame, **kwargs) -> pd.DataFrame:
    d = {name: __attempt(f=lambda x: NavSeries(x).ewm_volatility(**kwargs), argument=series) for name, series in frame.items()}
    return pd.DataFrame(d).dropna(axis=1, how="all")


