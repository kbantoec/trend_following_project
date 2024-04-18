import pandas as pd


def adjust(ts: pd.Series, value=100.0) -> pd.Series:
    try:
        return value * ts / (ts.dropna().iloc[0])
    except IndexError:
        # can happen if the series is empty (after droppning all NaNs)
        return None
        # pd.Series({})
