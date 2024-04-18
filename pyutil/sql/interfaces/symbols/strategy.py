import enum as _enum
import os

import pandas as pd
import sqlalchemy as sq
from sqlalchemy.types import Enum as _Enum

from pyutil.portfolio.portfolio import Portfolio
from pyutil.sql.base import Base
from pyutil.sql.product import Product
from pyutil.strategy.config import ConfigMaster


def _module(source):
    from types import ModuleType

    compiled = compile(source, '', 'exec')
    mod = ModuleType("module")
    exec(compiled, mod.__dict__)
    return mod


class StrategyType(_enum.Enum):
    mdt = 'mdt'
    conservative = 'conservative'
    balanced = 'balanced'
    dynamic = 'dynamic'


StrategyTypes = {s.value: s for s in StrategyType}


class Strategy(Product, Base):
    __searchable__ = ["name", "type"]
    active = sq.Column(sq.Boolean)
    source = sq.Column(sq.String)
    type = sq.Column(_Enum(StrategyType))

    def __init__(self, name, active=True, source="", type=StrategyType.conservative):
        super().__init__(name)
        self.active = active
        self.source = source
        self.type = type

    def configuration(self, reader=None) -> ConfigMaster:
        # Configuration only needs a reader to access the symbols...
        # Reader is a function taking the name of an asset as a parameter
        return _module(self.source).Configuration(reader=reader)

    @property
    def portfolio(self):
        prices = self.series["PRICES"]
        weights = self.series["WEIGHTS"]

        if prices is None and weights is None:
            return None
        else:
            return Portfolio(prices=prices, weights=weights)

    @portfolio.setter
    def portfolio(self, portfolio):
        self.series["WEIGHTS"] = portfolio.weights
        self.series["PRICES"] = portfolio.prices

    @property
    def assets(self):
        return self.configuration(reader=None).names

    @property
    def last_valid_index(self):
        try:
            return self.series["PRICES"].last_valid_index() #.last(key="PRICES", name=self.name)
        except AttributeError:
            return None

    @classmethod
    def reference_frame(cls, products, f=lambda x: x) -> pd.DataFrame:
        frame = Product.reference_frame(products=products, f=f)
        frame["source"] = pd.Series({f(s): s.source for s in products})
        frame["type"] = pd.Series({f(s): s.type for s in products})
        frame["active"] = pd.Series({f(s): s.active for s in products})
        frame.index.name = "strategy"
        return frame

    @staticmethod
    def navs(strategies, f=lambda x: x) -> pd.DataFrame:
        return pd.DataFrame({key: item.nav for key, item in Strategy.portfolios(strategies, f).items()})

        #return pd.DataFrame({f(strategy): strategy.portfolio.nav for strategy in strategies})

    @staticmethod
    def portfolios(strategies, f=lambda x: x) -> dict:
        return {f(strategy): strategy.portfolio for strategy in strategies if strategy.portfolio is not None}
