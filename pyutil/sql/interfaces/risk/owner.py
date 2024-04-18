import pandas as pd
import sqlalchemy as sq
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship as _relationship

from pyutil.sql.base import Base
#from pyutil.sql.interfaces.products import ProductInterface
from pyutil.sql.interfaces.risk.custodian import Currency
from pyutil.sql.product import Product


class Owner(Product, Base):
    fullname = sq.Column("fullname", sq.String, nullable=True)

    __currency_id = sq.Column("currency_id", sq.Integer, sq.ForeignKey(Currency.id), nullable=True)
    __currency = _relationship(Currency, foreign_keys=[__currency_id], lazy="joined")

    def __init__(self, name, currency=None, fullname=None):
        super().__init__(name=name)
        self.currency = currency
        self.fullname = fullname

    def __repr__(self):
        return "Owner({id}: {fullname}, {currency})".format(id=self.name, fullname=self.fullname,
                                                            currency=self.currency.name)

    @hybrid_property
    def currency(self):
        return self.__currency

    @currency.setter
    def currency(self, value):
        self.__currency = value

    # @property
    # def securities(self):
    #    return set([x[0] for x in self._position.keys()])

    # @property
    # def custodians(self):
    #    return set([x[1] for x in self._position.keys()])

    # @property
    # def position_frame(self):
    #     a = pd.DataFrame(dict(self._position))
    #     if not a.empty:
    #         a = a.transpose().stack()
    #
    #         a.index.names = ["Security", "Custodian", "Date"]
    #         return a.to_frame(name="Position")
    #     else:
    #         return a
    #
    # @property
    # def vola_security_frame(self):
    #     assert self.currency, "The currency for the owner is not specified!"
    #     x = pd.DataFrame({security: security.volatility(self.currency) for security in set(self.securities)}) \
    #
    #     if not x.empty:
    #         x = x.stack()
    #         x.index.names = ["Date", "Security"]
    #         return x.swaplevel().to_frame("Volatility")
    #
    #     return x

    # @property
    # def reference_securities(self):
    #    return Security.frame(self.securities)

    # @property
    # def position_reference(self):
    #    reference = self.reference_securities
    #    position = self.position_frame
    #    volatility = self.vola_security_frame
    #    try:
    #        position_reference = position.join(reference, on="Security")
    #        return position_reference.join(volatility, on=["Security", "Date"])
    #    except (KeyError, ValueError):
    #        return pd.DataFrame({})

    # def to_json(self):
    #    ts = fromReturns(r=self._returns)
    #    return {"name": self.name, "Nav": ts, "Volatility": ts.ewm_volatility(), "Drawdown": ts.drawdown}

    # def upsert_position(self, security, custodian, ts):
    #    assert isinstance(security, Security)
    #    assert isinstance(custodian, Custodian)

    #    key = (security, custodian)
    #    self._position[key] = merge(new=ts, old=self._position.get(key, default=None))
    #    return self.position(security=security, custodian=custodian)

    # def position(self, security, custodian):
    #    return self._position[(security, custodian)]

    # def flush(self):
    #    # delete all positions...
    #    self._position.clear()

    @staticmethod
    def reference_frame(owners, f=lambda x: x) -> pd.DataFrame:
        frame = Product.reference_frame(products=owners, f=f)
        # that's why owners can't be None
        frame["Currency"] = pd.Series({f(owner): owner.currency.name for owner in owners})
        frame["Entity ID"] = pd.Series({f(owner): owner.name for owner in owners})
        frame["Name"] = pd.Series({f(owner): owner.fullname for owner in owners})
        frame.index.name = "owner"
        return frame
