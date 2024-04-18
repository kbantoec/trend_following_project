import pandas as pd
import sqlalchemy as sq

from pyutil.sql.base import Base
#from pyutil.sql.interfaces.products import ProductInterface
from pyutil.sql.product import Product


class Security(Product, Base):
    fullname = sq.Column("fullname", sq.String, nullable=True)

    def __init__(self, name, fullname=None, **kwargs):
        super().__init__(str(name), **kwargs)
        self.fullname = fullname

    def __repr__(self):
        return "Security({id}: {name})".format(id=self.name, name=self.reference["Name"])

    @staticmethod
    def reference_frame(securities, f=lambda x: x):
        frame = Product.reference_frame(products=securities, f=f)
        frame["fullname"] = pd.Series({f(s): s.fullname for s in securities})
        frame.index.name = "security"
        return frame
