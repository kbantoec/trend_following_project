from pyutil.sql.base import Base
from pyutil.sql.product import Product


class Custodian(Product, Base):
    def __init__(self, name, **kwargs):
        super().__init__(name, **kwargs)


class Currency(Product, Base):
    def __init__(self, name, **kwargs):
        super().__init__(name, **kwargs)
