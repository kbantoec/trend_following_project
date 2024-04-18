from pyutil.sql.base import Base
from pyutil.sql.product import Product


class Frame(Product, Base):
    def __init__(self, name):
        super().__init__(name)
