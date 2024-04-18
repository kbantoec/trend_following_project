import pandas as pd
from sqlalchemy import String, Column

from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm.exc import NoResultFound

from pyutil.mongo.mongo import create_collection
from pyutil.timeseries.merge import merge


class _Mongo(object):
    def __init__(self, name, collection):
        self.__collection = collection
        self.__name = name

    @property
    def collection(self):
        return self.__collection

    @property
    def name(self):
        return self.__name

    def delete(self, **kwargs):
        return self.collection.delete(name=self.name, **kwargs)

    def __setitem__(self, key, value):
        self.collection.upsert(name=self.name, value=value, key=key)

    def __getitem__(self, item):
        return self.collection.read(key=item, name=self.name, default=None)

    def get(self, item, default=None, **kwargs):
        try:
            return self.collection.find_one(name=self.name, key=item, **kwargs).data
        except AttributeError:
            return default

    def write(self, data, key, **kwargs):
        self.collection.upsert(value=data, key=key, name=self.name, **kwargs)


class _Reference(_Mongo):
    def __init__(self, name, collection):
        super().__init__(name, collection)

    def __iter__(self):
        for a in self.collection.find(name=self.name):
            yield a.meta["key"]

    def items(self):
        for a in self.collection.find(name=self.name):
            yield a.meta["key"], a.data

    def keys(self):
        return set([a for a in self])


class _Timeseries(_Mongo):
    def __init__(self, name, collection):
        super().__init__(name, collection)

    def __iter__(self):
        for a in self.collection.find(name=self.name):
            yield a.meta

    def items(self, **kwargs):
        for a in self.collection.find(name=self.name, **kwargs):
            yield a.meta, a.data

    def keys(self, **kwargs):
        for a in self.collection.find(name=self.name, **kwargs):
            yield a.meta

    def last(self, key, **kwargs):
        try:
            return self.get(item=key, **kwargs).last_valid_index()
        except AttributeError:
            return None

    def merge(self, data, key, **kwargs):
        old = self.get(item=key, **kwargs)
        self.write(data=merge(new=data, old=old), key=key, **kwargs)


class Product(object):
    __name = Column("name", String(1000), unique=True, nullable=False)

    mongo_database = None

    @classmethod
    def collection(cls):
        # this is a very fast operation, as a new client is not created here...
        return create_collection(database=cls.mongo_database, name=cls.__name__.lower())

    @classmethod
    def collection_reference(cls):
        return create_collection(database=cls.mongo_database, name=cls.__name__.lower() + "_reference")

    def __init__(self, name, *args, **kwargs):
        self.__name = str(name)

    @hybrid_property
    def name(self):
        # the traditional way would be to make the __name public, but then it can be changed on the fly (which we would like to avoid)
        # if we make it a standard property stuff like session.query(Symbol).filter(Symbol.name == "Maffay").one() won't work
        # Thanks to this hybrid annotation sqlalchemy translates self.__name into proper sqlcode
        # print(session.query(Symbol).filter(Symbol.name == "Maffay"))
        return self.__name

    def __repr__(self):
        return "{name}".format(name=self.name)

    def __lt__(self, other):
        return self.name < other.name

    def __eq__(self, other):
        return self.__class__ == other.__class__ and self.name == other.name

    # we want to make a set of assets, etc....
    def __hash__(self):
        return hash(self.name)

    @property
    def reference(self):
        return _Reference(name=self.name, collection=self.__class__.collection_reference())

    @property
    def series(self):
        # isn't that very expensive
        return _Timeseries(name=self.name, collection=self.__class__.collection())

    @classmethod
    def reference_frame(cls, products, f=lambda x: x) -> pd.DataFrame:
        frame = pd.DataFrame({product: pd.Series({key: data for key, data in product.reference.items()}) for product in
                              products}).transpose()
        frame.index = map(f, frame.index)
        frame.index.name = cls.__name__.lower()
        return frame.sort_index()

    @classmethod
    def pandas_frame(cls, key, products, f=lambda x: x, **kwargs) -> pd.DataFrame:
        frame = pd.DataFrame({product: product.series.get(item=key, **kwargs) for product in products})
        frame = frame.dropna(axis=1, how="all").transpose()
        frame.index = map(f, frame.index)
        frame.index.name = cls.__name__.lower()
        return frame.sort_index().transpose()

    @classmethod
    def products(cls, session, names=None):
        # extract symbols from database
        if names is None:
            return session.query(cls).all()
        else:
            return session.query(cls).filter(cls.name.in_(names)).all()

    @classmethod
    def delete(cls, session, name):
        try:
            obj = session.query(cls).filter(cls.name == name).one()
            obj.series.delete()
            obj.reference.delete()
            session.delete(obj)
            session.commit()
        except NoResultFound:
            pass
