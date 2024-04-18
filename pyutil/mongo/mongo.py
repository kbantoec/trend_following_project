import os
import random
import string
import pandas as pd

from pymongo import MongoClient


def mongo_database(host=None, port=None, database=None, username=None, password=None, authSource=None):
    """ Create a Mongo Client """
    host = host or os.environ["MONGODB_HOST"]
    database = database or os.environ["MONGODB_DATABASE"]
    port = port or os.environ["MONGODB_PORT"]
    username = username or os.environ["MONGODB_USERNAME"]
    password = password or os.environ["MONGODB_PASSWORD"]
    authSource = authSource or os.environ["MONGODB_DATABASE"]

    return MongoClient(host=host, port=int(port), username=username, password=password, authSource=authSource)[database]


def create_collection(database, name=None):
    name = name or "".join(random.choices(string.ascii_lowercase, k=10))
    return _Collection(database[name])


class _MongoObject(object):
    @staticmethod
    def __parse(x=None):
        try:
            return pd.read_msgpack(x)
        except ValueError:
            return x

    def __init__(self, mongo_dict):
        self.__data = mongo_dict["data"]
        self.__t = mongo_dict.get("now", 0)
        self.__meta = {x: mongo_dict.get(x) for x in set(mongo_dict.keys()).difference({"_id", "data", "now"})}

    @property
    def data(self):
        return self.__parse(self.__data)

    @property
    def meta(self):
        return self.__meta

    @property
    def t(self):
        return self.__t


class _Collection(object):
    def __init__(self, collection):
        self.__col = collection

    @property
    def name(self):
        return self.collection.name

    def upsert(self, value=None, **kwargs):
        # check it's either unique or not there
        assert self.__col.count_documents(kwargs) <= 1, "Identifier not unique {a}. {b}".format(a=self.__col.count_documents(kwargs), b=kwargs)

        if value is not None:
            try:
                # make sure value is a series not a DataFrame!
                self.__col.update_one(kwargs, {"$set": {"data": value.to_msgpack(), "now": pd.Timestamp("now")}}, upsert=True)
            except AttributeError:
                self.__col.update_one(kwargs, {"$set": {"data": value, "now": pd.Timestamp("now")}}, upsert=True)

    def find(self, **kwargs):
        for a in self.__col.find(kwargs):
            yield _MongoObject(mongo_dict=a)

    def find_one(self, **kwargs):
        n = self.__col.count_documents(kwargs)
        if n == 0:
            return None

        assert n <= 1, "Could not find a unique document"

        return _MongoObject(self.__col.find_one(kwargs))

    @property
    def collection(self):
        return self.__col

    def __repr__(self):
        return self.__col.__repr__()

    def read(self, default=None, **kwargs):
        try:
            return self.find_one(**kwargs).data
        except AttributeError:
            return default

    def delete(self, **kwargs):
        return self.__col.delete_many(kwargs)
