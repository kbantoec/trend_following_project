from sqlalchemy import Column, Integer
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.ext.declarative import as_declarative


@as_declarative()
class Base(object):

    @declared_attr
    def __tablename__(cls):
        return cls.__name__.lower()

    id = Column("id", Integer, primary_key=True, autoincrement=True, nullable=False)
