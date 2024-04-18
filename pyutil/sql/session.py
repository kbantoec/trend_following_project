from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.exc import NoResultFound


@contextmanager
def session(connection_str, echo=False, base=None, expire_on_commit=True):
    """Provide a transactional scope around a series of operations."""
    try:
        s = session_factory(connection_str, echo=echo, base=base, expire_on_commit=expire_on_commit)
        yield s
        s.commit()
    except Exception as e:
        s.rollback()
        raise e
    finally:
        s.close()


def session_factory(connection_str, echo=False, base=None, expire_on_commit=True):
    engine = create_engine(connection_str, echo=echo)
    if base:
        base.metadata.create_all(engine)

    factory = sessionmaker(engine, expire_on_commit=expire_on_commit)
    return factory()


def get_one_or_create(session, model, **kwargs):
    #  see http://skien.cc/blog/2014/01/15/sqlalchemy-and-race-conditions-implementing/

    try:
        return session.query(model).filter_by(**kwargs).one(), True
    except NoResultFound:
        # create the test_model object
        a = model(**kwargs)
        # add it to session
        session.add(a)
        return a, False


def get_one_or_none(session, model, **kwargs):
    try:
        return session.query(model).filter_by(**kwargs).one()
    except NoResultFound:
        return None
