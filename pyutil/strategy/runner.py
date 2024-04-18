import logging
from functools import partial
import multiprocessing as mp

from pyutil.sql.interfaces.symbols.strategy import Strategy
from pyutil.sql.interfaces.symbols.symbol import Symbol


def _strategy_update(strategy_id, connection_str, logger=None):
    from pyutil.sql.session import session
    logger = logger or logging.getLogger(__name__)

    def reader(session):
        return lambda name: session.query(Symbol).filter(Symbol.name == name).one().series["PX_LAST"]

    # do a read is enough...
    with session(connection_str=connection_str) as session:
        # make fresh mongo clients to avoid forking issues
        #Strategy.refresh_mongo()
        #Symbol.refresh_mongo()

        strategy = session.query(Strategy).filter_by(id=strategy_id).one()
        logger.debug(strategy.name)

        return strategy.name, strategy.configuration(reader=reader(session)).portfolio


def run(strategies, connection_str, logger=None):
    pool = mp.Pool(mp.cpu_count())
    __update = partial(_strategy_update, connection_str=connection_str, logger=logger)
    return {r[0]: r[1] for r in pool.map(__update, [strategy.id for strategy in strategies])}
