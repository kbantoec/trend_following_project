import abc
import logging
import pandas as pd


class ConfigMaster(dict):
    """ Every strategy is described by a configuration object. Each such object inherits from the ConfigMaster class."""
    __metaclass__ = abc.ABCMeta

    def __init__(self, names, reader, logger=None, **kwargs):
        """
        Abstract base class for all strategies

        :param reader: is a function accepting a name and the field as an argument. Returns a timeseries...
        :param names: names of the assets used in the strategy
        :param logger: logger
        """
        super().__init__(**kwargs)

        # the logger
        self.logger = logger or logging.getLogger(__name__)
        self.__reader = reader
        self.__names = names

    @property
    @abc.abstractmethod
    def portfolio(self):
        """ Portfolio described by the Configuration """

    @property
    def names(self):
        return self.__names

    @property
    def reader(self):
        return self.__reader

    def history(self, t0=pd.Timestamp("2002-01-01")):
        h = pd.DataFrame({name: self.__reader(name=name) for name in self.__names})
        return h.truncate(before=t0).dropna(axis=0, how="all")

    @property
    def parameter(self):
        return {key: value for key, value in self.items()}
