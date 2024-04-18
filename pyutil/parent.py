import logging
import time

from contextlib import ExitStack


class Production(ExitStack):
    def __init__(self, log=None):
        super().__init__()
        self.__logger = log or logging.getLogger(__name__)
        self.__time = time.time()

    @property
    def logger(self):
        return self.__logger

    @property
    def elapsed(self):
        return time.time() - self.__time

    def __exit__(self, exc_type, exc_val, exc_tb):
        super().__exit__(exc_type, exc_val, exc_tb)
        if exc_type is not None:
            self.logger.exception(exc_val)
            return False

        self.logger.info("Time elapsed: {0}".format(self.elapsed))
        return True
