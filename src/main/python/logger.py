import logging

class Singleton(type):
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]

class Logger(object):
    __metaclass__ = Singleton

    def __init__(self):
        self._logger = logging.getLogger('[WikiTopK]')
        self._logger.setLevel(logging.INFO)
        formatter1 = logging.Formatter('%(name)s - %(message)s')
        fh = logging.FileHandler('log/debug.log',mode='w')
        fh.setLevel(logging.ERROR)
        fh.setFormatter(formatter1)
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(formatter1)

        self._logger.addHandler(fh)
        self._logger.addHandler(ch)


