from abc import ABCMeta, abstractmethod


class DatabaseAbstract(object):
    """ Defines the API for database classes """
    __metaclass__ = ABCMeta

    @abstractmethod
    def __init__(self):
        """ Construct a database object """
        return

    @abstractmethod
    def _connect(self, credentials):
        """ Connect to the database and return a cursor object """
        return

    @abstractmethod
    def retrieve(self, ip):
        """ Look up a single IP in the database """
        return

    @abstractmethod
    def persist(self, ip):
        """ Store a geocoded IP in the database """
        return
