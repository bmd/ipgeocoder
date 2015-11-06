import os
import sqlite3 as sqlite

from database import DatabaseAbstract


class SqliteDatabase(DatabaseAbstract):

    def __init__(self, path):
        super(SqliteDatabase, self).__init__()
        self.conn = self._connect(path)

    def _connect(self, credentials):
        """
        Connect to the sqlite database. If no database
        exists, an error will be raised.

        :param credentials: The database path
        :return: sqlite3.connection
        :raises IOError:
        """
        if not os.path.exists(credentials):
            raise IOError(
                "The IP database doesn't exist at the path you specified ({0}). Run the "
                "database migration if this is the first time you are using this script"
                .format(credentials)
            )

        return sqlite.connect(credentials)

    def persist(self, ip):
        super(SqliteDatabase, self).persist(ip)

    def retrieve(self, ip):
        super(SqliteDatabase, self).retrieve(ip)




