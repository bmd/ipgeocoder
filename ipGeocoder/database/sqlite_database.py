import logging
import os
import sqlite3 as sqlite

from database import DatabaseAbstract

logger = logging.getLogger("ipGeocoder.SqliteDatabase")


class SqliteDatabase(DatabaseAbstract):

    def __init__(self, path):
        logger.info("Constructing database connector")
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
        logger.info("Connecting to SQLITE database '{0}'".format(credentials))
        if not os.path.exists(credentials):
            logger.error("No database available at '{0}'".format(credentials))
            raise IOError(
                "The IP database doesn't exist at the path you specified ({0}). Run the "
                "database migration if this is the first time you are using this script"
                .format(credentials)
            )

        return sqlite.connect(credentials)

    def persist(self, data):
        """
        Store a geocoded IP in the database for easy retrieval in the future.

        :param data: Data to persist.
        :return: Boolean indicating success or failure of the persist operation
        """
        logger.debug("Caching ip '{0}' in database".format(data['ip']))

        c = self.conn.cursor()

        c.execute("""
          INSERT INTO ip_geo (
            ip_hash, ip_str, city, state, country, postal, create_dt, update_dt
          ) VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?
          );
        """, (hex(data['ip']), data['ip']))

    def retrieve(self, ip):
        logger.debug("")
        super(SqliteDatabase, self).retrieve(ip)

        c = self.conn.cursor()
        row = c.execute("SELECT * FROM ip_geo WHERE ip_hash = ? LIMIT 1", hex(ip))

        return row
