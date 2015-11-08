# coding: utf-8

import logging
import os
import sqlite3 as sqlite
import hashlib

from .database import DatabaseAbstract

logger = logging.getLogger("ipGeocoder.SqliteDatabase")


class SqlStatements(object):
    """ Avoid hardcoding these into the database class"""

    retrieve_row = "SELECT * FROM ip_geo WHERE ip_hash = ? LIMIT 1"

    insert_row = (
        "INSERT INTO ip_geo (ip_hash, ip_str, city, state, country, postal, latitude, longitude, timezone"
        ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);"
    )


class SqliteDatabase(DatabaseAbstract):

    def __init__(self, path):
        logger.info("Constructing database connector")
        super(SqliteDatabase, self).__init__()
        self.conn = self._connect(path)
        self.conn.text_factory = str

    @staticmethod
    def _int_hash_ip(ip, n=10):
        """
        Hash an IP and return an integer representation of the hashed
        value. This is useful because we want to look up the IPs in
        a SQLITE database, but don't want to use a string as the row
        PK. This provides a way to infer the PK of a row deterministically
        from the IP input.

        :param ip: The IP address to hash
        :param n: (optional) The length of the integer to return. This must
            be less than the maximum integer length allowed by the data
            storage type.
        :return: An integer representation of the IP address
        """
        ip_hash = int(hashlib.sha1(ip).hexdigest(), 16) % (10 ** n)

        return ip_hash

    def _connect(self, credentials):
        """
        Connect to the sqlite database. If no database
        exists, an error will be raised.

        :param credentials: The database path on disk
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
        c.execute(
            SqlStatements.insert_row, (
                self._int_hash_ip(data['ip']),
                data['ip'],
                data['city'] if 'city' in data else None,
                data['state'] if 'state' in data else None,
                data['country'] if 'country' in data else None,
                data['postal'] if 'postal' in data else None,
                data['lat'] if 'lat' in data else None,
                data['lng'] if 'lng' in data else None,
                data['time_zone'] if 'time_zone' in data else None
            )
        )
        self.conn.commit()

        return True

    def retrieve(self, ip):
        c = self.conn.cursor()
        row = c.execute(SqlStatements.retrieve_row, (self._int_hash_ip(ip), ))
        result = row.fetchall()

        try:
            return result[0]
        except IndexError:
            return []
