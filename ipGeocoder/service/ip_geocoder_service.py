import unicodecsv as csv
import logging
from datetime import timedelta

logger = logging.getLogger("ipGeocoder.IpGeocoderService")


class IpGeocoderService(object):

    cache_expires_days = 7

    def __init__(self, geocoder, database, infile):
        logger.info("Constructing geocoder service instance")
        self.file_reader = csv.DictReader
        self.geocoder = geocoder
        self.database = database
        self.source = self._get_file_generator(infile)

    def _get_file_generator(self, fpath):
        """
        Construct a generator from the input CSV file
        :param fpath:
        :return: csv.DictReader
        """
        logger.debug("Fetching data file from path '{0}'".format(fpath))
        inf = open(fpath, 'rU')
        return self.file_reader(inf)

    def _geocode_row(self, r):
        """
        Geocode a row and store it in the database

        :param r: A dict-like object
        :return: boolean
        """
        current_status = self.database.retrieve(r[self.geocoder.col_name])

        if current_status:
            logger.debug("Result already cached, skipping...")
            return True

        result = self.geocoder.geocode(r)
        geocode = result.json

        if geocode['ok']:
            logger.debug("Geocode successful")
            logger.debug(result.json)
            return self.database.persist(geocode)
        else:
            logger.warning("Geocode failed")
            logger.warning(result.json)
            return False

    def geocode_source(self):
        """
        Consume the source generator and attempt to geocode
        each element.

        :return: True
        """
        for x in self.source:
            logger.debug(x['ip_addr'])
            self._geocode_row(x)

        return True
