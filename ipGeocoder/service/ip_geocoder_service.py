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
        with open(fpath, 'rU') as inf:
            return self.file_reader(inf)

    def _geocode_row(self, r):
        """
        Geocode a row and store it in the database

        :param r: A dict-like object
        :return: boolean
        """
        current_status = self.database.retrieve(r)

        # no record of this IP at all
        if not current_status:
            result = self.geocoder.geocode(r)
            return self.database.persist(result)

        # record exists but it's out of date
        elif 2 == 3:
            result = self.geocoder.geocode(r)
            return self.database.persist(result)

        # record exists and ist not out of date!
        else:
            return True

    def geocode_source(self):
        """
        Consume the source generator and attempt to geocode
        each element.

        :return: True
        """
        for x in self.source:
            self._geocode_row(x)

        return True
