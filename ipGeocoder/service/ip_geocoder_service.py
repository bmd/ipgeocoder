import unicodecsv as csv
import logging

logger = logging.getLogger("ipGeocoder.IpGeocoderService")


class IpGeocoderService(object):

    cache_expires_days = 7

    def __init__(self, geocoder, database, infile, file_reader=csv.DictReader):
        logger.info("Constructing geocoder service instance")
        self.file_reader = file_reader
        self.geocoder = geocoder
        self.database = database
        self.source = self._get_file_generator(infile)

    def _get_file_generator(self, fpath):
        """
        Construct a generator from the input CSV file.

        :param fpath: The path to fetch a file from.
        :return: File reader class. The class itself doesn't matter
            as long as its API approximates csv.DictReader by consuming
            an open file resource and returning a generator that produces
            a key-value pair for each returned "row".
        """
        logger.debug("Fetching data file from path '{0}'".format(fpath))
        inf = open(fpath, 'rU')

        return self.file_reader(inf)

    def _geocode_row(self, r):
        """
        Geocode a row and store it in the database. If the row is already
        stored in the database, and hasn't expired, based on the value set
        in IpGeocoderService.cache_expires_days, then _geocode_row() will
        return the current database row.

        :param r: A dict-like object
        :return: boolean
        """
        current_status = self.database.retrieve(r[self.geocoder.col_name])

        if current_status:
            logger.debug("Result already cached, skipping...")
            return True

        geocode = self.geocoder.geocode(r)

        if geocode['ok']:
            logger.debug("Geocode successful")
            logger.debug(geocode)
            return self.database.persist(geocode)
        else:
            logger.warning("Geocode failed")
            logger.warning(geocode)
            return False

    def geocode_source(self):
        """
        Consume the source generator and attempt to geocode
        each element.

        :return: True
        """
        for x in self.source:
            self._geocode_row(x)

        return True
