import unicodecsv as csv


class IpGeocoderService(object):

    cache_expires_days = 7

    def __init__(self, geocoder, database, infile):
        self.geocoder = geocoder
        self.database = database
        self.source = self._get_file_generator(infile)

    def _get_file_generator(self, fpath):
        """
        Construct a generator from the input CSV file
        :param fpath:
        :return: csv.DictReader
        """
        with open(fpath, 'rU') as inf:
            return csv.DictReader(inf)

    def _geocode_row(self, r):
        """
        Geocode a row and store it in the database

        :param r: A dict-like object
        :return: boolean
        """
        result = self.geocoder.geocode(r)

        return self.database.persist(result)

    def geocode_source(self):
        """
        Consume the source generator and attempt to geocode
        each element.

        :return: True
        """
        for x in self.source:
            self._geocode_row(x)

        return True
