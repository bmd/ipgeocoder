import geocoder
import logging

logger = logging.getLogger("ipGeocoder.IpGeocoder")


class IpGeocoder(object):

    def __init__(self, ip_addr_col_name='ip_address'):
        logger.debug("Expecting IPs associated with key '{0}'".format(ip_addr_col_name))
        self.col_name = ip_addr_col_name

    def geocode(self, obj):
        """
        Proccess an IP with the FreeGeoIP API.

        :param obj: A dict-like object containing a key that returns an IP address
            to be geocoded.
        :return: dict
        """
        ip = obj[self.col_name]
        logger.info("Geocoding IP address: {0}".format(ip))

        geocode = geocoder.freegeoip(ip)
        return geocode.json