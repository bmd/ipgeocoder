import geocoder
import logging

logger = logging.getLogger("ipGeocoder.IpGeocoder")


class IpGeocoder(object):

    def __init__(self, ip_addr_col_name='ip_address'):
        logger.debug("Creating IpGeocoder instance")
        logger.debug("Expecting IPs in key '{0}'".format(ip_addr_col_name))

        self.col_name = ip_addr_col_name

    def geocode(self, obj):
        """
        Proccess an IP with the FreeGeoIP API.

        :param obj: A dict-like object containing a key that returns an IP address
            to be geocoded.
        :return: Dict
        """
        ip = obj[self.col_name]

        return geocoder.freegeoip(ip)
