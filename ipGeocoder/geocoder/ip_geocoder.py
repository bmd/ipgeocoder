import geocoder


class IpGeocoder(object):

    def __init__(self, ip_addr_col_name='ip_address'):
        self.col_name = ip_addr_col_name

    def geocode(self, obj):
        ip = obj[self.col_name]
        return geocoder.freegeoip(ip)
