import sys

from ipGeocoder import IpGeocoderService
from ipGeocoder import SqliteDatabase
from ipGeocoder import IpGeocoder


def geocode_file(infile, col_name, database):
    """
    Consume a file and geocode any IPs it contains, persisting the results in a data
    store that adheres to the IP Geocoder Database API.

    :param infile: The path of the CSV file to geocode
    :param col_name: The name of the column containing the IP address
    :param database: The credentials required to access the database. The
        type of this parameter will vary depending on the data type
        expected by the database instance.
    :return: true
    """
    service = IpGeocoderService(
        SqliteDatabase(database),
        IpGeocoder(ip_addr_col_name=col_name),
        infile
    )

    service.geocode_source()

    return True

if __name__ == '__main__':
    """
    USAGE: python geocode_file.py ips_to_geocode.csv ip_addr geo_ips.sqlite
    """
    geocode_file(sys.argv[1], sys.argv[2], sys.argv[3])
