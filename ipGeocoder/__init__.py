"""
@package ipGeocoder
"""
from service import IpGeocoderService
from database import SqliteDatabase
from geo import IpGeocoder

# silence base logger
import logging
logger = logging.getLogger('ipGeocoder').addHandler(logging.NullHandler())
