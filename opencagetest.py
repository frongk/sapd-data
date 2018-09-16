from opencage.geocoder import OpenCageGeocode
from key_file import OPENCAGE_KEY

geocoder = OpenCageGeocode(OPENCAGE_KEY)

query = "san jose mission, san antonio, tx"
result = geocoder.geocode(query)

import webbrowser
webbrowser.open(result[0]['annotations']['OSM']['url'])
