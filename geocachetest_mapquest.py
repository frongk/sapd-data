import urllib3
import sqlite3

# API KEYS
from key_file import MAPQUEST_KEY, MAPQUEST_SECRET


# get test data from db
conn = sqlite3.connect('sapd.db')
c = conn.cursor()
raw_ = c.execute('select Address from sapd limit 100')
test_locations = [x[0].replace(' ','%20') for x in raw_.fetchall()]


# compose request url
base_url = 'http://open.mapquestapi.com/geocoding/v1/batch?'
base_url += 'key='+MAPQUEST_KEY
locations = '&location=' + '&location='.join(test_locations)
format_ = '&outFormat=csv'
url = base_url + locations + format_

http = urllib3.PoolManager()
response = http.request('GET', url)

data = str(response.data).split('\\n')
data = [x.split(',') for x in data]
