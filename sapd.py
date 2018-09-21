import urllib3
import pandas as pd
import numpy as np

from bs4 import BeautifulSoup
import sqlite3

import time
import os

from opencage.geocoder import OpenCageGeocode
from key_file import OPENCAGE_KEY

import pdb

# don't need to see this warning every iteration
urllib3.disable_warnings()

class SAPDData(object):

    def __init__(self, db_name, table_name, geocode_table):
        self.db_name = db_name
        self.table_name = table_name
        self.geocode_table = geocode_table

        self.connect_db()
        self.geocode_connect()
        

    def connect_db(self):
        if os.path.isfile(self.db_name):
            try:
                self.conn = sqlite3.connect(self.db_name)
                self.cursor = self.conn.cursor()
                res = self.cursor.execute("select * from "+ self.table_name + " limit 1")
            except:
                self.conn = self.create_db()
                self.cursor = self.conn.cursor()
        else:
            self.conn = self.create_db()
            self.cursor = self.conn.cursor()


    def create_db(self):
        data = self.get_new_data()
        conn = sqlite3.connect(self.db_name)
        data.to_sql(self.table_name, conn)
        return conn

        
    def geocode_connect(self):
        # make sure geocode table exists (create it if it doesn't)
        try:
            self.cursor.execute("select * from "+ self.geocode_table + " limit 1")
        except:
            data = [[100000, 'Test, San Antonio', 29.4241, -98.4936]]
            df = pd.DataFrame(data, columns = ['id','Address', 'lat', 'long'])
            df.set_index('id',inplace=True)
            df.to_sql(self.geocode_table, self.conn)

            #add current addresses to db
            data = self.get_new_data()
            addresses = data['Address'].tolist()
            self.geocoder_upsert(addresses)

    
    def get_new_data(self):
        http = urllib3.PoolManager()
        
        page = http.request('GET', 'https://webapp3.sanantonio.gov/policecalls/Calls.aspx')
        soup = BeautifulSoup(page.data, "lxml")
        
        table = soup.find_all('table')[1]
        data = pd.read_html(str(table),displayed_only=False)[0]
        data.columns = data.iloc[0].str.replace(' ','').replace(np.nan,'nan_val')
        data = data.iloc[1:16]	
	
        if len(data.columns) == 6:
            data = data[data.columns[1:]]
        elif len(data.columns) >= 6:
            try:
                data = data[data.columns[1:6]]
            except:
                pdb.set_trace()

        # get google maps url (for zip code)
        addresses = []
        for tr in table.find_all('tr')[1:16]:
            map_ = tr.find_all('td')[0]
            try:
                link = map_.find_all('a')[0]['href']
                addresses.append(link.split('=')[1].replace('+', ', '))
                
            except:
                print('no link?: \n\t' + str(tr))
        
        data.Address = addresses
        return data
    
    
    def upsert_pd_data(self, data):
        min_incident = data.IncidentNumber.min()
        SQL = "SELECT IncidentNumber from " + self.table_name + \
              " WHERE IncidentNumber >= ?"

        incidents = [x[0] for x in self.cursor.execute(SQL,(min_incident,)).fetchall()]

        insert_idx = []
        for idx in data.index:

            if data.loc[idx]['IncidentNumber'] not in incidents:
                insert_idx.append(idx)

        try:
            data.loc[insert_idx].to_sql(self.table_name, self.conn, if_exists='append')
        except:
            pdb.set_trace()

        # geocode potential new addresses
        addresses = data.loc[insert_idx]['Address'].tolist()
        self.geocoder_upsert(addresses)

        return len(insert_idx)
            

    def geocoder_upsert(self, addresses):
        
        geocoder = OpenCageGeocode(OPENCAGE_KEY)

        for address in addresses:
            
            if not self.address_exist_check(address):
                query = address
                result = geocoder.geocode(query)

                try:
                    lat = result[0]['geometry']['lat']
                    lng = result[0]['geometry']['lng']
                except:
                    print(address)
                    print(result)

                    lat = 0
                    lng = 0 

                id_ = self.get_max_geo_id() + 1
                row = (id_, address, lat, lng)


                sql_ = "insert into " + self.geocode_table +\
                       "(id, address, lat, long) values" +\
                       "(?,?,?,?)"

                self.cursor.execute(sql_, row)
                self.conn.commit()

        
    def address_exist_check(self, address):
        sql_ = 'select * from ' + self.geocode_table + ' where address = ?'
        try:
            result = self.cursor.execute(sql_,(address,)).fetchall()
        except:
            pdb.set_trace()

        if result == []: # if empty result address does not exist
            return False
        else:
            return True

    def get_max_geo_id(self):
        max_id = self.cursor.execute("select max(id) from " + self.geocode_table).fetchall()[0][0]         
        return max_id



    def run_listener(self, sleep_interval=60):
        # press control-C to exit
        iter_no = 0
        while True:

            if iter_no % 20 == 0:
                print('iter: %d || ctrl+c to end script' % iter_no)

            iter_no += 1

            data = self.get_new_data()
            new_record_no = self.upsert_pd_data(data)
            print('record no added: %d ' % new_record_no)
            time.sleep(sleep_interval)


if __name__=="__main__":
    sapd = SAPDData('sapd.db','sapd','geodata')
    sapd.run_listener()

