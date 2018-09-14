import urllib3
import pandas as pd

from bs4 import BeautifulSoup
import sqlite3

import time
import os

import pdb

# don't need to see this warning every iteration
urllib3.disable_warnings()

class SAPDData(object):

    def __init__(self, db_name, table_name):
        self.db_name = db_name
        self.table_name = table_name

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


    def create_db(self):
        data = self.get_new_data()
        conn = sqlite3.connect(self.db_name)
        data.to_sql(self.table_name, conn)
        return conn
        
    
    def get_new_data(self):
        http = urllib3.PoolManager()
        
        page = http.request('GET', 'https://webapp3.sanantonio.gov/policecalls/Calls.aspx')
        soup = BeautifulSoup(page.data, "lxml")
        
        table = soup.find_all('table')[1]
        data = pd.read_html(str(table),displayed_only=False)[0]
        data.columns = data.iloc[0].str.replace(' ','')
        data = data.iloc[1:16]	
	
        if len(data.columns) == 6:
            data = data[data.columns[1:]]
        if len(data.columns) >= 6:
            data = data[data.columns[1:-2]]

        
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

        data.loc[insert_idx].to_sql(self.table_name, self.conn, if_exists='append')
        return len(insert_idx)
            

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
    sapd = SAPDData('sapd.db','sapd')
    sapd.run_listener()

