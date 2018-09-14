# sapd-data
For scraping San Antonio Polic Department (SAPD) call disposition data as it is generated live
on the [SAPD website](https://www.sanantonio.gov/SAPD/Calls).

## Details and Usage
Program defaults to update every 60 seconds and offloads new data (upsert) to sqlite db. The `SAPDData` class can be used as follows:

```python
db_name = 'sapd.db'
table_name = 'sapd'

sapd = SAPDData(db_name, table_name)
sapd.run_listener(sleep_interval=60)
```
