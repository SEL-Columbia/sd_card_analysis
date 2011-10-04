overall work flow for this set of scripts

- use csv2db.py to create a sqlite database from the csv files
in the nested directories
- create sql index of timestamp to speed up queries
- use resample_db to output a sampled csv of info
- import this csv into the database as a separate table
- graph/analyze data in database using graph_db


csv2db.py
---------
> python csv2db.py <data_directory> <database_name>

for example

> python csv2db.py data/ug02 ug02.db

database must not already exist

sometimes log files are corrupted, when the script fails with an error,
look at the most recent log file, open that log file and investigate.
usually the corruption is clear.  delete the corrupted line, save and
try again.


resample_db.py
--------------
now using resample_db.py to create hourly reports of variables output
to csv to be imported to a database.





(this is deprecated usage down here)
---------------------------
to run write_csv_from_db.py

edit date range on lines 72 and 73 for desired range
edit circuit_id variable for desired circuit (around line 70)
edit db variable for database filename (around line 9)

cut and paste output into file
name file something.csv
