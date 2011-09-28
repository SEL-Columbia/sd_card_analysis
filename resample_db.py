from __future__ import print_function
'''
assumes:
have data for circuit before first sample

initialization:
set sampling interval
set start time
set end time
set sample time to start time

flowchart:
pull database entries around sample time for accumulated_wh and credit

if no entries use last sample at sample time

if entries find closest before sample time and use for sample

place entries in database table or dict with timestamp as the key

increment sample time


database samples - ...      .......                      ....  ..
desired  samples -           |          |          |          |
                             1          2          3          4


select timestamp from logs where timestamp>'2011-08-01 00:00:00' and timestamp<'2011-08-01 01:00:00'
'''

import sqlite3
from datetime import datetime
from datetime import timedelta


db='ml01.db'
db_out = 'test.db'
csv_out = 'test.csv'

#select max(timestamp) from logs where timestamp>='2011-09-08 08:00:00' and timestamp<='2011-09-08 09:00:00' and circuitid=4;

'''
query the database and return the most recent timestamp
for a circuit in a certain date range
'''
def get_most_recent_timestamp_in_range(circuit,
                                       timeStart=datetime(2011,9,1),
                                       timeEnd=datetime(2011,9,2)):
    con = sqlite3.connect(db, detect_types=sqlite3.PARSE_COLNAMES)
    sql = '''select max(timestamp)
             from logs
             where circuitid=%s
             and timestamp between '%s' and '%s';
          ''' %(circuit,timeStart,timeEnd)

    for row in con.execute(sql):
        ts = row[0]
    con.close()

    return ts
'''
return all values for a circuit and timestamp from the database as tuple
'''
def get_data_for_timestamp_and_circuit(circuit, timestamp):
    con = sqlite3.connect(db, detect_types=sqlite3.PARSE_COLNAMES)
    cur = con.cursor()
    sql = '''select watts,
                    volts,
                    amps,
                    watthours_sc20,
                    watthours_today,
                    powerfactor,
                    frequency,
                    voltamps,
                    status,
                    machineid,
                    credit
             from logs
             where circuitid=%s
             and timestamp='%s';
          ''' %(circuit,timestamp)

    #for row in con.execute(sql):
    cur.execute(sql)
    row = cur.fetchone()
    if row!=None:
        return row
    else:
        return None

'''
takes a row and writes it to database
note: incomplete
'''
def write_to_db_out(ts, data_tuple):
    pass

datestamps = [datetime(2011,9,1),
              datetime(2011,9,2),
              datetime(2011,9,3),
              datetime(2011,9,4),
              datetime(2011,9,5)]

timeStart = datetime(2011,8,2)
timeEnd   = datetime(2011,9,1)

script_begin = datetime.now()
print(script_begin)

current_time = timeStart
sample_period = timedelta(hours = 1)

fout = open(csv_out, 'w')

while current_time <= timeEnd:
    for cid in range(1,2):
        print(str(current_time))
        print(str(current_time), end=',', file=fout)
        print(str(cid), end=',', file=fout)
        ts = get_most_recent_timestamp_in_range(cid, current_time - sample_period, current_time)
        data_tuple = get_data_for_timestamp_and_circuit(cid, ts)
        #write_to_db_out(ts, data_tuple)
        if data_tuple!=None:
            last_data_tuple = data_tuple;
        else:
            data_tuple = last_data_tuple
        print(*data_tuple, sep=',', file=fout)
    current_time += sample_period

fout.close()

script_end = datetime.now()
print (script_end - script_begin).total_seconds()