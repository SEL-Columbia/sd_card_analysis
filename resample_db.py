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

db='ml01.db'

#select max(timestamp) from logs where timestamp>='2011-09-08 08:00:00' and timestamp<='2011-09-08 09:00:00' and circuitid=4;

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

def get_wh_for_timestamp_and_circuit(circuit, timestamp):
    con = sqlite3.connect(db, detect_types=sqlite3.PARSE_COLNAMES)
    sql = '''select watthours_sc20
             from logs
             where circuitid=%s
             and timestamp='%s';
          ''' %(circuit,timestamp)

    for row in con.execute(sql):
        wh = row[0]
    con.close()
    print wh
    return wh


datestamps = [datetime(2011,9,1),
              datetime(2011,9,2),
              datetime(2011,9,3),
              datetime(2011,9,4),
              datetime(2011,9,5)]

for i in range(len(datestamps)-1):

    for cid in range(1,3):
        print 'circuitid',
        print cid,
        ts = get_most_recent_timestamp_in_range(cid, datestamps[i], datestamps[i+1])
        get_wh_for_timestamp_and_circuit(cid, ts)