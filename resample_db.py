from __future__ import print_function
'''
python resample_db.py <mlxx.db> <hourly.csv>

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
import sys


'''
query the database and return the most recent timestamps by circuitid
for a given date range
returns a list of lists with circuitid and most recent timestamp
'''
def get_most_recent_timestamps_in_range(timeStart=datetime(2011,9,1,0),
                                       timeEnd=datetime(2011,9,2,1)):
    con = sqlite3.connect(db, detect_types=sqlite3.PARSE_COLNAMES)
    # query database for table of circuits and last reported times in reporting interval
    sql = '''select circuitid, max(timestamp)
             from logs
             where timestamp between '%s' and '%s'
             group by circuitid;
          ''' %(timeStart,timeEnd)


    values_dict = {}
    for row in con.execute(sql):
        values_dict[int(row[0])] = str(row[1])
    con.close()

    return values_dict

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
    con.close()
    # fixme: this should return dict
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

def output_sampled_watthours(time_start=datetime(2011,8,2),
                             time_end=datetime(2011,9,1),
                             csv_out=None,
                             # placeholder for when we use dicts
                             quantity='watthours_sc20'):
    if csv_out == None:
        csv_out = 'hourly_out.csv'
    fout = open(csv_out, 'w')

    circuits = range(0, 22)

    # this is to keep the database in the same format i've been using (ts, watthours, credit, watts)
    dummy_value = 0

    last_watt_hours = {}
    for cid in circuits:
        last_watt_hours[cid] = 0

    current_time = time_start
    sample_period = timedelta(hours = 1)
    while current_time <= time_end:
        ts_dict = get_most_recent_timestamps_in_range(current_time - sample_period, current_time)
        # i.e. watt hours is ok to interpolate, volts or watts is not

        if current_time.hour == 1:
            for cid in circuits:
                last_watt_hours[cid] = 0

        # if ts_dict is empty do nothing
        # if ts_dict exists, stuff existing values and interpolate others as appropriate
        if len(ts_dict.keys()) != 0:
            for cid in circuits:
                print(str(current_time))
                print(str(current_time), end=',', file=fout)
                print(str(cid), end=',', file=fout)

                if cid in ts_dict.keys():
                    data_tuple = get_data_for_timestamp_and_circuit(cid, ts_dict[cid])
                    watthours = data_tuple[4];
                    last_watt_hours[cid] = watthours;
                else:
                    watthours = last_watt_hours[cid]

                print(watthours, sep=',', file=fout)
        current_time += sample_period


    fout.close()


if __name__ == "__main__":

    args = sys.argv[1:]
    time_start = datetime(2011,1,1)
    time_end   = datetime(2011,12,1)

    db = args[0]
    csv_out = args[1]

    output_sampled_watthours(time_start, time_end, csv_out=csv_out)