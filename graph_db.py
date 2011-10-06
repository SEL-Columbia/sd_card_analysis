from __future__ import print_function
import sqlite3, sys
from datetime import datetime
from datetime import timedelta
import matplotlib.pyplot as plt
import matplotlib.dates
import numpy as np
import dateutil

'''
these functions allow for pulling data from the database and plotting it.
these functions are written to allow pulling from either the raw database
or the decimated/resampled database.
'''

# specify database here
db = 'ml01.db'
#db = './data/ml06/ml06.db'

'''
this method pulls all data from the database for
<circuit> in the range specified by the time_start
and time_end datetime objects
'''
def getRawDataForCircuit(circuit,
                         time_start=datetime(2011,6,1),
                         time_end=datetime(2011,6,3)):

    con = sqlite3.connect(db, detect_types = sqlite3.PARSE_COLNAMES)
    sql = """select timestamp as 'ts [timestamp]',watthours_today,credit,watts
             from logs where circuitid=%s and timestamp between '%s' and '%s'
             order by timestamp asc;""" % (circuit, time_start, time_end)

    dates = []
    data = []
    credit = []
    watts = []

    for i, row in enumerate(con.execute(sql)):
        dates.append(row[0])
        data.append(row[1])
        credit.append(row[2])
        watts.append(row[3])
    con.close()

    # what if no data?

    dates = numpy.array(dates)
    data = numpy.array(data)
    credit = numpy.array(credit)
    watts = numpy.array(watts)
    return dates, data, credit, watts

'''
this function queries the hourly_watthours table for values between
time_start and time_end for watthour data
'''
def get_watthours_for_circuit(cid,
                              time_start=datetime(2011,8,1),
                              time_end=datetime(2011,9,1)):
    con = sqlite3.connect(db, detect_types = sqlite3.PARSE_COLNAMES)
    sql = """select timestamp as 'ts [timestamp]',watthours
             from hourly_watthours
             where circuitid=%s
             and timestamp between '%s' and '%s'
             order by timestamp asc;""" % (cid, time_start, time_end)

    dates = []
    watthours = []

    for i, row in enumerate(con.execute(sql)):
        dates.append(row[0])
        watthours.append(row[1])
    con.close()

    # what if no data?

    dates = numpy.array(dates)
    watthours = numpy.array(watthours)
    return dates, watthours

'''
this method reduces the amount of data returned from the database by
a simple dropping of samples.  every downsample-th sample is returned
by the function
'''
def getDecimatedDataForCircuit(circuit,
                               time_start,
                               time_end,
                               downsample=20 * 20):
    dates, data, credit, watts = getRawDataForCircuit(circuit, time_start, time_end)

    index = range(0, data.shape[0], downsample)
    dates = dates[index]
    data = data[index]
    credit = credit[index]
    watts = watts[index]
    return dates, data, credit, watts

def graph_credit(circuit,
                 time_start=datetime(2011, 8, 1),
                 time_end=datetime(2011, 9, 1),
                 plot_file_name=None):
    dates, data, credit, watts = getRawDataForCircuit(circuit, time_start, time_end)
    dates = matplotlib.dates.date2num(dates)

    fig = plt.figure()
    ax = fig.add_axes((0.1,0.2,0.8,0.7))
    ax.plot_date(dates, credit, 'kx')
    ax.set_ylabel("Credit")
    ax.set_xlabel("Time (hours passed)")
    ax.set_title(db + "\nCircuit %s between %s and %s" % (circuit, time_start, time_end))
    fig.autofmt_xdate()
    if plot_file_name==None:
        plot_file_name = db + '_' + str(circuit) + '_credit.pdf'
    fig.savefig(plot_file_name)

def graph_power(circuit,
                 time_start=datetime(2011, 8, 1),
                 time_end=datetime(2011, 9, 1),
                 plot_file_name=None):
    dates, data, credit, watts = getRawDataForCircuit(circuit, time_start, time_end)
    dates = matplotlib.dates.date2num(dates)

    fig = plt.figure()
    ax = fig.add_axes((0.1,0.2,0.8,0.7))
    ax.plot_date(dates, watts, 'kx')
    ax.set_ylabel("Power")
    ax.set_xlabel("Time (hours passed)")
    ax.set_title(db + "\nCircuit %s between %s and %s" % (circuit, time_start, time_end))
    fig.autofmt_xdate()
    if plot_file_name==None:
        plot_file_name = db + '_' + str(circuit) + '_power.pdf'
    fig.savefig(plot_file_name)


'''
simple plotting of watthour samples for circuit over a specified date range
'''
def graph_watthours(circuit,
                        time_start=datetime(2011, 8, 1),
                        time_end=datetime(2011, 9, 1),
                        plot_file_name=None):

    dates, watthours = get_watthours_for_circuit(circuit, time_start, time_end)
    dates = matplotlib.dates.date2num(dates)

    fig = plt.figure()
    ax = fig.add_axes((0.1,0.2,0.8,0.7))
    ax.plot_date(dates, watthours, 'kx-')
    ax.set_ylabel("Watt Hours")
    ax.set_xlabel("Time (hours passed)")
    ax.set_title(db + "\nCircuit %s between %s and %s" % (circuit, time_start, time_end))
    fig.autofmt_xdate()
    if plot_file_name==None:
        plot_file_name = db + '_' + str(circuit) + '.pdf'
    fig.savefig(plot_file_name)

def calculate_single_day_watthours(circuit=1,
                                   day=datetime(2011,8,31)):
    con = sqlite3.connect(db, detect_types=sqlite3.PARSE_COLNAMES)
    cur = con.cursor()
    sql = '''
          select count(*), max(watthours)
          from hourly_watthours
          where timestamp>'%s'
          and timestamp<='%s' and circuitid=%s;
          ''' %(day, day+timedelta(days=1), circuit)

    #for row in con.execute(sql):
    cur.execute(sql)
    row = cur.fetchone()
    con.close()
    return_dict = {'num_samples':row[0],
                   'max_watthours':row[1]}
    return return_dict

def output_daily_watthours(circuit=1,
                           time_start=datetime(2011,8,14),
                           time_end=datetime(2011,8,31),
                           minimum_samples=24):
    current_date = time_start
    # header is 'circuitid, timestamp, watthours, num_hours'
    while current_date <= time_end:
        rd = calculate_single_day_watthours(circuit, current_date)
        if rd['num_samples'] > 0:
            print(*('ml01',circuit, str(current_date), rd['max_watthours'], rd['num_samples']),
                  sep=',', file=fout)
        current_date += timedelta(days=1)

def get_daily_watthours_from_db(circuit=1,
                                time_start=datetime(2011,8,14),
                                time_end=datetime(2011,8,31)):
    con = sqlite3.connect(db, detect_types=sqlite3.PARSE_COLNAMES)
    cur = con.cursor()
    sql = '''
          select timestamp, watthours, numsamples
          from daily_watthours
          where timestamp>='%s'
          and timestamp<='%s' and circuitid=%s;
          ''' %(time_start, time_end, circuit)

    timestamp = []
    watthours = []
    numsamples = []
    for row in con.execute(sql):
        #cur.execute(sql)
        #row = cur.fetchone()
        timestamp.append(row[0])
        watthours.append(row[1])
        numsamples.append(row[2])
    con.close()
    return_dict = {'timestamp':timestamp,
                   'watthours':watthours,
                   'numsamples':numsamples}
    return return_dict

def plot_daily_watthours(circuit=1,
                         time_start=datetime(2011,8,1),
                         time_end=datetime(2011,8,31),
                         plot_file_name=None):
    data_dict = get_daily_watthours_from_db(circuit=circuit,
                                            time_start=time_start,
                                            time_end=time_end)
    dates = map(dateutil.parser.parse, data_dict['timestamp'])
    dates = matplotlib.dates.date2num(dates)
    watthours = np.array(data_dict['watthours'])
    numsamples = np.array(data_dict['numsamples'])

    mask = numsamples == 24

    # legend complete, incomplete
    fig = plt.figure()
    ax = fig.add_axes((0.1,0.2,0.8,0.7))
    ax.plot_date(dates[mask], watthours[mask], 'bo', label='Complete Data')
    ax.plot_date(dates, watthours, 'kx', label='Incomplete Data')
    ax.set_ylabel("Daily Watt Hours")
    ax.set_xlabel("Time")
    ax.legend()
    ax.grid()
    ax.set_title(db + "\nCircuit %s between %s and %s" % (circuit, time_start, time_end))
    fig.autofmt_xdate()
    if plot_file_name==None:
        plot_file_name = db + '_daily' + str(circuit) + '.pdf'
    fig.savefig(plot_file_name)

def plot_all_daily_watthours():
    for cid in range(1, 22):
        plot_daily_watthours(circuit=cid)

if __name__ == '__main__':
    args = sys.argv[1:]
    db = args[0]
    file_out = args[1]

    #dates, data, credit, watts = getRawDataForCircuit(1)
    fout = open('daily_watthours.csv', 'w')
    for cid in range(1,22):
        pass
        #print cid
        output_daily_watthours(circuit=cid,
                               time_start=datetime(2011,8,1),
                               time_end=datetime(2011,9,1))
        #graph_watthours(cid, datetime(2011, 8, 14), datetime(2011, 9, 1))
        #graph_credit(cid, datetime(2011, 8, 1), datetime(2011, 9, 1))
        #graph_power(cid, datetime(2011, 8, 1), datetime(2011, 9, 1))
    fout.close()
