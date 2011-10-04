import sqlite3, sys
from datetime import datetime
from datetime import timedelta
import matplotlib.pyplot as plt
import matplotlib.dates
import numpy

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
<circuit> in the range specified by the timeStart
and timeEnd datetime objects
'''
def getRawDataForCircuit(circuit,
                         timeStart=datetime(2011,6,1),
                         timeEnd=datetime(2011,6,3)):

    con = sqlite3.connect(db, detect_types = sqlite3.PARSE_COLNAMES)
    sql = """select timestamp as 'ts [timestamp]',watthours_today,credit,watts
             from logs where circuitid=%s and timestamp between '%s' and '%s'
             order by timestamp asc;""" % (circuit, timeStart, timeEnd)

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
                               timeStart,
                               timeEnd,
                               downsample=20 * 20):
    dates, data, credit, watts = getRawDataForCircuit(circuit, timeStart, timeEnd)

    index = range(0, data.shape[0], downsample)
    dates = dates[index]
    data = data[index]
    credit = credit[index]
    watts = watts[index]
    return dates, data, credit, watts

def graph_credit(circuit,
                 timeStart=datetime(2011, 8, 1),
                 timeEnd=datetime(2011, 9, 1),
                 plot_file_name=None):
    dates, data, credit, watts = getRawDataForCircuit(circuit, timeStart, timeEnd)
    dates = matplotlib.dates.date2num(dates)

    fig = plt.figure()
    ax = fig.add_axes((0.1,0.2,0.8,0.7))
    ax.plot_date(dates, credit, 'kx')
    ax.set_ylabel("Credit")
    ax.set_xlabel("Time (hours passed)")
    ax.set_title(db + "\nCircuit %s between %s and %s" % (circuit, timeStart, timeEnd))
    fig.autofmt_xdate()
    if plot_file_name==None:
        plot_file_name = db + '_' + str(circuit) + '_credit.pdf'
    fig.savefig(plot_file_name)

def graph_power(circuit,
                 timeStart=datetime(2011, 8, 1),
                 timeEnd=datetime(2011, 9, 1),
                 plot_file_name=None):
    dates, data, credit, watts = getRawDataForCircuit(circuit, timeStart, timeEnd)
    dates = matplotlib.dates.date2num(dates)

    fig = plt.figure()
    ax = fig.add_axes((0.1,0.2,0.8,0.7))
    ax.plot_date(dates, watts, 'kx')
    ax.set_ylabel("Power")
    ax.set_xlabel("Time (hours passed)")
    ax.set_title(db + "\nCircuit %s between %s and %s" % (circuit, timeStart, timeEnd))
    fig.autofmt_xdate()
    if plot_file_name==None:
        plot_file_name = db + '_' + str(circuit) + '_power.pdf'
    fig.savefig(plot_file_name)


'''
simple plotting of watthour samples for circuit over a specified date range
'''
def graph_watthours(circuit,
                        timeStart=datetime(2011, 8, 1),
                        timeEnd=datetime(2011, 9, 1),
                        plot_file_name=None):

    dates, watthours = get_watthours_for_circuit(circuit, timeStart, timeEnd)
    dates = matplotlib.dates.date2num(dates)

    fig = plt.figure()
    ax = fig.add_axes((0.1,0.2,0.8,0.7))
    ax.plot_date(dates, watthours, 'kx')
    ax.set_ylabel("Watt Hours")
    ax.set_xlabel("Time (hours passed)")
    ax.set_title(db + "\nCircuit %s between %s and %s" % (circuit, timeStart, timeEnd))
    fig.autofmt_xdate()
    if plot_file_name==None:
        plot_file_name = db + '_' + str(circuit) + '.pdf'
    fig.savefig(plot_file_name)

#dates, data, credit, watts = getRawDataForCircuit(1)
for cid in range(1,22):
    print cid
    graph_watthours(cid, datetime(2011, 8, 14), datetime(2011, 9, 1))
    #graph_credit(cid, datetime(2011, 8, 1), datetime(2011, 9, 1))
    #graph_power(cid, datetime(2011, 8, 1), datetime(2011, 9, 1))