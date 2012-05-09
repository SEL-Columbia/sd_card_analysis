import sqlalchemy as sa
import pandas as p
import datetime as dt
import os
import dateutil

# data import
def load_database_from_csv(data_directory,
                           date_start=dt.datetime(2012,1,1),
                           date_end=dt.datetime(2012,2,1)):
    # objects to connect to database
    # metadata = sa.MetaData('postgres://postgres:postgres@localhost:5432/sdcard')

    # walk file structure

    # import pandas file

    # decimate based on timestamp (dealing with duplicates)

    # build date from from directories
    # if not in date range, break out of loop
    for dirpath, dirnames, filenames in os.walk(data_directory):
        #print dirpath, dirnames, filenames
        dp = str(dirpath).split('/')
        print dp
        date = dt.datetime(dp[-4], dp[-3], dp[-2], dp[-1])
        print date

def read_and_sample_log_file(filename, date_start, date_end, interval_seconds=60):

    # load file
    #df = p.read_csv('../data/ml03/2012/01/01/20/192_168_1_200.log')
    df = p.read_csv(filename)

    # map datetime object onto timestamp column
    df['Time Stamp'] = df['Time Stamp'].map(str)
    df['Time Stamp'] = df['Time Stamp'].map(dateutil.parser.parse)

    # create pandas date range and iterate
    # if current timestamp is less than or equal to iterated timestamp
    # greater, mark blank and continue
    dr = p.DateRange(date_start, date_end, offset=p.DateOffset(seconds=5*60))
    index_list = []
    for i in range(len(dr) - 1):
        # get values in time range
        mask = (df['Time Stamp'] >= dr[i]) & (df['Time Stamp'] < dr[i + 1])

        # see if mask has any true values and if so, grab sample
        if mask.any():
            temp_frame = df[mask]
            first_sample = min(temp_frame.index)
            # select first value in range
            print dr[i], df.ix[first_sample]['Time Stamp']
            index_list.append(first_sample)

    # construct new data frame (ndf) from index list (idxl)
        #ndf = df.ix[idxl]

    #return ndf

# data retrieval
def get_watthours_sc20(meter_name, ip_address, date_start, date_end):
    metadata = sa.MetaData('postgres://postgres:postgres@localhost:5432/sdcard')
    t = sa.Table('logs', metadata, autoload=True)
    wh_column = t.c.watthours_sc20
    query = sa.select([wh_column,
                       #t.c.credit,
                       t.c.meter_timestamp],
                       whereclause=sa.and_(t.c.meter_name == meter_name,
                                           t.c.ip_address == int(ip_address[-2:]),
                                           t.c.meter_timestamp < date_end,
                                           t.c.meter_timestamp > date_start
                                           ),
                       order_by=t.c.meter_timestamp)
    result = query.execute()
    sd = p.DataFrame(result.fetchall(), columns=result.keys())
    sd = p.Series(sd['watthours_sc20'], index=sd['meter_timestamp'])
    return sd

def get_watthours_today(meter_name, ip_address, date_start, date_end):
    metadata = sa.MetaData('postgres://postgres:postgres@localhost:5432/sdcard')
    t = sa.Table('logs', metadata, autoload=True)
    wh_column = t.c.watthours_today
    query = sa.select([wh_column,
                       t.c.meter_timestamp],
                       whereclause=sa.and_(t.c.meter_name == meter_name,
                                           t.c.ip_address == int(ip_address[-2:]),
                                           t.c.meter_timestamp < date_end,
                                           t.c.meter_timestamp > date_start
                                           ),
                       order_by=t.c.meter_timestamp)
    result = query.execute()
    sd = p.DataFrame(result.fetchall(), columns=result.keys())
    sd = p.Series(sd['watthours_today'], index=sd['meter_timestamp'])
    return sd
