import sqlalchemy as sa
import pandas as p
import datetime as dt
import os
import dateutil

# data import
def load_database_from_csv(data_directory,
                           date_start=dt.datetime(2012,1,1),
                           date_end=dt.datetime(2012,1,2)):
    '''
    load_database_from_csv

    input:
    data_directory : top level of data_directory to be imported
    date_start :
    date_end :
    '''

    # build date from from directories
    # if not in date range, break out of loop
    for dirpath, dirnames, filenames in os.walk(data_directory):
        #print dirpath, dirnames, filenames
        for f in filenames:
            if f.endswith('.log'):
                dp = str(dirpath).split('/')[-4:]
                dp = map(int, dp)
                #print dp
                file_date_start = dt.datetime(dp[-4], dp[-3], dp[-2], dp[-1])
                file_date_end = file_date_start + dt.timedelta(hours=1)

                if file_date_end > date_end or file_date_start < date_start:
                    continue
                print dirpath, file_date_start, file_date_end, f
                read_and_sample_log_file(os.path.join(dirpath, f),
                                         file_date_start,
                                         file_date_end)

def read_and_sample_log_file(filename,
                             date_start,
                             date_end,
                             interval_seconds=10*60,
                             sample_method='first',
                             columns=['Time Stamp', 'Watts']):
    '''
    read_and_sample_log_file

    input:
    filename   : csv file to be parsed
    date_start :
    date_end   :
    interval_seconds : sampling interval
    sample_method : sampling heuristic
                  : 'first' = use first sample in interval

    output:
    df : data frame with sampled data for hour in filename
    '''
    # TODO : return data frame with appropriate columns and circuit information
    # TODO : add columns to dataframe for meter and circuit
    # TODO : remove unnecessary columns from dataframe

    # load file
    df = p.read_csv(filename)

    # map datetime object onto timestamp column
    df['Time Stamp'] = df['Time Stamp'].map(str)
    df['Time Stamp'] = df['Time Stamp'].map(dateutil.parser.parse)

    # create pandas date range and iterate
    # if current timestamp is less than or equal to iterated timestamp
    # greater, mark blank and continue
    dr = p.DateRange(date_start, date_end, offset=p.DateOffset(seconds=interval_seconds))
    index_list = []
    resample_list = []
    for i in range(len(dr) - 1):
        # get values in time range
        mask = (df['Time Stamp'] >= dr[i]) & (df['Time Stamp'] < dr[i + 1])

        # see if mask has any true values and if so, grab sample
        if mask.any():
            resample_list.append(dr[i])
            temp_frame = df[mask]
            first_sample = min(temp_frame.index)
            # select first value in range
            print dr[i], df.ix[first_sample]['Time Stamp']
            index_list.append(first_sample)

    # construct new data frame (ndf) from index list (idxl)
    ndf = df.ix[index_list]
    ndf = ndf[columns]
    ndf['Resampled_Time_Stamp'] = resample_list
    return ndf

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
