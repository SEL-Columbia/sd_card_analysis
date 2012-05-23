import sqlalchemy as sa
import pandas as p
import datetime as dt
import os
import dateutil

def export_sd_to_csv(data_directory,
                     date_start=dt.datetime(2012,1,1),
                     date_end=dt.datetime(2012,1,2),
                     interval_seconds=15*60,
                     meter_name='default_meter_name',
                     output_file='default.csv'):
    '''
    load_database_from_csv

    input:
    data_directory   : top level of data_directory to be imported
    date_start       :
    date_end         :
    interval_seconds : interval at which to attempt to sample data
    meter_name       : string specifying name of meter to identify data
    output_file      : string specifying name of csv output file

    output:
    df : dataframe with resampled data from files
    '''

    df = p.DataFrame()
    # build date from from directories
    # if not in date range, break out of loop
    for dirpath, dirnames, filenames in os.walk(data_directory):
        #print dirpath, dirnames, filenames
        for f in filenames:
            if f.endswith('.log'):
                circuit_id = int(f[-6:-4])
                dp = str(dirpath).split('/')[-4:]
                dp = map(int, dp)
                #print dp
                file_date_start = dt.datetime(dp[-4], dp[-3], dp[-2], dp[-1])
                file_date_end = file_date_start + dt.timedelta(hours=1)

                if file_date_end > date_end or file_date_start < date_start:
                    continue
                print dirpath, f
                tdf = read_and_sample_log_file(os.path.join(dirpath, f),
                                         file_date_start,
                                         file_date_end,
                                         interval_seconds=interval_seconds,
                                         meter_name=meter_name,
                                         column_mapping={'Time Stamp':'sc20_time_stamp',
                                                         'Watts':'watts',
                                                         'Volts':'volts',
                                                         'Amps':'amps',
                                                         'Watt Hours SC20':'watt_hours_sc20',
                                                         'Watt Hours Today':'watt_hours_today',
                                                         'Max Watts':'max_watts',
                                                         'Max Volts':'max_volts',
                                                         'Max Amps':'max_amps',
                                                         'Min Watts':'min_watts',
                                                         'Min Volts':'min_volts',
                                                         'Min Amps':'min_amps',
                                                         'Power Factor':'power_factor',
                                                         'Power Cycle':'power_cycle',
                                                         'Frequency':'frequency',
                                                         'Volt Amps':'volt_amps',
                                                         'Relay Not Closed':'relay_not_closed',
                                                         'Send Rate':'send_rate',
                                                         'Machine ID':'machine_id',
                                                         'Type':'type',
                                                         'Credit':'credit'},
                                         circuit_id=circuit_id)
                df = p.concat([df, tdf])
    # rewrite df to delete old index
    # this could be a performance or memory footprint problem
    df = p.DataFrame(df.values, columns=df.columns)

    # write out to file depending on file extension
    if '.xls' in output_file:
        writer = p.ExcelWriter(output_file)
        df.to_excel(writer, sheet_name=meter_name)
        writer.save()
    if '.csv' in output_file:
        df.to_csv(output_file)

    # return data frame
    return df

def read_and_sample_log_file(filename,
                             date_start,
                             date_end,
                             interval_seconds=10*60,
                             sample_method='first',
                             column_mapping={'Time Stamp':'sc20_time_stamp',
                                             'Watts':'watts',
                                             'Volts':'volts',
                                             'Watt Hours SC20':'watt_hours_sc20',
                                             'Credit':'credit'},
                             meter_name=None,
                             circuit_id=None):
    '''
    read_and_sample_log_file

    input:
    filename         : csv file to be parsed
    date_start       :
    date_end         :
    interval_seconds : sampling interval
    sample_method    : sampling heuristic
                     : 'first' = use first sample in interval
    column_mapping   : dictionary where keys are column names as they appear
                       in the .log files and values are the database compliant
                       names
    meter_name       : string to specify meter from which data is taken
    circuit_id       : physical circuit number in meter that file represents

    output:
    df : data frame with sampled data for hour in filename
    '''
    # TODO : what if file is missing header?
    # TODO : what if line has poorly formed line?

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
            #print dr[i], df.ix[first_sample]['Time Stamp']
            index_list.append(first_sample)

    # construct new data frame (ndf) from index list (idxl)
    ndf = df.ix[index_list]
    #ndf = ndf[columns]
    # if mains, do not ask for credit column
    if circuit_id==0:
        column_mapping.pop('Credit')
    ndf = ndf[column_mapping.keys()]
    ndf['meter_time_stamp'] = resample_list
    ndf['meter_name'] = meter_name
    ndf['physical_circuit'] = circuit_id

    # rename columns to be consistent with database labels
    ndf = ndf.rename(columns=column_mapping)

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
