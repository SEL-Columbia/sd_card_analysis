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
    traverse directory of files and assemble into single data frame

    calls:
    read_and_sample_log_file

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

def fail_mail():
    import smtplib
    fromaddr = 'drsautomate@gmail.com'
    toaddr = 'drdrsoto@gmail.com'
    msg = 'script_failure'
    username = 'drsautomate'
    password = username[::-1]

    server = smtplib.SMTP('smtp.gmail.com:587')
    server.starttls()
    server.login(username, password)
    server.sendmail(fromaddr, toaddr, msg)
    server.quit()


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
    try:
        df = p.read_csv(filename)
    except:
        fail_mail()

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

def all_heatmap(filename,
            truncate=None,
            column='watts',
            date_start=None,
            date_end=None,
            interval_seconds=None,
            plot_filename=None):
    '''
    takes a file output by export_sd_to_csv and writes it as a 2-d heat
    map
    this heatmap is for all consumers
    '''
    import dateutil

    # input file based on extension
    if '.h5' in filename:
        store = p.HDFStore(filename)
        temp_df = store['df']
        store.close()
    elif '.csv' in filename:
        temp_df = p.read_csv(filename)
        temp_df['meter_time_stamp'] = [dateutil.parser.parse(i) for i in
                                   temp_df['meter_time_stamp']]
    else:
        # TODO return file not found message
        return None

    df = temp_df.pivot(index='meter_time_stamp',
                       #columns='physical_circuit',
                       columns='meter_circuit_name',
                       values=column)

    if date_start is not None:
        #create date range
        date_range = p.DateRange(date_start, date_end,
                                 offset=p.DateOffset(seconds=interval_seconds))
        # apply this index to pivoted data frame
        df = df.reindex(date_range)

    if truncate is not None:
        df[df > truncate] = truncate

    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(1, 1)

    import matplotlib.cm as cm
    cmap = cm.jet

    cmap.set_bad('w')

    plot = ax.imshow(df, aspect='auto', interpolation='none', cmap=cmap)
    fig.colorbar(plot)

    labels = df.index

    # place subsampled date labels
    rng = range(0, len(labels), 12 * 24)
    ax.set_yticks(rng)
    ax.set_yticklabels(labels[rng])

    ax.set_xticks(range(len(df.columns)))
    ax.set_xticklabels(df.columns)

    if plot_filename is None:
        plt.show()
    else:
        fig.savefig(plot_filename)

def daily_heatmap():
    '''
    plots out the daily heatmap for consumer data where columns are date
    and rows are hours
    what data source do we use?
    we can take the big uganda hdf file and then pivot to get watts
    we then select a column and then maybe group by/ pivot by time and output to
    data frame?
    '''

    import pandas as p
    import numpy as np
    # read in HDF5 file to data frame
    store = p.HDFStore('ugall.h5')
    df = store['df']
    store.close()

    watts = df.pivot(index='meter_time_stamp',
                          columns='meter_circuit_name',
                          values='watts')
    for c in watts.columns:
    #for c in ['ug08_00']:
        s = watts[c]
        s = s.resample('H')
        sgb = s.groupby(s.index.hour)
        dfl = {}
        for name, group in sgb:
            temp = group.resample('H', fill_method='pad')
            temp = temp.resample('D')
            dfl[name]=temp
        df = p.DataFrame(dfl)
        # pad missing data
        df = df.fillna(0)
        import matplotlib.pyplot as plt
        fig, ax = plt.subplots(1, 1)
        plot = ax.imshow(df, aspect='auto', interpolation='none')
        fig.colorbar(plot)
        plt.show()

