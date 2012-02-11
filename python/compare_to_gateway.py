'''
'''

import sqlalchemy as sa
import matplotlib.pyplot as plt
import datetime as dt
import pandas as p

# query parameters
date_start = dt.datetime(2011, 1, 1)
date_end = dt.datetime(2012, 2, 1)
meter_name = 'ml03'
ip_address = '192.168.1.203'

def get_diff(meter_name, ip_address, date_start, date_end):
    # create sdcard timeseries for daterange
    metadata = sa.MetaData('postgres://postgres:postgres@localhost:5432/sdcard')
    t = sa.Table('hourly_logs', metadata, autoload=True)
    wh_column = t.c.watthours_today
    query = sa.select([wh_column,
                       t.c.meter_timestamp],
                       whereclause=sa.and_(t.c.meter_name == meter_name,
                                           t.c.ip_address == int(ip_address[-2:]),
                                           t.c.meter_timestamp<date_end,
                                           t.c.meter_timestamp>date_start,
                                           ),
                       order_by=t.c.meter_timestamp)
    result = query.execute()
    sd = p.DataFrame(result.fetchall(), columns=result.keys())
    sd = p.Series(sd['watthours_today'], index=sd['meter_timestamp'])


    # create gateway timeseries for daterange
    metadata = sa.MetaData('postgres://postgres:postgres@localhost:5432/gateway')
    t = sa.Table('view_primary_log', metadata, autoload=True)
    wh_column = t.c.watthours
    query = sa.select([wh_column,
                       t.c.meter_timestamp],
                       whereclause=sa.and_(t.c.meter_name == meter_name,
                                           t.c.ip_address == ip_address,
                                           t.c.meter_timestamp<date_end,
                                           t.c.meter_timestamp>date_start,
                                           ),
                       order_by=t.c.meter_timestamp)
    result = query.execute()
    gd = p.DataFrame(result.fetchall(), columns=result.keys())
    gd = p.Series(gd['watthours'], index=gd['meter_timestamp'])

    diff = gd-sd
    return diff

f, ax = plt.subplots(21, 1, figsize=(10,20), sharex=True)

for i in range(0,21):
    ip_address = '192.168.1.2' + '%02d' % i
    diff = get_diff(meter_name=meter_name,
                    ip_address=ip_address,
                    date_start=date_start,
                    date_end=date_end)
    diff.plot(ax=ax[i])

#diff.plot()
#plt.show()
f.savefig('compare_to_gateway.pdf')