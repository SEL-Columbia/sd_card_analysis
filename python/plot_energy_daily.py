'''
'''

import sqlalchemy as sa
import matplotlib.pyplot as plt
import datetime as dt

date_start = dt.datetime(2011, 1, 1)
date_end = dt.datetime(2012, 2, 1)

metadata = sa.MetaData('postgres://postgres:postgres@localhost:5432/sdcard')
#vpl = sa.Table('view_power_table', metadata, autoload=True)
#vpl = sa.Table('logs', metadata, autoload=True)
vpl = sa.Table('hourly_logs', metadata, autoload=True)

wh_column = vpl.c.watthours_sc20
#wh_column = vpl.c.watthours_today

query = sa.select([wh_column,
                   vpl.c.meter_timestamp],
                   whereclause=sa.and_(vpl.c.meter_name=='ml03',
                                       vpl.c.ip_address==3,
                                       vpl.c.meter_timestamp<date_end,
                                       vpl.c.meter_timestamp>date_start,
                                       ),
                   order_by=vpl.c.meter_timestamp)
result = query.execute()
dates = []
watthours = []
for r in result:
    dates.append(r.meter_timestamp)
    watthours.append(r.watthours_sc20)

import numpy as np
#dates = np.array(dates)
#watthours = np.array(watthours)

import pandas as p
df = p.DataFrame({'energy':watthours}, index=dates)
ts = p.Series(watthours, index=dates)

# offset by 1 day
daily = df - df.shift(1, offset=p.DateOffset(days=1))

# take only positive (sensible) data points
daily = daily[daily['energy']>0]

#rng = p.DateRange(date_start, date_end, offset=p.DateOffset(days=1))

#daily = daily.asfreq(p.DateOffset(days=1))

# plot each circuit daily energy values for all time
f, ax = plt.subplots(2,1, sharex=True)
#ax[0].plot_date(daily.index, daily['energy'], mfc='#dddddd')
ax[0].plot_date(daily.index, daily['energy'], mfc='#dddddd')
ax[0].set_xlabel('Date')
ax[0].set_ylabel('Daily Watthours')
ax[0].set_xlim((date_start, date_end))
#ax[0].set_title(filename)

f.autofmt_xdate()
#f.savefig(filename)
#plt.close()
plt.show()

