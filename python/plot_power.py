'''
'''

import sqlalchemy as sa
import matplotlib.pyplot as plt
import datetime as dt

date_start = dt.datetime(2011, 11, 1)
date_end = dt.datetime(2012, 2, 1)
meter_name = 'ml03'


metadata = sa.MetaData('postgres://postgres:postgres@localhost:5432/sdcard')
vpl = sa.Table('view_power_table', metadata, autoload=True)
#vpl = sa.Table('logs', metadata, autoload=True)

#wh_column = vpl.c.watthours_sc20
#wh_column = vpl.c.watthours_today

query = sa.select([vpl.c.power,
                   vpl.c.credit,
                   vpl.c.meter_timestamp],
                   whereclause=sa.and_(vpl.c.meter_name=='ml03',
                                       vpl.c.ip_address==3,
                                       vpl.c.meter_timestamp<date_end,
                                       vpl.c.meter_timestamp>date_start,
                                       vpl.c.time_difference=='01:00:00',
                                       vpl.c.power>=0
                                       ),
                   order_by=vpl.c.meter_timestamp)
result = query.execute()
dates = []
watthours = []
credit = []
for r in result:
    dates.append(r.meter_timestamp)
    watthours.append(r.power)
    credit.append(r.credit)
# plot each circuit daily energy values for all time
f, ax = plt.subplots(2,1, sharex=True)
ax[0].plot_date(dates, watthours, mfc='#dddddd')
ax[0].set_xlabel('Date')
ax[0].set_ylabel('Daily Watthours')
ax[0].set_xlim((date_start, date_end))
#ax[0].set_title(filename)
ax[1].plot_date(dates, credit, mfc='#dddddd', linestyle='-', color='k')
ax[1].set_ylabel('Credit at Midnight')
f.autofmt_xdate()
#f.savefig(filename)
#plt.close()
plt.show()