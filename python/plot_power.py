'''
'''

import sqlalchemy as sa
import matplotlib.pyplot as plt
import datetime as dt
import sd_card_analysis as sda
import pandas as p

date_start = dt.datetime(2011, 11, 1)
date_end = dt.datetime(2012, 2, 1)
meter_name = 'ml03'

f, ax = plt.subplots(21, 1, figsize=(10,20), sharex=True)
for i in range(0,21):
    ip_address = '192.168.1.2' + '%02d' % i
    df = sda.get_watthours_sc20(meter_name=meter_name,
                                ip_address=ip_address,
                                date_start=date_start,
                                date_end=date_end)

    # get hourly difference
    hourly_power = df - df.shift(1, offset=p.DateOffset(hours=1))
    hourly_power = hourly_power[hourly_power>=0]

    # plot to axes
    hourly_power.plot(ax=ax[i], style='ko')
    ax[i].text(1.1,0,ip_address[-2:],transform=ax[i].transAxes)

plt.show()

