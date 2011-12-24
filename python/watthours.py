import psycopg2
conn = psycopg2.connect('dbname=sdcard')
cursor = conn.cursor()

meter_name = 'ml06'
date_start = '20110401'
date_end = '20110901'

query = """select timestamp, watthours_sc20
           from hourly_logs
           where meter_name = '%s'
           and circuitid=0
               and  timestamp >= '%s' and
                 timestamp <= '%s'
           order by timestamp
        """ % (meter_name, date_start, date_end)

shniz = cursor.execute(query)
shniz = cursor.fetchall()

dates = []
watthours = []

for s in shniz:
    dates.append(s[0])
    watthours.append(s[1])

import pylab
pylab.plot_date(dates,watthours)
pylab.grid()
pylab.show()

