import sqlalchemy as sa
import pandas as p

def get_watthours_sc20(meter_name, ip_address, date_start, date_end):
    metadata = sa.MetaData('postgres://postgres:postgres@localhost:5432/sdcard')
    t = sa.Table('hourly_logs', metadata, autoload=True)
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
