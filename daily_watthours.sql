--this script creates a table in the sql database for
--daily watthour info

--drop table hourly_watthours;

create table daily_watthours(
metername string,
circuitid integer,
timestamp timestamp,
watthours float,
numsamples integer
);

.separator ","

.import daily_watthours.csv daily_watthours

