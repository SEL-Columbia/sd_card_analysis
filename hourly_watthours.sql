--this script creates a table in the sql database for hourly info

drop table hourly_watthours;

create table hourly_watthours(
timestamp timestamp,
circuitid integer,
watthours float
);


.separator ","

.import csv_out.csv hourly_watthours

--create index logs_idx on logs (timestamp);
