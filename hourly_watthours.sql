--this script creates a table in the sql database for hourly info

--drop table hourly_watthours;

create table hourly_watthours(
timestamp timestamp,
circuitid integer,
watthours float
);


.separator ","

.import hourly.csv hourly_watthours

