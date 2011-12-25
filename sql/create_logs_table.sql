-- creates table in postgres sdcard database

--CREATE TABLE logs (
create table hourly_logs(
meter_name TEXT,
circuitid INTEGER,
timestamp TIMESTAMP,
watts FLOAT,
volts FLOAT,
amps FLOAT,
watthours_sc20 FLOAT,
watthours_today FLOAT,
powerfactor INTEGER,
frequency FLOAT,
voltamps FLOAT,
status BOOLEAN,
machineid TEXT,
credit FLOAT)