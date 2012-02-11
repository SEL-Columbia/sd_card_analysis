-- creates table in postgres sdcard database

create table hourly_logs(
meter_name TEXT,
ip_address INTEGER,
meter_timestamp TIMESTAMP,
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
credit FLOAT,
PRIMARY KEY (meter_name, ip_address, meter_timestamp)
)