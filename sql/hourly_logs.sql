insert into hourly_logs(
select
	logs.meter_name,
	logs.circuitid,
	mts,
	logs.watts,
	logs.volts,
	logs.amps,
	logs.watthours_sc20,
	logs.watthours_today,
	logs.powerfactor,
	logs.frequency,
	logs.voltamps,
	logs.status,
	logs.machineid,
	logs.credit
from 
	logs 
join
-- pulls max timestamp for each hour from database
(select distinct
	meter_name,
	circuitid,
	min(timestamp) as mts
from 
	logs
where 
	meter_name='ml07'
-- 	and timestamp > '2011-09-01'
-- 	and timestamp < '2011-09-07'
group by
	meter_name,
	circuitid,
	extract(year from timestamp),
	extract(month from timestamp),
	extract(day from timestamp),
	extract(hour from timestamp)) as sq

on 
	(sq.meter_name=logs.meter_name
	and sq.circuitid=logs.circuitid
	and sq.mts=logs.timestamp)
order by 
	meter_name,
	circuitid,
	mts
);
	