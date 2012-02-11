-- queries database for hourly samples
-- and inserts into table

insert into hourly_logs(

select
	-- these need to be made so that only one occurs per time sample
	logs.meter_name,
	logs.ip_address as circuit_id,
	date_trunc('hour',mts) as meter_timestamp,
	-- add interval since truncated time
	-- mts as meter_timestamp,
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
-- pulls min timestamp for each hour from database

	(
	select distinct
		meter_name,
		ip_address,
		min(meter_timestamp) as mts
	from
		logs
	where
		meter_name='ml03'
		--and circuitid=0
		--and timestamp > '2011-06-01'
		--and timestamp < '2011-06-07'
	group by
		meter_name,
		ip_address,
		extract(year from meter_timestamp),
		extract(month from meter_timestamp),
		extract(day from meter_timestamp),
		extract(hour from meter_timestamp)
	) as date_sq
on
 	(date_sq.meter_name=logs.meter_name
 	and date_sq.ip_address=logs.ip_address
 	and date_sq.mts=logs.meter_timestamp)

order by
-- 	meter_name,
-- 	circuit_id,
 	mts
);