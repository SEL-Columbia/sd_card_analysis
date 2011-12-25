--calculates daily self consumption of the meter

select 'ml01', '25', mains_result.timestamp, mains_result.mains - consumer_result.consumers, mains_result.numsamples
from
(select timestamp,sum(watthours) as mains, numsamples
from daily_watthours where circuitid=1 group by timestamp)
mains_result,
(select timestamp,sum(watthours) as consumers
from daily_watthours where circuitid between 2 and 21 group by timestamp)
consumer_result
where mains_result.timestamp = consumer_result.timestamp
;