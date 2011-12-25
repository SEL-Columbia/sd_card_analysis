select circuitid, count(*), count(*)/720.
from hourly_watthours
where timestamp between '2011-06-01 00:00:00' and '2011-07-01 00:00:00'
group by circuitid;
