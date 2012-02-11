create table view_power_table as
    (select
        meter_timestamp,
        watthours_sc20,
        meter_name,
        ip_address,
        credit,
        --circuit_id,
        --pin,
        watthours_sc20 - lag(watthours_sc20, 1)
            over (partition by ip_address, meter_name
            order by meter_timestamp)
            as power,
        meter_timestamp - lag(meter_timestamp, 1)
            over (partition by ip_address, meter_name
            order by meter_timestamp)
            as time_difference
    from
         hourly_logs
    order by
        meter_timestamp)