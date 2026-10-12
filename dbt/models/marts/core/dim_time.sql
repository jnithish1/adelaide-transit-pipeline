-- Time-of-day dimension at minute grain (1,440 rows). Kept separate from
-- dim_date, as Kimball recommends, so the date dimension stays small.
-- Peak periods are an assumption for analysis, not an Adelaide Metro definition.
with minutes as (
    select row_number() over (order by seq4()) - 1 as minute_of_day
    from table(generator(rowcount => 1440))
)

select
    floor(minute_of_day / 60) * 100 + mod(minute_of_day, 60)       as time_key,   -- 17:45 -> 1745
    minute_of_day,
    floor(minute_of_day / 60)                                       as hour_of_day,
    mod(minute_of_day, 60)                                          as minute_of_hour,
    lpad(floor(minute_of_day / 60), 2, '0') || ':' || lpad(mod(minute_of_day, 60), 2, '0') as time_label,
    case
        when minute_of_day between 7*60  and 9*60 - 1  then 'AM peak'
        when minute_of_day between 9*60  and 15*60 - 1 then 'Interpeak'
        when minute_of_day between 15*60 and 18*60 - 1 then 'PM peak'
        when minute_of_day between 18*60 and 23*60 - 1 then 'Evening'
        else 'Night'
    end                                                             as day_period
from minutes

union all

-- unknown member, for stop events with no scheduled or predicted time
select -1, null, null, null, 'Unknown', 'Unknown'
