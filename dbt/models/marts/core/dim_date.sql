-- Kimball date dimension, one row per day. Smart key YYYYMMDD so facts stay
-- readable. Includes the Australian financial year and SA public holidays,
-- because transit timetables run on a weekday / Saturday / Sunday-and-holiday
-- pattern.
with spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="to_date('" ~ var('date_spine_start') ~ "')",
        end_date="to_date('" ~ var('date_spine_end') ~ "')"
    ) }}
),

holidays as (
    select * from {{ ref('sa_public_holidays') }}
),

days as (
    select date_day::date as date_day from spine
)

select
    {{ date_key('d.date_day') }}                                as date_key,
    d.date_day,
    dayofweekiso(d.date_day)                                    as day_of_week_iso,   -- 1 = Monday
    dayname(d.date_day)                                         as day_name_short,
    decode(dayofweekiso(d.date_day), 1, 'Monday', 2, 'Tuesday', 3, 'Wednesday',
           4, 'Thursday', 5, 'Friday', 6, 'Saturday', 7, 'Sunday')        as day_name,
    dayofweekiso(d.date_day) in (6, 7)                          as is_weekend,
    date_trunc('week', d.date_day)::date                        as week_start_date,
    weekiso(d.date_day)                                         as iso_week,
    day(d.date_day)                                             as day_of_month,
    month(d.date_day)                                           as month_number,
    monthname(d.date_day)                                       as month_name_short,
    to_char(d.date_day, 'YYYY-MM')                              as year_month,
    quarter(d.date_day)                                         as calendar_quarter,
    year(d.date_day)                                            as calendar_year,
    -- Australian financial year runs 1 July - 30 June, labelled by end year
    case when month(d.date_day) >= 7 then year(d.date_day) + 1
         else year(d.date_day) end                              as financial_year,
    h.holiday_date is not null and not h.is_part_day            as is_public_holiday,
    h.holiday_name,
    case
        when h.holiday_date is not null and not h.is_part_day then 'Sunday / Public holiday'
        when dayofweekiso(d.date_day) = 7                     then 'Sunday / Public holiday'
        when dayofweekiso(d.date_day) = 6                     then 'Saturday'
        else 'Weekday'
    end                                                         as service_day_type
from days d
left join holidays h
    on h.holiday_date = d.date_day
