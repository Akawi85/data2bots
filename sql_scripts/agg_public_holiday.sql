-- The total number of orders placed on a public holiday every month, for the past year

Drop Table if exists ifeaakaw4441_analytics.agg_public_holiday;

Create Table ifeaakaw4441_analytics.agg_public_holiday as (
	
with base as (
select extract(month from calendar_dt) public_holiday_months, calendar_dt, day_of_the_week_num, working_day
from if_common.dim_dates
where calendar_dt between '2021-01-01' and '2021-12-31' 
	
),

extract_months as (
select case when b.public_holiday_months = 1 and b.day_of_the_week_num in (1,2,3,4,5) and b.working_day = 'false' 
			then count(o.order_id) end as tt_order_hol_jan,
		case when b.public_holiday_months = 2 and b.day_of_the_week_num in (1,2,3,4,5) and b.working_day = 'false' 
			then count(o.order_id)
			end as tt_order_hol_feb,
		case when b.public_holiday_months = 3 and b.day_of_the_week_num in (1,2,3,4,5) and b.working_day = 'false' 
			then count(o.order_id)
			end as tt_order_hol_mar,
		case when b.public_holiday_months = 4 and b.day_of_the_week_num in (1,2,3,4,5) and b.working_day = 'false' 
			then count(o.order_id)
			end as tt_order_hol_apr,
		case when b.public_holiday_months = 5 and b.day_of_the_week_num in (1,2,3,4,5) and b.working_day = 'false' 
			then count(o.order_id)
			end as tt_order_hol_may,
		case when b.public_holiday_months = 6 and b.day_of_the_week_num in (1,2,3,4,5) and b.working_day = 'false' 
			then count(o.order_id)
			end as tt_order_hol_jun,
		case when b.public_holiday_months = 7 and b.day_of_the_week_num in (1,2,3,4,5) and b.working_day = 'false' 
			then count(o.order_id)
			end as tt_order_hol_jul,
		case when b.public_holiday_months = 8 and b.day_of_the_week_num in (1,2,3,4,5) and b.working_day = 'false' 
			then count(o.order_id)
			end as tt_order_hol_aug,
		case when b.public_holiday_months = 9 and b.day_of_the_week_num in (1,2,3,4,5) and b.working_day = 'false' 
			then count(o.order_id)
			end as tt_order_hol_sep,
		case when b.public_holiday_months = 10 and b.day_of_the_week_num in (1,2,3,4,5) and b.working_day = 'false' 
			then count(o.order_id)
			end as tt_order_hol_oct,
		case when b.public_holiday_months = 11 and b.day_of_the_week_num in (1,2,3,4,5) and b.working_day = 'false' 
			then count(o.order_id)
			end as tt_order_hol_nov,
		case when b.public_holiday_months = 12 and b.day_of_the_week_num in (1,2,3,4,5) and b.working_day = 'false' 
			then count(o.order_id)
			end as tt_order_hol_dec
from base b left join ifeaakaw4441_staging.orders o on o.order_date::Date = b.calendar_dt::Date
group by public_holiday_months, day_of_the_week_num, working_day
)

select current_date ingestion_date,
		coalesce(max(tt_order_hol_jan),0) tt_order_hol_jan, coalesce(max(tt_order_hol_feb),0) tt_order_hol_feb, 
		coalesce(max(tt_order_hol_mar),0) tt_order_hol_mar, coalesce(max(tt_order_hol_apr),0) tt_order_hol_apr,
		coalesce(max(tt_order_hol_may),0) tt_order_hol_may, coalesce(max(tt_order_hol_jun),0) tt_order_hol_jun,
		coalesce(max(tt_order_hol_jul),0) tt_order_hol_jul, coalesce(max(tt_order_hol_aug),0) tt_order_hol_aug,
		coalesce(max(tt_order_hol_sep),0) tt_order_hol_sep, coalesce(max(tt_order_hol_oct),0) tt_order_hol_oct,
		coalesce(max(tt_order_hol_nov),0) tt_order_hol_nov, coalesce(max(tt_order_hol_dec),0) tt_order_hol_dec
from extract_months
)