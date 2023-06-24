-- The product with the highest reviews
-- the day it was ordered the most, either that day was a public holiday, 
-- total review points, percentage distribution of the review points, and percentage distribution of early shipments
-- to late shipments for that particular product.

Drop Table if exists ifeaakaw4441_analytics.best_performing_product;

Create Table ifeaakaw4441_analytics.best_performing_product as (

with highest_reviews as (
select r.product_id, dp.product_name, sum(r.review) tt_review_points 
from ifeaakaw4441_staging.reviews r join if_common.dim_products dp  on r.product_id = dp.product_id
group by r.product_id, dp.product_name
order by tt_review_points desc
limit 1
),

day_ordered_most as (
select distinct o.product_id::int, max(o.quantity) highest_order_quantity, o.order_date::Date
from ifeaakaw4441_staging.orders o join if_common.dim_products dp on o.product_id::int = dp.product_id
where o.product_id::int = (select product_id from highest_reviews)
group by o.product_id, o.order_date
order by highest_order_quantity desc, order_date desc
limit 1
),

reviews_pct as (
select product_id, 
(SUM(CASE when review = 1 then 1.0 else 0 end) / COUNT(*)) * 100.0 as pct_one_star_review,
(SUM(CASE when review = 2 then 1.0 else 0 end) / COUNT(*)) * 100.0 as pct_two_star_review,
(SUM(CASE when review = 3 then 1.0 else 0 end) / COUNT(*)) * 100.0 as pct_three_star_review,
(SUM(CASE when review = 4 then 1.0 else 0 end) / COUNT(*)) * 100.0 as pct_four_star_review,
(SUM(CASE when review = 5 then 1.0 else 0 end) / COUNT(*)) * 100.0 as pct_five_star_review
from ifeaakaw4441_staging.reviews
group by product_id
),

base_shipments as (
select o.product_id, o.order_id, sd.delivery_date, shipment_date::Date  - order_date::Date date_diff_days
from ifeaakaw4441_staging.orders o left join ifeaakaw4441_staging.shipment_deliveries sd on o.order_id = sd.order_id
),

shipment_ratio as (
select product_id::int,
	(count(distinct case when date_diff_days >= 6 and delivery_date is null then 
				order_id else null end)::DECIMAL(5,2)) late_shipments,
	
	(count(distinct case when date_diff_days < 6 and delivery_date is not null then 
				order_id else null end)::DECIMAL(5,2)) early_shipments
from base_shipments bs
group by product_id
)

select current_date ingestion_date, hr.product_id, hr.product_name, dom.order_date most_ordered_day,
		case when dd.day_of_the_week_num in (1,2,3,4,5) and dd.working_day = 'false' 
			then true 
			when dd.day_of_the_week_num in (1,2,3,4,5) and dd.working_day = 'true'
			then false
		end is_public_holiday,
		hr.tt_review_points,
		round(rp.pct_one_star_review, 3) pct_one_star_review, round(rp.pct_two_star_review, 3) pct_two_star_review, 
		round(rp.pct_three_star_review, 3) pct_three_star_review, round(rp.pct_four_star_review, 3) pct_four_star_review,
		round(rp.pct_five_star_review, 3) pct_five_star_review,
		round(sr.late_shipments::DECIMAL(5,2) / (sr.early_shipments::DECIMAL(5,2) + 
												 sr.late_shipments::DECIMAL(5,2)), 2) * 100 pct_late_shipment,
		round(sr.early_shipments::DECIMAL(5,2) / (sr.late_shipments::DECIMAL(5,2) + 
												  sr.early_shipments::DECIMAL(5,2)), 2) * 100 pct_early_shipment
		
from highest_reviews hr left join day_ordered_most dom on hr.product_id = dom.product_id
join if_common.dim_dates dd on dom.order_date = dd.calendar_dt
join reviews_pct rp on hr.product_id = rp.product_id
join shipment_ratio sr on hr.product_id = sr.product_id
)