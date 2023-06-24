-- 1. Total number of late shipments
-- A late shipment is one with shipment_date greater than or equal to 6 days after the order_date and delivery_date is NULL

-- 2. Total number of undelivered shipments
-- An undelivered shipment is one with delivery_date as NULL and shipment_date as NULL and the current_date 15 days after
-- order_date.

Drop Table if exists ifeaakaw4441_analytics.agg_shipments;

Create Table ifeaakaw4441_analytics.agg_shipments as (
	
with base_late_shipments as (
select o.order_id, to_date(o.order_date, 'yyyy/mm/dd') as order_date, 
		to_date(sd.shipment_date, 'yyyy/mm/dd') as shipment_date
from public.orders o left join public.shipment_deliveries sd on o.order_id = sd.order_id
where sd.delivery_date isnull 
),

late_shipments_date_diff as (
select current_date ingestion_date, order_id, order_date, shipment_date, shipment_date  - order_date date_diff_days
from base_late_shipments
),

base_undelivered_shipments as (
select o.order_id, to_date(o.order_date, 'yyyy,mm,dd') as order_date
from orders o left join shipment_deliveries sd on o.order_id = sd.order_id
where sd.delivery_date isnull and
	  sd.shipment_date isnull
),

undelivered_date_diff as (
select current_date ingestion_date, order_id, order_date, '2022-09-05' - order_date diff_date
from base_undelivered_shipments
),

final_undelivered_orders as (
select udd.ingestion_date, count(udd.order_id) tt_undelivered_items
from undelivered_date_diff udd
where udd.diff_date > 15
group by udd.ingestion_date
),

final_late_shipments as (
select lsdd.ingestion_date, count(lsdd.order_id) tt_late_shipments
from late_shipments_date_diff lsdd
where lsdd.date_diff_days >= 6
group by lsdd.ingestion_date
)

select fuo.ingestion_date, fls.tt_late_shipments, fuo.tt_undelivered_items
from final_undelivered_orders fuo left join final_late_shipments fls on fuo.ingestion_date =  fls.ingestion_date
)