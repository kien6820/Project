-- Big project for SQL
-- Link instruction: https://docs.google.com/spreadsheets/d/1WnBJsZXj_4FDi2DyfLH1jkWtfTridO2icWbWCh7PLs8/edit#gid=0


-- Query 01: calculate total visit, pageview, transaction and revenue for Jan, Feb and March 2017 order by month
#standardSQL
select
    format_date("%Y%m",parse_date("%Y%m%d",date)) as month,
    sum(totals.visits) as visits,
    sum(totals.pageviews) as pageviews,
    sum(totals.transactions) as transactions,
    sum(totals.totalTransactionRevenue) / power(10,6) as revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
group by month
order by month asc
limit 3;

-- Query 02: Bounce rate per traffic source in July 2017
#standardSQL
SELECT 
      trafficSource.source,
      sum(totals.visits) as total_visits,
      sum(totals.bounces) as total_no_of_bounces,
      round((sum(totals.bounces)*100.0) / sum(totals.visits),8) as bounce_rate
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
group by trafficSource.source
order by total_visits desc;

-- Query 3: Revenue by traffic source by week, by month in June 2017
#standardSQL
with m as (
select
      format_date("%Y%m",parse_date("%Y%m%d",date)) as time,
      trafficSource.source as source,
      round(totals.totalTransactionRevenue / power(10,6),2) as m_revenue
from `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`
),

w as (
select
      format_date("%Y%W",parse_date("%Y%m%d",date)) as time,
      trafficSource.source as source,
      round(totals.totalTransactionRevenue / power(10,6),2) as w_revenue
from `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`
)

select 
      "month" as time_type,
      m.time,
      m.source,
      round(sum(m.m_revenue),2) as revenue
from m
group by m.source, m.time
union all
select 
      "week" as time_type,
      w.time,
      w.source,
      round(sum(w.w_revenue),2) as revenue
from w
group by w.source, w.time
order by revenue desc;

--Query 04: Average number of product pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017. Note: totals.transactions >=1 for purchaser and totals.transactions is null for non-purchaser
#standardSQL
with pur as(
select
      month,
      round(sum(pur.m) / count(distinct pur.fullVisitorId),8) as avg_pageviews_purchase
from (select
      format_date("%Y%m",parse_date("%Y%m%d",date)) as month,
      totals.pageviews as m,
      totals.transactions,
      fullVisitorId
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
where _table_suffix between '20170601' and '20170731'and totals.transactions >=1) as pur
group by month
order by month),

non_pur as( 
select
      month,
      round(sum(non_pur.m) / count(distinct non_pur.fullVisitorId),9) as avg_pageviews_non_purchase
from (select
      format_date("%Y%m",parse_date("%Y%m%d",date)) as month,
      totals.pageviews as m,
      totals.transactions,
      fullVisitorId
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
where _table_suffix between '20170601' and '20170731'and totals.transactions is null) as non_pur
group by month
order by month)

select
      month,
      avg_pageviews_purchase,
      avg_pageviews_non_purchase
from pur
left join non_pur using(month)
order by month;

-- Query 05: Average number of transactions per user that made a purchase in July 2017
#standardSQL
select
    format_date("%Y%m",parse_date("%Y%m%d",date)) as Month,
    round(sum(totals.transactions) / count(distinct fullVisitorId),9) as Avg_total_transactions_per_user
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
where totals.transactions >=1
group by Month;

-- Query 06: Average amount of money spent per session
#standardSQL
select
    format_date("%Y%m",parse_date("%Y%m%d",date)) as Month,
    round(sum(totals.totalTransactionRevenue) / count(fullVisitorId),2) as avg_revenue_by_user_per_visit
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
where totals.transactions is not null
group by Month;

-- Query 07: Products purchased by customers who purchased product A (Classic Ecommerce)
#standardSQL
with a as(
select
      distinct fullVisitorId
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
  UNNEST (hits) as hits,
  UNNEST (hits.product) as product
  where product.v2ProductName="YouTube Men's Vintage Henley" and product.productRevenue is not null),

b as(
select
      fullVisitorId, 
      product.v2ProductName as other_purchased_products,
      product.productQuantity as quantity
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
  UNNEST (hits) as hits,
  UNNEST (hits.product) as product
 where product.productRevenue is not null),
  
 c as(

select 
      b.fullVisitorId, 
      other_purchased_products,
      quantity
from b
inner join a using(fullVisitorId))

select
      other_purchased_products,
      sum(quantity) as quantity
from c
where other_purchased_products <> "YouTube Men's Vintage Henley"
group by other_purchased_products
order by quantity desc;

--Query 08: Calculate cohort map from pageview to addtocart to purchase in last 3 month. For example, 100% pageview then 40% add_to_cart and 10% purchase.
#standardSQL
with sub as(
select
      format_date("%Y%m",parse_date("%Y%m%d",date)) as month,
      count(case when eCommerceAction.action_type = '2' then product.v2ProductName else null end) as num_product_view,
      count(case when eCommerceAction.action_type = '3' then product.v2ProductName else null end) as num_addtocart,
      count(case when eCommerceAction.action_type = '6' then product.v2ProductName else null end) as num_purchase
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
  UNNEST (hits) as hits,
  UNNEST (hits.product) as product
where _table_suffix between '20170101' and '20170331'
group by month
order by month asc)

select 
    month,
    num_product_view,
    num_addtocart,
    num_purchase,
    round((num_addtocart / num_product_view)*100.0,2) as add_to_cart_rate,
    round((num_purchase / num_product_view)*100.0,2) as purchase_rate
from sub
order by month asc;
