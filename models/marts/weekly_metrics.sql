{{ config(materialized='table') }}

with date_bounds as (
    -- Get the range of auction dates from actual data
    select
        min(auctiondate) as first_auction_date,
        max(auctiondate) as last_auction_date
    from {{ ref('items_v2') }}
    where auctiondate is not null
),

fiscal_year_weeks as (
    -- Generate all Sunday-Saturday weeks from first auction through last auction
    select
        date_series::date as week_start_date,
        (date_series + interval '6 days')::date as week_end_date,
        (case 
            when extract(month from date_series) >= 8 
            then extract(year from date_series) + 1
            else extract(year from date_series)
        end)::int as fiscal_year
    from date_bounds,
    generate_series(
        -- First Sunday on/before the first auction date
        (date_trunc('week', first_auction_date + interval '1 day')::date - interval '1 day'),
        -- Last Sunday on/before the last auction date  
        (date_trunc('week', last_auction_date + interval '1 day')::date - interval '1 day'),
        '1 week'::interval
    ) as date_series
),

item_fees as (
    select
        item_id,
        sum(fee_amount) as total_fees
    from {{ ref('fees') }}
    group by item_id
),

auction_data as (
    select
        date_trunc('week', i.auctiondate + interval '1 day')::date - interval '1 day' as week_start_date,
        count(distinct i.unique_id) as total_items_sold,
        avg(i.contract_price) as avg_lot_value,
        -- Calculate buyers premium per item, then sum
        sum(i.contract_price - i.hammer) as total_buyers_premium,
        -- Total fees for the week
        sum(coalesce(f.total_fees, 0)) as total_fees,
        -- Revenue = buyers premium + fees
        sum((i.contract_price - i.hammer) + coalesce(f.total_fees, 0)) as total_revenue
    from {{ ref('items_v2') }} i
    left join item_fees f on i.unique_id = f.item_id
    where i.auctiondate is not null
    group by date_trunc('week', i.auctiondate + interval '1 day')::date - interval '1 day'
),

bid_counts as (
    select
        date_trunc('week', i.auctiondate + interval '1 day')::date - interval '1 day' as week_start_date,
        count(b.bid_id) as total_bids
    from {{ ref('bids') }} b
    join {{ ref('items_v2') }} i on b.item_id = i.unique_id
    where i.auctiondate is not null
    group by date_trunc('week', i.auctiondate + interval '1 day')::date - interval '1 day'
)

select
    -- Calculate fiscal week number
    floor(
        (fw.week_start_date - ((fw.fiscal_year - 1) || '-08-01')::date) / 7
    )::int + 1 as fiscal_week_number,
    fw.fiscal_year,
    fw.week_start_date,
    fw.week_end_date,
    coalesce(ad.total_items_sold, 0) as total_items_sold,
    round(coalesce(ad.avg_lot_value, 0))::int as avg_lot_value,
    round(coalesce(ad.total_revenue, 0))::int as total_revenue,
    round(coalesce(ad.total_fees, 0))::int as total_fees,
    coalesce(bc.total_bids, 0) as total_bids
from fiscal_year_weeks fw
left join auction_data ad on fw.week_start_date = ad.week_start_date
left join bid_counts bc on fw.week_start_date = bc.week_start_date
order by fw.week_start_date