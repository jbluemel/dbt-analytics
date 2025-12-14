-- Test that all category totals are positive
select *
from {{ ref('category_performance') }}
where total_winning_bid_amount < 0