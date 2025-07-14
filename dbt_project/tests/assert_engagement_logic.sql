-- Custom test: Assert engagement score calculation is correct
-- This test validates that engagement_score = views + (forwards * 2) + (replies * 3)

select 
    message_key,
    views,
    forwards,
    replies,
    engagement_score,
    (views + (forwards * 2) + (replies * 3)) as calculated_engagement_score
from {{ ref('fct_messages') }}
where engagement_score != (views + (forwards * 2) + (replies * 3)) 