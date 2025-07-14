-- Custom test: Assert no duplicate messages per channel
-- This test ensures that each message_id + channel combination appears only once

select 
    message_id,
    channel,
    count(*) as duplicate_count
from {{ ref('fct_messages') }}
group by message_id, channel
having count(*) > 1 