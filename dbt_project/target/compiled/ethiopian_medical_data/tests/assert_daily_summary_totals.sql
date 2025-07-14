-- Custom test: Assert daily summary totals match detail records
-- This test ensures that daily summary aggregations are consistent with detail data

with daily_detail as (
    select 
        channel_key,
        message_date_key,
        count(*) as detail_total_messages,
        sum(engagement_score) as detail_total_engagement
    from "ethiopian_medical_data"."marts"."fct_messages"
    group by channel_key, message_date_key
),

daily_summary as (
    select 
        channel_key,
        date_day as message_date_key,
        total_messages as summary_total_messages,
        total_engagement_score as summary_total_engagement
    from "ethiopian_medical_data"."marts"."fct_channel_daily_summary"
)

select 
    dd.channel_key,
    dd.message_date_key,
    dd.detail_total_messages,
    ds.summary_total_messages,
    dd.detail_total_engagement,
    ds.summary_total_engagement
from daily_detail dd
join daily_summary ds 
    on dd.channel_key = ds.channel_key 
    and dd.message_date_key = ds.message_date_key
where 
    dd.detail_total_messages != ds.summary_total_messages 
    or dd.detail_total_engagement != ds.summary_total_engagement