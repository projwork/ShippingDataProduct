
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from "ethiopian_medical_data"."dbt_test__audit"."assert_engagement_logic"
    
      
    ) dbt_internal_test