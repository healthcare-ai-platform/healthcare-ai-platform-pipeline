select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with all_values as (

    select
        flag as value_field,
        count(*) as n_records

    from HEALTHCARE.staging_marts.fct_lab_results
    group by flag

)

select *
from all_values
where value_field not in (
    'critical','high','low','borderline','normal'
)



      
    ) dbt_internal_test