select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select result_key
from HEALTHCARE.staging_marts.fct_lab_results
where result_key is null



      
    ) dbt_internal_test