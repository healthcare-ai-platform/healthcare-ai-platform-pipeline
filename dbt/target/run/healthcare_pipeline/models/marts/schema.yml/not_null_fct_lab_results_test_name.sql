select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select test_name
from HEALTHCARE.staging_marts.fct_lab_results
where test_name is null



      
    ) dbt_internal_test