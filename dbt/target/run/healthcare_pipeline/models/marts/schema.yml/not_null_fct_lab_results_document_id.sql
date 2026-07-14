select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select document_id
from HEALTHCARE.staging_marts.fct_lab_results
where document_id is null



      
    ) dbt_internal_test