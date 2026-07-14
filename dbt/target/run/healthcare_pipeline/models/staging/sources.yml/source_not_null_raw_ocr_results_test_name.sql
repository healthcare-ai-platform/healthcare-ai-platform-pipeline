select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select test_name
from HEALTHCARE.RAW.ocr_results
where test_name is null



      
    ) dbt_internal_test