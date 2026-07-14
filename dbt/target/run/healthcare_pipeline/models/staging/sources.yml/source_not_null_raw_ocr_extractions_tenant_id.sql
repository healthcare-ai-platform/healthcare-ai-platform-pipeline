select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select tenant_id
from HEALTHCARE.RAW.ocr_extractions
where tenant_id is null



      
    ) dbt_internal_test