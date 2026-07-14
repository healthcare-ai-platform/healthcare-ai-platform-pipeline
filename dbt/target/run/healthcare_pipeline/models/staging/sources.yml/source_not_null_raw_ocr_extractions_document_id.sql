select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select document_id
from HEALTHCARE.RAW.ocr_extractions
where document_id is null



      
    ) dbt_internal_test