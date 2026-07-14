select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select patient_key
from HEALTHCARE.staging_marts.dim_patients
where patient_key is null



      
    ) dbt_internal_test