select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select patient_name
from HEALTHCARE.staging_marts.dim_patients
where patient_name is null



      
    ) dbt_internal_test