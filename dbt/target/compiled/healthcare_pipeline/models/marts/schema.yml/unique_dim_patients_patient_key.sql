
    
    

select
    patient_key as unique_field,
    count(*) as n_records

from HEALTHCARE.staging_marts.dim_patients
where patient_key is not null
group by patient_key
having count(*) > 1


