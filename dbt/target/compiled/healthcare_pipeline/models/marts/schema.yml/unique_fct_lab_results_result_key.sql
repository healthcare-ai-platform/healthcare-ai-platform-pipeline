
    
    

select
    result_key as unique_field,
    count(*) as n_records

from HEALTHCARE.staging_marts.fct_lab_results
where result_key is not null
group by result_key
having count(*) > 1


