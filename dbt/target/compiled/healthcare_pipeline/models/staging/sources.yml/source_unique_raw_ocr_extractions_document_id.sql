
    
    

select
    document_id as unique_field,
    count(*) as n_records

from HEALTHCARE.RAW.ocr_extractions
where document_id is not null
group by document_id
having count(*) > 1


