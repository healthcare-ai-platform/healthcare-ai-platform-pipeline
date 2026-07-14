
  create or replace   view HEALTHCARE.staging_staging.stg_ocr_results
  
   as (
    with source as (
    select * from HEALTHCARE.RAW.ocr_results
),

cleaned as (
    select
        document_id,
        tenant_id,

        -- normalise test name and unit
        trim(lower(test_name))                          as test_name,
        trim(value)                                     as value,
        try_to_number(value)                            as value_numeric,   -- null when non-numeric (e.g. "Positive")
        trim(lower(unit))                               as unit,
        trim(reference_range)                           as reference_range,
        lower(trim(flag))                               as flag,            -- critical | high | low | borderline | normal

        extracted_at::timestamp_tz                      as extracted_at
    from source
    where document_id is not null
      and test_name    is not null
)

select * from cleaned
  );

