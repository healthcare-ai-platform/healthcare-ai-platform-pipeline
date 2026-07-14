with source as (
    select * from HEALTHCARE.RAW.ocr_extractions
),

cleaned as (
    select
        document_id,
        tenant_id,
        report_type,

        -- patient demographics — normalise whitespace and casing
        trim(patient_name)                              as patient_name,
        trim(patient_external_id)                       as patient_external_id,
        try_to_date(patient_dob,    'YYYY-MM-DD')       as patient_dob,
        lower(trim(patient_gender))                     as patient_gender,

        -- report metadata
        try_to_date(report_date, 'YYYY-MM-DD')          as report_date,
        trim(doctor)                                    as doctor,
        trim(facility)                                  as facility,

        -- quality signals
        extraction_status,
        extraction_confidence::float                    as extraction_confidence,
        extracted_at::timestamp_tz                      as extracted_at
    from source
    where document_id is not null
)

select * from cleaned