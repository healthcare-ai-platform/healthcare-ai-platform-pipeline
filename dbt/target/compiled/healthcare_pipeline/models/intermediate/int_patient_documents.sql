-- Join document-level metadata with every test result it contains.
-- One row per (document, test_result) pair.
with extractions as (
    select * from HEALTHCARE.staging_staging.stg_ocr_extractions
),

results as (
    select * from HEALTHCARE.staging_staging.stg_ocr_results
),

joined as (
    select
        -- document identity
        e.document_id,
        e.tenant_id,
        e.report_type,

        -- patient context
        e.patient_name,
        e.patient_external_id,
        e.patient_dob,
        e.patient_gender,

        -- report context
        e.report_date,
        e.doctor,
        e.facility,
        e.extraction_confidence,

        -- test result
        r.test_name,
        r.value,
        r.value_numeric,
        r.unit,
        r.reference_range,
        r.flag,

        e.extracted_at
    from extractions e
    left join results r
        on e.document_id = r.document_id
)

select * from joined