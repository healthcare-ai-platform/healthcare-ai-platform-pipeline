-- Fact table: one row per (document, test_result).
-- Joined with dim_patients so every result carries a stable patient_key.
with documents as (
    select * from {{ ref('int_patient_documents') }}
    where test_name is not null
),

patients as (
    select * from {{ ref('dim_patients') }}
),

final as (
    select
        md5(d.document_id || '|' || d.test_name)       as result_key,
        d.document_id,
        p.patient_key,
        d.tenant_id,
        d.report_type,

        -- test result
        d.test_name,
        d.value,
        d.value_numeric,
        d.unit,
        d.reference_range,
        d.flag,

        -- report context
        d.report_date,
        d.doctor,
        d.facility,
        d.extraction_confidence,
        d.extracted_at
    from documents d
    left join patients p
        on  d.tenant_id = p.tenant_id
        and {{ patient_identity_key('d.patient_external_id', 'd.patient_name', 'd.patient_dob') }}
          = {{ patient_identity_key('p.patient_external_id', 'p.patient_name', 'p.patient_dob') }}
)

select * from final
