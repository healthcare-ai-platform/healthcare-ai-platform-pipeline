-- One row per patient per tenant.
-- Deduplication key: patient_external_id when present, else patient_name + patient_dob
-- (see macros/patient_identity_key.sql — DOB guards against merging two different
-- patients who happen to share a name). Latest extracted record wins.
with extractions as (
    select
        *,
        {{ patient_identity_key('patient_external_id', 'patient_name', 'patient_dob') }} as identity_key
    from {{ ref('stg_ocr_extractions') }}
    where patient_name is not null
),

ranked as (
    select
        *,
        row_number() over (
            partition by tenant_id, identity_key
            order by extracted_at desc
        ) as rn
    from extractions
),

final as (
    select
        md5(tenant_id || '|' || identity_key)           as patient_key,
        tenant_id,
        patient_name,
        patient_external_id,
        patient_dob,
        patient_gender,
        extracted_at                                    as last_seen_at
    from ranked
    where rn = 1
)

select * from final
