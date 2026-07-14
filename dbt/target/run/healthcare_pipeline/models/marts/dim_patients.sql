
        

    
        create dynamic table HEALTHCARE.staging_marts.dim_patients
        target_lag = '60 seconds'
        warehouse = COMPUTE_WH
        as (
            -- One row per patient per tenant.
-- Deduplication key: patient_external_id when present, else patient_name.
-- Latest extracted record wins.
with extractions as (
    select * from HEALTHCARE.staging_staging.stg_ocr_extractions
    where patient_name is not null
),

ranked as (
    select
        *,
        row_number() over (
            partition by tenant_id,
                         coalesce(nullif(trim(patient_external_id), ''), trim(patient_name))
            order by extracted_at desc
        ) as rn
    from extractions
),

final as (
    select
        md5(tenant_id || '|' || coalesce(nullif(trim(patient_external_id), ''), trim(patient_name))) as patient_key,
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
        )

    


    