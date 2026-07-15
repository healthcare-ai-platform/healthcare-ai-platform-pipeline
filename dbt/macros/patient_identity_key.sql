{% macro patient_identity_key(external_id_col, name_col, dob_col) %}
    {#
        Prefer a tenant-issued external_id when present — it already
        disambiguates individuals. Without one, fall back to name + DOB
        together so two different patients who happen to share a name in the
        same tenant aren't silently merged into a single patient record.
        Used identically in dim_patients and fct_lab_results — keep it in one
        place so the two can't drift apart and silently break the join.
    #}
    coalesce(
        nullif(trim({{ external_id_col }}), ''),
        trim({{ name_col }}) || '|' || coalesce({{ dob_col }}::string, '')
    )
{% endmacro %}
