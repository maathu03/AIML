"""
Generic SCD-2 Back-fill — Configuration Examples
==================================================
This file shows how the same scd2_backfill_generic.py handles
completely different table pairs with zero code changes.

Run any example by passing --args to AWS Glue, or by setting
the JobConfig directly for local testing.
"""

from scd2_backfill_generic import JobConfig, main

# ─────────────────────────────────────────────────────────────────────────────
# EXAMPLE 1 — Original use case (KYC + Salary)
# Exactly as built: back-fill salary + status into KYC_application
# ─────────────────────────────────────────────────────────────────────────────
kyc_salary_config = JobConfig(
    catalog          = "my_catalog",
    catalog_type     = "glue",
    database         = "kyc_db",
    target_table     = "kyc_application",
    source_table     = "salary_status",
    join_key         = "id",
    tgt_start_col    = "eff_start_date",
    tgt_end_col      = "eff_end_date",
    src_start_col    = "eff_start_date",
    src_end_col      = "eff_end_date",
    active_col       = "active",
    high_date        = "3499-12-31",
    new_cols         = ["salary", "status"],
    passthrough_cols = ["id", "name", "place",
                        "eff_start_date", "eff_end_date", "active"],
    shuffle_partitions = 800,
    num_output_parts   = 200,
    salt_buckets       = 50,
    source_size_mb     = 500,
    zorder_cols        = ["id", "eff_start_date"],
)

# AWS Glue --args equivalent:
# --catalog my_catalog --database kyc_db
# --target_table kyc_application --source_table salary_status
# --join_key id
# --new_cols salary,status
# --passthrough_cols id,name,place,eff_start_date,eff_end_date,active


# ─────────────────────────────────────────────────────────────────────────────
# EXAMPLE 2 — Customer profile + Address history
# Back-fill city, country, zip into customer_profile from address_history
# ─────────────────────────────────────────────────────────────────────────────
customer_address_config = JobConfig(
    catalog          = "prod_catalog",
    catalog_type     = "glue",
    database         = "crm_db",
    target_table     = "customer_profile",
    source_table     = "address_history",
    join_key         = "customer_id",
    tgt_start_col    = "valid_from",
    tgt_end_col      = "valid_to",
    src_start_col    = "valid_from",
    src_end_col      = "valid_to",
    active_col       = "is_current",
    high_date        = "9999-12-31",          # different sentinel
    new_cols         = ["city", "country", "zip_code"],
    passthrough_cols = ["customer_id", "full_name", "segment",
                        "valid_from", "valid_to", "is_current"],
    shuffle_partitions = 600,
    num_output_parts   = 150,
    salt_buckets       = 40,
    source_size_mb     = 300,
    zorder_cols        = ["customer_id", "valid_from"],
)

# AWS Glue --args equivalent:
# --catalog prod_catalog --database crm_db
# --target_table customer_profile --source_table address_history
# --join_key customer_id
# --tgt_start_col valid_from --tgt_end_col valid_to
# --src_start_col valid_from --src_end_col valid_to
# --active_col is_current --high_date 9999-12-31
# --new_cols city,country,zip_code
# --passthrough_cols customer_id,full_name,segment,valid_from,valid_to,is_current


# ─────────────────────────────────────────────────────────────────────────────
# EXAMPLE 3 — Policy master + Premium history
# Back-fill premium_amount and risk_band into policy_master
# Composite join key: policy_id + product_code
# ─────────────────────────────────────────────────────────────────────────────
policy_premium_config = JobConfig(
    catalog          = "ins_catalog",
    catalog_type     = "hive",
    database         = "insurance_db",
    target_table     = "policy_master",
    source_table     = "premium_history",
    join_key         = "policy_id,product_code",   # composite key
    tgt_start_col    = "eff_start_date",
    tgt_end_col      = "eff_end_date",
    src_start_col    = "eff_start_date",
    src_end_col      = "eff_end_date",
    active_col       = "active",
    high_date        = "3499-12-31",
    new_cols         = ["premium_amount", "risk_band"],
    passthrough_cols = ["policy_id", "product_code", "policy_holder",
                        "coverage_type", "eff_start_date", "eff_end_date", "active"],
    shuffle_partitions = 1000,
    num_output_parts   = 300,
    salt_buckets       = 60,
    source_size_mb     = 800,
    zorder_cols        = ["policy_id", "eff_start_date"],
)

# AWS Glue --args equivalent:
# --catalog ins_catalog --catalog_type hive --database insurance_db
# --target_table policy_master --source_table premium_history
# --join_key policy_id,product_code
# --new_cols premium_amount,risk_band
# --passthrough_cols policy_id,product_code,policy_holder,coverage_type,...


# ─────────────────────────────────────────────────────────────────────────────
# EXAMPLE 4 — Employee record + Grade history
# Back-fill grade, band, designation — large table (50M rows)
# ─────────────────────────────────────────────────────────────────────────────
employee_grade_config = JobConfig(
    catalog          = "hr_catalog",
    catalog_type     = "glue",
    database         = "hr_db",
    target_table     = "employee_record",
    source_table     = "grade_history",
    join_key         = "employee_id",
    tgt_start_col    = "effective_from",
    tgt_end_col      = "effective_to",
    src_start_col    = "grade_from",
    src_end_col      = "grade_to",
    active_col       = "is_active",
    high_date        = "2099-12-31",
    new_cols         = ["grade", "band", "designation"],
    passthrough_cols = ["employee_id", "full_name", "department",
                        "cost_centre", "effective_from", "effective_to", "is_active"],
    # Scaled up for 50M rows
    shuffle_partitions = 2000,
    num_output_parts   = 500,
    salt_buckets       = 100,
    source_size_mb     = 2500,    # > 2048 → shuffle join + salting applied
    broadcast_threshold= 2048,
    zorder_cols        = ["employee_id", "effective_from"],
    run_compaction     = True,
)


# ─────────────────────────────────────────────────────────────────────────────
# LOCAL TEST RUNNER
# Plug in any config above and run: python config_examples.py
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import sys
    import os

    # Override sys.argv so JobConfig.from_glue_args() picks up nothing
    # and we inject the config directly for local testing
    sys.argv = ["config_examples.py"]

    # Choose which config to test
    active_config = kyc_salary_config  # ← swap to any config above

    # Monkey-patch from_glue_args to return the local config
    from scd2_backfill_generic import JobConfig as _JC
    _JC.from_glue_args = classmethod(lambda cls: active_config)

    main()
