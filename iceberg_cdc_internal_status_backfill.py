"""
Iceberg Schema Evolution – Full History Back-fill with Internal Status Expansion
=================================================================================
Target : KYC_application       (SCD-2, Iceberg)
Source : Salary_status          (SCD-2, Iceberg)

Problem with previous approach
-------------------------------
The original overlap-rank logic kept only ONE source row per target row (highest
overlap wins). This silently dropped every internal status transition that happened
WITHIN a single target row's date window — downstream systems never received those
intermediate statuses.

Correct Approach
----------------
For every (target row ↔ source row) overlap, produce ONE output row with:
    eff_start_date = MAX(tgt.eff_start_date, src.eff_start_date)   ← intersection start
    eff_end_date   = MIN(tgt.eff_end_date,   src.eff_end_date)     ← intersection end

This "clips" each source status to exactly the sub-window it applies inside the
target row, preserving ALL internal transitions without any gaps or overlaps.

Example (id=101, place=Mumbai  2018-11-19 → 2023-02-13):
  source IS  2018-11-19 → 2021-09-28   clips to:  2018-11-19 → 2021-09-28  ✓
  source KS  2021-09-28 → 2023-03-13   clips to:  2021-09-28 → 2023-02-13  ✓
  → 2 rows instead of 1, no gaps, no overlap

CDC / Merge strategy
---------------------
After expansion the enriched dataset may contain:
  • Net-new rows  (internal splits not in target)  → INSERT
  • Changed rows  (salary/status now populated)    → UPDATE
  • Unchanged rows (no source — 107, 114)          → skip / no-op
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# 1. Spark + Iceberg session
# ---------------------------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("iceberg_internal_status_cdc_backfill")
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.my_catalog.type", "hive")        # adjust: hadoop / glue
    # .config("spark.sql.catalog.my_catalog.warehouse", "s3://…")
    .getOrCreate()
)

CATALOG   = "my_catalog"
DB        = "your_database"                                      # ← change
TARGET    = f"{CATALOG}.{DB}.kyc_application"
SOURCE    = f"{CATALOG}.{DB}.salary_status"
HIGH_DATE = "3499-12-31"


# ---------------------------------------------------------------------------
# 2. Read tables and normalise types
# ---------------------------------------------------------------------------
tgt = (
    spark.table(TARGET)
    .withColumn("eff_start_date", F.col("eff_start_date").cast("date"))
    .withColumn("eff_end_date",   F.col("eff_end_date").cast("date"))
)

src = (
    spark.table(SOURCE)
    .toDF(*[c.lower() for c in spark.table(SOURCE).columns])    # lower-case columns
    .withColumn("eff_start_date", F.col("eff_start_date").cast("date"))
    .withColumn("eff_end_date",   F.col("eff_end_date").cast("date"))
    .select("id", "salary", "status", "eff_start_date", "eff_end_date")
    .withColumnRenamed("eff_start_date", "src_start")
    .withColumnRenamed("eff_end_date",   "src_end")
)


# ---------------------------------------------------------------------------
# 3. Boundary-condition join — keep ALL overlapping rows (no rank/filter)
#
#    Overlap condition:
#        src.src_start  <  tgt.eff_end_date        (source started before tgt ended)
#        src.src_end    >  tgt.eff_start_date       (source ended after tgt started)
#
#    Using LEFT JOIN so ids with no source (107, 114) flow through with NULL
#    salary/status and retain their original target dates.
# ---------------------------------------------------------------------------
joined = (
    tgt.alias("t")
    .join(
        src.alias("s"),
        on=(
            (F.col("t.id")             == F.col("s.id"))
            & (F.col("s.src_start")    <  F.col("t.eff_end_date"))
            & (F.col("s.src_end")      >  F.col("t.eff_start_date"))
        ),
        how="left"
    )
)


# ---------------------------------------------------------------------------
# 4. Date clipping — the key fix
#
#    For matched rows:
#        new eff_start_date = MAX(tgt.eff_start_date, src.src_start)
#        new eff_end_date   = MIN(tgt.eff_end_date,   src.src_end  )
#
#    For unmatched rows (no source):
#        keep original tgt dates unchanged
# ---------------------------------------------------------------------------
enriched = (
    joined
    .withColumn(
        "eff_start_date",
        F.when(
            F.col("s.id").isNotNull(),
            F.greatest(F.col("t.eff_start_date"), F.col("s.src_start"))
        ).otherwise(F.col("t.eff_start_date"))
    )
    .withColumn(
        "eff_end_date",
        F.when(
            F.col("s.id").isNotNull(),
            F.least(F.col("t.eff_end_date"), F.col("s.src_end"))
        ).otherwise(F.col("t.eff_end_date"))
    )
    # Recompute active: 1 only for the open-ended (current) row per id
    .withColumn(
        "active",
        F.when(F.col("eff_end_date") == F.lit(HIGH_DATE).cast("date"), 1).otherwise(0)
    )
    .select(
        F.col("t.id"),
        F.col("t.name"),
        F.col("t.place"),
        F.col("eff_start_date"),
        F.col("eff_end_date"),
        F.col("active"),
        F.col("s.salary"),
        F.col("s.status"),
    )
    .orderBy("id", "eff_start_date")
)

print("[INFO] Enriched dataset preview (all internal transitions expanded):")
enriched.show(50, truncate=False)


# ---------------------------------------------------------------------------
# 5. CDC detection
#    Compare enriched rows against current target to classify each row as:
#      • NEW   — (id + eff_start_date) not present in target → INSERT
#      • CHANGED — present but salary/status differ          → UPDATE
#      • SAME   — no change                                  → skip
# ---------------------------------------------------------------------------
tgt_keys = tgt.select("id", "eff_start_date", "salary", "status")

cdc = (
    enriched.alias("e")
    .join(
        tgt_keys.alias("k"),
        on=["id", "eff_start_date"],
        how="left"
    )
    .withColumn(
        "cdc_action",
        F.when(F.col("k.eff_start_date").isNull(), F.lit("INSERT"))   # net-new row
         .when(
             (F.col("e.salary").isNull()  != F.col("k.salary").isNull())
             | (F.col("e.salary")         != F.col("k.salary"))
             | (F.col("e.status").isNull() != F.col("k.status").isNull())
             | (F.col("e.status")         != F.col("k.status")),
             F.lit("UPDATE")
         )
         .otherwise(F.lit("SAME"))
    )
)

print("[INFO] CDC action breakdown:")
cdc.groupBy("cdc_action").count().show()


# ---------------------------------------------------------------------------
# 6. Register staging view for MERGE
# ---------------------------------------------------------------------------
STAGING = "v_enriched_staging"
enriched.createOrReplaceTempView(STAGING)


# ---------------------------------------------------------------------------
# 7. Iceberg MERGE statement
#
#    Match key  : id + eff_start_date
#      WHEN MATCHED AND values differ   → UPDATE salary + status only
#      WHEN NOT MATCHED                 → INSERT full new row
#        (these are the newly split rows from internal status transitions)
# ---------------------------------------------------------------------------
merge_sql = f"""
MERGE INTO {TARGET} AS tgt
USING {STAGING}     AS src
ON  tgt.id             = src.id
AND tgt.eff_start_date = src.eff_start_date

WHEN MATCHED AND (
      tgt.salary  IS DISTINCT FROM src.salary
   OR tgt.status  IS DISTINCT FROM src.status
   OR tgt.eff_end_date IS DISTINCT FROM src.eff_end_date
) THEN
  UPDATE SET
    tgt.eff_end_date = src.eff_end_date,
    tgt.active       = src.active,
    tgt.salary       = src.salary,
    tgt.status       = src.status

WHEN NOT MATCHED THEN
  INSERT (id, name, place, eff_start_date, eff_end_date, active, salary, status)
  VALUES (src.id, src.name, src.place,
          src.eff_start_date, src.eff_end_date, src.active,
          src.salary, src.status)
"""

spark.sql(merge_sql)
print("[INFO] MERGE completed.")


# ---------------------------------------------------------------------------
# 8. Post-merge: close out original parent rows that got SPLIT
#
#    After inserting new split rows (e.g. Mumbai 2018-11-19→2021-09-28 and
#    Mumbai 2021-09-28→2023-02-13), the original unsplit row
#    (Mumbai 2018-11-19→2023-02-13) still exists in the target.
#    We must expire it by setting its eff_end_date = its first child's start_date
#    so it is no longer a duplicate/overlapping record.
#
#    Strategy: delete original rows whose eff_start_date does NOT appear in
#    the enriched set (they were parent rows replaced by children).
# ---------------------------------------------------------------------------
valid_starts_view = "v_valid_starts"
enriched.select("id", "eff_start_date").createOrReplaceTempView(valid_starts_view)

cleanup_sql = f"""
DELETE FROM {TARGET}
WHERE (id, eff_start_date) NOT IN (
    SELECT id, eff_start_date FROM {valid_starts_view}
)
"""

spark.sql(cleanup_sql)
print("[INFO] Stale parent rows cleaned up.")


# ---------------------------------------------------------------------------
# 9. Validation
# ---------------------------------------------------------------------------
final = spark.table(TARGET).orderBy("id", "eff_start_date")
print("\n[VALIDATION] Final table state:")
final.show(50, truncate=False)

# Gap check
gap_sql = f"""
WITH ordered AS (
    SELECT
        id, place, eff_start_date, eff_end_date,
        LAG(eff_end_date) OVER (PARTITION BY id ORDER BY eff_start_date) AS prev_end
    FROM {TARGET}
)
SELECT id, place, prev_end AS gap_from, eff_start_date AS gap_to
FROM ordered
WHERE prev_end IS NOT NULL
  AND prev_end <> eff_start_date
ORDER BY id, eff_start_date
"""
gaps = spark.sql(gap_sql)
if gaps.count() == 0:
    print("[VALIDATION] ✓ No date gaps — history is fully continuous.")
else:
    print(f"[VALIDATION] ✗ Gaps detected:")
    gaps.show(truncate=False)

# Overlap check
overlap_sql = f"""
WITH ordered AS (
    SELECT
        id, eff_start_date, eff_end_date,
        LAG(eff_end_date) OVER (PARTITION BY id ORDER BY eff_start_date) AS prev_end
    FROM {TARGET}
)
SELECT id, eff_start_date, prev_end AS overlapping_end
FROM ordered
WHERE prev_end IS NOT NULL
  AND prev_end > eff_start_date
"""
overlaps = spark.sql(overlap_sql)
if overlaps.count() == 0:
    print("[VALIDATION] ✓ No overlapping date ranges.")
else:
    print("[VALIDATION] ✗ Overlaps detected:")
    overlaps.show(truncate=False)

spark.stop()
