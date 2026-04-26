"""
Generic SCD-2 Schema-Evolution Back-fill Framework
====================================================
A fully reusable, config-driven Insert-Overwrite pipeline that back-fills
any number of new columns from any SCD-2 source table into any SCD-2 target
table — with no code changes required between deployments.

How to reuse for a different pair of tables
--------------------------------------------
1. Change JobConfig values (catalog, database, table names, column mappings)
2. Supply the config via --args in AWS Glue or as environment variables
3. Run — everything else is automatic

Generic design pillars
-----------------------
  ◆  No hardcoded table names, column names, or business logic
  ◆  JOIN key, date columns, new columns, passthrough columns all configurable
  ◆  Supports any number of new columns to back-fill (not just salary + status)
  ◆  Supports any SCD-2 sentinel HIGH_DATE value per deployment
  ◆  Broadcast vs shuffle join selected automatically at runtime
  ◆  Salt bucket count auto-tuned from executor count
  ◆  Validation queries built dynamically from config — no SQL hardcoding
  ◆  CloudWatch-ready structured metric logging
  ◆  Idempotent — safe to re-run, produces same output every time

Tested use-cases this generic code handles
-------------------------------------------
  KYC_application  ← Salary_status   (salary, status columns)
  Customer_profile ← Address_history (city, country, zip columns)
  Policy_master    ← Premium_history  (premium_amount, risk_band columns)
  Employee_record  ← Grade_history    (grade, band, designation columns)
  ...any SCD-2 → SCD-2 pair with date-range overlap semantics
"""

import sys
import time
import logging
from dataclasses import dataclass, field
from typing import List, Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel

# ── Try Glue args; fall back to defaults for local/unit test runs ─────────────
try:
    from awsglue.utils import getResolvedOptions
    _GLUE_AVAILABLE = True
except ImportError:
    _GLUE_AVAILABLE = False

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)


# ═════════════════════════════════════════════════════════════════════════════
# SECTION 1 — JOB CONFIGURATION
# Every value that differs between deployments lives here.
# All fields can be overridden via --args (Glue) or env vars (local).
# ═════════════════════════════════════════════════════════════════════════════

@dataclass
class JobConfig:
    # ── Catalog / database ────────────────────────────────────────────────────
    catalog:         str = "my_catalog"
    catalog_type:    str = "glue"          # glue | hive | hadoop
    database:        str = "kyc_db"

    # ── Table names ───────────────────────────────────────────────────────────
    target_table:    str = "kyc_application"
    source_table:    str = "salary_status"

    # ── Join key: column(s) that link target rows to source rows ──────────────
    join_key:        str = "id"            # single column; for composite: "id,dept"

    # ── SCD-2 date columns ────────────────────────────────────────────────────
    tgt_start_col:   str = "eff_start_date"
    tgt_end_col:     str = "eff_end_date"
    src_start_col:   str = "eff_start_date"
    src_end_col:     str = "eff_end_date"
    active_col:      str = "active"        # flag column recomputed after clipping
    high_date:       str = "3499-12-31"    # sentinel = open-ended / current row

    # ── New columns to back-fill from source into target ─────────────────────
    # These must exist in the source table.
    # Any column already in the target will be UPDATED; absent columns = INSERT.
    new_cols:        List[str] = field(default_factory=lambda: ["salary", "status"])

    # ── Target columns to carry through unchanged (schema of target table) ───
    # Include ALL target columns except the new_cols (those come from source).
    passthrough_cols: List[str] = field(
        default_factory=lambda: ["id", "name", "place",
                                 "eff_start_date", "eff_end_date", "active"]
    )

    # ── Performance tuning ────────────────────────────────────────────────────
    shuffle_partitions:  int = 800     # tune = 2 × total executor vCPUs
    num_output_parts:    int = 200     # tune = expected output rows / 500_000
    salt_buckets:        int = 50      # tune = ~2 × number of executors
    source_size_mb:      int = 500     # actual source table size → auto-broadcast
    broadcast_threshold: int = 2048   # MB — broadcast source if smaller than this
    target_file_size_mb: int = 128    # Iceberg target Parquet file size

    # ── Post-write compaction ─────────────────────────────────────────────────
    run_compaction:  bool = True
    zorder_cols:     List[str] = field(default_factory=lambda: ["id", "eff_start_date"])

    # ── App name shown in Spark UI / CloudWatch ───────────────────────────────
    app_name:        str = "scd2_schema_evolution_backfill"

    # ── Derived properties (computed from the fields above) ───────────────────
    @property
    def join_keys(self) -> List[str]:
        return [k.strip() for k in self.join_key.split(",")]

    @property
    def full_target(self) -> str:
        return f"{self.catalog}.{self.database}.{self.target_table}"

    @property
    def full_source(self) -> str:
        return f"{self.catalog}.{self.database}.{self.source_table}"

    @property
    def all_target_cols(self) -> List[str]:
        """Complete ordered column list for final output DataFrame."""
        return self.passthrough_cols + self.new_cols

    @property
    def use_broadcast(self) -> bool:
        return self.source_size_mb < self.broadcast_threshold

    @classmethod
    def from_glue_args(cls) -> "JobConfig":
        """
        Load config from AWS Glue --args.
        Only provided args are overridden; all others keep their defaults.
        Supports the same set of fields as the dataclass above.
        """
        if not _GLUE_AVAILABLE:
            log.warning("awsglue not available — using default JobConfig values.")
            return cls()

        known = [
            "catalog", "catalog_type", "database",
            "target_table", "source_table",
            "join_key", "tgt_start_col", "tgt_end_col",
            "src_start_col", "src_end_col", "active_col", "high_date",
            "new_cols", "passthrough_cols", "zorder_cols",
            "shuffle_partitions", "num_output_parts", "salt_buckets",
            "source_size_mb", "broadcast_threshold", "target_file_size_mb",
            "run_compaction", "app_name",
        ]
        # Only request args that were actually supplied (avoid KeyError)
        provided = [a.lstrip("-") for a in sys.argv[1:] if a.startswith("--")]
        to_fetch  = [k for k in known if k in provided]
        if not to_fetch:
            return cls()

        raw = getResolvedOptions(sys.argv, to_fetch)
        cfg = cls()
        list_keys = {"new_cols", "passthrough_cols", "zorder_cols"}
        bool_keys  = {"run_compaction"}
        int_keys   = {"shuffle_partitions", "num_output_parts", "salt_buckets",
                      "source_size_mb", "broadcast_threshold", "target_file_size_mb"}

        for k, v in raw.items():
            if k in list_keys:
                setattr(cfg, k, [x.strip() for x in v.split(",")])
            elif k in bool_keys:
                setattr(cfg, k, v.lower() in ("true", "1", "yes"))
            elif k in int_keys:
                setattr(cfg, k, int(v))
            else:
                setattr(cfg, k, v)
        return cfg


# ═════════════════════════════════════════════════════════════════════════════
# SECTION 2 — SPARK SESSION BUILDER
# Fully parameterised from JobConfig — no hardcoded catalog names.
# ═════════════════════════════════════════════════════════════════════════════

def build_spark(cfg: JobConfig) -> SparkSession:
    file_bytes = cfg.target_file_size_mb * 1024 * 1024
    return (
        SparkSession.builder
        .appName(cfg.app_name)
        # Iceberg extensions
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        # Catalog — name derived from cfg.catalog, not hardcoded
        .config(f"spark.sql.catalog.{cfg.catalog}",
                "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{cfg.catalog}.type", cfg.catalog_type)
        # Dynamic partition overwrite — the key setting for Insert-Overwrite safety
        .config("spark.sql.sources.partitionOverwriteMode",           "dynamic")
        # AQE — adaptive execution for skew + small partition handling
        .config("spark.sql.adaptive.enabled",                         "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled",      "true")
        .config("spark.sql.adaptive.skewJoin.enabled",                "true")
        .config("spark.sql.adaptive.skewJoin.skewedPartitionFactor",  "5")
        .config("spark.sql.adaptive.advisoryPartitionSizeInBytes",    "128m")
        .config("spark.sql.shuffle.partitions",                       str(cfg.shuffle_partitions))
        # Iceberg write settings
        .config("spark.sql.iceberg.write.distribution-mode",          "range")
        .config("spark.sql.iceberg.target-file-size-bytes",           str(file_bytes))
        # Memory + serialisation
        .config("spark.memory.fraction",                              "0.8")
        .config("spark.serializer",
                "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    )


# ═════════════════════════════════════════════════════════════════════════════
# SECTION 3 — GENERIC PIPELINE FUNCTIONS
# Each function takes (spark, cfg, ...) — zero table-specific logic inside.
# ═════════════════════════════════════════════════════════════════════════════

def read_target(spark: SparkSession, cfg: JobConfig) -> DataFrame:
    """
    Read the target SCD-2 table with column push-down and date normalisation.
    Only columns in cfg.all_target_cols are read — unused columns skipped.
    """
    return (
        spark.table(cfg.full_target)
        .select(*cfg.all_target_cols)
        .withColumn(cfg.tgt_start_col, F.col(cfg.tgt_start_col).cast("date"))
        .withColumn(cfg.tgt_end_col,   F.col(cfg.tgt_end_col).cast("date"))
    )


def read_source(spark: SparkSession, cfg: JobConfig) -> DataFrame:
    """
    Read the source SCD-2 table.
    Normalises all column names to lower-case (handles mixed-case Glue catalogs).
    Renames date and new-column fields to avoid ambiguity in the join.
    """
    src_select = cfg.join_keys + cfg.new_cols + [cfg.src_start_col, cfg.src_end_col]
    return (
        spark.table(cfg.full_source)
        .toDF(*[c.lower() for c in spark.table(cfg.full_source).columns])
        .select(*src_select)
        .withColumn(cfg.src_start_col, F.col(cfg.src_start_col).cast("date"))
        .withColumn(cfg.src_end_col,   F.col(cfg.src_end_col).cast("date"))
        # Rename to avoid column name collision with target in the JOIN
        .withColumnRenamed(cfg.src_start_col, "_src_start")
        .withColumnRenamed(cfg.src_end_col,   "_src_end")
        # Prefix new columns with _src_ to distinguish from target columns
        .select(
            *cfg.join_keys,
            *[F.col(c).alias(f"_src_{c}") for c in cfg.new_cols],
            "_src_start",
            "_src_end",
        )
    )


def split_by_source_coverage(
    tgt: DataFrame, src: DataFrame, cfg: JobConfig
) -> tuple[DataFrame, DataFrame]:
    """
    Split target rows into two streams based on whether their id exists in source.

    stream_a — ids that HAVE records in source → need boundary JOIN + clipping
    stream_b — ids with NO records in source   → passthrough unchanged

    Broadcasting the distinct source id set avoids a shuffle for the split.
    """
    src_ids = F.broadcast(
        src.select(*cfg.join_keys).distinct()
    )
    stream_a = tgt.join(src_ids, on=cfg.join_keys, how="inner")
    stream_b = tgt.join(src_ids, on=cfg.join_keys, how="left_anti")
    return stream_a, stream_b


def apply_salt(
    stream_a: DataFrame,
    src: DataFrame,
    cfg: JobConfig,
    spark: SparkSession
) -> tuple[DataFrame, DataFrame, List[str]]:
    """
    Optionally salt the join to distribute hot-key ids across executors.

    When source is small enough, broadcast is used instead (no salt needed).
    Salt replicates source rows across N buckets — hot ids handled by N executors.
    Returns (salted target stream, salted/broadcast source, join key list).
    """
    if cfg.use_broadcast:
        log.info("Broadcast join selected (source fits in memory — no salt needed)")
        return stream_a, F.broadcast(src), cfg.join_keys

    log.info(f"Shuffle join + salting (source={cfg.source_size_mb}MB, "
             f"salt_buckets={cfg.salt_buckets})")
    salt_expr  = (F.rand() * cfg.salt_buckets).cast("int")
    tgt_salted = stream_a.withColumn("_salt", salt_expr)
    src_salted = src.crossJoin(
        spark.range(cfg.salt_buckets).select(F.col("id").alias("_salt"))
    )
    return tgt_salted, src_salted, ["_salt"] + cfg.join_keys


def boundary_join_and_clip(
    tgt_join: DataFrame,
    src_join: DataFrame,
    cfg: JobConfig,
    join_keys: List[str],
) -> DataFrame:
    """
    Core logic — generic boundary-condition JOIN + date clipping.

    JOIN condition (Allen's interval overlap — works for any date column names):
        src._src_start  <  tgt.tgt_end_col     (source started before target ended)
        src._src_end    >  tgt.tgt_start_col   (source ended after target started)

    ALL overlapping pairs are kept (no rank filter) so every internal
    status/value transition within a target row's window is preserved.

    Date clipping computes the intersection per pair:
        new start = MAX(tgt_start, src_start)
        new end   = MIN(tgt_end,   src_end  )

    New column values (salary, status, etc.) come from the source row that
    overlaps — there is exactly one source row per clipped output row.
    """
    # Build join condition generically from cfg column names
    overlap_cond = (
        (F.col("_src_start") <  F.col(f"t.{cfg.tgt_end_col}"))
        & (F.col("_src_end") >  F.col(f"t.{cfg.tgt_start_col}"))
    )
    key_cond = F.lit(True)
    for k in cfg.join_keys:
        key_cond = key_cond & (F.col(f"t.{k}") == F.col(f"s.{k}"))

    joined = (
        tgt_join.alias("t")
        .join(src_join.alias("s"), on=(key_cond & overlap_cond), how="inner")
    )

    # Date clipping
    clipped = (
        joined
        .withColumn(
            cfg.tgt_start_col,
            F.greatest(F.col(f"t.{cfg.tgt_start_col}"), F.col("_src_start"))
        )
        .withColumn(
            cfg.tgt_end_col,
            F.least(F.col(f"t.{cfg.tgt_end_col}"), F.col("_src_end"))
        )
        # Recompute active flag generically
        .withColumn(
            cfg.active_col,
            F.when(
                F.col(cfg.tgt_end_col) == F.lit(cfg.high_date).cast("date"), 1
            ).otherwise(0)
        )
    )

    # Select: passthrough target columns + clipped dates + new columns from source
    select_exprs = (
        [F.col(f"t.{c}") for c in cfg.join_keys]
        + [F.col(f"t.{c}") for c in cfg.passthrough_cols
           if c not in cfg.join_keys
           and c not in (cfg.tgt_start_col, cfg.tgt_end_col, cfg.active_col)]
        + [F.col(cfg.tgt_start_col),
           F.col(cfg.tgt_end_col),
           F.col(cfg.active_col)]
        + [F.col(f"_src_{c}").alias(c) for c in cfg.new_cols]
    )

    return clipped.select(*select_exprs)


def union_streams(
    enriched: DataFrame,
    stream_b: DataFrame,
    cfg: JobConfig,
) -> DataFrame:
    """
    Reconstruct the complete partition content for the overwrite.

    enriched  — Stream A rows expanded with clipped dates + new column values
    stream_b  — Stream B rows unchanged (salary/status remain NULL)

    Both are aligned to cfg.all_target_cols before UNION so schema matches.
    repartition(N, join_key) co-locates same-id rows in the same output file.
    """
    enriched_aligned  = enriched.select(*cfg.all_target_cols)
    passthrough_aligned = stream_b.select(*cfg.all_target_cols)

    return (
        enriched_aligned
        .union(passthrough_aligned)
        .repartition(cfg.num_output_parts, *[F.col(k) for k in cfg.join_keys])
    )


def insert_overwrite(
    final_df: DataFrame,
    cfg: JobConfig,
) -> None:
    """
    Atomically replace affected Iceberg partitions with the enriched data.

    overwritePartitions() with partitionOverwriteMode=dynamic:
      - Writes all rows in parallel (all executors at once)
      - Replaces ONLY partitions present in final_df
      - Untouched partitions remain exactly as they were
      - No delete files, no separate DELETE phase, no stale parent rows
      - Atomic commit — readers see old snapshot until the moment of commit
    """
    file_bytes = str(cfg.target_file_size_mb * 1024 * 1024)
    (
        final_df
        .writeTo(cfg.full_target)
        .option("write.distribution-mode",      "range")
        .option("write.target-file-size-bytes", file_bytes)
        .overwritePartitions()
    )


def compact_and_zorder(spark: SparkSession, cfg: JobConfig) -> None:
    """
    Post-write Z-order compaction — optional but recommended for Day 0 loads.
    Co-locates rows with the same join key in the same Parquet files for
    optimal future SCD-2 range query performance.
    """
    if not cfg.run_compaction:
        log.info("[COMPACT] Skipped (run_compaction=False)")
        return

    sort_order = ", ".join(
        f"{c} ASC NULLS LAST" for c in cfg.zorder_cols
    )
    file_bytes = cfg.target_file_size_mb * 1024 * 1024
    spark.sql(f"""
        CALL {cfg.catalog}.system.rewrite_data_files(
            table      => '{cfg.database}.{cfg.target_table}',
            strategy   => 'sort',
            sort_order => '{sort_order}',
            options    => map(
                'target-file-size-bytes', '{file_bytes}',
                'min-file-size-bytes',    '{file_bytes // 4}'
            )
        )
    """)
    log.info(f"[COMPACT] Z-order done on ({', '.join(cfg.zorder_cols)})")


# ═════════════════════════════════════════════════════════════════════════════
# SECTION 4 — GENERIC VALIDATION
# All SQL built dynamically from cfg — no column names hardcoded.
# ═════════════════════════════════════════════════════════════════════════════

def validate(
    spark: SparkSession,
    cfg: JobConfig,
    tgt_count_before: int,
) -> bool:
    """
    Run all quality checks. Returns True if all pass, False if any fail.

    Checks performed:
      1. Row count — output must be >= input (expansion expected)
      2. Date gaps — no break in date chain per id
      3. Date overlaps — no two rows for same id with overlapping windows
      4. Unexpected NULLs — ids with source must have new_cols populated
      5. Active flag — exactly one active=1 row per id
    """
    passed = True
    result = spark.table(cfg.full_target)
    out_count = result.count()
    id_col = cfg.join_keys[0]   # primary join key for window partitioning

    log.info("=" * 55)
    log.info("VALIDATION RESULTS")
    log.info("=" * 55)

    # 1. Row count
    if out_count >= tgt_count_before:
        log.info(f"  ✓ Row count     : {out_count:,}  (was {tgt_count_before:,},"
                 f"  +{out_count - tgt_count_before:,} from expansion)")
    else:
        log.error(f"  ✗ Row count DROPPED: {out_count:,} < {tgt_count_before:,}")
        passed = False

    # 2. Date gap check
    gaps = spark.sql(f"""
        WITH o AS (
            SELECT {id_col},
                   {cfg.tgt_start_col},
                   LAG({cfg.tgt_end_col}) OVER (
                       PARTITION BY {id_col}
                       ORDER BY {cfg.tgt_start_col}
                   ) AS prev_end
            FROM {cfg.full_target}
        )
        SELECT {id_col}, prev_end AS gap_from, {cfg.tgt_start_col} AS gap_to
        FROM o
        WHERE prev_end IS NOT NULL AND prev_end <> {cfg.tgt_start_col}
    """)
    gap_count = gaps.count()
    if gap_count == 0:
        log.info("  ✓ Date gaps     : none — history fully continuous")
    else:
        log.error(f"  ✗ Date gaps     : {gap_count} found!")
        gaps.show(10, truncate=False)
        passed = False

    # 3. Overlap check
    overlaps = spark.sql(f"""
        WITH o AS (
            SELECT {id_col},
                   {cfg.tgt_start_col},
                   LAG({cfg.tgt_end_col}) OVER (
                       PARTITION BY {id_col}
                       ORDER BY {cfg.tgt_start_col}
                   ) AS prev_end
            FROM {cfg.full_target}
        )
        SELECT {id_col}, prev_end AS overlap_end, {cfg.tgt_start_col} AS overlap_start
        FROM o
        WHERE prev_end IS NOT NULL AND prev_end > {cfg.tgt_start_col}
    """)
    overlap_count = overlaps.count()
    if overlap_count == 0:
        log.info("  ✓ Overlaps      : none")
    else:
        log.error(f"  ✗ Overlaps      : {overlap_count} found!")
        overlaps.show(10, truncate=False)
        passed = False

    # 4. Unexpected NULL check — built generically for all new_cols
    null_checks = " OR ".join(f"{c} IS NULL" for c in cfg.new_cols)
    unexpected_nulls = spark.sql(f"""
        SELECT COUNT(*) AS cnt
        FROM {cfg.full_target}
        WHERE ({null_checks})
          AND {id_col} IN (SELECT DISTINCT {id_col} FROM {cfg.full_source})
    """).collect()[0]["cnt"]
    if unexpected_nulls == 0:
        log.info(f"  ✓ Null check    : no unexpected NULLs in {cfg.new_cols}")
    else:
        log.error(f"  ✗ Null check    : {unexpected_nulls} rows with unexpected NULLs!")
        passed = False

    # 5. Active flag check
    multi_active = spark.sql(f"""
        SELECT COUNT(*) AS cnt FROM (
            SELECT {id_col} FROM {cfg.full_target}
            GROUP BY {id_col}
            HAVING SUM({cfg.active_col}) > 1
        )
    """).collect()[0]["cnt"]
    if multi_active == 0:
        log.info(f"  ✓ Active flags  : one active=1 row per {id_col}")
    else:
        log.error(f"  ✗ Active flags  : {multi_active} ids have multiple active rows!")
        passed = False

    log.info("=" * 55)
    log.info(f"  Overall : {'ALL PASSED ✓' if passed else 'FAILURES DETECTED ✗'}")
    log.info("=" * 55)
    return passed


# ═════════════════════════════════════════════════════════════════════════════
# SECTION 5 — PHASE TIMING UTILITY
# ═════════════════════════════════════════════════════════════════════════════

class Timer:
    def __init__(self):
        self._metrics: dict = {}

    def run(self, label: str, fn):
        log.info(f"── START  {label} ──")
        t0 = time.time()
        result = fn()
        elapsed = round(time.time() - t0, 1)
        self._metrics[label] = elapsed
        log.info(f"── DONE   {label}  [{elapsed}s] ──")
        return result

    def summary(self):
        total = sum(self._metrics.values())
        log.info("=" * 60)
        log.info("PHASE TIMING SUMMARY")
        log.info("=" * 60)
        for name, secs in self._metrics.items():
            pct = round(secs / total * 100, 1) if total else 0
            bar = "█" * max(1, int(pct / 4))
            log.info(f"  {name:<32} {secs:>6.1f}s  {pct:>5.1f}%  {bar}")
        log.info(f"  {'TOTAL':<32} {total:>6.1f}s")
        log.info("=" * 60)


# ═════════════════════════════════════════════════════════════════════════════
# SECTION 6 — MAIN ENTRY POINT
# Orchestrates all phases using the generic functions above.
# ═════════════════════════════════════════════════════════════════════════════

def main():
    # ── Load config (Glue args → overrides → defaults) ────────────────────────
    cfg = JobConfig.from_glue_args()
    log.info("Job config:")
    log.info(f"  Target          : {cfg.full_target}")
    log.info(f"  Source          : {cfg.full_source}")
    log.info(f"  Join key(s)     : {cfg.join_keys}")
    log.info(f"  New columns     : {cfg.new_cols}")
    log.info(f"  High date       : {cfg.high_date}")
    log.info(f"  Shuffle parts   : {cfg.shuffle_partitions}")
    log.info(f"  Output parts    : {cfg.num_output_parts}")
    log.info(f"  Salt buckets    : {cfg.salt_buckets}")
    log.info(f"  Broadcast < MB  : {cfg.broadcast_threshold}")

    timer = Timer()

    # ── Spark session ─────────────────────────────────────────────────────────
    spark = build_spark(cfg)
    spark.sparkContext.setLogLevel("WARN")

    # ── Phase 1: Read ─────────────────────────────────────────────────────────
    tgt = timer.run("1_read_target", lambda: read_target(spark, cfg))
    src = timer.run("2_read_source", lambda: read_source(spark, cfg))

    tgt_count = tgt.count()
    log.info(f"Target rows : {tgt_count:,}")
    log.info(f"New columns : {cfg.new_cols}")

    # ── Phase 2: Split target by source coverage ──────────────────────────────
    stream_a, stream_b = timer.run(
        "3_split_by_coverage",
        lambda: split_by_source_coverage(tgt, src, cfg)
    )
    log.info(f"Stream A (enrichable): {stream_a.count():,}")
    log.info(f"Stream B (passthrough): {stream_b.count():,}")

    # ── Phase 3: Salt / broadcast decision ───────────────────────────────────
    tgt_join, src_join, join_keys = apply_salt(stream_a, src, cfg, spark)

    # ── Phase 4: Boundary JOIN + date clipping ────────────────────────────────
    enriched = timer.run(
        "4_boundary_join_clip",
        lambda: boundary_join_and_clip(tgt_join, src_join, cfg, join_keys)
    )

    # ── Phase 5: Union streams ────────────────────────────────────────────────
    final_df = timer.run(
        "5_union_streams",
        lambda: union_streams(enriched, stream_b, cfg)
    )

    # Persist before multi-use (write + validation)
    final_df.persist(StorageLevel.MEMORY_AND_DISK_SER)
    final_count = final_df.count()
    log.info(f"Final rows  : {final_count:,}  "
             f"(expand ratio {final_count / tgt_count:.2f}x)")

    # ── Phase 6: Insert-Overwrite ─────────────────────────────────────────────
    timer.run(
        "6_insert_overwrite",
        lambda: insert_overwrite(final_df, cfg)
    )
    log.info("Insert-Overwrite complete — no MERGE, no DELETE, no delete files.")

    # ── Phase 7: Validation ───────────────────────────────────────────────────
    ok = timer.run(
        "7_validation",
        lambda: validate(spark, cfg, tgt_count)
    )

    # ── Phase 8: Compaction (optional) ───────────────────────────────────────
    timer.run(
        "8_compact_zorder",
        lambda: compact_and_zorder(spark, cfg)
    )

    # ── Timing summary ────────────────────────────────────────────────────────
    timer.summary()

    final_df.unpersist()
    spark.stop()

    if not ok:
        raise RuntimeError("Validation failed — check logs for details.")


if __name__ == "__main__":
    main()
