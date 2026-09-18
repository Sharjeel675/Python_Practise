"""
╔══════════════════════════════════════════════════════════════════════╗
║         PySpark ETL Pipeline — SQL Server  →  PostgreSQL             ║
║         AUTO MODE — Koi hardcode nahi, sab automatic                 ║
║                                                                      ║
║  ✅ SQL Server ki SAARI tables apne aap detect hoti hain             ║
║  ✅ PostgreSQL mein tables apne aap ban jaati hain                   ║
║  ✅ Sab data TEXT/STRING mein jata hai — koi type error nahi         ║
║  ✅ Sirf credentials edit karo — baaki sab automatic                 ║
╚══════════════════════════════════════════════════════════════════════╝
  RUN:  python spark.py
"""

import os
import logging
import time
import pyodbc
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# ══════════════════════════════════════════════════════════════════════
#  ✏️  SIRF YE SECTION EDIT KARO
# ══════════════════════════════════════════════════════════════════════

# ── SQL Server (source) ───────────────────────────────────────────────
MSSQL_HOST     = "localhost"
MSSQL_PORT     = 1433
MSSQL_DATABASE = "Sales_Data"
MSSQL_USERNAME = "sa"
MSSQL_PASSWORD = "Admin@1234"

# ── PostgreSQL (destination) ──────────────────────────────────────────
PG_HOST     = "localhost"
PG_PORT     = 5432
PG_DATABASE = "postgres"
PG_USERNAME = "postgres"
PG_PASSWORD = "postgres"

# ── JDBC JAR files ────────────────────────────────────────────────────
MSSQL_JAR    = r"C:\Users\Noman Traders\Desktop\Practise Code\jars\mssql-jdbc-12.4.2.jre11.jar"
POSTGRES_JAR = r"C:\Users\Noman Traders\Desktop\Practise Code\jars\postgresql-42.7.3.jar"

# ── Schema ────────────────────────────────────────────────────────────
SOURCE_SCHEMA = "dbo"      # SQL Server schema
TARGET_SCHEMA = "public"   # PostgreSQL schema

# ── Spark memory ──────────────────────────────────────────────────────
SPARK_DRIVER_MEMORY   = "4g"
SPARK_EXECUTOR_MEMORY = "4g"

# ══════════════════════════════════════════════════════════════════════
#  YE NEECHE MAT CHHEDO
# ══════════════════════════════════════════════════════════════════════

os.environ["HADOOP_HOME"]           = r"C:\hadoop"
os.environ["PYSPARK_PYTHON"]        = "python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "python"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [%(levelname)s]  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("ETL")


# ── Connection helpers ────────────────────────────────────────────────

def mssql_jdbc_url() -> str:
    return (
        f"jdbc:sqlserver://{MSSQL_HOST}:{MSSQL_PORT};"
        f"databaseName={MSSQL_DATABASE};"
        f"encrypt=false;"
        f"trustServerCertificate=true;"
        f"integratedSecurity=false;"
    )

def pg_jdbc_url() -> str:
    return f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DATABASE}"


# ══════════════════════════════════════════════════════════════════════
#  AUTO DISCOVERY — pyodbc se SQL Server ki saari tables lo
# ══════════════════════════════════════════════════════════════════════

def get_all_tables() -> list:
    """
    pyodbc ke zariye SQL Server se connect karo aur
    SOURCE_SCHEMA ki saari user tables ki list lo.
    Returns: ['dbo.orders', 'dbo.products', ...]
    """
    log.info("🔍 SQL Server se tables detect ho rahi hain ...")

    conn_str = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={MSSQL_HOST},{MSSQL_PORT};"
        f"DATABASE={MSSQL_DATABASE};"
        f"UID={MSSQL_USERNAME};"
        f"PWD={MSSQL_PASSWORD};"
        f"Encrypt=no;"
        f"TrustServerCertificate=yes;"
    )

    with pyodbc.connect(conn_str) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_TYPE = 'BASE TABLE'
              AND TABLE_SCHEMA = ?
            ORDER BY TABLE_NAME
        """, SOURCE_SCHEMA)
        tables = [f"{SOURCE_SCHEMA}.{row[0]}" for row in cursor.fetchall()]

    log.info("   ✅ %d tables mili:", len(tables))
    for t in tables:
        log.info("      • %s  →  %s.%s", t, TARGET_SCHEMA, t.split(".")[-1])

    return tables


# ══════════════════════════════════════════════════════════════════════
#  STEP 1 — Spark Session
# ══════════════════════════════════════════════════════════════════════

def create_spark_session() -> SparkSession:
    log.info("🔥 Spark start ho raha hai ...")

    spark = (
        SparkSession.builder
        .appName("SQLServer-to-Postgres-ETL-AUTO")
        .master("local[*]")
        .config("spark.driver.memory",          SPARK_DRIVER_MEMORY)
        .config("spark.executor.memory",         SPARK_EXECUTOR_MEMORY)
        .config("spark.jars",                    f"{MSSQL_JAR},{POSTGRES_JAR}")
        .config("spark.sql.adaptive.enabled",    "true")
        .config("spark.sql.shuffle.partitions",  "8")
        .config("spark.local.dir",               r"C:\tmp\spark")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    log.info("✅ Spark %s ready!", spark.version)
    return spark


# ══════════════════════════════════════════════════════════════════════
#  STEP 2 — Extract
# ══════════════════════════════════════════════════════════════════════

def extract(spark: SparkSession, source_table: str) -> DataFrame:
    log.info("   📥 Read: %s", source_table)

    df = (
        spark.read
        .format("jdbc")
        .option("url",       mssql_jdbc_url())
        .option("dbtable",   source_table)
        .option("user",      MSSQL_USERNAME)
        .option("password",  MSSQL_PASSWORD)
        .option("driver",    "com.microsoft.sqlserver.jdbc.SQLServerDriver")
        .option("fetchsize", 1000)
        .load()
    )

    log.info("   ✅ %d rows, %d columns", df.count(), len(df.columns))
    return df


# ══════════════════════════════════════════════════════════════════════
#  STEP 3 — Transform — sab STRING mein
# ══════════════════════════════════════════════════════════════════════

def transform(df: DataFrame, pipeline_name: str) -> DataFrame:
    log.info("   🔄 Transform ...")

    # ① Sab columns STRING/TEXT mein cast karo
    for col_name in df.columns:
        df = df.withColumn(col_name, F.col(col_name).cast(StringType()))

    # ② Column names clean karo — lowercase + underscore
    for col_name in df.columns:
        clean = (
            col_name.strip()
            .lower()
            .replace(" ", "_")
            .replace("-", "_")
            .replace(".", "_")
            .replace("(", "")
            .replace(")", "")
        )
        if clean != col_name:
            df = df.withColumnRenamed(col_name, clean)

    # ③ Poori khali rows hatao
    df = df.dropna(how="all")

    # ④ Whitespace trim karo
    for col_name in df.columns:
        df = df.withColumn(col_name, F.trim(F.col(col_name)))

    # ⑤ Audit columns add karo
    df = (
        df
        .withColumn("etl_loaded_at",     F.lit(datetime.utcnow().isoformat()))
        .withColumn("etl_pipeline_name", F.lit(pipeline_name))
    )

    log.info("   ✅ %d rows, %d columns ready", df.count(), len(df.columns))
    return df


# ══════════════════════════════════════════════════════════════════════
#  STEP 4 — Load — table auto-create hogi
# ══════════════════════════════════════════════════════════════════════

def load(df: DataFrame, target_table: str) -> None:
    log.info("   📤 Load → '%s'", target_table)

    (
        df.write
        .format("jdbc")
        .option("url",                      pg_jdbc_url())
        .option("dbtable",                  target_table)
        .option("user",                     PG_USERNAME)
        .option("password",                 PG_PASSWORD)
        .option("driver",                   "org.postgresql.Driver")
        .option("batchsize",                1000)
        .option("stringtype",               "unspecified")
        .mode("overwrite")
        .save()
    )

    log.info("   ✅ '%s' load ho gaya", target_table)


# ══════════════════════════════════════════════════════════════════════
#  STEP 5 — Validate
# ══════════════════════════════════════════════════════════════════════

def validate(spark: SparkSession, target_table: str, expected: int) -> bool:
    actual = (
        spark.read
        .format("jdbc")
        .option("url",      pg_jdbc_url())
        .option("dbtable",  target_table)
        .option("user",     PG_USERNAME)
        .option("password", PG_PASSWORD)
        .option("driver",   "org.postgresql.Driver")
        .load()
        .count()
    )

    if actual >= expected:
        log.info("   ✅ Validation PASS — %d rows confirm", actual)
        return True
    else:
        log.warning("   ⚠️  Mismatch — expected %d, mila %d", expected, actual)
        return False


# ══════════════════════════════════════════════════════════════════════
#  ORCHESTRATOR
# ══════════════════════════════════════════════════════════════════════

def run_table(spark: SparkSession, source_table: str) -> bool:
    table_only   = source_table.split(".")[-1]
    target_table = f"{TARGET_SCHEMA}.{table_only}"

    t0 = time.time()
    log.info("─" * 60)
    log.info("  %s  →  %s", source_table, target_table)
    log.info("─" * 60)

    try:
        raw_df       = extract(spark, source_table)
        source_count = raw_df.count()
        clean_df     = transform(raw_df, f"{source_table}>{target_table}")
        load(clean_df, target_table)
        ok           = validate(spark, target_table, source_count)

        log.info("  %s  (%.1fs)", "✅ KAMYAB" if ok else "⚠️  CHECK", time.time() - t0)
        return ok

    except Exception as exc:
        log.error("  ❌ FAIL (%.1fs): %s", time.time() - t0, exc)
        import traceback; traceback.print_exc()
        return False


# ══════════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════════

def main() -> None:
    start = time.time()

    log.info("=" * 60)
    log.info("  ETL AUTO PIPELINE — %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    log.info("  Source : %s @ %s:%s", MSSQL_DATABASE, MSSQL_HOST, MSSQL_PORT)
    log.info("  Target : %s @ %s:%s", PG_DATABASE,    PG_HOST,    PG_PORT)
    log.info("=" * 60)

    # ── Step 0: Tables auto detect ────────────────────────────────────
    try:
        all_tables = get_all_tables()
    except Exception as e:
        log.error("❌ Tables detect nahi huin: %s", e)
        import traceback; traceback.print_exc()
        return

    if not all_tables:
        log.error("❌ Koi table nahi mili '%s' schema mein!", SOURCE_SCHEMA)
        return

    log.info("=" * 60)
    log.info("  Migrate hone wali %d tables:", len(all_tables))
    for t in all_tables:
        log.info("    %s  →  %s.%s", t, TARGET_SCHEMA, t.split(".")[-1])
    log.info("=" * 60)

    # ── Step 1: Spark start ───────────────────────────────────────────
    spark   = create_spark_session()
    results = {}

    try:
        for source_table in all_tables:
            success    = run_table(spark, source_table)
            table_only = source_table.split(".")[-1]
            results[f"{source_table} → {TARGET_SCHEMA}.{table_only}"] = (
                "✅ KAMYAB" if success else "❌ FAIL"
            )
    finally:
        spark.stop()

    # ── Final Summary ─────────────────────────────────────────────────
    log.info("=" * 60)
    log.info("  NATEEJA / SUMMARY")
    log.info("=" * 60)
    passed = sum(1 for s in results.values() if "KAMYAB" in s)
    failed = len(results) - passed
    for label, status in results.items():
        log.info("  %s  |  %s", status, label)
    log.info("─" * 60)
    log.info("  ✅ Kamyab : %d  |  ❌ Fail : %d  |  Total : %d",
             passed, failed, len(results))
    log.info("  ⏱  Total time : %.1f seconds", time.time() - start)
    log.info("=" * 60)


if __name__ == "__main__":
    main()