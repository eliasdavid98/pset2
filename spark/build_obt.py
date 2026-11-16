import os
import re
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


SNOWFLAKE_SOURCE = "net.snowflake.spark.snowflake"
FACT_TABLE = "FACT_CUSTOMER_CHURN"
DIMENSION_TABLES = {
    "geography": ("DIM_GEOGRAPHY", "geography_key"),
    "gender": ("DIM_GENDER", "gender_key"),
    "age_band": ("DIM_AGE_BAND", "age_band_key"),
    "product_count": ("DIM_PRODUCT_COUNT", "product_count_key"),
}


def required_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise ValueError(f"Required environment variable {name} is not set.")
    return value


def snowflake_options() -> dict[str, str]:
    jdbc_url = required_env("SNOWFLAKE_URL")
    prefix = "jdbc:snowflake://"
    if not jdbc_url.startswith(prefix):
        raise ValueError("SNOWFLAKE_URL must start with 'jdbc:snowflake://'.")
    url = jdbc_url[len(prefix):].split("?", maxsplit=1)[0].rstrip("/")
    if not url:
        raise ValueError("SNOWFLAKE_URL must include a Snowflake account host.")

    options = {
        "sfURL": url,
        "sfUser": required_env("SNOWFLAKE_USERNAME"),
        "sfPassword": required_env("SNOWFLAKE_PASSWORD"),
        "sfDatabase": required_env("SNOWFLAKE_DATABASE"),
        "sfSchema": os.environ.get("SNOWFLAKE_GOLD_SCHEMA", "GOLD").strip() or "GOLD",
        "sfWarehouse": required_env("SNOWFLAKE_WAREHOUSE"),
    }
    role = os.environ.get("SNOWFLAKE_ROLE", "").strip()
    if role:
        options["sfRole"] = role
    return options


def load_table(spark: SparkSession, options: dict[str, str], table: str) -> DataFrame:
    frame = (
        spark.read.format(SNOWFLAKE_SOURCE)
        .options(**options)
        .option("dbtable", table)
        .load()
    )
    return frame.toDF(*[column.lower() for column in frame.columns])


def require_columns(frame: DataFrame, expected: set[str], label: str) -> None:
    missing = expected.difference(frame.columns)
    if missing:
        raise ValueError(f"{label} is missing required columns: {', '.join(sorted(missing))}")


def require_unique_key(frame: DataFrame, key: str, label: str) -> None:
    if frame.filter(F.col(key).isNull()).limit(1).count():
        raise ValueError(f"{label}.{key} contains NULL values.")
    if frame.groupBy(key).count().filter(F.col("count") > 1).limit(1).count():
        raise ValueError(f"{label}.{key} is not unique.")


def split_counts(frame: DataFrame) -> dict[str, int]:
    return {row["source_split"]: row["count"] for row in
            frame.groupBy("source_split").count().collect()}


def build_obt(fact: DataFrame, dimensions: dict[str, DataFrame]) -> DataFrame:
    result = fact.alias("fact")
    joins = {
        "geography": ("dim_geography", "geography_key"),
        "gender": ("dim_gender", "gender_key"),
        "age_band": ("dim_age_band", "age_band_key"),
        "product_count": ("dim_product_count", "product_count_key"),
    }
    for name, (dimension_alias, key) in joins.items():
        result = result.join(
            dimensions[name].alias(dimension_alias),
            F.col(f"fact.{key}") == F.col(f"{dimension_alias}.{key}"),
            "left",
        )

    return result.select(
        F.col("fact.observation_id"),
        F.col("fact.source_split"),
        F.col("fact.customer_id"),
        F.col("fact.customer_id_raw"),
        F.col("fact.surname"),
        F.col("dim_geography.geography_name").alias("geography"),
        F.col("dim_gender.gender_name").alias("gender"),
        F.col("dim_age_band.age_band_key").alias("age_band"),
        F.col("dim_age_band.min_age").alias("age_band_min_age"),
        F.col("dim_age_band.max_age").alias("age_band_max_age"),
        F.col("dim_product_count.number_of_products"),
        F.col("fact.credit_score"),
        F.col("fact.age"),
        F.col("fact.tenure"),
        F.col("fact.balance"),
        F.col("fact.has_credit_card"),
        F.col("fact.is_active_member"),
        F.col("fact.estimated_salary"),
        F.col("fact.exited"),
    )


def validate_obt(
    fact: DataFrame,
    obt: DataFrame,
    dimensions: dict[str, DataFrame],
) -> int:
    fact_count = fact.count()
    if fact_count == 0:
        raise ValueError("Gold fact table is empty; refusing to write an empty OBT.")
    require_unique_key(fact, "observation_id", FACT_TABLE)
    require_columns(
        obt,
        {
            "observation_id", "source_split", "customer_id", "customer_id_raw",
            "surname", "geography", "gender", "age_band", "age_band_min_age",
            "age_band_max_age", "number_of_products", "credit_score", "age",
            "tenure", "balance", "has_credit_card", "is_active_member",
            "estimated_salary", "exited",
        },
        "OBT",
    )

    for name, frame in dimensions.items():
        key = DIMENSION_TABLES[name][1]
        require_unique_key(frame, key, DIMENSION_TABLES[name][0])

    output_count = obt.count()
    if output_count != fact_count:
        raise ValueError(
            f"OBT row count changed during joins: fact={fact_count}, OBT={output_count}."
        )
    require_unique_key(obt, "observation_id", "OBT")

    expected_splits = split_counts(fact)
    actual_splits = split_counts(obt)
    if expected_splits != actual_splits:
        raise ValueError(
            f"Train/test row counts changed during joins: "
            f"fact={expected_splits}, OBT={actual_splits}."
        )

    unmatched = {
        name: obt.filter(F.col(column).isNull()).count()
        for name, column in (
            ("geography", "geography"),
            ("gender", "gender"),
            ("age_band", "age_band"),
            ("product_count", "number_of_products"),
        )
    }
    unmatched = {name: count for name, count in unmatched.items() if count}
    if unmatched:
        raise ValueError(f"OBT has unmatched dimension rows: {unmatched}.")

    invalid_train_labels = fact.filter(
        (F.col("source_split") == "train")
        & (F.col("exited").isNull() | ~F.col("exited").isin(0, 1))
    ).limit(1).count()
    test_labels = fact.filter(
        (F.col("source_split") == "test") & F.col("exited").isNotNull()
    ).limit(1).count()
    invalid_splits = fact.filter(
        F.col("source_split").isNull() | ~F.col("source_split").isin("train", "test")
    ).limit(1).count()
    if invalid_train_labels or test_labels or invalid_splits:
        raise ValueError(
            "Gold fact has invalid label/split values: "
            f"invalid train labels={invalid_train_labels}, "
            f"test labels present={test_labels}, invalid splits={invalid_splits}."
        )
    return fact_count


def main() -> None:
    options = snowflake_options()
    schema = os.environ.get("SNOWFLAKE_GOLD_SCHEMA", "GOLD").strip() or "GOLD"
    table = os.environ.get("SNOWFLAKE_OBT_TABLE", "OBT_CUSTOMER_CHURN").strip()
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_$]*", schema):
        raise ValueError("SNOWFLAKE_GOLD_SCHEMA must be an unquoted Snowflake identifier.")
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_$]*", table):
        raise ValueError("SNOWFLAKE_OBT_TABLE must be an unquoted Snowflake identifier.")

    spark = (
        SparkSession.builder
        .appName("pset2-build-customer-churn-obt")
        .getOrCreate()
    )
    try:
        fact = load_table(spark, options, FACT_TABLE).cache()
        require_columns(
            fact,
            {
                "observation_id", "source_split", "customer_id", "customer_id_raw",
                "surname", "geography_key", "gender_key", "age_band_key",
                "product_count_key", "credit_score", "age", "tenure", "balance",
                "has_credit_card", "is_active_member", "estimated_salary", "exited",
            },
            FACT_TABLE,
        )
        dimensions = {
            name: load_table(spark, options, table_name).cache()
            for name, (table_name, _) in DIMENSION_TABLES.items()
        }
        for name, frame in dimensions.items():
            required = {DIMENSION_TABLES[name][1]}
            if name == "geography":
                required.add("geography_name")
            elif name == "gender":
                required.add("gender_name")
            elif name == "age_band":
                required.update({"min_age", "max_age"})
            elif name == "product_count":
                required.add("number_of_products")
            require_columns(frame, required, DIMENSION_TABLES[name][0])

        obt = build_obt(fact, dimensions).cache()
        row_count = validate_obt(fact, obt, dimensions)
        split_summary = split_counts(obt)

        (
            obt.write.format(SNOWFLAKE_SOURCE)
            .options(**{**options, "sfSchema": schema})
            .option("dbtable", table)
            .mode("overwrite")
            .save()
        )
        print(
            f"Successfully wrote {row_count} OBT rows to "
            f"{options['sfDatabase']}.{schema}.{table}; split counts={split_summary}."
        )
        obt.unpersist()
        fact.unpersist()
        for frame in dimensions.values():
            frame.unpersist()
    finally:
        spark.stop()


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"Spark OBT job failed: {error}", file=sys.stderr)
        raise
