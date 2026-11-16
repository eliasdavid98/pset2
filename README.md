# PSet 2: Kestra ingestion and dbt modeling

This first pipeline loads the local churn competition CSV snapshot into Snowflake:

`data/*.csv` → Kestra → Snowflake internal stage → `RAW.TRAIN`, `RAW.TEST`, `RAW.SAMPLE_SUBMISSION` → dbt Bronze → Silver → Gold

The raw columns are strings so the Bronze/Silver dbt models can parse and validate values without losing the source representation.

## Configure and start Kestra

1. Copy `.env.example` to `.env`.
2. Set `POSTGRES_PASSWORD` and the Snowflake URL, username, database, warehouse, and schema in `.env`. The configured Snowflake database must already exist.
3. Start the local services:

   ```bash
   docker compose up -d
   ```

4. Open [http://localhost:8080](http://localhost:8080). In Kestra, add the `SNOWFLAKE_PASSWORD` secret for the `pset2` namespace.
5. Import `kestra/flows/ingest_churn.yml` in the Kestra UI, then execute `ingest_churn_csv`.

The Kestra container mounts the local `data/` directory read-only at `/app/data`. The flow checks that all three CSVs exist and that their headers match the expected source schema before uploading.

## Run and rerun

The current source is a local snapshot, so the flow has no time-based schedule: each run is started manually after the files are made available or refreshed. Snowflake's default `COPY INTO` load history prevents reloading the same staged files on a retry. To backfill a new snapshot, replace the local files and execute the flow again; review the Snowflake copy history first if the staged files have been replaced under the same names.

The Snowflake tasks retry transient errors up to three times. They fail the execution on CSV validation or load errors rather than silently accepting rejected rows.

## dbt: data quality and star schema

Step 3 cleans and audits the raw train/test tables with Bronze and Silver models. Step 4 builds Gold dimensions and `GOLD.FACT_CUSTOMER_CHURN`, whose grain is one row per source `id`. The measured quality evidence, cleaning decisions, star-schema diagram, and execution instructions are in [`docs/data_quality_and_dimensional_model.md`](docs/data_quality_and_dimensional_model.md).

After running the Kestra ingestion:

1. Copy `.env.example` to `.env` and set `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USERNAME`, `SNOWFLAKE_PASSWORD`, `SNOWFLAKE_DATABASE`, and `SNOWFLAKE_WAREHOUSE` as well as `SNOWFLAKE_URL`.
2. Install dbt using `dbt/requirements.txt` in Python 3.12, then export `.env` variables into the shell as shown in the modeling document.
3. Run `dbt debug --project-dir dbt --profiles-dir dbt`, followed by `dbt build --project-dir dbt --profiles-dir dbt`.

The dbt profile reads credentials only from environment variables. Do not commit `.env` or paste credentials into the profile or flow files.

## Spark: One Big Table

After `dbt build` succeeds, the Gold tables are ready. Set `SNOWFLAKE_GOLD_SCHEMA=GOLD` and `SNOWFLAKE_OBT_TABLE=OBT_CUSTOMER_CHURN` in `.env`, then run:

```bash
docker compose --profile spark run --rm spark
```

The Spark service reads the Gold fact and four dimensions, validates unique keys, left-joins descriptive dimension attributes, and checks that the OBT has the same row count, unique observation IDs, and train/test counts as the fact. It fails instead of writing if a dimension is unmatched or a join changes the fact grain. On success it replaces `GOLD.OBT_CUSTOMER_CHURN` with the validated snapshot. The Snowflake role must be able to read Gold and create/replace a table in that schema.

The star schema remains the analytics source for flexible joins and dimensional reporting; the OBT is a denormalized snapshot for downstream feature engineering and model training, where a single row per observation is more convenient. The OBT retains `source_split` and leaves `exited` NULL for test observations to prevent target leakage.
