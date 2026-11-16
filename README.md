# PSet 2: ingesta con Kestra y modelado con dbt

Este pipeline carga en Snowflake los archivos CSV locales de la competencia de churn:

`data/*.csv` → Kestra → stage interno de Snowflake → `RAW.TRAIN`, `RAW.TEST`, `RAW.SAMPLE_SUBMISSION` → dbt Bronze → Silver → Gold

Las columnas de Raw se almacenan como texto para que los modelos Bronze y Silver de dbt puedan convertirlas y validarlas sin perder la representación original de la fuente.

## Configurar e iniciar Kestra

1. Copia `.env.example` como `.env`.
2. Configura `POSTGRES_PASSWORD` y la URL, el usuario, la base de datos, el warehouse y el esquema de Snowflake en `.env`. La base de datos indicada debe existir.
3. Inicia los servicios locales:

   ```bash
   docker compose up -d
   ```

4. Abre [http://localhost:8080](http://localhost:8080). En Kestra, agrega el secreto `SNOWFLAKE_PASSWORD` al namespace `pset2`.
5. Importa `kestra/flows/ingest_churn.yml` en la interfaz de Kestra y ejecuta `ingest_churn_csv`.

El contenedor de Kestra monta el directorio local `data/` en modo de solo lectura en `/app/data`. Antes de cargar, el flujo comprueba que existan los tres CSV y que sus encabezados coincidan con el esquema esperado.

## Ejecutar y volver a ejecutar la ingesta

La fuente actual es una copia local, por lo que el flujo no tiene una ejecución programada: se inicia manualmente cada vez que los archivos estén disponibles o se actualicen. El historial de carga predeterminado de `COPY INTO` de Snowflake evita volver a cargar los mismos archivos del stage durante un reintento. Para cargar una nueva copia histórica, reemplaza los archivos locales y ejecuta el flujo de nuevo; si reutilizaste los mismos nombres para archivos distintos, revisa primero el historial de carga de Snowflake.

Las tareas de Snowflake reintentan errores transitorios hasta tres veces. Si falla la validación de los CSV o la carga, la ejecución falla explícitamente en lugar de aceptar filas rechazadas de manera silenciosa.

## dbt: calidad de datos y esquema estrella

El paso 3 limpia y audita las tablas Raw de train y test mediante modelos Bronze y Silver. El paso 4 crea las dimensiones Gold y `GOLD.FACT_CUSTOMER_CHURN`, cuyo grano es una fila por `id` de origen. La evidencia de calidad, las decisiones de limpieza, el diagrama del esquema estrella y las instrucciones de ejecución están en [`docs/data_quality_and_dimensional_model.md`](docs/data_quality_and_dimensional_model.md).

Después de ejecutar la ingesta con Kestra:

1. Copia `.env.example` como `.env` y configura `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USERNAME`, `SNOWFLAKE_PASSWORD`, `SNOWFLAKE_DATABASE`, `SNOWFLAKE_WAREHOUSE` y `SNOWFLAKE_URL`.
2. Instala dbt desde `dbt/requirements.txt` usando Python 3.12 y exporta las variables de `.env` a la terminal, como se indica en el documento de modelado.
3. Ejecuta `dbt debug --project-dir dbt --profiles-dir dbt` y luego `dbt build --project-dir dbt --profiles-dir dbt`.

El perfil de dbt lee las credenciales únicamente desde variables de entorno. No subas `.env` ni pegues credenciales en el perfil o en los archivos del flujo.

## Spark: One Big Table (OBT)

Cuando `dbt build` termine correctamente, las tablas Gold estarán listas. Configura `SNOWFLAKE_GOLD_SCHEMA=GOLD` y `SNOWFLAKE_OBT_TABLE=OBT_CUSTOMER_CHURN` en `.env` y ejecuta:

```bash
docker compose --profile spark run --rm spark
```

El servicio de Spark lee la tabla de hechos Gold y sus cuatro dimensiones, valida que las claves sean únicas y realiza joins left para incorporar los atributos descriptivos de las dimensiones. También comprueba que la OBT conserve el total de filas, los `observation_id` únicos y los conteos de train y test de la tabla de hechos. Si una dimensión no tiene correspondencia o un join cambia el grano, el job falla antes de escribir. Si las validaciones pasan, reemplaza `GOLD.OBT_CUSTOMER_CHURN` con la copia validada. El rol de Snowflake debe tener permisos para leer Gold y crear o reemplazar una tabla en ese esquema.

El esquema estrella sigue siendo útil para análisis y reportes con joins flexibles y dimensiones reutilizables. La OBT, en cambio, es una copia desnormalizada para preparar features o entrenar modelos, donde conviene disponer de una sola fila por observación. La OBT conserva `source_split` y deja `exited` en NULL para las observaciones de test, evitando filtrar la etiqueta objetivo.
