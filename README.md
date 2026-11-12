# Currency Exchange ETL

MLOps Task 3. An Apache Airflow DAG retrieves daily EUR rates for USD, GBP and JPY from Frankfurter and writes them to MongoDB.

## Run locally

Install Astro CLI and Docker with the Buildx plugin. From this directory:

```bash
DOCKER_BUILDKIT=1 astro dev start
astro dev run dags list-import-errors
```

Open [Airflow](http://127.0.0.1:8080), enable `currency_exchange_etl`, and open its latest scheduled run or trigger it manually. The graph should show all three tasks succeeding.

Open [Mongo Express](http://127.0.0.1:8081), sign in with the local demo credentials in `docker-compose.override.yml`, and select `currency_db → exchange_rates`.

## Data flow

1. `extract_currency_rates` fetches the latest rates and pushes the `raw_rates` XCom.
2. `transform_exchange_rates` validates and maps the API response to the required schema, then pushes `transformed_rates`.
3. `load_into_mongodb` writes one document per base currency, target currency and rate date. The unique index and upsert prevent duplicate records on retries.

The Docker Compose override attaches MongoDB and Mongo Express to the Airflow network. The `mongodb_data` volume stores the database across container replacements. Mongo Express is bound to localhost for this demo. The Dockerfile labels the custom Airflow image.
