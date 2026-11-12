import logging
from datetime import datetime, timedelta, timezone

import requests
from pymongo import MongoClient

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator


def extract_rates(ti):
    response = requests.get(
        "https://api.frankfurter.dev/v2/rates",
        params={"base": "EUR", "quotes": "USD,GBP,JPY"},
        timeout=30,
    )
    response.raise_for_status()

    rates = response.json()
    if not isinstance(rates, list) or not rates:
        raise ValueError("Expected a non-empty list of exchange rates")

    ti.xcom_push(
        key="raw_rates",
        value={
            "rates": rates,
            "retrieved_at": datetime.now(timezone.utc).isoformat(),
        },
    )
    logging.info("Extracted %s exchange rates", len(rates))


def transform_rates(ti):
    raw = ti.xcom_pull(
        task_ids="extract_currency_rates",
        key="raw_rates",
    )
    if not raw:
        raise ValueError("Missing raw_rates XCom")

    records = []
    for row in raw["rates"]:
        rate = float(row["rate"])
        if not 0 < rate < float("inf"):
            raise ValueError(f"Invalid exchange rate: {rate}")

        records.append(
            {
                "base_currency": row["base"].upper(),
                "target_currency": row["quote"].upper(),
                "exchange_rate": rate,
                "rate_date": datetime.strptime(
                    row["date"], "%Y-%m-%d"
                ).date().isoformat(),
                "source": "Frankfurter API",
                "retrieved_at": raw["retrieved_at"],
            }
        )

    ti.xcom_push(key="transformed_rates", value=records)
    logging.info("Transformed records: %s", records)


def load_rates(ti):
    records = ti.xcom_pull(
        task_ids="transform_exchange_rates",
        key="transformed_rates",
    )
    if not records:
        raise ValueError("Missing transformed_rates XCom")

    with MongoClient(
        "mongodb://mongodb:27017/",
        serverSelectionTimeoutMS=10000,
    ) as client:
        collection = client["currency_db"]["exchange_rates"]

        collection.create_index(
            [
                ("base_currency", 1),
                ("target_currency", 1),
                ("rate_date", 1),
            ],
            unique=True,
        )

        for record in records:
            collection.update_one(
                {
                    "base_currency": record["base_currency"],
                    "target_currency": record["target_currency"],
                    "rate_date": record["rate_date"],
                },
                {"$set": record},
                upsert=True,
            )

    logging.info("Loaded %s records into MongoDB", len(records))


with DAG(
    dag_id="currency_exchange_etl",
    description="Daily currency rates from Frankfurter to MongoDB",
    start_date=datetime(2026, 11, 1, tzinfo=timezone.utc),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "student",
        "retries": 2,
        "retry_delay": timedelta(seconds=30),
    },
    tags=["currency", "etl", "mongodb"],
) as dag:
    extract = PythonOperator(
        task_id="extract_currency_rates",
        python_callable=extract_rates,
    )

    transform = PythonOperator(
        task_id="transform_exchange_rates",
        python_callable=transform_rates,
    )

    load = PythonOperator(
        task_id="load_into_mongodb",
        python_callable=load_rates,
    )

    extract >> transform >> load