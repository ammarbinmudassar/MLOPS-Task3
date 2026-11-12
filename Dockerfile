FROM astrocrpublic.azurecr.io/runtime:3.3-12

LABEL org.opencontainers.image.title="Currency Exchange ETL"
LABEL org.opencontainers.image.description="Daily Frankfurter currency rates ETL using Airflow and MongoDB"
LABEL org.opencontainers.image.version="1.0.0"