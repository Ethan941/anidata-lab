FROM apache/airflow:2.8.1-python3.11

USER root

# Bundle project artifacts needed at runtime.
COPY airflow/dags /opt/airflow/dags
COPY airflow/scripts /opt/airflow/scripts
COPY anidata-scraper /opt/airflow/anidata-scraper
RUN mkdir -p /opt/airflow/plugins

RUN chown -R airflow:root /opt/airflow/dags /opt/airflow/scripts /opt/airflow/plugins /opt/airflow/anidata-scraper

USER airflow

# Install runtime dependencies and the local scraper package.
RUN pip install --no-cache-dir \
    pandas \
    elasticsearch \
    matplotlib \
    seaborn \
    apache-airflow-providers-elasticsearch \
    /opt/airflow/anidata-scraper
