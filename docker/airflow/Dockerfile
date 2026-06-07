FROM apache/airflow:slim-latest-python3.13

# Install UV
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Install Python dependencies (must use Airflow user)
COPY --chown=airflow:airflow requirements.txt /opt/airflow/requirements.txt
USER airflow
RUN /bin/uv pip install --no-cache-dir -r /opt/airflow/requirements.txt
