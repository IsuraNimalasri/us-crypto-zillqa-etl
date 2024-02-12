#FROM python:3.10.13
#
#COPY requirements_airflow.txt ./
#
#
#RUN export AIRFLOW_HOME=~/airflow
#RUN export AIRFLOW_VERSION=2.8.1
#RUN export PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
#RUN export CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
#
#RUN pip install --upgrade pip && \
#    pip install -r requirements_airflow.txt
#
#ENTRYPOINT ["airflow"]


FROM apache/airflow:2.2.3
USER root

# Install AWS and GCP providers
RUN pip install apache-airflow-providers-amazon apache-airflow-providers-google

# Example script to echo credentials (For demonstration purposes only!)
RUN echo "echo 'Database User: \$POSTGRES_USER' && echo 'Database Password: \$POSTGRES_PASSWORD'" > /entrypoint.sh \
    && chmod +x /entrypoint.sh

USER airflow
