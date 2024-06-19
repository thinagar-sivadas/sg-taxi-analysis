FROM bitnami/spark:latest

# USER root

# Copy the requirements.txt file to the container
COPY ./spark_service/requirements.txt .
COPY ./spark_service/log4j2.properties "$SPARK_HOME/conf"
COPY ./spark_service/spark-defaults.conf "$SPARK_HOME/conf"
# Install the Python dependencies
RUN pip install --no-cache-dir -r requirements.txt
