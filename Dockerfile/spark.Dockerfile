FROM bitnami/spark:latest

# Copy the requirements.txt file to the container
COPY ./spark_service/requirements.txt .
# Install the Python dependencies
RUN pip install --no-cache-dir -r requirements.txt
