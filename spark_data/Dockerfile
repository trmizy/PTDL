# Use the latest Spark image
FROM apache/spark:latest

# Switch to root to set permissions and install packages
USER root

# Install pip for Python package installations
RUN apt-get update && apt-get install -y python3-pip

# Install necessary Python packages
RUN pip install pandas openpyxl psycopg2-binary

# Copy MongoDB Spark Connector and PostgreSQL JDBC Driver
COPY jars/mongo-spark-connector_2.12-3.0.1.jar /opt/spark/jars/
COPY jars/mongo-java-driver-3.12.10.jar /opt/spark/jars/
COPY jars/postgresql-42.7.4.jar /opt/spark/jars/

# Create the .ivy2 directory and give permissions to spark user
RUN mkdir -p /home/spark/.ivy2/cache && chown -R spark:spark /home/spark

# Ensure the spark user has write permissions on the necessary directories
RUN mkdir -p /spark_data && chown -R spark:spark /spark_data

# Switch back to spark user
USER spark

# Copy the Python script to the container
COPY data_cleaning.py /spark_data/data_cleaning.py

# Set the entrypoint to run the Python script with Spark
ENTRYPOINT ["/bin/bash", "-c", "/opt/spark/bin/spark-submit --jars /opt/spark/jars/mongo-spark-connector_2.12-3.0.1.jar,/opt/spark/jars/mongo-java-driver-3.12.10.jar,/opt/spark/jars/postgresql-42.7.4.jar /spark_data/data_cleaning.py"]
