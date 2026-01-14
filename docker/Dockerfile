FROM spark:3.5.1

# Explicit Spark paths
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

# Python import resolution
ENV PYTHONPATH=/opt/app

WORKDIR /opt/app

# Copy project code
COPY ./ingestion ./ingestion
COPY ./transform ./transform
COPY ./analytics ./analytics
COPY ./visualization ./visualization
COPY ./src ./src
COPY ./requirements.txt .

# Install Python deps once
USER root
RUN pip install --no-cache-dir -r requirements.txt

USER spark

# IMPORTANT: do NOT auto-run spark-submit
CMD ["tail", "-f", "/dev/null"]
