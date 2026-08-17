FROM python:3.10

# Install Java and dependencies
RUN apt-get update && apt-get install -y \
    default-jdk \
    git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy project files
COPY . .

# Upgrade pip and install build tools (avoids setuptools issues with PyPI)
RUN pip install --upgrade pip setuptools wheel

# Install package dependencies
RUN pip install -e .

# Configure Spark to download S3A and Hadoop AWS libraries at runtime
ENV SPARK_OPTS="-Dspark.jars.packages=org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.11.0,org.apache.hadoop:hadoop-aws:3.3.4"

# Set default profile
ENV BENCH_PROFILE=ec2
ENV AWS_REGION=us-east-1

# Create results directory
RUN mkdir -p /app/results

# Default command
ENTRYPOINT ["python", "-m", "iceberg_benchmark.run_sweep"]
CMD ["--smoke"]
