# Running Iceberg Benchmark in Docker

This Docker setup works on **Mac, EC2, or any Linux machine** with S3 integration.

## Quick Start

### 1. Build the Image

```bash
docker build -t iceberg-benchmark:latest .
```

### 2. Run with S3 Backend

```bash
# Set your AWS bucket name
export S3_BUCKET="my-iceberg-benchmark-bucket"
export AWS_REGION="us-east-1"

# Run smoke test
docker run -it \
  -e AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID} \
  -e AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY} \
  -e AWS_REGION=${AWS_REGION} \
  -e S3_BUCKET=${S3_BUCKET} \
  -v $(pwd)/results:/app/results \
  iceberg-benchmark:latest \
  --smoke
```

### 3. Run Full Benchmark

```bash
docker run -it \
  -e AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID} \
  -e AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY} \
  -e AWS_REGION=${AWS_REGION} \
  -e S3_BUCKET=${S3_BUCKET} \
  -v $(pwd)/results:/app/results \
  iceberg-benchmark:latest
  # No --smoke flag runs full benchmark
```

Expected runtime: **4-8 hours** on Mac Pro or EC2

---

## Using Docker Compose (Easier)

### 1. Create `.env` file

```bash
cat > .env << 'EOF'
AWS_ACCESS_KEY_ID=your-access-key
AWS_SECRET_ACCESS_KEY=your-secret-key
AWS_REGION=us-east-1
S3_BUCKET=your-iceberg-benchmark-bucket
BENCH_PROFILE=ec2
EOF
```

### 2. Run

```bash
# Smoke test
docker-compose run iceberg-benchmark --smoke

# Full benchmark
docker-compose run iceberg-benchmark
```

Results appear in `./results/` automatically.

---

## AWS Setup (First Time Only)

### 1. Create S3 Bucket

```bash
aws s3 mb s3://my-iceberg-benchmark-bucket --region us-east-1
```

### 2. Get AWS Credentials

```bash
# If using AWS CLI profile
export AWS_ACCESS_KEY_ID=$(aws configure get aws_access_key_id --profile benchmark-user)
export AWS_SECRET_ACCESS_KEY=$(aws configure get aws_secret_access_key --profile benchmark-user)
```

Or manually export your credentials.

---

## Running on Different Platforms

### Mac Pro (Local)

```bash
docker run -it \
  -e AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID} \
  -e AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY} \
  -e S3_BUCKET=my-bucket \
  -v $(pwd)/results:/app/results \
  iceberg-benchmark:latest
```

### EC2 Instance

```bash
# Launch EC2 with Docker installed
# (Ubuntu 22.04 with docker.io)

# Clone repo
git clone https://github.com/YOUR_USERNAME/iceberg-benchmark.git
cd iceberg-benchmark

# Build image
docker build -t iceberg-benchmark:latest .

# Run (uses IAM role for S3 access)
docker run -it \
  -e AWS_REGION=us-east-1 \
  -e S3_BUCKET=my-bucket \
  -v $(pwd)/results:/app/results \
  iceberg-benchmark:latest
```

EC2 IAM role automatically provides credentials—no need to pass them.

---

## Viewing Results

Results appear in `./results/`:

```bash
# Per-batch metrics
cat results/per_batch.csv

# Summary by cadence
cat results/summary.csv

# Plot results
python3 -m iceberg_benchmark.plot
```

---

## Troubleshooting

**"S3AFileSystem not found"**
- Spark is downloading S3A libraries (normal on first run)
- Wait 2-3 minutes, it will succeed

**"AccessDenied to S3"**
- Check AWS credentials
- Check S3 bucket name
- Verify IAM permissions (S3FullAccess)

**"Out of memory"**
- Docker memory is too low
- Increase Docker memory: `--memory 8g`
- Or reduce base_rows in config.py

---

## Production Deployment

For CI/CD or automated runs:

```bash
docker build -t iceberg-benchmark:${VERSION} .
docker tag iceberg-benchmark:${VERSION} your-registry/iceberg-benchmark:${VERSION}
docker push your-registry/iceberg-benchmark:${VERSION}

# Then run from registry
docker run -it \
  -e AWS_CREDENTIALS \
  -e S3_BUCKET \
  your-registry/iceberg-benchmark:${VERSION}
```

---

## Performance Notes

| Hardware | Runtime | Cost |
|----------|---------|------|
| Mac Pro (8-16GB) | 4-8 hours | Electricity only |
| t3.2xlarge EC2 | 2-4 hours | ~$1-2 |
| t3.4xlarge EC2 | 1-2 hours | ~$2-4 |

All produce the same results—hardware just affects speed.
