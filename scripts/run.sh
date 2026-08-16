docker run -it \
    -e AWS_ACCESS_KEY_ID=$(aws configure get aws_access_key_id --profile benchmark-user) \
    -e AWS_SECRET_ACCESS_KEY=$(aws configure get aws_secret_access_key --profile benchmark-user) \
    -e AWS_REGION=us-east-1 \
    -e S3_BUCKET=$AWS_BUCKET_NAME \
    -v $(pwd)/results:/app/results \
    iceberg-maintenance-policy
