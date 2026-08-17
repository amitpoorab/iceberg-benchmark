# run the pipeline 

    docker build -t iceberg-maintenance-policy . --no-cache
    # docker compose uses .env file but here we can pass on like this
    BENCH_PROFILE=local docker-compose run iceberg-benchmark --smoke
    watch -n 5 "tail -5 results/per_batch.csv results/summary.csv"

# Analyze results: 

    python -m iceberg_benchmark.analyze

    
