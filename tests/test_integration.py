"""
Minimal integration test: create Spark session, write and read a small table.
"""

import os
from iceberg_benchmark.config import get_config
from iceberg_benchmark.spark_session import create_spark_session
from pyspark.sql.functions import lit
import pyspark.sql.types as T

cfg = get_config()
spark = create_spark_session(cfg.warehouse_path)

try:
    print("Creating test table...")

    # Create a simple DataFrame
    schema = T.StructType([
        T.StructField("id", T.IntegerType(), False),
        T.StructField("value", T.StringType(), False),
    ])

    data = [(1, "test"), (2, "data"), (3, "here")]
    df = spark.createDataFrame(data, schema=schema)

    # Write as Iceberg table
    test_table = "iceberg.benchmark.integration_test"
    df.write.format("iceberg").mode("overwrite").saveAsTable(test_table)

    # Read back
    result = spark.table(test_table).collect()
    print(f"✓ Successfully wrote and read {len(result)} rows")

    # Cleanup
    spark.sql(f"DROP TABLE {test_table}")
    print("✓ Integration test passed!")

except Exception as e:
    print(f"✗ Integration test failed: {e}")
    import traceback
    traceback.print_exc()

finally:
    spark.stop()
