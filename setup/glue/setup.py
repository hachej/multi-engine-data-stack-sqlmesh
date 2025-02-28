# /// script
# dependencies = [
#   "pyiceberg[glue]",
#   "pyarrow",
# ]
# ///

from pyiceberg.catalog import load_catalog
import pyarrow.parquet as pq
import pyarrow as pa
import os 

## CREATE CATALOG
catalog = load_catalog("glue", **{"type": "glue", "s3.region":"eu-central-1", "glue.region":"eu-central-1"})

## CREATE SCHEMA
ns = catalog.list_namespaces()
if "multiengine" not in [n[0] for n in ns]:
    print("Creating namespace")
    catalog.create_namespace("multiengine")

tables = catalog.list_tables("multiengine")

if ("multiengine", "events") not in tables:
    print("Creating table")
    schema = pa.schema(
        [
            pa.field("event_id", pa.string(), nullable=False),
            pa.field("user_id", pa.string(), nullable=False),
            pa.field("action", pa.string(), nullable=True),
            pa.field("timestamp", pa.int64(), nullable=False),
        ]
    )

    catalog.create_table(
        "multiengine.events",
        schema=schema,
        location="s3://sumeo-parquet-data-lake/multiengine/events",
    )

if ("multiengine", "metrics") not in tables:
    print("Creating metrics table")
    schema = pa.schema(
        [
            pa.field("metric_id", pa.string(), nullable=False),
            pa.field("metric_name", pa.string(), nullable=False),
            pa.field("metric_value", pa.float64(), nullable=False),
            pa.field("timestamp", pa.int64(), nullable=False),
        ]
    )

    catalog.create_table(
        "multiengine.metrics",
        schema=schema,
        location="s3://sumeo-parquet-data-lake/multiengine/metrics",
    )
