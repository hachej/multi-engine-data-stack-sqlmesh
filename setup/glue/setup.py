# /// script
# dependencies = [
#   "pyiceberg[glue]",
#   "pyarrow",
# ]
# ///
from pyiceberg.schema import Schema
from pyiceberg.catalog import load_catalog
from pyiceberg.transforms import DayTransform, HourTransform
import pyarrow.parquet as pq
import pyarrow as pa
import os 
from pyiceberg.types import (
    TimestampType,
    FloatType,
    DoubleType, 
    StringType,
    NestedField,
    StructType,
    BooleanType
)
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
    schema = Schema(
        NestedField(field_id=1, name="event_id", field_type=StringType(), required=False),
        NestedField(field_id=2, name="user_id", field_type=StringType(), required=False),
        NestedField(field_id=3, name="action", field_type=StringType(), required=False),
        NestedField(field_id=4, name="timestamp", field_type=TimestampType(), required=False)
    )

    catalog.create_table(
        "multiengine.events",
        schema=schema,
        location="s3://sumeo-parquet-data-lake/multiengine/events",
    )

# Add partitioning by minute to events table
print("Adding partition spec to events table")
events_table = catalog.load_table("multiengine.events")
with events_table.transaction() as transaction:
    with transaction.update_spec() as update_spec:
        update_spec.add_field("timestamp", HourTransform(), "timestamp_hour")
