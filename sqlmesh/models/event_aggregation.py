from pyiceberg.catalog import load_catalog
import polars as pl
import os 
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
import datetime
import typing as t
import pandas as pd


@model(
    "multiengine.event",
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_TIME_RANGE,
        time_column="timestamp"
    ),
    start='2025-03-10 10:00:00',
    cron='*/5 * * * *',
    columns={
        "timestamp": "timestamp NOT NULL",
        "action": "string NOT NULL",
        "action_count": "int NOT NULL"
    },
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> None:
    print(start, end)
    catalog = load_catalog("glue", **{"type": "glue", "s3.region":"eu-central-1", "glue.region":"eu-central-1"})
    
    event_table = catalog.load_table("multiengine.events")
    df = pl.scan_iceberg(event_table)
    df = df.filter(
        (pl.col("timestamp").cast(pl.Datetime("us", "UTC")) >= start.astimezone(datetime.timezone.utc)) &
        (pl.col("timestamp").cast(pl.Datetime("us", "UTC")) < end.astimezone(datetime.timezone.utc)))
    
    df = df.group_by(pl.col("timestamp").dt.truncate("5m"), pl.col("action")).agg(pl.count().alias("action_count"))
    # Add timezone info to timestamp column
    df = df.with_columns(pl.col("timestamp").cast(pl.Datetime("us", "UTC")).dt.strftime("%Y-%m-%d %H:%M:%S"))
    
    df = df.collect().to_pandas()

    if df.shape[0] == 0:
        yield from ()
    else:
        yield df
    