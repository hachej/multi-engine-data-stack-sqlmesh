from pyiceberg.catalog import load_catalog
import polars as pl
import os 
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
import datetime
import typing as t
import pandas as pd


@model(
    "multiengine.events",
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_TIME_RANGE,
        time_column="event_timestamp"
    ),
    start='2025-02-27',
    cron='*/5 * * * *',
    gateway='duckdb_local',
    columns={
        "event_id": "string NOT NULL",
        "user_id": "string NOT NULL", 
        "action": "string",
        "event_timestamp": "timestamp NOT NULL",
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
    catalog = load_catalog("glue", **{"type": "glue",
                                "glue.region":"eu-central-1",
                                "s3.region":"eu-central-1"
                        })

    # Generate list of minutes between start and end
    minutes = pd.date_range(start, end, freq='T')
    
    # Loop through each minute
    for minute in minutes:
        #df = pl.scan_ndjson(f"s3://sumeo-parquet-data-lake/multiengine/events/{minute.strftime('%Y-%m-%d/%H/%M')}/events_*.jsonl")
        df = pl.scan_ndjson(f"s3://sumeo-parquet-data-lake/multiengine/row/2025-02-28/14/23/events_*.jsonl").collect().to_arrow()
        # add some transformation to the df
        catalog.load_table("multiengine.events").append(df)
    
    return pd.DataFrame()