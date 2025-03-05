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
        time_column="timestamp"
    ),
    start='2025-03-05 19:20:00',
    cron='*/5 * * * *',
    gateway='duckdb_local',
    columns={
        "event_id": "string NOT NULL",
        "user_id": "string NOT NULL", 
        "action": "string",
        "timestamp": "timestamp NOT NULL",
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

    # Create empty DataFrame with required schema
    empty_df = pl.DataFrame(
        schema={
            "event_id": pl.String,
            "user_id": pl.String,
            "action": pl.String,
            "timestamp": pl.Datetime
        }
    )

    # Loop through each minute
    for minute in minutes:
        print(f"Processing {minute.strftime('%Y-%m-%d/%H/%M')}")
        # Check if any files exist in the S3 path for this minute
        import boto3
        s3 = boto3.client('s3')
        s3_path = f"multiengine/row/{minute.strftime('%Y-%m-%d/%H/%M')}/"

        objects = s3.list_objects_v2(
            Bucket='sumeo-parquet-data-lake',
            Prefix=s3_path
        )
        
        if 'Contents' not in objects:
            print(f"No files found for {s3_path}")
            continue
        else:
            key = f"s3://sumeo-parquet-data-lake/multiengine/row/{minute.strftime('%Y-%m-%d/%H/%M')}/events_*.jsonl"
            print(key)
            df = pl.scan_ndjson(key).collect()
            # Append the new data to empty_df
            df = df.with_columns(pl.col("timestamp").cast(pl.Int64).cast(pl.Datetime("us")))
            empty_df = pl.concat([empty_df, df])
            catalog.load_table("multiengine.events").append(df.to_arrow())

    return empty_df
