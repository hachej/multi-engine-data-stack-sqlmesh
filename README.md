# Project setup

![infra](./architecture.excalidraw.png)

## Setup glue catalog
````
export AWS_PROFILE=<your-profile>
cd setup/glue
uv run setup.py
````

## Run ingestion

```
export AWS_PROFILE=<your-profile>
cd scripts/
uv run generate_event_data.py
```

## Setup SQLMesh

```
export AWS_PROFILE=<your-profile>
cd sqlmesh
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Run SQLMesh

```
cd sqlmesh
sqlmesh plan
```
