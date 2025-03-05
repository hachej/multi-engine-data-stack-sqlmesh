# /// script
# dependencies = [
#   "duckdb",
#   "faker",
# ]
# ///


import duckdb
from duckdb.typing import *
from faker import Faker

def generate_random_name():
    fake = Faker()
    return fake.name()


# Connect to DuckDB file
conn = duckdb.connect('duckdb.db')

# Create a macro
conn.create_function("random_name", generate_random_name, [], VARCHAR)

# Use the macro
res = conn.sql("SELECT random_name()").fetchall()
print(res)
