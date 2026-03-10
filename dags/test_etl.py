from scripts.extract import extract
from scripts.transform import transform
from scripts.load import load

# Simulate Airflow run_date
run_date = "2026-04-01"

# Step 1: Extract
extract_result = extract(run_date)
print("Extract Output:", extract_result)

# Step 2: Transform
transform_result = transform(extract_result["file_path"], run_date)
print("Transform Output:", transform_result)

# Step 3: Load
load_result = load(transform_result["file_path"], run_date)
print("Load Output:", load_result)
