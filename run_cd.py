import sys
import os

print("Current working directory:", os.getcwd())
print("Files in root:", os.listdir('.'))

# Add all possible locations
paths_to_add = [
    os.getcwd(),
    os.path.join(os.getcwd(), 'orchenstration'),
    os.path.join(os.getcwd(), 'Prefect'),
    os.path.join(os.getcwd(), 'ETL-pipeline'),
]

for p in paths_to_add:
    if os.path.exists(p):
        sys.path.insert(0, p)
        print(f"Added to path: {p}")

print("Final Python path:", sys.path)

# Try to import
try:
    from etl_flow import etl_pipeline
    print("[GREEN] Successfully imported etl_flow!")
except ImportError as e:
    print("[RED] Import failed:", e)
    # Search for the file
    for root, dirs, files in os.walk('.'):
        if 'etl_flow.py' in files:
            print(f"[INFO] Found etl_flow.py in: {root}")
    sys.exit(1)

# Run the pipeline
etl_pipeline(target_date='2027-01-01', dry_run=False)
print('[GREEN] Prefect ETL Pipeline executed successfully via CD!')
