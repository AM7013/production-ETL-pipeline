import sys
import os

print("Current working directory:", os.getcwd())
print("Files in root:", os.listdir('.'))

# Correct path
prefect_folder = os.path.join(os.getcwd(), 'orchenstration', 'Prefect')
sys.path.insert(0, prefect_folder)

print(f"Added to Python path: {prefect_folder}")

# Import and run
try:
    from etl_flow import etl_pipeline
    print("[GREEN] Successfully imported etl_flow!")

    etl_pipeline(target_date='2027-01-01', dry_run=False)
    print('[GREEN] ✅ Prefect ETL Pipeline executed successfully via CD!')

except ImportError as e:
    print("[RED] Import failed:", e)
    sys.exit(1)
except Exception as e:
    print("[RED] Error running pipeline:", e)
    sys.exit(1)
