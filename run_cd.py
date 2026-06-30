import sys
import os

print("Current working directory:", os.getcwd())
print("Files in root:", os.listdir('.'))

target_path = os.path.join(os.getcwd(), 'orchenstration', 'Prefect')
sys.path.insert(0, target_path)
print(f"Added to Python path: {target_path}")

if os.path.exists(target_path):
    print("Files in Prefect folder:", os.listdir(target_path))
else:
    print("[ERROR] Folder not found:", target_path)

try:
    from etl_flow import etl_pipeline
    print("[GREEN] Successfully imported etl_flow!")
    
    etl_pipeline(target_date='2027-01-01', dry_run=False)
    print('[GREEN] ✅ Prefect ETL Pipeline executed successfully via CD!')
    
except ImportError as e:
    print("[RED] Import failed:", e)
    sys.exit(1)
except Exception as e:
    print("[RED] Error during execution:", e)
    sys.exit(1)
