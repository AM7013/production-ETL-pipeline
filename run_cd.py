import sys
import os
from pathlib import Path

print("=" * 60)
print("🚀 CD Pipeline - Production ETL")
print("=" * 60)

# Current directory
root_dir = Path(os.getcwd())
print(f"Working Directory: {root_dir}")

# Possible locations for etl_flow.py
possible_paths = [
    root_dir / "orchestration" / "Prefect",
    root_dir / "Prefect",
    root_dir / "ETL-pipeline" / "Prefect",
    root_dir / "orchestration",
]

# Add paths and find etl_flow
etl_flow_found = False
for path in possible_paths:
    if path.exists():
        sys.path.insert(0, str(path))
        print(f"✅ Added to Python path: {path}")
        
        if (path / "etl_flow.py").exists():
            print(f"✅ Found etl_flow.py in: {path}")
            etl_flow_found = True
            break

if not etl_flow_found:
    print("❌ Could not find etl_flow.py")
    sys.exit(1)

# Import and run
try:
    from etl_flow import etl_pipeline
    print("✅ Successfully imported etl_flow")

    print("🚀 Starting Prefect ETL Pipeline in PRODUCTION...")
    etl_pipeline(target_date='2027-01-01', dry_run=False)
    
    print("[GREEN] ✅ Prefect ETL Pipeline executed successfully via CD!")
    print("=" * 60)

except Exception as e:
    print(f"❌ Error during pipeline execution: {e}")
    sys.exit(1)
