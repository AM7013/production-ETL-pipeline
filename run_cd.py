import sys
import os
from pathlib import Path
import subprocess

print("=" * 60)
print("🚀 CD Pipeline - Production ETL")
print("=" * 60)


PROJECT_ROOT = Path(__file__).resolve().parent
DBT_PATH = PROJECT_ROOT / "dbt"

csv_path = PROJECT_ROOT / "samples" / "cleaned_data_test.csv"
root_dir = Path(os.getcwd())
print(f"Working Directory: {root_dir}")

print(f"[CD] Using CSV: {csv_path}")
if not csv_path.exists():
    raise FileNotFoundError(f"CSV not found: {csv_path}")
    
# Find the Prefect folder
print(f"[DEBUG] Checking: {(root_dir / 'orchestration' / 'Prefect' / 'etl_flow.py')}")
print(f"[DEBUG] Exists?: {(root_dir / 'orchestration' / 'Prefect' / 'etl_flow.py').exists()}")

prefect_folder = None
possible_paths = [
    root_dir / "orchestration" / "Prefect",
    root_dir / "Prefect",
    root_dir / "ETL-pipeline" / "Prefect",
]

for path in possible_paths:
    etl_file = path / "etl_flow.py"
    if etl_file.exists():
        prefect_folder = path
        print(f"✅ Found etl_flow.py in: {prefect_folder}")
        break

if not prefect_folder:
    print("❌ Could not find etl_flow.py")
    sys.exit(1)

# Change to the Prefect folder and run etl_flow.py directly
print(f"📂 Changing to: {prefect_folder}")
os.chdir(prefect_folder)

# Run etl_flow.py as a script
cmd = ["python", "etl_flow.py"]
print("🚀 Running: python etl_flow.py")
print("=" * 60)

result = subprocess.run(cmd, capture_output=False, text=True)

if result.returncode == 0:
    print("\n[GREEN] ✅ Prefect ETL Pipeline executed successfully via CD!")
    print("=" * 60)
else:
    print(f"\n[RED] ❌ Pipeline failed with exit code: {result.returncode}")
    print("=" * 60)
    sys.exit(result.returncode)


dbt_candidates = [
    PROJECT_ROOT / "dbt",
]

DBT_PATH = None
for path in dbt_candidates:
    if path.exists():
        DBT_PATH = path
        break

if not DBT_PATH:
    raise FileNotFoundError("dbt folder not found")
