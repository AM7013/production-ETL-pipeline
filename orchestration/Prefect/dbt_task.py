# dbt_task.py
from pathlib import Path
import subprocess
from prefect import task

# Get the project root (where Prefect/ and DBT/ are both located)
PROJECT_ROOT = Path(__file__).parent.parent.parent  # Goes up one level from Prefect/
DBT_PATH = PROJECT_ROOT / "DBT" / "pipeline_dbt"

@task(name="Run dbt models", log_prints=True)
def run_dbt_models():
    """Run dbt models."""
    if not DBT_PATH.exists():
        raise FileNotFoundError(f"dbt folder not found at: {DBT_PATH}")

    # Check for dbt_project.yml inside pipeline_dbt
    if not (DBT_PATH / "dbt_project.yml").exists():
        raise FileNotFoundError(f"dbt_project.yml not found in {DBT_PATH}")

    command = [
        "dbt",
        "run",
        "--project-dir", str(DBT_PATH),
        "--profiles-dir", str(DBT_PATH)
    ]

    result = subprocess.run(command, capture_output=True, text=True)
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise RuntimeError(f"dbt run failed (exit {result.returncode})")
    return result.stdout

@task(name="Test dbt models", log_prints=True)
def test_dbt_models():
    """Run dbt tests."""
    if not DBT_PATH.exists():
        raise FileNotFoundError(f"dbt folder not found at: {DBT_PATH}")

    if not (DBT_PATH / "dbt_project.yml").exists():
        raise FileNotFoundError(f"dbt_project.yml not found in {DBT_PATH}")

    command = [
        "dbt",
        "test",
        "--project-dir", str(DBT_PATH),
        "--profiles-dir", str(DBT_PATH)
    ]

    result = subprocess.run(command, capture_output=True, text=True)
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise RuntimeError(f"dbt test failed (exit {result.returncode})")
    return result.stdout
