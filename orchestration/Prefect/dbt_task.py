# dbt_task.py
from pathlib import Path
import subprocess
from prefect import task

# From orchestration/Prefect/dbt_task.py → go up to repo root
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DBT_PATH = PROJECT_ROOT / "dbt"

@task(name="Run dbt models", log_prints=True)
def run_dbt_models():
    if not DBT_PATH.exists():
        raise FileNotFoundError(f"dbt folder not found at: {DBT_PATH}")

    if not (DBT_PATH / "dbt_project.yml").exists():
        raise FileNotFoundError(f"dbt_project.yml not found in {DBT_PATH}")

    command = [
        "dbt", "run",
        "--project-dir", str(DBT_PATH),
        "--profiles-dir", str(DBT_PATH),
    ]

    result = subprocess.run(command, capture_output=True, text=True)
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise RuntimeError(
            f"dbt run failed (exit {result.returncode})\n"
            f"--- stdout ---\n{result.stdout}\n"
            f"--- stderr ---\n{result.stderr}"
        )

@task(name="Test dbt models", log_prints=True)
def test_dbt_models():
    if not DBT_PATH.exists():
        raise FileNotFoundError(f"dbt folder not found at: {DBT_PATH}")

    command = [
        "dbt", "test",
        "--project-dir", str(DBT_PATH),
        "--profiles-dir", str(DBT_PATH),
    ]

    result = subprocess.run(command, capture_output=True, text=True)
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise RuntimeError(f"dbt test failed (exit {result.returncode})")