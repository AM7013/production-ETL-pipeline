import sys
import os


sys.path.insert(0, os.path.join(os.getcwd(), 'Prefect'))

print('Current working directory:', os.getcwd())
print('Files in root:', os.listdir('.'))

from etl_flow import etl_pipeline

if __name__ == "__main__":
    etl_pipeline(target_date='2027-01-01', dry_run=False)
    print('[GREEN] Prefect ETL Pipeline executed successfully via CD!')