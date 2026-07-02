with engine.connect() as conn:
    logger.info("[CLEAN] Cleaning up duplicates...") 
    conn.execute(text(f"""
        DELETE FROM {table_name} t1
        WHERE t1.ctid != (
            SELECT MIN(t2.ctid)
            FROM {table_name} t2
            WHERE t2."OrderID" = t1."OrderID"
        )
    """))
    conn.commit()
    logger.info("[SPARKLE] Duplicates removed! Table is now clean.")


logger.info("[PROCESS] UNIQUE is being added...")
with engine.connect() as conn:
    conn.execute(text(f"ALTER TABLE {table_name} ADD CONSTRAINT unique_order_id UNIQUE (\"OrderID\");"))
    conn.execute(text(f"ALTER TABLE {table_name} ADD CONSTRAINT unique_order_date UNIQUE (\"OrderDate\");"))
    conn.commit()
    logger.info(f"[PROCESS] UNIQUE CONSTRAINT added to the table")


logger.info('[ADD] Adding Indexes to improve query performance...')
with engine.connect() as conn:
    conn.execute(text(f"CREATE INDEX IF NOT EXISTS idx_test_date ON {table_name} (\"OrderDate\");"))
    conn.execute(text(f"CREATE INDEX IF NOT EXISTS idx_test_region ON {table_name} (\"Region\");"))
    conn.execute(text(f"CREATE INDEX IF NOT EXISTS idx_test_customer ON {table_name} (\"CustomerName\");"))
    conn.execute(text(f"CREATE INDEX IF NOT EXISTS idx_test_status ON {table_name} (\"OrderID\" ,\"Status\");"))
    conn.commit()
    logger.info(f"[HAND] Indexes created on columns: OrderDate, Region, CustomerName, OrderID+Status")

logger.info("[FILES] Indexes added!")

logger.info("\n[SEARCH] Testing SELECT query from the DB...")

with engine.connect() as conn:
    result = conn.execute(text("SELECT * FROM test limit 10;"))
    logger.info("[INFO] Data Base:")
    for row in result:
        logger.info(row)

    filtered_pandas_df = pd.read_sql(text("SELECT * FROM test WHERE \"OrderDate\" = '2024-01-15';"), conn)
    logger.info("\nRows where OrderDate = '2024-01-15':")
    logger.info(filtered_pandas_df)

logger.info("\n[SEARCH] Analyzing Query Performance with EXPLAIN ANALYZE...")
with engine.connect() as conn:
    query = """ 
    EXPLAIN ANALYZE
    SELECT * FROM test WHERE "OrderDate" = '2024-01-15';
     """
    result = conn.execute(text(query))
    logger.info("\n[METRIC] Query Execution Plan:")
    for row in result:
        logger.info(row[0])

logger.info("\n[ADD] Adding NOT NULL CONSTRAINT to critical columns...")
with engine.connect() as conn:
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS test (
            "OrderID" TEXT NOT NULL,
            "CustomerName" TEXT,
            "Email" TEXT,
            "ProductName" TEXT,
            "Quantity" INTEGER,
            "UnitPrice" TEXT,
            "TotalAmount" TEXT,
            "OrderDate" DATE NOT NULL,
            "Region" TEXT,
            "Status" TEXT,
            "Discount" TEXT
        )
    """))
    logger.info(f"[SHIELD] NOT NULL CONSTRAINT added to the table")
