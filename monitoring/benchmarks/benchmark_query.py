from trino.dbapi import connect
import time
import os
import sys

# Config
TRINO_HOST = os.getenv("TRINO_HOST", "localhost")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8060"))
USER = "airflow"

def run_query(query_name, sql):
    try:
        conn = connect(
            host=TRINO_HOST, 
            port=TRINO_PORT, 
            user=USER, 
            catalog="iceberg", 
            schema="edgex"
        )
        cur = conn.cursor()
        
        start = time.time()
        cur.execute(sql)
        rows = cur.fetchall()
        end = time.time()
        
        duration = end - start
        print(f"| {query_name:.<25} | {duration:>8.4f}s | {len(rows):>10} rows |")
        
        cur.close()
        conn.close()
    except Exception as e:
        print(f"❌ Error running query '{query_name}': {e}")

if __name__ == "__main__":
    print(f"🧐 [Query Performance] Connecting to Trino at {TRINO_HOST}:{TRINO_PORT}...")
    print("-" * 60)
    print(f"| {'Query Name':<25} | {'Duration':<9} | {'Result Count':<12} |")
    print("-" * 60)

    queries = {
        "Count Bronze (Raw)": "SELECT count(*) FROM edgex_bronze",
        "Count Silver (Cleaned)": "SELECT count(*) FROM edgex_silver",
        "Count Gold (Agg)": "SELECT count(*) FROM edgex_gold",
        "Avg Metric (Analysis)": "SELECT avg(avg_value) FROM edgex_gold",
        "Gold Top 10 (Serving)": "SELECT * FROM edgex_gold ORDER BY window_start DESC LIMIT 10"
    }

    for name, sql in queries.items():
        run_query(name, sql)
    
    print("-" * 60)
