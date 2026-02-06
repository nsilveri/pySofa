import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'analyze_score_frequency', 'modules')))

import psycopg2
from config import DB_CONFIG

def add_columns():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    # Add home_score_ht if not exists
    try:
        cur.execute("ALTER TABLE matches ADD COLUMN IF NOT EXISTS home_score_ht INT;")
    except Exception as e:
        print(f"Error adding home_score_ht: {e}")
        conn.rollback()
    else:
        conn.commit()
        print("Added home_score_ht column.")

    # Add away_score_ht if not exists
    try:
        cur.execute("ALTER TABLE matches ADD COLUMN IF NOT EXISTS away_score_ht INT;")
    except Exception as e:
        print(f"Error adding away_score_ht: {e}")
        conn.rollback()
    else:
        conn.commit()
        print("Added away_score_ht column.")

    conn.close()

if __name__ == "__main__":
    add_columns()
