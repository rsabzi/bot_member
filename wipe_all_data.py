import sqlite3
import os

DB_FILE = "members.db"

def main():
    if not os.path.exists(DB_FILE):
        print(f"❌ Database file {DB_FILE} not found.")
        return

    print(f"Opening {DB_FILE}...")
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()

    tables = ["members", "scan_checkpoints", "channel_prefs"]
    
    try:
        for table in tables:
            # Check if table exists
            c.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table}';")
            if c.fetchone():
                print(f"Deleting all data from '{table}'...")
                c.execute(f"DELETE FROM {table}")
                print(f"  - Deleted {c.rowcount} rows.")
            else:
                print(f"⚠️ Table '{table}' does not exist.")

        conn.commit()
        
        print("Running VACUUM...")
        c.execute("VACUUM")
        print("✅ VACUUM complete.")
        
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        conn.close()
        print("✅ Database wipe complete.")

if __name__ == "__main__":
    main()
