"""
@TODO - replace this with a real migration script
"""

import os
import sys
import sqlite3 as sqlite

if os.path.exists(sys.argv[1]):
    sys.exit("Database already exists. Delete or move the database to allow it to be re-created")

conn = sqlite.connect(sys.argv[1])
c = conn.cursor()

c.execute("""
    CREATE TABLE ip_geo (
      ip_hash INT PRIMARY KEY,
      ip_str TEXT,
      city TEXT,
      state TEXT,
      country TEXT,
      postal TEXT,
      create_dt TEXT,
      update_dt TEXT
    );
""")

conn.commit()
