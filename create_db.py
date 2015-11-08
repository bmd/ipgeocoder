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
      latitude REAL,
      longitude REAL,
      timezone TEXT,
      create_dt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
      update_dt TIMESTAMP DEFAULT NULL
    );
""")

# Shadows the functionality of MySQL's ON UPDATE CURRENT_TIMESTAMP
c.execute("""
    CREATE TRIGGER [UpdateLastTime]
      AFTER UPDATE ON ip_geo
      FOR EACH ROW
      WHEN NEW.update_dt < OLD.update_dt    --- this avoid infinite loop
      BEGIN
        UPDATE ip_geo SET update_dt = CURRENT_TIMESTAMP WHERE ip_hash = old.ip_hash;
      END
""")

conn.commit()
