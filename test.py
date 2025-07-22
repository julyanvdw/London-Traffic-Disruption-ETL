
import psycopg2
import datetime

conn = psycopg2.connect(
    "postgresql://postgres.stmxtgfmlvovmdomnhfq:julyanvdwlondonetl@aws-0-ap-south-1.pooler.supabase.com:5432/postgres"
)
cur = conn.cursor()
cur.execute(
    "INSERT INTO disruptions_history (tims_id, snapshot_time) VALUES (%s, %s) RETURNING id",
    ("min-test", datetime.datetime.utcnow())
)
inserted_id = cur.fetchone()[0]
conn.commit()
cur.close()
conn.close()

# Read and print the last 5 rows from disruptions_history
conn = psycopg2.connect(
    "postgresql://postgres.stmxtgfmlvovmdomnhfq:julyanvdwlondonetl@aws-0-ap-south-1.pooler.supabase.com:5432/postgres"
)
cur = conn.cursor()
cur.execute("SELECT id, tims_id, snapshot_time FROM disruptions_history ORDER BY id DESC LIMIT 5")
rows = cur.fetchall()
print("Last 5 rows in disruptions_history:")
for row in rows:
    print(row)
cur.close()
conn.close()
