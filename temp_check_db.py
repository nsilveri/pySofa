import psycopg2
conn = psycopg2.connect('dbname=postgres user=postgres password=your_password host=localhost')
conn.autocommit = True
cur = conn.cursor()
cur.execute("SELECT datname FROM pg_database WHERE datname = 'football_db';")
exists = cur.fetchone()
if exists:
    print('Database exists')
else:
    cur.execute('CREATE DATABASE football_db;')
    print('Database created')
cur.close()
conn.close()