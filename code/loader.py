import psycopg2

conn = psycopg2.connect(
        host='your-database-host',
        database='your-database-name',
        user='your-database-user',
        password='your-database-password',
        port=5432  # Replace with your PostgreSQL port
    )

cursor = conn.cursor()

# Create the table if it doesn't exist
create_table_query = '''
    CREATE TABLE IF NOT EXISTS your_table (
        id SERIAL PRIMARY KEY,
        date_key DATE,
        time_key TIMESTAMP,
        close NUMERIC,
        volume NUMERIC
    );
'''
cursor.execute(create_table_query)