import sqlalchemy
engine = sqlalchemy.create_engine('postgresql+psycopg2://postgres:postgres@localhost:5432/football_db')
inspector = sqlalchemy.inspect(engine)
columns = inspector.get_columns('matches')
with open('schema_output.txt', 'w', encoding='utf-8') as f:
    for col in columns:
        f.write(f"Column: {col['name']} - Type: {col['type']}\n")
