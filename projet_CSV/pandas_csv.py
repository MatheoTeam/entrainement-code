import pandas as pd
import mssql_python

df = pd.read_csv("projet_CSV/clients.csv")
df.columns = df.columns.str.strip()

df = df[["id", "nom", "email", "age"]]  

conn = mssql_python.connect(
    server=r"UC00350\SQLEXPRESS",
    database="test",
    trusted_connection="yes",
    trust_server_certificate="yes"
)
cursor = conn.cursor()

cursor.execute("""
    DROP TABLE IF EXISTS clients;
    CREATE TABLE clients (
        id INT PRIMARY KEY,
        nom NVARCHAR(100),
        email NVARCHAR(150),
        age INT
    );
""")

data_to_insert = df.values.tolist()
cursor.executemany(
    "INSERT INTO clients (id, nom, email, age) VALUES (?, ?, ?, ?)",
    data_to_insert
)

conn.commit()
conn.close()

print(df)