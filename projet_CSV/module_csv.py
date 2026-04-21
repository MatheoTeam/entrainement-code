import csv
import mssql_python

with open("projet_CSV/clients.csv", newline='', encoding="utf-8") as csvfile:
    reader = csv.DictReader(csvfile, delimiter=",")
    data = [
        {k.strip().lower(): (v.strip() if v is not None else v) for k, v in row.items()}
        for row in reader
    ]
    for row in data:
        print(row)

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

for row in data:
    cursor.execute("""
        INSERT INTO clients (id, nom, email, age)
        VALUES (?, ?, ?, ?)
    """, (int(row['id']), row['nom'], row['email'], int(row['age'])))

conn.commit()
cursor.close()
conn.close()