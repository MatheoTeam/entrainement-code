import mssql_python

def lecture_csv(fichier, sep=","):
    with open(fichier, encoding="utf-8") as f:
        for ligne in f: print(ligne.strip().split(sep))

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

with open("projet_CSV/clients.csv", encoding="utf-8") as f:
    next(f)
    data = [tuple(line.strip().split(",")) for line in f]

cursor.executemany("INSERT INTO clients VALUES (?, ?, ?, ?)", data)

conn.commit()
cursor.close()  
conn.close()

lecture_csv("projet_CSV/clients.csv")