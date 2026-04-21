import mssql_python
from flask import Flask, jsonify

chemin = r"projet_CSV\clients.ex.csv"
chemin = chemin.replace('\\', '/')
nom_table = chemin.split("/")[-1].rsplit(".", 1)[0].replace(".", "_", 1)

fichier = open(chemin, encoding="utf-8")
lignes = fichier.readlines()
fichier.close()

colonnes = lignes[0].strip().split(",")
data = []
for i in range(1, len(lignes)):
    ligne = lignes[i].strip().split(",")
    data.append(tuple(ligne))

conn = mssql_python.connect(
    server=r"UC00350\SQLEXPRESS",
    database="test",
    trusted_connection="yes",
    trust_server_certificate="yes"
)
cursor = conn.cursor()

cursor.execute("DROP TABLE IF EXISTS " + nom_table)
colonnes_sql = ""
for col in colonnes:
    colonnes_sql += col + " NVARCHAR(255),"
colonnes_sql = colonnes_sql[:-1]

cursor.execute("CREATE TABLE " + nom_table + " (" + colonnes_sql + ")")

champs = ""
for i in range(len(colonnes)):
    champs += "?," 
champs = champs[:-1]

cursor.executemany("INSERT INTO " + nom_table + " VALUES (" + champs + ")", data)
conn.commit()

app = Flask(__name__)

@app.route("/")
def afficher_json():
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM " + nom_table)

    resultat_json = []

    for ligne in cursor:
        dico = {}
        for i in range(len(colonnes)):
            dico[colonnes[i]] = ligne[i]
        resultat_json.append(dico)

    return jsonify(resultat_json)

if __name__ == "__main__":
    app.run(debug=True)