import mssql_python

chemin_csv = "projet_CSV/clients.csv"
nom_table = "clients_ex"

with open(chemin_csv, encoding="utf-8") as fichier:
    lignes = fichier.readlines()

colonnes = lignes[0].strip().split(",")
données_CSV = [tuple(ligne.strip().split(",")) for ligne in lignes[1:]]

print("DONNEES CSV")
for ligne in données_CSV:
    print(ligne)

conn = mssql_python.connect(
    server=r"UC00350\SQLEXPRESS",
    database="test",
    trusted_connection="yes",
    trust_server_certificate="yes"
)

cursor = conn.cursor()
cursor.execute(f"SELECT * FROM {nom_table}")
données_SQL = [tuple(row) for row in cursor]

print("\nDONNEES SQL")
for ligne in données_SQL:
    print(ligne)

def trouver_uniques(source, reference):
    uniques = []
    for ligne in source:
        trouve = False
        for ref in reference:
            identique = True
            for col_index in range(len(ref)):
                champ_ligne = ligne[col_index]
                champ_ref = ref[col_index]
                if str(champ_ligne) != str(champ_ref):
                    identique = False
                    break
            if identique:
                trouve = True
                break
        if not trouve:
            uniques.append(ligne)
    return uniques

unique_CSV = trouver_uniques(données_CSV, données_SQL)
unique_SQL = trouver_uniques(données_SQL, données_CSV)

if unique_CSV:
    print(f"\nLignes présentes dans le CSV mais pas dans la table SQL '{nom_table}' :")
    for ligne in unique_CSV:
        print(ligne)

if unique_SQL:
    print(f"\nLignes présentes dans la table SQL '{nom_table}' mais pas dans le CSV :")
    for ligne in unique_SQL:
        print(ligne)

cursor.close()
conn.close()