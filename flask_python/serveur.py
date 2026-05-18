from flask import Flask, request, session
import mssql_python
import datetime
import re
import yaml

def charger_config():
    """Charge et retourne la configuration depuis le fichier YAML."""
    with open("flask_python/config.yml", encoding="utf-8") as f:
        return yaml.safe_load(f)

def fichier_clients(nom):
    """Retourne True si les validations strictes doivent s'appliquer."""
    return nom.lower() in {"clients.csv", "clients.ex.csv"}

def creer_app():
    """Crée et configure l'application Flask."""
    cfg = charger_config()
    app = Flask(__name__)
    app.secret_key = "votre_cle_secrete"
    app.config['MAX_CONTENT_LENGTH'] = cfg['flask']['max_upload_size_mb'] * 1024 * 1024
    return app

app = creer_app()

PATTERN = re.compile(
    r"^(?:"
    r"|[0-9]{3}A?[BNGCTO][LBCPFM][0-9]{3}[VIC](?:\/[0-9]{2})?"
    r"|850[1-6][0-9]{4}"
    r"|2[17][0-9]{4}"
    r"|[0-9]{3}[EVIPMA][0-9]{3}"
    r"|[0-9]{7}(?:-[0-9])?"
    r"|B[0-9]{5}"
    r"|L.+"
    r"|E[PC][0-9]{5}"
    r")$"
)

def detect_type(value):
    """Détecte le type interprété d'une valeur CSV."""
    v = value.strip()
    if v.lower() in {"true", "false"}:
        return "booleen"
    try:
        datetime.datetime.strptime(v, '%d/%m/%Y')
        return "date"
    except ValueError:
        pass
    try:
        datetime.datetime.strptime(v, '%Y%m%d')
        return "date"
    except ValueError:
        pass
    try:
        int(v)
        return "entier"
    except ValueError:
        pass
    try:
        float(v)
        return "flottant"
    except ValueError:
        pass
    return "string"

def gestion_date_pmi(date_str):
    """Valide une date PMI (YYYYMMDD) et la formate en JJ/MM/AAAA.
       Retourne None si la date est invalide."""
    if len(date_str) != 8 or not date_str.isdigit():
        return None
    try:
        date_obj = datetime.datetime.strptime(date_str, '%Y%m%d')
        return date_obj.strftime('%d/%m/%Y')
    except ValueError:
        return None

def conformité(code_article):
    """Retourne 'conforme' ou 'non-conforme' selon le code article."""
    code = str(code_article).strip()
    return "conforme" if PATTERN.match(code) else "non-conforme"

def ajouter_colonne_conformite(lignes):
    """Ajoute la colonne conformité aux lignes du fichier articles.csv."""
    if not lignes:
        return lignes

    entetes = [col.strip() for col in lignes[0]]
    if "conformité" not in [col.lower() for col in entetes]:
        entetes.append("conformité")
    lignes[0] = entetes

    for index in range(1, len(lignes)):
        ligne = lignes[index]
        if len(ligne) == len(entetes) - 1:
            ligne.append(conformité(ligne[0] if ligne else ""))
        else:
            ligne[-1] = conformité(ligne[0] if ligne else "")
        lignes[index] = ligne

    return lignes

def verifier_format_colonne(ancien_tableau, nouveau_tableau):
    """Vérifie que chaque colonne garde le même format. Retourne (True, None) ou (False, message_erreur)."""
    for col_idx in range(len(ancien_tableau[0])):
        ancien_type = detect_type(str(ancien_tableau[1][col_idx]))
        for row_idx, row in enumerate(nouveau_tableau[1:], 2):
            nouveau_type = detect_type(str(row[col_idx]))
            if nouveau_type != ancien_type:
                return False, f"Colonne '{ancien_tableau[0][col_idx]}': type changé de '{ancien_type}' à '{nouveau_type}' (ligne {row_idx})"

    return True, None

def lire_csv(fichier, strict=False):
    lignes = [ligne.decode("utf-8").strip().split(";") for ligne in fichier]
    if not lignes:
        return [], None

    lignes[0] = [col.replace("\ufeff", "").strip() for col in lignes[0]]

    for i in range(len(lignes)):
        for j in range(len(lignes[i])):
            if lignes[i][j].strip() == "":
                return None, f"Valeur vide trouvée à la ligne {i+1}, colonne {j+1}"

    if fichier.filename == "articles.csv":
        lignes = ajouter_colonne_conformite(lignes)

    if not strict:
        return lignes, None

    derniere_col = len(lignes[0]) - 1
    for i in range(1, len(lignes)):
        for j in range(len(lignes[i])):
            cellule = lignes[i][j].strip()

            if j == derniere_col:
                if cellule.isdigit():
                    if len(cellule) != 8:
                        return None, f"Date incomplète trouvée: {cellule} (ligne {i})"
                    if not gestion_date_pmi(cellule):
                        return None, f"Date invalide trouvée: {cellule} (ligne {i})"
                    lignes[i][j] = gestion_date_pmi(cellule)
                else:
                    return None, f"Date invalide trouvée: {cellule} (ligne {i})"
            else:
                lignes[i][j] = cellule

    return lignes, None

def connecter_bdd():
    """Établit une connexion à la base de données MSSQL."""
    cfg = charger_config()
    conn = mssql_python.connect(
        server=cfg['mssql']['server'],
        database=cfg['mssql']['database'],
        trusted_connection=cfg['mssql']['trusted_connection'],
        trust_server_certificate=cfg['mssql']['trust_server_certificate']
    )
    return conn

def recuperer_colonnes_table(nom_table):
    """Récupère les colonnes d'une table existante via information_schema.columns."""
    conn = connecter_bdd()
    cursor = conn.cursor()

    query = f"""
    SELECT COLUMN_NAME, DATA_TYPE
    FROM information_schema.columns
    WHERE TABLE_NAME = '{nom_table}'
    ORDER BY ORDINAL_POSITION
    """
    cursor.execute(query)
    colonnes = cursor.fetchall()
    conn.close()

    return colonnes

def inserer_bdd(tableau, nom_table):
    """Insère les données d'un tableau dans une table MSSQL (mode tolérant)."""
    conn = connecter_bdd()
    cursor = conn.cursor()

    colonnes = [c.replace("\ufeff", "").strip() for c in tableau[0]]
    data = tableau[1:]
    nb_colonnes = len(colonnes)
    data_nettoyee = []

    for ligne in data:
        if len(ligne) < nb_colonnes:
            ligne = ligne + [None] * (nb_colonnes - len(ligne))
        elif len(ligne) > nb_colonnes:
            ligne = ligne[:nb_colonnes]
        data_nettoyee.append(ligne)

    cursor.execute(f"DROP TABLE IF EXISTS {nom_table}")

    colonnes_sql = ",".join([f"[{col}] NVARCHAR(MAX)" for col in colonnes])
    cursor.execute(f"CREATE TABLE {nom_table} ({colonnes_sql})")

    champs = ",".join(["?" for _ in colonnes])
    colonnes_sql_insert = ",".join([f"[{c}]" for c in colonnes])

    cursor.executemany(
        f"INSERT INTO {nom_table} ({colonnes_sql_insert}) VALUES ({champs})",
        data_nettoyee
    )

    conn.commit()
    conn.close()

def trouver_uniques(source, reference):
    """Trouve les lignes uniques dans source qui ne sont pas dans reference."""
    uniques = []
    for ligne in source:
        trouve = False
        for ref in reference:
            identique = True
            if len(ligne) != len(ref):
                identique = False
            else:
                for col_index in range(len(ref)):
                    if str(ligne[col_index]) != str(ref[col_index]):
                        identique = False
            if identique:
                trouve = True
        if not trouve:
            uniques.append(ligne)
    return uniques

def comparer_fichiers(ancien_tableau, nouveau_tableau):
    """Compare deux tableaux et retourne les différences."""
    donnees_ancien = ancien_tableau[1:]
    donnees_nouveau = nouveau_tableau[1:]

    ajoutees = trouver_uniques(donnees_nouveau, donnees_ancien)
    supprimees = trouver_uniques(donnees_ancien, donnees_nouveau)

    return {
        'ajoutees': ajoutees,
        'supprimees': supprimees,
        'colonnes': nouveau_tableau[0]
    }

@app.route("/", methods=["GET"])
def index_get():
    page = """
    <h1>Upload CSV</h1>
    <form method='post' enctype='multipart/form-data'>
        <input type='file' name='fichier' accept='.csv' required>
        <input type='submit'>   
    </form>
    """
    return page

@app.route("/", methods=["POST"])
def index_post():
    fichier = request.files["fichier"]
    tableau, erreur = lire_csv(fichier, strict=fichier_clients(fichier.filename))

    page = """
    <h1>Upload CSV</h1>
    <form method='post' enctype='multipart/form-data'>
        <input type='file' name='fichier' accept='.csv' required>
        <input type='submit'>   
    </form>
    """

    if erreur:
        page += f"<p><b>ERREUR: {erreur}</b></p>"
        return page

    nom_table = fichier.filename.replace(".csv", "").replace(".", "_")
    nom_table = nom_table.replace("\ufeff", "")
    ancien_tableau = session.get('ancien_tableau')

    try:
        colonnes_bdd = recuperer_colonnes_table(nom_table)
        if colonnes_bdd:
            colonnes_csv = [col.strip() for col in tableau[0]]
            colonnes_bdd_names = [col[0] for col in colonnes_bdd]
            if colonnes_csv != colonnes_bdd_names:
                page += "<p><b>ERREUR: structure différente entre le CSV et la BDD.</b></p>"
                page += f"<p>Colonnes attendues: {', '.join(colonnes_bdd_names)}</p>"
                page += f"<p>Colonnes du CSV: {', '.join(colonnes_csv)}</p>"
                return page
    except Exception:
        pass

    inserer_bdd(tableau, nom_table)
    page += f"<p>Table '{nom_table}' créée </p>"

    if ancien_tableau is not None and fichier_clients(fichier.filename):
        valide, erreur = verifier_format_colonne(ancien_tableau, tableau)
        if not valide:
            page += f"<p><b>ERREUR FORMAT:</b> {erreur}</p>"
            return page

        differences = comparer_fichiers(ancien_tableau, tableau)

        page += "<h3>Différences détectées:</h3>"
        page += "<table border='1'>"

        page += "<tr>"
        for col in differences['colonnes']:
            page += f"<th>{col}</th>"
        page += "<th>Statut</th>"
        page += "</tr>"

        for ligne in differences['supprimees']:
            page += "<tr>"
            for cellule in ligne:
                page += f"<td>{cellule}</td>"
            page += "<td><b>Supprimée</b></td>"
            page += "</tr>"

        for ligne in differences['ajoutees']:
            page += "<tr>"
            for cellule in ligne:
                page += f"<td>{cellule}</td>"
            page += "<td><b>Ajoutée</b></td>"
            page += "</tr>"

        page += "</table>"

    page += "<h3>Contenu du fichier:</h3>"
    page += "<table border='1'>"

    types_colonnes = []
    if len(tableau) > 1:
        for col_index in range(len(tableau[1])):
            types_colonnes.append(detect_type(tableau[1][col_index]))
    else:
        types_colonnes = ["unknown"] * len(tableau[0])

    premiere = True
    for ligne in tableau:
        page += "<tr>"
        for col_index, cellule in enumerate(ligne):
            if premiere:
                page += f"<th>{cellule} <small>({types_colonnes[col_index]})</small></th>"
            else:
                page += f"<td>{cellule}</td>"
        page += "</tr>"
        premiere = False
    page += "</table>"

    session['ancien_tableau'] = tableau
    return page

if __name__ == "__main__":
    app.run(debug=True)