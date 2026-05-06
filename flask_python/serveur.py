from flask import Flask, request, session
import mssql_python
import datetime

def fichier_clients(nom):
    """Retourne True si les validations strictes doivent s'appliquer."""
    return nom.lower() in {"clients.csv", "clients.ex.csv"}

# Création de l'application Flask
app = Flask(__name__)
app.secret_key = 'votre_cle_secrete'  # Nécessaire pour les sessions
# Limite de taille pour les fichiers uploadés (50 Mo)
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  

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

    # Nettoyage BOM + espaces colonnes
    lignes[0] = [col.replace("\ufeff", "").strip() for col in lignes[0]]

    # Vérifier les valeurs vides
    for i in range(len(lignes)):
        for j in range(len(lignes[i])):
            if lignes[i][j].strip() == "":
                return None, f"Valeur vide trouvée à la ligne {i+1}, colonne {j+1}"

    # SI strict = false => pas de validation date
    if not strict:
        return lignes, None

    # Sinon : on valide la dernière colonne comme date PMI (uniquement pour clients)
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

# Connexion à la BDD
def connecter_bdd():
    """Établit une connexion à la base de données MSSQL."""
    conn = mssql_python.connect(
        server=r"UC00350\SQLEXPRESS",
        database="test",
        trusted_connection="yes",
        trust_server_certificate="yes"
    )
    return conn

def recuperer_colonnes_table(nom_table):
    """Récupère les colonnes d'une table existante via information_schema.columns."""
    conn = connecter_bdd()
    cursor = conn.cursor()
    
    # Requête sur information_schema.columns pour récupérer les métadonnées
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

    lignes_ignorees = 0

    for ligne in data:
        if len(ligne) < nb_colonnes:
            ligne = ligne + [None] * (nb_colonnes - len(ligne))

        elif len(ligne) > nb_colonnes:
            ligne = ligne[:nb_colonnes]

        data_nettoyee.append(ligne)

    cursor.execute(f"DROP TABLE IF EXISTS {nom_table}")

    colonnes_sql = ",".join(
        [f"[{col}] NVARCHAR(MAX)" for col in colonnes]
    )

    cursor.execute(f"CREATE TABLE {nom_table} ({colonnes_sql})")

    champs = ",".join(["?" for _ in colonnes])

    colonnes_sql_insert = ",".join([f"[{c}]" for c in colonnes])

    cursor.executemany(
        f"INSERT INTO {nom_table} ({colonnes_sql_insert}) VALUES ({champs})",
        data_nettoyee
    )

    conn.commit()
    conn.close()

# Recherche les différences
def trouver_uniques(source, reference):
    """Trouve les lignes uniques dans source qui ne sont pas dans reference."""
    uniques = []
    for ligne in source:
        trouve = False
        for ref in reference:
            identique = True
            # Vérifier que les deux lignes ont le même nombre de colonnes
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

# Création de la page Web
@app.route("/", methods=["GET"])
def index_get():
    """Page principale pour uploader et traiter les fichiers CSV."""
    
    # Formulaire d'upload de fichier CSV
    page = """
    <h1>Upload CSV</h1>
    <form method='post' enctype='multipart/form-data'>
        <input type='file' name='fichier' accept='.csv' required>
        <input type='submit'>   
    </form>
    """

    return page

# Création de la page Web
@app.route("/", methods=["POST"])
def index_post():
    """Page principale pour uploader et traiter les fichiers CSV."""
    
    fichier = request.files["fichier"]
    tableau, erreur = lire_csv(fichier, strict=fichier_clients(fichier.filename))
    
    page = """
    <h1>Upload CSV</h1>
    <form method='post' enctype='multipart/form-data'>
        <input type='file' name='fichier' accept='.csv' required>
        <input type='submit'>   
    </form>
    """
    
    # Vérifier s'il y a une erreur de validation
    if erreur:
        page += f"<p><b>ERREUR: {erreur}</b></p>"
        return page
    
    nom_table = fichier.filename.replace(".csv", "").replace(".", "_")
    nom_table = nom_table.replace("\ufeff", "")
    ancien_tableau = session.get('ancien_tableau')

    # Vérifier les en-têtes via information_schema.columns (table de référence en BDD)
    try:
        colonnes_bdd = recuperer_colonnes_table(nom_table)
        if colonnes_bdd:  # Si la table existe déjà
            colonnes_csv = [col.strip() for col in tableau[0]]
            colonnes_bdd_names = [col[0] for col in colonnes_bdd]
            if colonnes_csv != colonnes_bdd_names:
                page += "<p><b>ERREUR: structure différente entre le CSV et la BDD.</b></p>"
                page += f"<p>Colonnes attendues: {', '.join(colonnes_bdd_names)}</p>"
                page += f"<p>Colonnes du CSV: {', '.join(colonnes_csv)}</p>"
                return page
    except Exception as e:
        # La table de référence n'existe pas encore, c'est normal à la première insertion
        pass

    inserer_bdd(tableau, nom_table)
    page += f"<p>Table '{nom_table}' créée </p>"

    if ancien_tableau is not None and fichier_clients(fichier.filename):
        # Vérifier que le format des colonnes n'a pas changé
        valide, erreur = verifier_format_colonne(ancien_tableau, tableau)
        if not valide:
            page += f"<p><b>ERREUR FORMAT:</b> {erreur}</p>"
            return page
        
        if fichier_clients(fichier.filename):
            differences = comparer_fichiers(ancien_tableau, tableau)
        else:
            differences = None
                
        page += "<h3>Différences détectées:</h3>"
        page += "<table border='1'>"
        
        page += "<tr>"
        for col in differences['colonnes']:
            page += f"<th>{col}</th>"
        page += "<th>Statut</th>"
        page += "</tr>"
        
        for ligne in differences['supprimees']:
            for cellule in ligne:
                page += f"<td>{cellule}</td>"
            page += "<td><b>Supprimée</b></td>"
            page += "</tr>"
        
        for ligne in differences['ajoutees']:
            for cellule in ligne:
                page += f"<td>{cellule}</td>"
            page += "<td><b>Ajoutée</b></td>"
            page += "</tr>"
        
        page += "</table>"

    # Création de la table des données du fichier
    page += "<h3>Contenu du fichier:</h3>"
    page += "<table border='1'>"
    
    types_colonnes = []
    if len(tableau) > 1:
        for col_index in range(len(tableau[1])):
            typ = detect_type(tableau[1][col_index])
            types_colonnes.append(typ)
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

# Lancement de l'application en mode debug
if __name__ == "__main__":
    app.run(debug=True)