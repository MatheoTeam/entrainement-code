from flask import Flask, request, session
import mssql_python
import datetime

# Création de l'application Flask
app = Flask(__name__)
app.secret_key = 'votre_cle_secrete'  # Nécessaire pour les sessions

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

def lire_csv(fichier):
    """Lit un fichier CSV et valide les dates. Retourne (tableau, erreur)."""
    lignes = [ligne.decode("utf-8").strip().split(",") for ligne in fichier]
    if not lignes:
        return [], None
    # Vérifier les valeurs vides
    for i in range(len(lignes)):
        for j in range(len(lignes[i])):
            cellule = lignes[i][j].strip()
            if cellule == "":
                return None, f"Valeur vide trouvée à la ligne {i+1}, colonne {j+1}"
    # Valider et formater les dates dans les données (dernière colonne)
    derniere_col = len(lignes[0]) - 1
    for i in range(1, len(lignes)):
        for j in range(len(lignes[i])):
            cellule = lignes[i][j].strip()
            # Vérifier si c'est la dernière colonne
            if j == derniere_col:
                # La dernière colonne doit être une date en 8 chiffres
                if cellule.isdigit():
                    if len(cellule) != 8:
                        return None, f"Date incomplète trouvée: {cellule} (doit avoir 8 chiffres (ligne {i}))"
                    if not gestion_date_pmi(cellule):
                        return None, f"Date invalide trouvée: {cellule} (ligne {i})"
                    lignes[i][j] = gestion_date_pmi(cellule)
                else:
                    return None, f"Date invalide trouvée: {cellule} (doit être 8 chiffres (ligne {i}))"
            else:
                lignes[i][j] = cellule
    return lignes, None

# Vérifie que les en-têtes sont exactement les mêmes entre deux CSV
def comparer_entetes(ancien_entetes, nouveaux_entetes):
    ancien = [col.strip().lower() for col in ancien_entetes]
    nouveau = [col.strip().lower() for col in nouveaux_entetes]
    return ancien == nouveau

# Connexion à la BDD 
def inserer_bdd(tableau, nom_table):
    """Insère les données d'un tableau dans une table MSSQL."""
    conn = mssql_python.connect(
        server=r"UC00350\SQLEXPRESS",
        database="test",
        trusted_connection="yes",
        trust_server_certificate="yes"
    )
    cursor = conn.cursor()

    colonnes = tableau[0]
    data = tableau[1:]

    cursor.execute(f"DROP TABLE IF EXISTS {nom_table}")

    colonnes_sql = ",".join([col + " NVARCHAR(255)" for col in colonnes])
    cursor.execute(f"CREATE TABLE {nom_table} ({colonnes_sql})")

    champs = ",".join(["?" for _ in colonnes])
    cursor.executemany(f"INSERT INTO {nom_table} VALUES ({champs})", data)

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
    tableau, erreur = lire_csv(fichier)
    
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

    ancien_tableau = session.get('ancien_tableau')
    if ancien_tableau is not None:
        if not comparer_entetes(ancien_tableau[0], tableau[0]):
            page += "<p><b>ERREUR: structure différente du CSV.</b></p>"
            return page

    inserer_bdd(tableau, nom_table)
    page += f"<p>Table '{nom_table}' créée </p>"

    if ancien_tableau is not None:
        differences = comparer_fichiers(ancien_tableau, tableau)
        
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