from flask import Flask, request, session
import mssql_python

# Création de l'application Flask
app = Flask(__name__)
app.secret_key = 'votre_cle_secrete'  # Nécessaire pour les sessions

def detect_type(value):
    """Détecte le type interprété d'une valeur CSV."""
    v = value.strip()
    if v == "":
        return "vide"
    if v.lower() in {"true", "false"}:
        return "booleen"
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

def formater_date_pmi(date_str):
    """Convertit une date PMI (YYYYMMDD) en format JJ/MM/AAAA."""
    if len(date_str) == 8 and date_str.isdigit():
        annee = date_str[0:4]
        mois = date_str[4:6]
        jour = date_str[6:8]
        return f"{jour}/{mois}/{annee}"
    return date_str

def valider_date_pmi(date_str):
    """Valide une date affichage PMI (8 chiffres YYYYMMDD)."""
    if len(date_str) != 8:
        return False
    if not date_str.isdigit():
        return False
    try:
        annee = int(date_str[0:4])
        mois = int(date_str[4:6])
        jour = int(date_str[6:8])
        
        if mois < 1 or mois > 12:
            return False
        if jour < 1:
            return False
        
        # Nombre de jours par mois
        jours_par_mois = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
        
        # Vérifier si c'est une année bissextile pour février
        if mois == 2:
            if (annee % 4 == 0 and annee % 100 != 0) or (annee % 400 == 0):
                jours_par_mois[1] = 29
        
        if jour > jours_par_mois[mois - 1]:
            return False
        
        return True
    except ValueError:
        return False

def lire_csv(fichier):
    """Lit un fichier CSV et valide les dates. Retourne (tableau, erreur)."""
    lignes = [ligne.decode("utf-8").strip().split(",") for ligne in fichier]
    if not lignes:
        return [], None
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
                        return None, f"Date incomplète trouvée: {cellule} (doit avoir 8 chiffres)"
                    if not valider_date_pmi(cellule):
                        return None, f"Date invalide trouvée: {cellule}"
                    lignes[i][j] = formater_date_pmi(cellule)
                else:
                    return None, f"Date invalide trouvée: {cellule} (doit être 8 chiffres)"
            else:
                lignes[i][j] = cellule
    return lignes, None

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
    inserer_bdd(tableau, nom_table)
    page += f"<p>Table '{nom_table}' créée </p>"

    ancien_tableau = session.get('ancien_tableau')
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
    premiere = True
    for ligne in tableau:
        page += "<tr>"
        for cellule in ligne:
            if premiere:
                page += "<th>" + cellule + "</th>"
            else:
                typ = detect_type(cellule)
                page += f"<td>{cellule} <small>({typ})</small></td>"
        page += "</tr>"
        premiere = False
    page += "</table>"

    session['ancien_tableau'] = tableau

    return page

# Lancement de l'application en mode debug
if __name__ == "__main__":
    app.run(debug=True)