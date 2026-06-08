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
        return datetime.datetime.strptime(date_str, '%Y%m%d').strftime('%d/%m/%Y')
    except ValueError:
        return None

class Article:
    def __init__(self, code, libelle, pz):
        self.code = code
        self.libelle = libelle
        self.pz = pz
    def is_conformity(self):
        """Retourne 'conforme' ou 'non-conforme' selon le code article."""
        return "conforme" if PATTERN.match(str(self.code).strip()) else "non-conforme"

class Engine:
    def __init__(self, name):
        self.name = name
    def open(self):
        raise NotImplementedError
    def close(self):
        raise NotImplementedError

class EngineDB(Engine):
    def __init__(self, server, login, database):
        super().__init__("EngineDB")
        self.server = server
        self.login = login
        self.database = database
        self._conn = None
        self._cursor = None
    def open(self):
        """Établit une connexion à la base de données MSSQL."""
        cfg = charger_config()
        self._conn = mssql_python.connect(
            server=self.server,
            database=self.database,
            trusted_connection=cfg['mssql']['trusted_connection'],
            trust_server_certificate=cfg['mssql']['trust_server_certificate']
        )
        self._cursor = self._conn.cursor()
    def read(self, table_name):
        """Récupère les colonnes d'une table existante via information_schema.columns."""
        self._cursor.execute(f"""
            SELECT COLUMN_NAME, DATA_TYPE
            FROM information_schema.columns
            WHERE TABLE_NAME = '{table_name}'
            ORDER BY ORDINAL_POSITION
        """)
        return self._cursor.fetchall()
    def write(self, table_name, content):
        """Insère les données d'un tableau dans une table MSSQL (mode tolérant)."""
        colonnes = [c.replace("\ufeff", "").strip() for c in content[0]]
        nb_colonnes = len(colonnes)
        data_nettoyee = []
        for ligne in content[1:]:
            if len(ligne) < nb_colonnes:
                ligne = ligne + [None] * (nb_colonnes - len(ligne))
            elif len(ligne) > nb_colonnes:
                ligne = ligne[:nb_colonnes]
            data_nettoyee.append(ligne)
        self._cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        self._cursor.execute(f"CREATE TABLE {table_name} ({','.join([f'[{col}] NVARCHAR(MAX)' for col in colonnes])})")
        self._cursor.executemany(
            f"INSERT INTO {table_name} ({','.join([f'[{c}]' for c in colonnes])}) VALUES ({','.join(['?' for _ in colonnes])})",
            data_nettoyee
        )
        self._conn.commit()
    def close(self):
        if self._conn:
            self._conn.close()
            self._conn = None
            self._cursor = None

class EngineCSV(Engine):
    def __init__(self, filename):
        super().__init__("EngineCSV")
        self.filename = filename
        self._file = None
        self._lignes = []
    def open(self):
        self._file = self.filename
    def read(self, strict=False):
        lignes = [ligne.decode("utf-8").strip().split(";") for ligne in self._file]
        if not lignes:
            return [], None
        lignes[0] = [col.replace("\ufeff", "").strip() for col in lignes[0]]
        for i in range(len(lignes)):
            for j in range(len(lignes[i])):
                if lignes[i][j].strip() == "":
                    return None, f"Valeur vide trouvée à la ligne {i+1}, colonne {j+1}"
        if self._file.filename == "articles.csv":
            lignes = self._ajouter_colonne_conformite(lignes)
        if not strict:
            self._lignes = lignes
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
        self._lignes = lignes
        return lignes, None
    def close(self):
        self._file = None
        self._lignes = []
    def _ajouter_colonne_conformite(self, lignes):
        """Ajoute la colonne conformité aux lignes du fichier articles.csv."""
        if not lignes:
            return lignes
        entetes = [col.strip() for col in lignes[0]]
        if "conformité" not in [col.lower() for col in entetes]:
            entetes.append("conformité")
        lignes[0] = entetes
        for index in range(1, len(lignes)):
            ligne = lignes[index]
            article = Article(
                code=ligne[0] if ligne else "",
                libelle=ligne[1] if len(ligne) > 1 else "",
                pz=ligne[2] if len(ligne) > 2 else ""
            )
            conformite_val = article.is_conformity()
            if len(ligne) == len(entetes) - 1:
                ligne.append(conformite_val)
            else:
                ligne[-1] = conformite_val
            lignes[index] = ligne
        return lignes

def trouver_uniques(source, reference):
    """Trouve les lignes uniques dans source qui ne sont pas dans reference."""
    uniques = []
    for ligne in source:
        trouve = False
        for ref in reference:
            identique = len(ligne) == len(ref) and all(str(ligne[i]) == str(ref[i]) for i in range(len(ref)))
            if identique:
                trouve = True
        if not trouve:
            uniques.append(ligne)
    return uniques

def comparer_fichiers(ancien_tableau, nouveau_tableau):
    """Compare deux tableaux et retourne les différences."""
    return {
        'ajoutees': trouver_uniques(nouveau_tableau[1:], ancien_tableau[1:]),
        'supprimees': trouver_uniques(ancien_tableau[1:], nouveau_tableau[1:]),
        'colonnes': nouveau_tableau[0]
    }

def verifier_format_colonne(ancien_tableau, nouveau_tableau):
    """Vérifie que chaque colonne garde le même format. Retourne (True, None) ou (False, message_erreur)."""
    for col_idx in range(len(ancien_tableau[0])):
        ancien_type = detect_type(str(ancien_tableau[1][col_idx]))
        for row_idx, row in enumerate(nouveau_tableau[1:], 2):
            nouveau_type = detect_type(str(row[col_idx]))
            if nouveau_type != ancien_type:
                return False, (
                    f"Colonne '{ancien_tableau[0][col_idx]}': type changé "
                    f"de '{ancien_type}' à '{nouveau_type}' (ligne {row_idx})"
                )
    return True, None

def creer_app():
    """Crée et configure l'application Flask."""
    cfg = charger_config()
    app = Flask(__name__)
    app.secret_key = "votre_cle_secrete"
    app.config['MAX_CONTENT_LENGTH'] = cfg['flask']['max_upload_size_mb'] * 1024 * 1024
    return app

app = creer_app()

FORM_HTML = """
    <h1>Upload CSV</h1>
    <form method='post' enctype='multipart/form-data'>
        <input type='file' name='fichier' accept='.csv' required>
        <input type='submit'>
    </form>
    """

@app.route("/", methods=["GET"])
def index_get():
    return FORM_HTML

@app.route("/", methods=["POST"])
def index_post():
    fichier = request.files["fichier"]
    csv_engine = EngineCSV(fichier)
    csv_engine.open()
    tableau, erreur = csv_engine.read(strict=fichier_clients(fichier.filename))
    csv_engine.close()
    page = FORM_HTML
    if erreur:
        page += f"<p><b>ERREUR: {erreur}</b></p>"
        return page
    nom_table = fichier.filename.replace(".csv", "").replace(".", "_").replace("\ufeff", "")
    ancien_tableau = session.get('ancien_tableau')
    cfg = charger_config()
    db_engine = EngineDB(
        server=cfg['mssql']['server'],
        login=cfg['mssql'].get('login', ''),
        database=cfg['mssql']['database']
    )
    try:
        db_engine.open()
        colonnes_bdd = db_engine.read(nom_table)
        if colonnes_bdd:
            colonnes_csv = [col.strip() for col in tableau[0]]
            colonnes_bdd_names = [col[0] for col in colonnes_bdd]
            if colonnes_csv != colonnes_bdd_names:
                page += "<p><b>ERREUR: structure différente entre le CSV et la BDD.</b></p>"
                page += f"<p>Colonnes attendues: {', '.join(colonnes_bdd_names)}</p>"
                page += f"<p>Colonnes du CSV: {', '.join(colonnes_csv)}</p>"
                db_engine.close()
                return page
        db_engine.write(nom_table, tableau)
        db_engine.close()
    except Exception:
        try:
            db_engine.close()
        except Exception:
            pass
    page += f"<p>Table '{nom_table}' créée </p>"
    if ancien_tableau is not None and fichier_clients(fichier.filename):
        valide, erreur = verifier_format_colonne(ancien_tableau, tableau)
        if not valide:
            page += f"<p><b>ERREUR FORMAT:</b> {erreur}</p>"
            return page
        differences = comparer_fichiers(ancien_tableau, tableau)
        page += "<h3>Différences détectées:</h3><table border='1'><tr>"
        for col in differences['colonnes']:
            page += f"<th>{col}</th>"
        page += "<th>Statut</th></tr>"
        for ligne in differences['supprimees']:
            page += "<tr>" + "".join(f"<td>{c}</td>" for c in ligne) + "<td><b>Supprimée</b></td></tr>"
        for ligne in differences['ajoutees']:
            page += "<tr>" + "".join(f"<td>{c}</td>" for c in ligne) + "<td><b>Ajoutée</b></td></tr>"
        page += "</table>"
    types_colonnes = [detect_type(tableau[1][i]) for i in range(len(tableau[1]))] if len(tableau) > 1 else ["unknown"] * len(tableau[0])
    page += "<h3>Contenu du fichier:</h3><table border='1'>"
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