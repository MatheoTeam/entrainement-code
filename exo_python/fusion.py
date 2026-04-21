from flask import Flask, jsonify

app = Flask(__name__)

def quick_sort(liste):
    if len(liste) <= 1:
        return liste
    pivot = liste[0]
    gauche = [x for x in liste[1:] if x <= pivot]
    droite = [x for x in liste[1:] if x > pivot]
        
    return quick_sort(gauche) + [pivot] + quick_sort(droite)

def comparaison(ancienne, nouvelle):
    i = j = 0
    resultat = []

    while i < len(ancienne) and j < len(nouvelle):
        if ancienne[i] == nouvelle[j]:
            resultat.append((ancienne[i], "="))
            i += 1
            j += 1
        elif ancienne[i] < nouvelle[j]:
            resultat.append((ancienne[i], "supprimé"))
            i += 1
        else:
            resultat.append((nouvelle[j], "ajouté"))
            j += 1

    while i < len(ancienne):
        resultat.append((ancienne[i], "supprimé"))
        i += 1

    while j < len(nouvelle):
        resultat.append((nouvelle[j], "ajouté"))
        j += 1

    return resultat

@app.route("/")
def accueil():
    ancienne = [42, 19, 7, 85, 63, 4, 30]
    nouvelle = [42, 27, 51, 63, 12, 19, 67]

    ancienne_triee = quick_sort(ancienne)
    nouvelle_triee = quick_sort(nouvelle)

    return jsonify({
        "ancienne": ancienne_triee,
        "nouvelle": nouvelle_triee,
        "resultat": comparaison(ancienne_triee, nouvelle_triee)
    })

if __name__ == "__main__":
    app.run(debug=True)