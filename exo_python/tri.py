liste = [42, 19, 7, 85, 63, 4, 30]

# 1.TRI A BULLE
# n = len(liste)

# for i in range(n):
#     for j in range(0, n - i - 1):
#         if liste[j] > liste[j + 1]:
#             liste[j], liste[j + 1] = liste[j + 1], liste[j]
#             print(liste)

# print("voici la liste triée :", liste)


# 2.TRI PAR DICHOTOMIE
# for i in range(1, len(liste)):
#     cle = liste[i]

#     gauche = 0
#     droite = i - 1

#     while gauche <= droite:
#         milieu = (gauche + droite) // 2

#         if cle < liste[milieu]:
#             droite = milieu - 1
#         else:
#             gauche = milieu + 1

#     for j in range(i-1, gauche-1, -1):
#         liste[j+1] = liste[j]
#         print(j)

#     liste[gauche] = cle
#     print(liste)

# print('liste triée :', liste)


# 3.TRI PAR REPARTITION
# count = [0] * (max(liste) + 1)

# for n in liste:
#     count[n] += 1

# liste_triee = []
# for i in range(len(count)):
#     for _ in range(count[i]):
#         liste_triee.append(i)
#         print(liste_triee)
# print("voici la liste triée :", liste_triee)


# 4.TRI RAPIDE (QUICK SORT)
def quick_sort(liste):
    if len(liste) <= 1:
        return liste
    pivot = liste[0]
    gauche = [x for x in liste[1:] if x <= pivot]
    droite = [x for x in liste[1:] if x > pivot]
    
    print("Gauche + Pivot + Droite :", gauche, "+", [pivot], "+", droite)
    
    return quick_sort(gauche) + [pivot] + quick_sort(droite)

liste_triee = quick_sort(liste)
print("Liste triée :", liste_triee)