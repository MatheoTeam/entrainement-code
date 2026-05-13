from serveur import foo

print(foo("4030004"))  # conforme

# def foo(code_article):
#     """Retourne 'conforme' ou 'non-conforme' selon le code article."""
#     code = str(code_article).strip()
#     if len(code) == 9 and code[:7].isdigit() and code[7] == '-' and code[8].isdigit():
#         return "conforme"
#     chiffres = 0
#     for caractere in code:
#         if caractere.isdigit():
#             chiffres += 1
#             if chiffres > 7:
#                 return "non-conforme"
#     return "conforme"

# import re

# PATTERN = re.compile(
#     r"^(?:"
#     r"[LB]?[0-9A-Z]{2,3}[A-Z]?[0-9]{2,4}[A-Z]?(?:\/[0-9]{2})?(?:-[0-9])?"  # cas général
#     r"|[12][0-9]{5}"          # main d'œuvre
#     r"|L.+"                   # libellés
#     r"|EP[0-9]{5}"            # emballages palettes
#     r"|EC[0-9]{5}"            # emballages coiffes
#     r")$"
# )

# def foo(code_article):
#     """Retourne 'conforme' ou 'non-conforme' selon le code article."""
#     code = str(code_article).strip()
#     return "conforme" if PATTERN.match(code) else "non-conforme"