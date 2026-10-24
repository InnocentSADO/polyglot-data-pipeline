"""DEVOIR 1 - Partie B : enrichissement documentaire MongoDB.

Entrees :
  - /home/jovyan/data/profils_utilisateurs.jsonl.gz   (50 000 profils JSON)
  - /home/jovyan/work/output/segments_utilisateurs.csv (sortie Hive, partie A)
Sortie :
  - /home/jovyan/work/output/audience_cible.jsonl      (entree de Cassandra)
Execution : python /home/jovyan/work/02_mongodb_enrichissement.py
Rejouable : la collection est videe puis reconstruite a chaque execution.
"""
import csv, gzip, json
from datetime import datetime
from pathlib import Path
from pymongo import MongoClient, UpdateOne, ASCENDING
from pymongo.errors import DuplicateKeyError

DATA = Path("/home/jovyan/data")
OUT = Path("/home/jovyan/work/output")
PROFILS_GZ = DATA / "profils_utilisateurs.jsonl.gz"
SEGMENTS_CSV = OUT / "segments_utilisateurs.csv"
AUDIENCE_JSONL = OUT / "audience_cible.jsonl"
SEGMENTS_CIBLES = ["intensif", "actif"]
LOT = 5000

client = MongoClient("mongodb://inf6303:inf6303@mongo:27017/?authSource=admin")
db = client["inf6303_devoir1"]
profiles = db["profils"]


def titre(t):
    print(f"\n=== {t} ===")


# ---------------------------------------------------------------------
# B1 - Vider la collection et importer les 50 000 profils JSON -> BSON
#      Les sous-documents (preferences) et tableaux (devices,
#      preferences.genres) sont conserves tels quels (pas d'aplatissement).
#      signup_date : chaine "AAAA-MM-JJ" -> date BSON.
# ---------------------------------------------------------------------
titre("B1 - Import documentaire")
profiles.drop()
lignes, inseres, lot = 0, 0, []
with gzip.open(PROFILS_GZ, "rt", encoding="utf-8") as f:
    for ligne in f:
        doc = json.loads(ligne)
        lignes += 1
        doc["signup_date"] = datetime.strptime(doc["signup_date"], "%Y-%m-%d")
        lot.append(doc)
        if len(lot) == LOT:
            inseres += len(profiles.insert_many(lot).inserted_ids)
            lot = []
if lot:
    inseres += len(profiles.insert_many(lot).inserted_ids)

print(f"lignes JSON lues             : {lignes}")
print(f"documents inseres            : {inseres}")
print(f"documents en base            : {profiles.count_documents({})}")
print(f"preferences = sous-document  : {profiles.count_documents({'preferences': {'$type': 'object'}})}")
print(f"preferences.genres = tableau : {profiles.count_documents({'preferences.genres': {'$type': 'array'}})}")
print(f"devices = tableau            : {profiles.count_documents({'devices': {'$type': 'array'}})}")
print(f"signup_date = date BSON      : {profiles.count_documents({'signup_date': {'$type': 'date'}})}")
print(f"consent_analytics = true     : {profiles.count_documents({'consent_analytics': True})}")
print("exemple u000001 :", profiles.find_one({"user_id": "u000001"}, {"_id": 0}))

# ---------------------------------------------------------------------
# B2 (index) - Index unique sur user_id + index compose
#  * ux_user_id : cle d'interconnexion Hive -> MongoDB -> Cassandra ;
#    garantit l'absence de doublon et accelere les mises a jour par user_id.
#  * ix_segment_consent_country : correspond au filtre de l'export B3
#    (segment IN [intensif, actif] ET consent_analytics = true) ; le
#    3e champ (country) sert les recherches par segment et pays.
# ---------------------------------------------------------------------
titre("B1 - Index")
profiles.create_index([("user_id", ASCENDING)], unique=True, name="ux_user_id")
profiles.create_index(
    [("analytics.segment", ASCENDING), ("consent_analytics", ASCENDING), ("country", ASCENDING)],
    name="ix_segment_consent_country",
)
for nom, spec in profiles.index_information().items():
    print(f"{nom:28s} {spec['key']}  unique={spec.get('unique', False)}")
try:
    profiles.insert_one({"user_id": "u000001"})
    print("ERREUR : un doublon de user_id a ete accepte")
except DuplicateKeyError:
    print("Unicite verifiee : insertion d'un 2e u000001 refusee (E11000 duplicate key)")

# ---------------------------------------------------------------------
# B2 - Enrichissement interconnecte : sortie Hive -> sous-document analytics
#  * mise a jour par user_id ($set), par lots (bulk_write), SANS upsert :
#    aucun profil ni identifiant n'est cree a cette etape.
#  * analytics.source / enrichi_le : tracabilite de la provenance.
#  * controle croise : pays calcule par Hive vs pays du profil.
# ---------------------------------------------------------------------
titre("B2 - Enrichissement par la sortie Hive")
COLONNES = ["user_id", "country", "nb_events", "total_watch_sec", "segment"]
SOURCE = "hive:/inf6303/devoir1/sorties/segments"
horodatage = datetime.now()
pays_profil = {d["user_id"]: d["country"]
               for d in profiles.find({}, {"_id": 0, "user_id": 1, "country": 1})}

lignes_csv = absents = ecarts_pays = trouves = modifies = 0
ops = []


def envoyer(ops):
    global trouves, modifies
    res = profiles.bulk_write(ops, ordered=False)
    trouves += res.matched_count
    modifies += res.modified_count


with open(SEGMENTS_CSV, newline="", encoding="utf-8") as f:
    for r in csv.DictReader(f, fieldnames=COLONNES):
        lignes_csv += 1
        uid = r["user_id"]
        if uid not in pays_profil:
            absents += 1
        elif pays_profil[uid] != r["country"]:
            ecarts_pays += 1
        ops.append(UpdateOne(
            {"user_id": uid},
            {"$set": {"analytics": {
                "nb_events": int(r["nb_events"]),
                "total_watch_sec": int(r["total_watch_sec"]),
                "segment": r["segment"],
                "source": SOURCE,
                "enrichi_le": horodatage,
            }}},
        ))
        if len(ops) == LOT:
            envoyer(ops)
            ops = []
if ops:
    envoyer(ops)

enrichis = profiles.count_documents({"analytics": {"$exists": True}})
print(f"profils en base (MongoDB)          : {profiles.count_documents({})}")
print(f"lignes segments (sortie Hive)      : {lignes_csv}")
print(f"profils trouves par user_id        : {trouves}")
print(f"profils modifies                   : {modifies}")
print(f"profils enrichis (analytics)       : {enrichis}")
print(f"profils sans analytics             : {profiles.count_documents({'analytics': {'$exists': False}})}")
print(f"user_id Hive absents de MongoDB    : {absents}")
print(f"ecarts de pays Hive vs profil      : {ecarts_pays}")
print("repartition des segments dans MongoDB :")
for g in profiles.aggregate([{"$group": {"_id": "$analytics.segment", "n": {"$sum": 1}}},
                             {"$sort": {"_id": 1}}]):
    print(f"   {g['_id']:12s} {g['n']}")
print("exemple u000001 analytics :",
      profiles.find_one({"user_id": "u000001"}, {"_id": 0, "analytics": 1})["analytics"])

# ---------------------------------------------------------------------
# B2 - explain() : requete de l'export B3 avec l'index compose,
#      comparee a la meme requete forcee en parcours complet ($natural).
# ---------------------------------------------------------------------
titre("B2 - explain() de la requete d'export")
FILTRE_AUDIENCE = {"analytics.segment": {"$in": SEGMENTS_CIBLES}, "consent_analytics": True}


def chaine_plan(plan):
    plan = plan.get("queryPlan", plan)          # format MongoDB 8 (moteur SBE)
    etapes, bornes = [], None
    while plan:
        nom = plan["stage"]
        if "indexName" in plan:
            nom += f"[{plan['indexName']}]"
            bornes = plan.get("indexBounds")
        etapes.append(nom)
        plan = plan.get("inputStage")
    return " <- ".join(etapes), bornes


for libelle, curseur in [
    ("avec index compose", profiles.find(FILTRE_AUDIENCE)),
    ("parcours complet", profiles.find(FILTRE_AUDIENCE).hint([("$natural", 1)])),
]:
    ex = curseur.explain()
    es = ex["executionStats"]
    plan, bornes = chaine_plan(ex["queryPlanner"]["winningPlan"])
    print(f"[{libelle}] plan : {plan}")
    if bornes:
        print(f"   bornes d'index : {bornes}")
    print(f"   nReturned={es['nReturned']}  totalKeysExamined={es['totalKeysExamined']}  "
          f"totalDocsExamined={es['totalDocsExamined']}  executionTimeMillis={es['executionTimeMillis']}")

# ---------------------------------------------------------------------
# B3 - Sortie transmise a Cassandra : output/audience_cible.jsonl
#  * filtre : profils consentants des segments intensif et actif
#  * projection minimale : user_id, country, plan, analytics
#  * tri par user_id : fichier deterministe (rejouable a l'identique)
# ---------------------------------------------------------------------
titre("B3 - Export de l'audience cible")
projection = {"_id": 0, "user_id": 1, "country": 1, "plan": 1,
              "analytics.nb_events": 1, "analytics.total_watch_sec": 1,
              "analytics.segment": 1, "analytics.source": 1}
exportes = 0
with open(AUDIENCE_JSONL, "w", encoding="utf-8") as f:
    for doc in profiles.find(FILTRE_AUDIENCE, projection).sort("user_id", ASCENDING):
        f.write(json.dumps(doc, ensure_ascii=False) + "\n")
        exportes += 1
print(f"documents exportes : {exportes} -> {AUDIENCE_JSONL}")

# ---------------------------------------------------------------------
# B5 - Controles de coherence de la sortie
# ---------------------------------------------------------------------
titre("B5 - Controles de l'export")
attendus = profiles.count_documents(FILTRE_AUDIENCE)
with open(AUDIENCE_JSONL, encoding="utf-8") as f:
    lignes_export = [json.loads(l) for l in f]
ids = [d["user_id"] for d in lignes_export]
print(f"documents MongoDB correspondant au filtre : {attendus}")
print(f"lignes du fichier audience_cible.jsonl    : {len(lignes_export)}")
print(f"user_id distincts dans le fichier         : {len(set(ids))}")
print(f"segments presents                          : {sorted({d['analytics']['segment'] for d in lignes_export})}")
print("exemple :", lignes_export[0])

titre("B5 - Pourquoi la sortie est plus petite que la source Hive")
cibles = profiles.count_documents({"analytics.segment": {"$in": SEGMENTS_CIBLES}})
print(f"evenements analyses par Hive               : 1 000 000")
print(f"lignes segments_utilisateurs.csv (Hive)    : {lignes_csv}   (agregation : 1 ligne par user_id)")
print(f"  - occasionnels exclus                    : {lignes_csv - cibles}")
print(f"= segments intensif + actif                : {cibles}")
print(f"  - non consentants exclus                 : {cibles - attendus}")
print(f"= audience exportee                        : {attendus}")
print(f"taille segments_utilisateurs.csv           : {SEGMENTS_CSV.stat().st_size:>9} octets")
print(f"taille audience_cible.jsonl                : {AUDIENCE_JSONL.stat().st_size:>9} octets")

titre("B5 - Audience par (pays, segment) : futures partitions Cassandra")
for g in profiles.aggregate([
    {"$match": FILTRE_AUDIENCE},
    {"$group": {"_id": {"pays": "$country", "segment": "$analytics.segment"}, "n": {"$sum": 1}}},
    {"$sort": {"n": -1}},
]):
    print(f"   {g['_id']['pays']:14s} {g['_id']['segment']:9s} {g['n']}")

titre("B5 - explain() d'une requete selective (index pertinent)")
FILTRE_SELECTIF = {"analytics.segment": "intensif", "consent_analytics": True, "country": "Tunisie"}
for libelle, curseur in [
    ("avec index compose", profiles.find(FILTRE_SELECTIF)),
    ("parcours complet", profiles.find(FILTRE_SELECTIF).hint([("$natural", 1)])),
]:
    ex = curseur.explain()
    es = ex["executionStats"]
    plan, _ = chaine_plan(ex["queryPlanner"]["winningPlan"])
    print(f"[{libelle}] plan : {plan}")
    print(f"   nReturned={es['nReturned']}  totalKeysExamined={es['totalKeysExamined']}  "
          f"totalDocsExamined={es['totalDocsExamined']}  executionTimeMillis={es['executionTimeMillis']}")
