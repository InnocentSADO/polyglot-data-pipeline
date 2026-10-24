"""DEVOIR 1 - Partie D1 : tracabilite de 3 user_id, de Hive a MongoDB puis Cassandra.

Un utilisateur par chemin possible dans le pipeline :
  u000001 : intensif, consentant     -> present dans les 3 systemes
  u000004 : actif, NON consentant    -> exclu a l'export MongoDB (consentement)
  u000006 : occasionnel, consentant  -> exclu a l'export MongoDB (segment)
Execution : python /home/jovyan/work/05_tracabilite.py
"""
import csv, json, subprocess
from pathlib import Path
from pymongo import MongoClient
from cassandra.cluster import Cluster

USERS = ["u000001", "u000004", "u000006"]
OUT = Path("/home/jovyan/work/output")
IN = ",".join(f"'{u}'" for u in USERS)
BEELINE = ["beeline", "-u", "jdbc:hive2://hive:10000/default", "-n", "jovyan",
           "--silent=true", "--showHeader=false", "--outputformat=csv2"]


def titre(t):
    print(f"\n=== {t} ===")


def hive(sql):
    r = subprocess.run(BEELINE + ["-e", sql], capture_output=True, text=True)
    lignes = [l.split(",") for l in r.stdout.splitlines() if l.startswith("u0")]
    if not lignes:
        raise SystemExit("Hive : aucun resultat\n" + r.stderr[-1500:])
    return {l[0]: l for l in lignes}


# 1. Hive : recalcul a partir des evenements bruts (source de verite HDFS)
titre("1. Hive - recalcul depuis les 1 000 000 evenements bruts (HDFS)")
brut = hive("SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat; "
            "SELECT user_id, country, COUNT(*), SUM(watch_seconds) "
            "FROM inf6303_devoir1.evenements_massifs "
            f"WHERE user_id IN ({IN}) GROUP BY user_id, country;")
for u in USERS:
    print(f"   {u} : pays={brut[u][1]} nb_events={brut[u][2]} total_watch_sec={brut[u][3]}")

# 2. Hive : table de sortie posee sur /inf6303/devoir1/sorties/segments
titre("2. Hive - table segments_utilisateurs (/inf6303/devoir1/sorties/segments)")
sortie = hive("SELECT user_id, country, nb_events, total_watch_sec, segment "
              f"FROM inf6303_devoir1.segments_utilisateurs WHERE user_id IN ({IN});")
for u in USERS:
    print(f"   {u} : {sortie[u][1:]}")

# 3. Fichier de passage Hive -> MongoDB
titre("3. Fichier de passage output/segments_utilisateurs.csv")
csv_ = {r[0]: r for r in csv.reader(open(OUT / "segments_utilisateurs.csv", encoding="utf-8"))
        if r[0] in USERS}
for u in USERS:
    print(f"   {u} : {csv_[u][1:]}")

# 4. MongoDB : profil enrichi
titre("4. MongoDB - inf6303_devoir1.profils")
profils = MongoClient("mongodb://inf6303:inf6303@mongo:27017/?authSource=admin")["inf6303_devoir1"]["profils"]
docs = {d["user_id"]: d for d in profils.find(
    {"user_id": {"$in": USERS}},
    {"_id": 0, "user_id": 1, "country": 1, "plan": 1, "consent_analytics": 1,
     "analytics.nb_events": 1, "analytics.total_watch_sec": 1, "analytics.segment": 1})}
for u in USERS:
    print(f"   {u} : {docs[u]}")

# 5. Fichier de passage MongoDB -> Cassandra
titre("5. Fichier de passage output/audience_cible.jsonl")
jsonl = {}
for l in open(OUT / "audience_cible.jsonl", encoding="utf-8"):
    d = json.loads(l)
    if d["user_id"] in USERS:
        jsonl[d["user_id"]] = d
for u in USERS:
    print(f"   {u} : {'PRESENT ' + str(jsonl[u]) if u in jsonl else 'ABSENT'}")

# 6. Cassandra : lecture par cle primaire complete + rang dans la partition
titre("6. Cassandra - audience_par_pays_segment (cle primaire complete)")
cluster = Cluster(["cassandra"])
s = cluster.connect("inf6303_devoir1")
point = s.prepare("SELECT user_id, score, plan, nb_events FROM audience_par_pays_segment "
                  "WHERE country = ? AND segment = ? AND score = ? AND user_id = ?")
rang = s.prepare("SELECT COUNT(*) FROM audience_par_pays_segment "
                 "WHERE country = ? AND segment = ? AND score > ?")
cass = {}
for u in USERS:
    a = docs[u]["analytics"]
    cle = (docs[u]["country"], a["segment"], a["total_watch_sec"])
    row = s.execute(point, cle + (u,)).one()
    if row:
        cass[u] = (row, s.execute(rang, cle).one()[0] + 1)
        print(f"   {u} : PRESENT score={row.score} plan={row.plan} nb_events={row.nb_events} "
              f"rang={cass[u][1]} dans {cle[0]}/{cle[1]}")
    else:
        print(f"   {u} : ABSENT (partition {cle[0]}/{cle[1]})")
cluster.shutdown()

# Synthese : tableau comparatif + explication des differences
titre("Synthese D1 (tableau Markdown pour le rapport)")
print("| user_id | Hive brut (nb / duree) | Hive sortie (segment) | CSV | MongoDB analytics (nb / duree / segment) "
      "| consent | plan | audience_cible.jsonl | Cassandra (score / rang) | Explication |")
print("|---|---|---|---|---|---|---|---|---|---|")
for u in USERS:
    a = docs[u]["analytics"]
    coherent = (int(brut[u][2]) == int(sortie[u][2]) == int(csv_[u][2]) == a["nb_events"]
                and int(brut[u][3]) == int(sortie[u][3]) == int(csv_[u][3]) == a["total_watch_sec"]
                and sortie[u][4] == csv_[u][4] == a["segment"])
    if u in cass:
        expl = "present partout, valeurs identiques"
    elif not docs[u]["consent_analytics"]:
        expl = "absent de Cassandra : non consentant (filtre de l'export B3)"
    elif a["segment"] not in ("intensif", "actif"):
        expl = f"absent de Cassandra : segment {a['segment']} (filtre de l'export B3)"
    else:
        expl = "ANOMALIE : devrait etre present"
    print(f"| {u} | {brut[u][2]} / {brut[u][3]} | {sortie[u][4]} | {csv_[u][4]} "
          f"| {a['nb_events']} / {a['total_watch_sec']} / {a['segment']} "
          f"| {docs[u]['consent_analytics']} | {docs[u]['plan']} "
          f"| {'oui' if u in jsonl else 'non'} "
          f"| {str(cass[u][0].score) + ' / ' + str(cass[u][1]) if u in cass else 'absent'} "
          f"| {expl}{'' if coherent else ' -- ECART DE VALEURS'} |")
