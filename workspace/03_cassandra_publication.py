"""DEVOIR 1 - Partie C : publication faible latence dans Cassandra.

Entree : /home/jovyan/work/output/audience_cible.jsonl (sortie MongoDB, partie B)
Table  : inf6303_devoir1.audience_par_pays_segment
Requete servie : top utilisateurs d'un (pays, segment), score decroissant.
Execution : python /home/jovyan/work/03_cassandra_publication.py
Rejouable : la table est recreee a chaque execution.
"""
import json, time
from collections import Counter
from pathlib import Path
from cassandra.cluster import Cluster, NoHostAvailable
from cassandra.concurrent import execute_concurrent_with_args

AUDIENCE_JSONL = Path("/home/jovyan/work/output/audience_cible.jsonl")
KEYSPACE = "inf6303_devoir1"
TABLE = "audience_par_pays_segment"


def titre(t):
    print(f"\n=== {t} ===")


# ---------------------------------------------------------------------
# C1 - Connexion (Cassandra peut demander ~2 min avant d'accepter) + keyspace
# ---------------------------------------------------------------------
titre("C1 - Connexion et keyspace")
for essai in range(1, 25):
    try:
        cluster = Cluster(["cassandra"], port=9042)
        session = cluster.connect()
        break
    except NoHostAvailable:
        print(f"Cassandra pas encore prete (essai {essai}/24), nouvel essai dans 5 s")
        time.sleep(5)
else:
    raise SystemExit("Cassandra injoignable apres 2 minutes")
session.default_timeout = 60
print("Connecte a", cluster.metadata.cluster_name,
      "| version", session.execute("SELECT release_version FROM system.local").one().release_version)

# 1 seul noeud -> SimpleStrategy, RF=1 (en production : NetworkTopologyStrategy, RF=3)
session.execute(f"""
    CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
    WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
""")
session.set_keyspace(KEYSPACE)

# ---------------------------------------------------------------------
# C1 - Table concue pour LA requete : top utilisateurs par (pays, segment)
#  * cle de partition composee (country, segment) : 1 requete = 1 partition
#  * clustering (score DESC, user_id) : lignes deja triees sur disque ;
#    user_id departage les scores egaux (sinon ecrasement par upsert)
#  * score = total_watch_sec (mesure qui definit les segments)
#  * plan, nb_events : denormalises (pas de jointure en Cassandra)
# ---------------------------------------------------------------------
session.execute(f"DROP TABLE IF EXISTS {TABLE}")
session.execute(f"""
    CREATE TABLE {TABLE} (
      country   text,
      segment   text,
      score     bigint,
      user_id   text,
      plan      text,
      nb_events int,
      PRIMARY KEY ((country, segment), score, user_id)
    ) WITH CLUSTERING ORDER BY (score DESC, user_id ASC)
""")
print(cluster.metadata.keyspaces[KEYSPACE].tables[TABLE].export_as_string())

# ---------------------------------------------------------------------
# C2 - Chargement de audience_cible.jsonl avec une requete PREPAREE
#      (analysee une seule fois par Cassandra, puis executee 34 855 fois
#       avec des valeurs liees : plus rapide et sans injection CQL)
# ---------------------------------------------------------------------
titre("C2 - Chargement avec requete preparee")
insert = session.prepare(f"""
    INSERT INTO {TABLE} (country, segment, score, user_id, plan, nb_events)
    VALUES (?, ?, ?, ?, ?, ?)
""")
valeurs, par_partition = [], Counter()
with open(AUDIENCE_JSONL, encoding="utf-8") as f:
    for ligne in f:
        d = json.loads(ligne)
        a = d["analytics"]
        valeurs.append((d["country"], a["segment"], a["total_watch_sec"],
                        d["user_id"], d["plan"], a["nb_events"]))
        par_partition[(d["country"], a["segment"])] += 1

debut = time.time()
resultats = execute_concurrent_with_args(session, insert, valeurs,
                                         concurrency=64, raise_on_first_error=False)
reussis = sum(1 for ok, _ in resultats if ok)
duree = time.time() - debut
print(f"documents dans audience_cible.jsonl : {len(valeurs)}")
print(f"insertions reussies                 : {reussis}  (echecs : {len(valeurs) - reussis})")
print(f"duree du chargement                 : {duree:.1f} s ({reussis / duree:.0f} lignes/s)")

titre("C2 - Comparaison exportes MongoDB vs lignes chargees Cassandra")
total = session.execute(f"SELECT COUNT(*) FROM {TABLE}").one()[0]
print(f"lignes dans la table (COUNT global) : {total}")
compte = session.prepare(f"SELECT COUNT(*) FROM {TABLE} WHERE country = ? AND segment = ?")
ecarts = 0
print(f"{'partition (pays, segment)':32s} {'fichier':>8s} {'Cassandra':>10s}")
for (pays, seg), n in sorted(par_partition.items(), key=lambda x: -x[1]):
    nc = session.execute(compte, (pays, seg)).one()[0]
    ecarts += (nc != n)
    print(f"{pays + ' / ' + seg:32s} {n:8d} {nc:10d}{'' if nc == n else '  <-- ECART'}")
print(f"partitions : {len(par_partition)} | partitions en ecart : {ecarts}")

# ---------------------------------------------------------------------
# C3 - Requetes de service
# ---------------------------------------------------------------------
from cassandra import InvalidRequest

top_par_partition = session.prepare(f"""
    SELECT user_id, score, plan, nb_events FROM {TABLE}
    WHERE country = ? AND segment = ? LIMIT ?
""")


def afficher_top(pays, segment, n=10):
    titre(f"C3 - Top {n} {pays} / {segment} (score decroissant)")
    rs = session.execute(top_par_partition, (pays, segment, n), trace=True)
    lignes = list(rs)
    print(f"{'rang':>4s} {'user_id':8s} {'score':>8s} {'plan':9s} {'nb_events':>9s}")
    for i, r in enumerate(lignes, 1):
        print(f"{i:4d} {r.user_id:8s} {r.score:8d} {r.plan:9s} {r.nb_events:9d}")
    trace = rs.get_query_trace()
    lectures = [e.description for e in trace.events if "partition" in e.description.lower()]
    print(f"trace : duree serveur {trace.duration.total_seconds() * 1000:.1f} ms ; "
          f"{len(trace.events)} evenements ; ex. : {lectures[:1]}")
    return [r.user_id for r in lignes]


# 1) Top 10 Canada/intensif + verification croisee avec le fichier MongoDB
top_cassandra = afficher_top("Canada", "intensif")
fichier = [json.loads(l) for l in open(AUDIENCE_JSONL, encoding="utf-8")]
top_fichier = [d["user_id"] for d in sorted(
    (d for d in fichier if d["country"] == "Canada" and d["analytics"]["segment"] == "intensif"),
    key=lambda d: (-d["analytics"]["total_watch_sec"], d["user_id"]))[:10]]
print("verification : top 10 Cassandra == top 10 recalcule depuis audience_cible.jsonl ->",
      top_cassandra == top_fichier)

# 2) Autre couple pertinent : 2e marche, segment intensif
afficher_top("Senegal", "intensif")

# 3) Recherche globale par plan : non naturelle pour cette table
titre("C3 - Recherche globale par plan (premium) sur la table (pays, segment)")
requete_plan = f"SELECT user_id, country, segment, score FROM {TABLE} WHERE plan = 'premium' LIMIT 5"
try:
    session.execute(requete_plan)
    print("ERREUR : la requete a ete acceptee")
except InvalidRequest as e:
    print("REFUSEE par Cassandra :", str(e).split("message=")[-1][:220])

rs = session.execute(f"SELECT COUNT(*) FROM {TABLE} WHERE plan = 'premium' ALLOW FILTERING", trace=True)
trace = rs.get_query_trace()
print(f"avec ALLOW FILTERING : {rs.one()[0]} lignes premium trouvees, mais "
      f"{trace.duration.total_seconds() * 1000:.1f} ms et {len(trace.events)} evenements de trace : "
      f"balayage de TOUTES les partitions puis filtrage (cout proportionnel a la table entiere)")

# 4) Table supplementaire adaptee au nouveau besoin (une table par requete)
titre("C3 - Table supplementaire audience_par_plan : PRIMARY KEY ((plan, country), score, user_id)")
session.execute("DROP TABLE IF EXISTS audience_par_plan")
session.execute("""
    CREATE TABLE audience_par_plan (
      plan      text,
      country   text,
      score     bigint,
      user_id   text,
      segment   text,
      nb_events int,
      PRIMARY KEY ((plan, country), score, user_id)
    ) WITH CLUSTERING ORDER BY (score DESC, user_id ASC)
""")
insert_plan = session.prepare("""
    INSERT INTO audience_par_plan (plan, country, score, user_id, segment, nb_events)
    VALUES (?, ?, ?, ?, ?, ?)
""")
res = execute_concurrent_with_args(
    session, insert_plan,
    [(d["plan"], d["country"], d["analytics"]["total_watch_sec"], d["user_id"],
      d["analytics"]["segment"], d["analytics"]["nb_events"]) for d in fichier],
    concurrency=64)
print(f"lignes chargees : {sum(1 for ok, _ in res if ok)} / {len(fichier)}")
rs = session.execute(
    "SELECT user_id, score, segment FROM audience_par_plan "
    "WHERE plan = 'premium' AND country = 'Canada' LIMIT 5", trace=True)
for r in rs:
    print(f"   {r.user_id}  {r.score:8d}  {r.segment}")
print(f"trace : {rs.get_query_trace().duration.total_seconds() * 1000:.1f} ms, lecture d'une seule partition")

cluster.shutdown()
