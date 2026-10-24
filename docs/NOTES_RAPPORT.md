# Notes de rapport - Pipeline polyglotte

## Corrections de l'environnement fourni
| Problème | Cause | Correction |
|---|---|---|
| Build notebook échoue | `pyspark-notebook:latest` = Python 3.13, `cassandra-driver 3.29.2` sans wheel 3.13 | Image figée par digest + `cassandra-driver==3.29.3` |
| ResourceManager s'arrête | `hadoop-config/` remplace toute la conf : `capacity-scheduler.xml` absent | Ajout de `capacity-scheduler.xml` (file `default`, AM 60 %) |
| Hive s'arrête | Volume `/opt/hive/data` appartient à root, Hive tourne en `hive` | `user: "0:0"` pour le service hive |
| Jobs MR (préventif) | `HADOOP_MAPRED_HOME` absent des conteneurs YARN | Variables `*.env` dans `mapred-site.xml` |
| `hdfs` : JAVA_HOME | Dockerfile ne définit pas JAVA_HOME | `ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64` |
| Beeline : impersonation | `-n jovyan` + HiveServer2 en root, pas de proxyuser | `hive.server2.enable.doAs=false` |
| Hive ne redémarre pas | Fichier `.pid` orphelin après un plantage | Toujours `up -d --force-recreate hive`, jamais `restart` |
| Map : ClassNotFound `commons.collections` | Hadoop 3.4 ne fournit que Commons Collections 4 | `hive.aux.jars.path` = commons-collections-3.2.2.jar |
| Reduce : KryoException (plan) | Hive en Java 8, NodeManager en Java 11 | Image NodeManager en Java 8 (JRE copié depuis l'image Hive, `docker/nodemanager.Dockerfile`) |
| `UNION ALL` : KryoException | Bug du moteur MR (déprécié) de Hive 4 | Requêtes séparées |
| En-têtes comptés (2001 contenus) | `CombineHiveInputFormat` ignore `skip.header.line.count` | `SET hive.input.format=HiveInputFormat` (bonus : 8 maps = 1 par gzip) |
| Map join : MapredLocalTask | Sous-processus local échoue dans le conteneur | `SET hive.exec.submit.local.task.via.child=false` |
| Job de stats après INSERT échoue | Hive 4 calcule les stats de colonnes automatiquement (job MR) | `SET hive.stats.column.autogather=false` |

## Section 3 - Contrôle du volume
- Intégrité : 11/11 fichiers OK (SHA-256) -> preuves/1-0_sha256_integrite.txt
- HDFS : 8 fichiers, 23,4 Mio (24 517 828 o), 1 bloc de 128 Mio par fichier, réplication 1
- Lignes : 1 000 008 = 1 000 000 événements + 8 en-têtes
- Décompressé : 89,4 Mo -> ratio ~3,6:1 (-73 %)
- Parallélisme : gzip non découpable -> 1 map par fichier ; 8 fichiers = 8 splits = jusqu'à 8 maps parallèles
- Compressé vs logique : HDFS ne voit que des octets ; le nombre de lignes n'apparaît qu'à la décompression (lecture Hive)
- Reprise : 1re exécution du dépôt interrompue après 7 fichiers ; script idempotent (rm puis put) relancé sans risque

## Partie A1 - Tables et qualité
- 2 tables EXTERNAL typées (10 et 5 colonnes) sur les répertoires imposés ; DROP/CREATE = script rejouable sans perte de données
- Volumes : 1 000 000 événements, 2 000 contenus (preuves/A1_resultats.txt)
- Contrôles en un seul passage : doublons, format des id (RLIKE), durées NULL/négatives/>24 h, horodatage, domaine event_type -> tous à 0
- Intégrité référentielle (LEFT JOIN catalogue) : 0 orphelin
- 50 000 user_id distincts = nombre de profils (clé d'interconnexion cohérente)
- Anomalie détectée puis corrigée : en-têtes lus comme données (2001 contenus, 8 lignes « country »)

## Partie A2 - Analyse distribuée
- Pays, types, top 10 : identiques au manifeste (preuves/A2_resultats.txt)
- 8 tâches map (1 par gzip) grâce à HiveInputFormat
- Durée moyenne ~uniforme par appareil/qualité (~2 020 s tous types, ~3 600 s lectures) ; l'écart vient du type d'événement (lecture ~3 600 s vs autres ~90 s)
- EXPLAIN top 10 : Stage-6 map join (catalogue diffusé en table de hachage) ; Stage-2 jointure + pré-agrégation côté map puis SHUFFLE (Reduce Output Operator) = opération coûteuse ; Stage-3 ORDER BY global sur 1 reducer (Top-N)
- Statistiques estimées (237 575 lignes) ≠ réelles (1 000 000) : pas de stats sur table externe -> ANALYZE TABLE recommandé

## Partie A3 - Sortie transmise à MongoDB
- Table EXTERNAL segments_utilisateurs sur /inf6303/devoir1/sorties/segments (CSV sans en-tête : user_id,country,nb_events,total_watch_sec,segment)
- INSERT OVERWRITE ... GROUP BY user_id, country (1 pays par utilisateur vérifié) ; segment = CASE sur SUM(watch_seconds) (complément 6 oct.)
- Contrôles : 50 000 lignes = 50 000 user_id ; Σ nb_events = 1 000 000 ; Σ durées = 2 025 552 181 = source
- Segments : actif 30 901, intensif 7 430, occasionnel 11 669 (= manifeste) ; bornes 55 000 / 29 999 / 54 999 respectées
- getmerge -> workspace/output/segments_utilisateurs.csv (50 000 lignes, 1,65 Mo)
- Observation : nb_events moyen croît avec le segment (16,3 / 20,3 / 24,8)

## Partie B - MongoDB
### B1 - Import documentaire
- 50 000 lignes JSON -> 50 000 documents BSON (insert_many par lots de 5 000) ; collection vidée à chaque exécution (rejouable)
- Structure préservée : preferences = sous-document, preferences.genres et devices = tableaux (50 000/50 000) ; signup_date -> date BSON
- Index unique ux_user_id (clé d'interconnexion ; doublon u000001 refusé E11000)
- Index composé ix_segment_consent_country (analytics.segment, consent_analytics, country) = filtre de l'export B3 + accès par segment/pays
### B2 - Enrichissement interconnecté
- segments_utilisateurs.csv (Hive) -> $set analytics {nb_events, total_watch_sec, segment, source, enrichi_le} par user_id, bulk_write, SANS upsert
- 50 000 profils = 50 000 lignes Hive = 50 000 trouvés = 50 000 enrichis ; 0 absent ; 0 écart de pays Hive/MongoDB
- explain() export (70 % des docs) : FETCH <- IXSCAN, docsExamined 34 855 = nReturned, mais 39 ms vs 29 ms en COLLSCAN -> requête peu sélective
- explain() sélectif (Tunisie/intensif) : 382 docs examinés en 0 ms vs 50 000 en 22 ms -> l'index est rentable quand la requête est sélective
### B3 - Sortie transmise à Cassandra
- audience_cible.jsonl : consentants + segments intensif/actif, projection user_id, country, plan, analytics ; tri par user_id
- 34 855 = filtre MongoDB = lignes = user_id distincts
- Réduction : 1 000 000 événements -> 50 000 utilisateurs -> 38 331 (sans 11 669 occasionnels) -> 34 855 (sans 3 476 non consentants)
- En octets : JSONL 6,8 Mo > CSV 1,65 Mo (JSON autodescriptif, noms de champs répétés) mais 13x < 89 Mo d'événements bruts
- Déséquilibre (pays, segment) : Canada/actif 8 792 vs Tunisie/intensif 382 (x23) -> risque de partition chaude en Cassandra

## Partie C - Cassandra
### C1 - Modèle orienté requête
- Requête cible : top utilisateurs d'un (pays, segment) par score décroissant
- PRIMARY KEY ((country, segment), score, user_id) WITH CLUSTERING ORDER BY (score DESC, user_id ASC)
- Partition (country, segment) : 1 requête = 1 partition (1 nœud) ; clustering score DESC : déjà trié sur disque, top N = N premières lignes
- user_id en clustering : départage les scores égaux (sinon écrasement par upsert) -> 34 855 lignes conservées
- score = total_watch_sec (mesure de segmentation) ; plan et nb_events dénormalisés (pas de jointure) ; Cassandra = vue de service, MongoDB = source
- Keyspace SimpleStrategy RF=1 (1 nœud) ; production : NetworkTopologyStrategy RF=3
- Partition chaude : Canada/actif 8 792 lignes vs Tunisie/intensif 382 (x23) ; parade : bucket ((country, segment, bucket)) avec bucket = hash(user_id) % N
- PLAN est un mot réservé CQL (Cassandra 5) : colonne affichée "plan"
### C2 - Chargement
- Lecture de audience_cible.jsonl ; INSERT préparé + execute_concurrent_with_args (64 en parallèle)
- 34 855 exportés = 34 855 insérés (0 échec) = COUNT global 34 855 ; 16/16 partitions identiques ; 5,6 s (~6 300 lignes/s)
### C3 - Requêtes de service
- Top 10 Canada/intensif : 1 partition, 3,8 ms ; identique au top 10 recalculé depuis le JSONL (True)
- Top 10 Senegal/intensif : 1 partition, 1,8 ms
- WHERE plan = 'premium' : refusée (data filtering) ; avec ALLOW FILTERING : 87 ms, balayage complet -> non naturel
- Nouvelle table audience_par_plan PRIMARY KEY ((plan, country), score, user_id) : 34 855 lignes, top 5 premium/Canada en 1,7 ms
- (plan) seul = 3 partitions énormes (gratuit 16 831) -> chaudes ; (plan, country) = 24 partitions ; top global par plan = 8 lectures + fusion applicative
- Cohérence inter-tables : u049062 (95 407) et u029858 (90 175) identiques dans les deux tables