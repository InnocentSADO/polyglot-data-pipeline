# Notes de rapport - Pipeline polyglotte

## Corrections de l'environnement fourni
| Problème | Cause | Correction |
|---|---|---|
| Build notebook échoue | `pyspark-notebook:latest` = Python 3.13, `cassandra-driver 3.29.2` sans wheel 3.13 | Image figée par digest + `cassandra-driver==3.29.3` |
| ResourceManager s'arrête | `hadoop-config/` remplace toute la conf : `capacity-scheduler.xml` absent | Ajout de `capacity-scheduler.xml` (file `default`, AM 60 %) |
| Hive s'arrête | Volume `/opt/hive/data` appartient à root, Hive tourne en `hive` | `user: "0:0"` pour le service hive |
| Jobs MR (préventif) | `HADOOP_MAPRED_HOME` absent des conteneurs YARN | Variables `*.env` dans `mapred-site.xml` |
| `hdfs` : JAVA_HOME | Dockerfile ne définit pas JAVA_HOME | `ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64` |

## Section 3 - Contrôle du volume
- Intégrité : 11/11 fichiers OK (SHA-256) -> preuves/1-0_sha256_integrite.txt
- HDFS : 8 fichiers, 23,4 Mio (24 517 828 o), 1 bloc de 128 Mio par fichier, réplication 1
- Lignes : 1 000 008 = 1 000 000 événements + 8 en-têtes
- Décompressé : 89,4 Mo -> ratio ~3,6:1 (-73 %)
- Parallélisme : gzip non découpable -> 1 map par fichier ; 8 fichiers = 8 splits = jusqu'à 8 maps parallèles
- Compressé vs logique : HDFS ne voit que des octets ; le nombre de lignes n'apparaît qu'à la décompression (lecture Hive)
- Reprise : 1re exécution du dépôt interrompue après 7 fichiers ; script idempotent (rm puis put) relancé sans risque

## Partie A2 - Analyse distribuée
- Pays, types, top 10 : identiques au manifeste (preuves/A2_resultats.txt)
- 8 tâches map (1 par gzip) grâce à HiveInputFormat
- Durée moyenne ~uniforme par appareil/qualité (~2 020 s tous types, ~3 600 s lectures) ; l'écart vient du type d'événement (lecture ~3 600 s vs autres ~90 s)
- EXPLAIN top 10 : Stage-6 map join (catalogue diffusé en table de hachage) ; Stage-2 jointure + pré-agrégation côté map puis SHUFFLE (Reduce Output Operator) = opération coûteuse ; Stage-3 ORDER BY global sur 1 reducer (Top-N)
- Statistiques estimées (237 575 lignes) ≠ réelles (1 000 000) : pas de stats sur table externe -> ANALYZE TABLE recommandé