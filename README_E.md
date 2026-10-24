# INF6303 - Devoir 1 : README_E
<!--
## Membre
| Nom | Matricule | Courriel |
|---|---|---|
| Innocent Sado | À COMPLÉTER | À COMPLÉTER |

## Répartition du travail
Travail réalisé **seul** : l'ensemble des parties a été conçu, exécuté, vérifié et documenté par le même membre.

| Partie | Contenu | Responsable |
|---|---|---|
| Environnement | Diagnostic et correction de l'environnement Docker fourni (13 corrections, voir `docs/NOTES_RAPPORT.md`) | Innocent Sado |
| Section 3 | Dépôt HDFS, contrôles d'intégrité (SHA-256) et de volume | Innocent Sado |
| A - Hive | Tables externes, qualité, analyse distribuée, EXPLAIN, segmentation | Innocent Sado |
| B - MongoDB | Import documentaire, index, enrichissement, explain(), export | Innocent Sado |
| C - Cassandra | Modèle orienté requête, chargement préparé, requêtes de service | Innocent Sado |
| D - Validation | Traçabilité de 3 user_id, architecture, test de remise à zéro | Innocent Sado |

-->

## Prérequis
- Docker Desktop (testé avec Docker 29.7.2, Compose v5.5.0), **au moins 10 Go de RAM** alloués à Docker pour tous les services.
- **Chemin du projet sans accents ni espaces** (ex. `C:\projets\polyglot-data-pipeline`) : un chemin non ASCII fait échouer la construction simultanée des images (`x-docker-expose-session-sharedkey contains non-printable ASCII characters`).
- Les données fournies par le cours, copiées dans `data/` à la racine (non versionnées) :
  `data/evenements/evenements_part_01..08.csv.gz`, `data/catalogue_contenus.csv`, `data/profils_utilisateurs.jsonl.gz`, `data/MANIFESTE_DONNEES.json`, `data/SHA256SUMS.txt`.

## Procédure de reproduction (environnement remis à zéro)

**1. PowerShell, à la racine du projet : remise à zéro puis démarrage**
```powershell
docker compose --profile hadoop --profile nosql down -v
docker compose --profile hadoop --profile nosql up -d --build
docker compose --profile hadoop --profile nosql ps   # 8 services Up, Cassandra (healthy) après ~2 min
```

**2. JupyterLab** : ouvrir http://localhost:8888/lab?token=inf6303, puis *File → New → Terminal*.

**3. Contrôle d'intégrité des données (facultatif)**
```bash
cd /home/jovyan/data && sha256sum -c SHA256SUMS.txt
```

**4. Pipeline complet en une commande (20 à 25 min)**
```bash
bash /home/jovyan/work/run_pipeline.sh
```
Étapes enchaînées : dépôt HDFS → Hive (A1, A2, A3) → `getmerge` → MongoDB (B) → Cassandra (C) → traçabilité (D1) → validation.
Preuves produites : `workspace/preuves/run_*.txt` ; sorties : `workspace/output/`.

**5. Exécution étape par étape (équivalent)**
```bash
cd /home/jovyan/work
bash 00_deposer_dans_hdfs.sh
beeline -u jdbc:hive2://hive:10000/default -n jovyan -f 01_hive_analyse.sql
hdfs dfs -getmerge /inf6303/devoir1/sorties/segments output/segments_utilisateurs.csv
python 02_mongodb_enrichissement.py
python 03_cassandra_publication.py
python 05_tracabilite.py
python 04_validation_pipeline.py
```

## Résultats attendus
| Contrôle | Valeur |
|---|---|
| Événements / contenus / profils | 1 000 000 / 2 000 / 50 000 |
| Segments (Hive = MongoDB) | actif 30 901, intensif 7 430, occasionnel 11 669 |
| `segments_utilisateurs.csv` | 50 000 lignes, 1 654 528 octets |
| `audience_cible.jsonl` = lignes Cassandra | 34 855 lignes, 6 802 704 octets |

Reproductibilité vérifiée : après `down -v` et réexécution, les deux fichiers de sortie sont identiques à l'octet près à la version de référence.

## Points d'attention
- Pour redémarrer Hive : `docker compose --profile hadoop up -d --force-recreate hive` (**jamais** `restart` : fichier `.pid` orphelin).
- Les commandes `hdfs`, `beeline` et `python` s'exécutent dans le **terminal JupyterLab** (conteneur `notebook`), pas dans PowerShell.
- Les avertissements `log4j.properties is not found` et `SLF4J multiple bindings` sont sans conséquence.

## Contenu de la remise
| Élément | Emplacement |
|---|---|
| Rapport | `rapport.pdf` |
| Script Hive complété | `workspace/01_hive_analyse.sql` |
| Script MongoDB complété | `workspace/02_mongodb_enrichissement.py` |
| Script Cassandra complété | `workspace/03_cassandra_publication.py` |
| Sorties | `workspace/output/segments_utilisateurs.csv`, `workspace/output/audience_cible.jsonl` |
| Scripts ajoutés | `workspace/05_tracabilite.py`, `workspace/run_pipeline.sh` |
| Environnement corrigé | `compose.yaml`, `docker/`, `hadoop-config/`, `hive-config/` |