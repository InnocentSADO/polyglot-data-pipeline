#!/usr/bin/env bash
# =====================================================================
# Rejoue le pipeline complet Hive -> MongoDB -> Cassandra, de bout en bout.
# A lancer dans le terminal JupyterLab (conteneur notebook) :
#   bash /home/jovyan/work/run_pipeline.sh
# Prerequis : docker compose --profile hadoop --profile nosql up -d --build
# Duree indicative : 20 a 25 minutes (dont ~15 min de MapReduce).
# =====================================================================
set -euo pipefail
cd /home/jovyan/work
mkdir -p output preuves
HIVE_URL="jdbc:hive2://hive:10000/default"
debut=$(date +%s)
etape() { echo; echo "=== [$1] $2 ($(date +%H:%M:%S)) ==="; }

etape "0/7" "Attente de HiveServer2"
for i in $(seq 1 30); do
  if beeline -u "$HIVE_URL" -n jovyan --silent=true -e "SELECT 1;" >/dev/null 2>&1; then
    echo "HiveServer2 pret"; break
  fi
  [ "$i" -eq 30 ] && { echo "HiveServer2 injoignable"; exit 1; }
  echo "HiveServer2 pas encore pret ($i/30), nouvel essai dans 10 s"; sleep 10
done

etape "1/7" "Depot des donnees dans HDFS"
bash 00_deposer_dans_hdfs.sh 2>/dev/null | tee preuves/run_1_depot_hdfs.txt

etape "2/7" "Partie A - Hive (A1 qualite, A2 analyse, A3 segments)"
beeline -u "$HIVE_URL" -n jovyan -f 01_hive_analyse.sql \
  2> preuves/run_2_hive_journal.txt | tee preuves/run_2_hive_resultats.txt

etape "3/7" "Rapatriement de la sortie Hive (getmerge)"
rm -f output/segments_utilisateurs.csv output/.segments_utilisateurs.csv.crc
hdfs dfs -getmerge /inf6303/devoir1/sorties/segments output/segments_utilisateurs.csv 2>/dev/null
echo "segments_utilisateurs.csv : $(wc -l < output/segments_utilisateurs.csv) lignes"

etape "4/7" "Partie B - MongoDB"
python 02_mongodb_enrichissement.py | tee preuves/run_4_mongodb.txt

etape "5/7" "Partie C - Cassandra"
python 03_cassandra_publication.py | tee preuves/run_5_cassandra.txt

etape "6/7" "Partie D1 - Tracabilite de 3 user_id"
python 05_tracabilite.py | tee preuves/run_6_tracabilite.txt

etape "7/7" "Partie D1 - Validation de l'enonce"
python 04_validation_pipeline.py | tee preuves/run_7_validation.txt

echo; echo "Pipeline termine en $(( ($(date +%s) - debut) / 60 )) min."
