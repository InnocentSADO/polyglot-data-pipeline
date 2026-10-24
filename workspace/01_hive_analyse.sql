-- =====================================================================
-- INF6303 - Devoir 1 - Partie A : Hive/HiveQL
-- Execution :
--   beeline -u jdbc:hive2://hive:10000/default -n jovyan \
--           -f /home/jovyan/work/01_hive_analyse.sql
-- Script rejouable : DROP/CREATE des tables externes (les fichiers HDFS
-- ne sont jamais supprimes par un DROP d'une table EXTERNAL).
-- =====================================================================

CREATE DATABASE IF NOT EXISTS inf6303_devoir1;
USE inf6303_devoir1;


-- Format d'entree : HiveInputFormat (1 split par fichier gzip).
-- Le format par defaut (CombineHiveInputFormat) regroupe les 8 fichiers
-- dans un seul split et IGNORE skip.header.line.count (constate :
-- 2001 contenus et 8 lignes d'en-tete comptees comme evenements).
SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;

-- Map join : la table de hachage du petit cote (catalogue, 2 000 lignes)
-- est construite dans le processus HiveServer2 plutot que dans un
-- sous-processus local (qui echoue dans ce conteneur : MapredLocalTask).
SET hive.exec.submit.local.task.via.child=false;

-- =====================================================================
-- A1. TABLES ET QUALITE
-- =====================================================================

-- A1.1 Table externe des evenements (10 colonnes typees)
--  * EXTERNAL : Hive ne gere que le schema ; les donnees restent dans HDFS.
--  * Les .csv.gz sont lus directement (codec gzip detecte par l'extension).
--  * skip.header.line.count=1 : ignore l'en-tete de CHACUN des 8 fichiers
--    (sinon 1 000 008 lignes au lieu de 1 000 000).
--  * timestamp.formats : event_ts est au format ISO 8601 avec un 'T'.
DROP TABLE IF EXISTS evenements_massifs;
CREATE EXTERNAL TABLE evenements_massifs (
  event_id      STRING,
  user_id       STRING,
  content_id    STRING,
  event_type    STRING,
  event_ts      TIMESTAMP,
  country       STRING,
  device        STRING,
  watch_seconds INT,
  quality       STRING,
  session_id    STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'field.delim'       = ',',
  'timestamp.formats' = "yyyy-MM-dd'T'HH:mm:ss"
)
STORED AS TEXTFILE
LOCATION '/inf6303/devoir1/evenements'
TBLPROPERTIES ('skip.header.line.count' = '1');

-- A1.2 Table externe du catalogue (5 colonnes typees)
DROP TABLE IF EXISTS catalogue_contenus;
CREATE EXTERNAL TABLE catalogue_contenus (
  content_id   STRING,
  title        STRING,
  genre        STRING,
  release_year INT,
  duration_sec INT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/inf6303/devoir1/catalogue'
TBLPROPERTIES ('skip.header.line.count' = '1');

-- A1.3 Volumes attendus : exactement 1 000 000 evenements et 2 000 contenus
-- (deux requetes separees : le moteur MapReduce de Hive 4 echoue a
--  deserialiser le plan reducer d'un UNION ALL -> KryoException)
SELECT COUNT(*) AS nb_evenements FROM evenements_massifs;
SELECT COUNT(*) AS nb_contenus   FROM catalogue_contenus;

-- A1.4 Qualite des evenements : tous les controles en UN SEUL passage
--  * Identifiants : NULL ou format non conforme (expression reguliere)
--  * Duree : NULL (valeur non numerique -> NULL a la lecture), negative,
--    ou superieure a 24 h (aberrante pour un evenement unique)
--  * Horodatage non interpretable, type d'evenement hors domaine
--  * Doublons : COUNT(DISTINCT event_id) doit egaler COUNT(*)
SELECT
  COUNT(*)                                                                  AS nb_lignes,
  COUNT(DISTINCT event_id)                                                  AS event_id_distincts,
  COUNT(DISTINCT user_id)                                                   AS user_id_distincts,
  SUM(IF(event_id   IS NULL OR NOT event_id   RLIKE '^e[0-9]{9}$', 1, 0))  AS event_id_invalides,
  SUM(IF(user_id    IS NULL OR NOT user_id    RLIKE '^u[0-9]{6}$', 1, 0))  AS user_id_invalides,
  SUM(IF(content_id IS NULL OR NOT content_id RLIKE '^c[0-9]{5}$', 1, 0))  AS content_id_invalides,
  SUM(IF(watch_seconds IS NULL, 1, 0))                                      AS duree_nulle_non_numerique,
  SUM(IF(watch_seconds < 0, 1, 0))                                          AS duree_negative,
  SUM(IF(watch_seconds > 86400, 1, 0))                                      AS duree_sup_24h,
  SUM(IF(event_ts IS NULL, 1, 0))                                           AS horodatage_invalide,
  SUM(IF(event_type NOT IN ('lecture','pause','recherche','abandon','ajout_favori'), 1, 0)) AS type_hors_domaine,
  MIN(watch_seconds)                                                        AS duree_min,
  MAX(watch_seconds)                                                        AS duree_max,
  MIN(event_ts)                                                             AS premier_evenement,
  MAX(event_ts)                                                             AS dernier_evenement
FROM evenements_massifs;

-- A1.5 Integrite referentielle : evenements dont le contenu est absent du catalogue
SELECT COUNT(*) AS evenements_orphelins
FROM evenements_massifs e
LEFT JOIN catalogue_contenus c ON e.content_id = c.content_id
WHERE c.content_id IS NULL;

-- A1.6 Qualite du catalogue
SELECT
  COUNT(*)                                                                  AS nb_contenus,
  COUNT(DISTINCT content_id)                                                AS content_id_distincts,
  SUM(IF(content_id IS NULL OR NOT content_id RLIKE '^c[0-9]{5}$', 1, 0))  AS content_id_invalides,
  SUM(IF(duration_sec IS NULL OR duration_sec <= 0, 1, 0))                  AS duree_catalogue_invalide,
  MIN(release_year)                                                         AS annee_min,
  MAX(release_year)                                                         AS annee_max
FROM catalogue_contenus;

-- TODO A2 : analyse distribuee
-- TODO A3 : segments par utilisateur -> /inf6303/devoir1/sorties/segments