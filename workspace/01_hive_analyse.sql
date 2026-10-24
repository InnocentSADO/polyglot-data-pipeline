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

-- Statistiques de colonnes automatiques apres INSERT : desactivees.
-- Hive 4 lance un job MR supplementaire apres chaque INSERT ; ce job
-- echoue (meme bug de plan que UNION ALL sur le moteur MR deprecie).
SET hive.stats.column.autogather=false;

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

-- =====================================================================
-- A2. ANALYSE DISTRIBUEE
-- =====================================================================

-- A2.1 Repartition des evenements par pays
SELECT country, COUNT(*) AS nb_evenements
FROM evenements_massifs
GROUP BY country
ORDER BY nb_evenements DESC;

-- A2.2 Repartition des evenements par type
SELECT event_type, COUNT(*) AS nb_evenements
FROM evenements_massifs
GROUP BY event_type
ORDER BY nb_evenements DESC;

-- A2.3 Croisement pays x type (tableau pivot, 1 seul passage)
SELECT country,
       SUM(IF(event_type = 'lecture',      1, 0)) AS lecture,
       SUM(IF(event_type = 'pause',        1, 0)) AS pause,
       SUM(IF(event_type = 'recherche',    1, 0)) AS recherche,
       SUM(IF(event_type = 'abandon',      1, 0)) AS abandon,
       SUM(IF(event_type = 'ajout_favori', 1, 0)) AS ajout_favori,
       COUNT(*)                                   AS total
FROM evenements_massifs
GROUP BY country
ORDER BY total DESC;

-- A2.4 Top 10 des contenus selon la duree totale (jointure avec le catalogue)
SELECT e.content_id, c.title, c.genre,
       SUM(e.watch_seconds) AS total_watch_sec,
       COUNT(*)             AS nb_evenements
FROM evenements_massifs e
JOIN catalogue_contenus c ON e.content_id = c.content_id
GROUP BY e.content_id, c.title, c.genre
ORDER BY total_watch_sec DESC
LIMIT 10;

-- A2.5 Duree moyenne par appareil et qualite
--  * duree_moy_tous    : tous types d'evenements (pause/recherche ~ 90 s)
--  * duree_moy_lecture : evenements 'lecture' seulement (plus representatif)
SELECT device, quality,
       COUNT(*)                                                       AS nb_evenements,
       ROUND(AVG(watch_seconds), 1)                                   AS duree_moy_tous,
       ROUND(AVG(IF(event_type = 'lecture', watch_seconds, NULL)), 1) AS duree_moy_lecture
FROM evenements_massifs
GROUP BY device, quality
ORDER BY device, quality;

-- A2.6 Plan d'execution de la requete A2.4 (jointure + agregation + tri)
EXPLAIN
SELECT e.content_id, c.title, c.genre,
       SUM(e.watch_seconds) AS total_watch_sec,
       COUNT(*)             AS nb_evenements
FROM evenements_massifs e
JOIN catalogue_contenus c ON e.content_id = c.content_id
GROUP BY e.content_id, c.title, c.genre
ORDER BY total_watch_sec DESC
LIMIT 10;

-- =====================================================================
-- A3. SORTIE TRANSMISE A MONGODB
-- =====================================================================

-- A3.1 Table externe de sortie posee sur le repertoire HDFS impose.
--      CSV sans en-tete, separateur virgule. Ordre des colonnes :
--      user_id, country, nb_events, total_watch_sec, segment
DROP TABLE IF EXISTS segments_utilisateurs;
CREATE EXTERNAL TABLE segments_utilisateurs (
  user_id         STRING,
  country         STRING,
  nb_events       BIGINT,
  total_watch_sec BIGINT,
  segment         STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/inf6303/devoir1/sorties/segments';

-- A3.2 Agregation par utilisateur + segmentation (seuils du complement
--      du 6 octobre 2026 : seule la duree totale determine le segment ;
--      nb_events est calcule et conserve mais n'intervient pas).
--      INSERT OVERWRITE : rejouable, remplace la sortie precedente.
INSERT OVERWRITE TABLE segments_utilisateurs
SELECT
  user_id,
  country,
  COUNT(*)           AS nb_events,
  SUM(watch_seconds) AS total_watch_sec,
  CASE
    WHEN SUM(watch_seconds) >= 55000 THEN 'intensif'
    WHEN SUM(watch_seconds) >= 30000 THEN 'actif'
    ELSE 'occasionnel'
  END AS segment
FROM evenements_massifs
GROUP BY user_id, country;

-- A3.3 Controles de coherence de la sortie
--  * 1 ligne par utilisateur (lignes = user_id distincts = 50 000)
--  * conservation des volumes : somme nb_events = 1 000 000
SELECT COUNT(*)                AS nb_lignes,
       COUNT(DISTINCT user_id) AS user_id_distincts,
       SUM(nb_events)          AS somme_nb_events,
       SUM(total_watch_sec)    AS somme_total_watch_sec
FROM segments_utilisateurs;

-- Reference : duree totale calculee directement sur les evenements
SELECT SUM(watch_seconds) AS somme_watch_seconds_source
FROM evenements_massifs;

-- A3.4 Repartition par segment (attendu : actif 30 901, intensif 7 430,
--      occasionnel 11 669) et verification des bornes des seuils
SELECT segment,
       COUNT(*)                     AS nb_utilisateurs,
       MIN(total_watch_sec)         AS duree_min,
       MAX(total_watch_sec)         AS duree_max,
       ROUND(AVG(nb_events), 1)     AS nb_events_moyen
FROM segments_utilisateurs
GROUP BY segment
ORDER BY segment;

