USE WAREHOUSE SPARROW_WH;

CREATE OR REPLACE DATABASE FOOTBALL_STATS_DWH;

CREATE OR REPLACE SCHEMA FOOTBALL_STATS_DWH.STAGING;
CREATE OR REPLACE SCHEMA FOOTBALL_STATS_DWH.DWH;

USE DATABASE FOOTBALL_STATS_DWH;
USE SCHEMA STAGING;

//staging tabuljy
CREATE OR REPLACE TABLE STG_GAME AS
SELECT * FROM OPTA_DATA_FOOTBALL__SAMPLE.EPL.GAME;

CREATE OR REPLACE TABLE STG_TEAM AS
SELECT * FROM OPTA_DATA_FOOTBALL__SAMPLE.EPL.TEAM;

CREATE OR REPLACE TABLE STG_PLAYER AS
SELECT * FROM OPTA_DATA_FOOTBALL__SAMPLE.EPL.PLAYER;

CREATE OR REPLACE TABLE STG_EVENT AS
SELECT * FROM OPTA_DATA_FOOTBALL__SAMPLE.EPL.EVENT;

CREATE OR REPLACE TABLE STG_EVENT_TYPE AS
SELECT * FROM OPTA_DATA_FOOTBALL__SAMPLE.EPL.EVENT_TYPE;

CREATE OR REPLACE TABLE STG_EVENT_TYPE_QUALIFIER AS
SELECT * 
FROM OPTA_DATA_FOOTBALL__SAMPLE.EPL.EVENT_TYPE_QUALIFIER;

CREATE OR REPLACE TABLE STG_VENUE AS
SELECT * FROM OPTA_DATA_FOOTBALL__SAMPLE.EPL.VENUE;

SHOW SCHEMAS IN DATABASE FOOTBALL_STATS_DWH;
SHOW TABLES IN SCHEMA FOOTBALL_STATS_DWH.STAGING;


SHOW DATABASES;
SHOW SCHEMAS IN DATABASE FOOTBALL_DWH;
SHOW TABLES IN SCHEMA FOOTBALL_DWH.STAGING;


//Dim tabulky
USE DATABASE FOOTBALL_STATS_DWH;
USE SCHEMA DWH;

CREATE OR REPLACE TABLE DIM_TEAM (
  team_key INT AUTOINCREMENT PRIMARY KEY,
  team_id INT,
  team_name STRING,
  short_name STRING,
  country STRING,
  stadium_name STRING,
  valid_from DATE,
  valid_to DATE,
  is_current BOOLEAN
);

INSERT INTO DIM_TEAM (
  team_id,
  team_name,
  short_name,
  country,
  stadium_name,
  valid_from,
  valid_to,
  is_current
)
SELECT DISTINCT
  t.id AS team_id,
  t.name AS team_name,
  SUBSTR(t.name,1,3) AS short_name,
  'England' AS country,
  v.name AS stadium_name,
  CURRENT_DATE() AS valid_from,
  '9999-12-31' AS valid_to,
  TRUE AS is_current
FROM STAGING.STG_TEAM t
LEFT JOIN STAGING.STG_GAME g ON g.home_team = t.id
LEFT JOIN STAGING.STG_VENUE v ON g.venue_id = v.id;





CREATE OR REPLACE TABLE DIM_PLAYER (
  player_key INT AUTOINCREMENT PRIMARY KEY,
  player_id INT,
  player_last_name STRING,
  team_key INT,
  valid_from DATE,
  valid_to DATE,
  is_current BOOLEAN
);


INSERT INTO DIM_PLAYER (
  player_id,
  player_last_name,
  team_key,
  valid_from,
  valid_to,
  is_current
)
SELECT DISTINCT
  p.id AS player_id,
  p.player_last_name,
  dt.team_key,
  CURRENT_DATE(),
  '9999-12-31',
  TRUE
FROM STAGING.STG_PLAYER p
LEFT JOIN DIM_TEAM dt
  ON p.affiliation_id = dt.team_id
 AND dt.is_current = TRUE
WHERE dt.team_key IS NOT NULL;
SELECT COUNT(*) FROM DIM_PLAYER;






CREATE OR REPLACE TABLE DIM_EVENT_TYPE (
  event_type_key INT AUTOINCREMENT PRIMARY KEY,
  event_type_id INT,
  event_type_name STRING,
  is_goal_attempt BOOLEAN
);

INSERT INTO DIM_EVENT_TYPE
SELECT
  id,
  id,
  name,
  CASE WHEN id IN (13,14,15,16) THEN TRUE ELSE FALSE END
FROM STAGING.STG_EVENT_TYPE;


CREATE OR REPLACE TABLE DIM_DATE (
  date_key INT PRIMARY KEY,
  full_date DATE,
  year INT,
  month INT,
  day INT
);

INSERT INTO DIM_DATE
SELECT DISTINCT
  TO_NUMBER(TO_CHAR(game_date,'YYYYMMDD')),
  game_date,
  YEAR(game_date),
  MONTH(game_date),
  DAY(game_date)
FROM STAGING.STG_GAME;



CREATE OR REPLACE TABLE DIM_MATCH (
  match_key INT AUTOINCREMENT PRIMARY KEY,
  match_id INT,
  matchday INT,
  attendance INT
);

INSERT INTO DIM_MATCH
SELECT
  id,
  id,
  matchday,
  attendance
FROM STAGING.STG_GAME;


CREATE OR REPLACE TABLE FACT_SHOTS (
  shot_key INT AUTOINCREMENT PRIMARY KEY,
  date_key INT,
  match_key INT,
  player_key INT,
  team_key INT,
  event_type_key INT,
  x FLOAT,
  y FLOAT,
  is_inside_box BOOLEAN,
  xg FLOAT,
  period_minute INT,
  period_second INT,
  xg_rank INT,
  avg_xg_per_player FLOAT
);

INSERT INTO FACT_SHOTS (
  date_key,
  match_key,
  player_key,
  team_key,
  event_type_key,
  x,
  y,
  is_inside_box,
  xg,
  period_minute,
  period_second,
  xg_rank,
  avg_xg_per_player
)
SELECT
  TO_NUMBER(TO_CHAR(g.game_date,'YYYYMMDD')) AS date_key,
  dm.match_key,
  dp.player_key,
  dt.team_key,
  det.event_type_key,
  e.x,
  e.y,
  CASE WHEN e.x > 83 AND e.y BETWEEN 21 AND 79 THEN TRUE ELSE FALSE END,
  etq.value AS xg,
  e.period_minute,
  e.period_second,

  -- WINDOW FUNCTIONS (POVINNÉ)
  RANK() OVER (ORDER BY etq.value DESC) AS xg_rank,
  AVG(etq.value) OVER (PARTITION BY dp.player_key) AS avg_xg_per_player
FROM STAGING.STG_EVENT e
JOIN STAGING.STG_EVENT_TYPE_QUALIFIER etq
  ON e.id = etq.event_id AND etq.qualifier_id = 321
JOIN STAGING.STG_GAME g ON e.game_id = g.id
JOIN DIM_MATCH dm ON dm.match_id = g.id
JOIN DIM_PLAYER dp
  ON dp.player_id = e.player_id AND dp.is_current = TRUE
JOIN DIM_TEAM dt
  ON dt.team_key = dp.team_key AND dt.is_current = TRUE
JOIN DIM_EVENT_TYPE det
  ON det.event_type_id = e.event_type_id
WHERE e.event_type_id IN (13,14,15,16);

Select * from fact_shots limit 10;  




/*SELECT COUNT(*) 
FROM STAGING.STG_EVENT
WHERE event_type_id IN (13,14,15,16);
SELECT COUNT(*)
FROM STAGING.STG_EVENT_TYPE_QUALIFIER
WHERE qualifier_id = 321;
SELECT COUNT(*)
FROM STAGING.STG_EVENT e
JOIN STAGING.STG_EVENT_TYPE_QUALIFIER q
  ON e.id = q.event_id
WHERE q.qualifier_id = 321;
SELECT COUNT(*) FROM DIM_PLAYER;    
SELECT COUNT(*)
FROM STAGING.STG_EVENT e
JOIN STAGING.STG_EVENT_TYPE_QUALIFIER q
  ON e.id = q.event_id
JOIN STAGING.STG_GAME g
  ON e.game_id = g.id
WHERE e.event_type_id IN (13,14,15,16);
SELECT COUNT(*) FROM DIM_MATCH;
SELECT COUNT(*) FROM DIM_EVENT_TYPE;
*/









