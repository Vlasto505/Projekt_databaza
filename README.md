# Záverečný projekt – ELT proces a dátový sklad v Snowflake  
Tento repozitár predstavuje implementáciu ELT procesu v prostredí Snowflake a návrh dátového skladu s dimenzionálnym modelom typu Star Schema. Projekt pracuje s datasetom Opta Data: Football – SAMPLE zo Snowflake Marketplace, ktorý obsahuje detailné eventové dáta zo zápasov anglickej Premier League.
Projekt sa zameriava na analýzu výkonnosti hráčov a tímov na základe metriky Expected Goals (xG), ktorá umožňuje hodnotiť kvalitu streleckých príležitostí nezávisle od výsledného skóre.
# 1. Uvod a popis zdrojových dát 
Cieľom tochto semestrálneho projektu je analyzovať rozsiahle dátové subory zo športovej domény, konkrétne z oblasti futbal, pomocou dát dostupných v Snowflake MarketPlace.
Projekt pracuje s datasetom Opta Data: Football – SAMPLE, ktorý obsahuje detailné informácie o zápasoch anglickej Premier League, hráčoch, tímoch a jednotlivých herných udalostiach.
Zdrojové dáta pochádzajú zo Snowflake Marketplace a sú poskytované v rámci databázy EPL. Dataset obsahuje niekoľko hlavných tabuliek:
- Game
- Team
- Player
- Event
- Event_type
- Event_type_qualifier
- Venue

Účelom ELT procesu bolo tieto dáta extrahovať zo Snowflake Marketplace, transformovať do vhodnej podoby a sprístupniť ich prostredníctvom dimenzionálneho dátoveho skladu so schémou Star Schema, ktorý umožňuje viacdimenyionálnu analytickú analýzu a tvorbu vizualizácií kľučových metrík.
# 1.1 Dátová architektúra
**ERD diagram**
Surové dáta sú usporiadané v relačnom modeli, ktorý je znázornený na entitno-relačnom diagrame (ERD):
<p align="center">
  <img src="https://github.com/Jokovic00/Projekt_databaza/blob/main/Projekt_ERD.png" alt="ERD Schema"></p>

# 2 Dimenzionálny model
V ukážke bola navrhnutá **schéma hviezdy (star schema)** podľa Kimballovej metodológie, ktorá obsahuje 1 tabuľku faktov **`fact_shot`**, ktorá je prepojená s nasledujúcimi 4 dimenziami:
- **`dim_team`**: Obsahuje podrobné informácie o teame (názov, rok, vydavateľ).Typ SCD je 2 (valid_from,valid_to,is_current)
- **`dim_player`**: Obsahuje podrobné informácie o hráčoch.Typ SCD je 2 (valid_from,valid_to,is_current)
- **`dim_date`**: Zahrňuje informácie o dátumoch (deň, mesiac, rok, štvrťrok).
- **`dim_match`**: Obsahuje zápasove údaje (matchday, attendance).
- **`dim_even_type`**: Obsahuje informácie o evente(is_goal_attempt,event_type_name)




<p align="center">
  <img src="https://github.com/Jokovic00/Projekt_databaza/blob/main/Projekt_star.png" alt = "Star Schema"></p> 

## 3. ELT proces v Snowflake
ETL proces zahŕňal tri kľúčové fázy: extrakciu (Extract), transformáciu (Transform) a nahrávanie (Load). V prostredí Snowflake bol tento proces realizovaný s cieľom spracovať zdrojové dáta zo staging vrstvy a pripraviť ich do viacdimenzionálneho dátového modelu vhodného na analytické spracovanie a vizualizáciu.
### 3.1 Extract(Extrahovanie dát)
#### Príklad kódu:
```sql
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

Tento príkaz predstavuje Extract krok, pri ktorom boli do staging vrstvy skopírované všetky dáta o zápasoch Premier League zo Snowflake Marketplace. Tabuľka obsahuje základné informácie o zápasoch, ako sú dátum, sezóna, hracie kolo, domáci a hosťujúci tím, skóre a identifikátor štadióna. V tomto kroku nedochádza k žiadnej transformácii dát, cieľom je len zachovať surové dáta v pôvodnej podobe.
```
### 3.2 Load
#### Príklad kódu:
```sql
INSERT INTO DIM_TEAM
SELECT DISTINCT
  t.id,
  t.name,
  SUBSTR(t.name,1,3),
  'England',
  v.name,
  CURRENT_DATE(),
  '9999-12-31',
  TRUE
FROM STAGING.STG_TEAM t
LEFT JOIN STAGING.STG_GAME g ON g.home_team = t.id
LEFT JOIN STAGING.STG_VENUE v ON g.venue_id = v.id;

```
Tento príkaz predstavuje Load krok, pri ktorom sa napĺňa dimenzia tímov v dátovom sklade. Počas tohto procesu dochádza zároveň k transformáciám, keďže sa vytvára skrátený názov tímu, dopĺňa sa krajina pôvodu a názov domáceho štadióna. Použitie atribútov valid_from, valid_to a is_current umožňuje historizáciu údajov podľa princípu SCD Type 2.
### 3.3 Transfer
#### Príklad kódu
```sql
CASE
  WHEN e.x > 83 AND e.y BETWEEN 21 AND 79 THEN TRUE
  ELSE FALSE
END AS is_inside_box
```
Tento CASE výraz slúži na určenie, či bola strela vykonaná z vnútra pokutového územia. Na základe súradníc strely sa kontroluje, či hodnota x je väčšia ako 83, čo znamená, že strela bola vykonaná v blízkosti súperovej brány, a zároveň či hodnota y spadá do intervalu medzi 21 a 79, čo zodpovedá šírke pokutového územia. Ak sú obe podmienky splnené, výsledkom je hodnota TRUE, ktorá označuje strelu z vnútra šestnástky, v opačnom prípade je výsledkom hodnota FALSE, teda strela bola vykonaná mimo pokutového územia.
### 4 Visualizácia dát 
Dashboard obsahuje 5 vizualizácií, ktoré poskytujú základný prehlad o potencialnom gole a XG timov a hračov v Premier League. Tieto vizualizácie odpovedajú na dôležité otázky a umožňujú lepšie pochopiť šutovanie na gol hrača a ich preferencie.

<p align="center">
  <img src="https://github.com/Jokovic00/Projekt_databaza/blob/main/Vizualizacie.png" alt="Graf"></p>
                                                                                            
# Graf 1: Ktorí hrači mali najvyššie celkové xG?
Graf 1 zobrazuje hráčov s najvyššou sumou očakávaných gólov (xG). Títo hráči sa najčastejšie dostávali do kvalitných streleckých pozícií
```sql
SELECT
  dp.player_last_name AS player,
  SUM(fs.xg) AS total_xg
FROM FACT_SHOTS fs
JOIN DIM_PLAYER dp ON fs.player_key = dp.player_key
GROUP BY dp.player_last_name
ORDER BY total_xg DESC
LIMIT 10;
```

# Graf 2: Ktoré tímy mali najvyššie celkové xG?
Graf 2 Vizualizácia porovnáva ofenzívnu výkonnosť tímov na základe celkového xG. Vyššie hodnoty indikujú konzistentné vytváranie gólových šancí.
```sql
SELECT
  dt.team_name,
  SUM(fs.xg) AS team_xg
FROM FACT_SHOTS fs
JOIN DIM_TEAM dt ON fs.team_key = dt.team_key
GROUP BY dt.team_name
ORDER BY team_xg DESC;
```

# Graf 3: Ako sa menilo xG počas zápasu?
Graf 3 znázorňuje rozloženie očakávaných gólov v čase zápasu. Vyššie hodnoty v neskorších minútach naznačujú zvýšený tlak tímov ku koncu zápasu.
```sql
SELECT
  period_minute,
  SUM(xg) AS total_xg
FROM FACT_SHOTS
GROUP BY period_minute
ORDER BY period_minute;
```

# Graf 4: Strely z vnútra vs mimo pokutového územia
Graf 4  Strely z vnútra pokutového územia majú výrazne vyššie xG, čo potvrdzuje dôležitosť pozičnej hry a prenikania do nebezpečných zón.
```sql
SELECT
  is_inside_box,
  COUNT(*) AS shot_count,
  SUM(xg) AS total_xg
FROM FACT_SHOTS
GROUP BY is_inside_box;
```

# Graf 5: Top 10 najkvalitnejších striel (podľa xG)
Graf 5 zobrazuje jednotlivé strely s najvyššou hodnotou očakávaného gólu, ktoré predstavovali najväčšie gólové príležitosti v analyzovanom kole.
```sql
SELECT
  dp.player_last_name,
  fs.xg,
  fs.period_minute
FROM FACT_SHOTS fs
JOIN DIM_PLAYER dp ON fs.player_key = dp.player_key
ORDER BY fs.xg DESC
LIMIT 10;
```
