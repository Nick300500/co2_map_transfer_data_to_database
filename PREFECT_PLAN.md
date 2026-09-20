# Prefect-Automatisierung: Plan & Machbarkeit

Ziel: die Datenübertragungs-Skripte in diesem Repo sollen langfristig
**komplett serverseitig** laufen (Prefect auf dem OEDS-Server), ohne Umweg
über ein lokales Gerät, und wo möglich direkt aus der Originalquelle laden
statt aus lokal vorbereiteten Zwischendateien.

Aktuell laden alle Skripte von lokalen Pfaden (`CAPACITIES_DIR`,
`MASTR_SQLITE_PATH`, `DEMAND_REG_DATA_DIR` usw.) auf dem Windows-Rechner des
Nutzers — das ist als einmaliger Erstupload okay, aber für wiederkehrende
Updates soll das nicht dauerhaft von einem lokalen Gerät abhängen.

Ausgewählt für Automatisierung (Stand 2026-09-06): `transfer_capacities.py`,
`transfer_mastr.py`, `transfer_demand_reg_factors.py`. `transfer_shapefiles.py`
und `transfer_generation_data.py` (statische Referenzdaten) wurden bisher
nicht geprüft/ausgewählt.

## Recherche-Ergebnisse pro Skript

### transfer_mastr.py / MaStR-Daten

- Echte Quelle: Marktstammdatenregister (Bundesnetzagentur), abgerufen über
  das Python-Paket `open-mastr` (Bulk-Download).
- `open_mastr.Mastr(engine=<sqlalchemy.engine.Engine>)` akzeptiert eine
  beliebige SQLAlchemy-Engine statt der Default-SQLite — kann also **direkt
  gegen Postgres** downloaden, ganz ohne lokale Zwischendatei. Bestätigt durch
  Quellcode-Inspektion (`open_mastr/utils/helpers.py::create_database_engine`,
  `validate_parameter_format_for_mastr_init`).
- `.download(data=["wind","solar","biomass","hydro","gsgk","combustion",
  "nuclear","storage"])` lädt gezielt nur die Technologien, die wir brauchen.
- Im co2map-Repo existiert dazu bereits ein dokumentierter (aber nicht
  umgesetzter) Plan (`oeds_integration_plan.md`), MaStR "in der Datenbank,
  nicht als Datei" zu haben — deckt sich mit unserem Vorhaben.
- **Fazit: technisch sauber machbar, komplett serverseitig, keine lokale
  Zwischendatei nötig.** Nächster Schritt: `transfer_mastr.py` so umbauen,
  dass es `open_mastr` direkt mit einer Postgres-Engine (Ziel-Schema
  `cosema_inputs`) aufruft statt die aktuelle lokale SQLite zu migrieren.

### transfer_capacities.py / Kapazitäten

- Baut auf den MaStR-`*_extended`-Tabellen auf (via
  `cosema/capacities/mastr.py::calculate_total_capacities_for_cosema` /
  `get_pp_MaStr`, liest per SQL) plus zwei statischen Referenzdateien
  (Postleitzahlen-Shapefile, Bundesländer-Shapefile — beide schon in
  `cosema_inputs` via `transfer_shapefiles.py`).
- Die Berechnungslogik selbst lebt im co2map-Repo (`cosema/capacities/
  mastr.py`), nicht in diesem Transfer-Repo. Um serverseitig direkt aus
  Postgres zu rechnen (statt aus lokalen Parquet-Snapshots), müsste diese
  Logik entweder nach hierhin portiert/vendored werden, oder dieses Repo
  bekäme eine Abhängigkeit auf co2map/cosema (aktuell bewusst vermieden, s.
  README: "eigenständig lauffähig, keine Abhängigkeit von dessen Code").
- **Fazit: machbar, aber braucht eine bewusste Entscheidung**, ob/wie die
  Berechnungslogik dupliziert oder importiert wird.

### transfer_demand_reg_factors.py / Demand Regionalization Factors

- **Keine echte externe Quelle vorhanden.** Ursprung ist eine einzelne
  statische, historische CSV aus 2018 (`demand_reg_factors.csv`), deren
  Herkunft selbst nirgends im co2map-Repo dokumentiert/generiert wird — sie
  liegt einfach im Repo.
- `scripts/prepare_demand_data.py` extrapoliert diese jährlich neu
  (Kalender-/Feiertags-Matching) zu `demand_reg_factors_{jahr}.csv`.
- **Fazit: "von der Quelle laden" ist hier nicht sinnvoll möglich** — es gibt
  keine API/Download. Automatisierung kann hier nur heißen: die Basis-CSV
  liegt einmalig auf dem Server, und das Extrapolations-Skript läuft dort
  jährlich neu (kein häufigeres Update nötig/möglich).

## Prefect-Architektur (zur Erinnerung)

Server (Port 4200) macht nur Orchestrierung (UI, Scheduler, API). Ein
**Worker**-Prozess (den man selbst mit `prefect worker start --pool
<pool-name>` startet) fragt einen **Work Pool** ab und führt den Flow dort
aus, wo der Worker läuft. Der Poolname "local-pool" aus der Server-Doku ist
kein Zwang zu lokaler Ausführung — es ist möglich, den Worker auf dem
OEDS-Server selbst laufen zu lassen, wenn der Flow-Code keine
lokalen-Rechner-Pfade mehr braucht (was für `transfer_mastr.py` nach dem
open-mastr-direkt-Postgres-Umbau der Fall wäre).

## Nächste Schritte

1. `transfer_mastr.py` umbauen: `open_mastr` direkt gegen Postgres statt
   SQLite-Migration (klarster, unabhängigster Fall).
2. `transfer_capacities.py`: Entscheidung treffen, ob die
   Kapazitätsberechnung aus co2map/cosema portiert oder importiert wird.
3. `transfer_demand_reg_factors.py`: niedrige Priorität, da kein echter
   Automatisierungsgewinn möglich (keine externe Quelle) — allenfalls
   jährlicher Lauf des Extrapolations-Skripts auf dem Server.
4. Prefect-Anbindung — Stand 2026-09-20:
   - Erledigt (ohne Server): `flows.py` (Wrapper-Flows, Skripte unverändert),
     `prefect.yaml` (3 Deployments auf `local-pool`), lokales `git init`
     (ohne Commit/Remote), `.venv` mit `prefect==3.6.17`.
   - Offen, braucht Entscheidung: GitHub-Remote anlegen und die
     Platzhalter-URL (`REPLACE-ME`) in `prefect.yaml` ersetzen; Schedules;
     woher die Flows ihre Quelldaten auf dem Server bekommen (siehe oben).
   - Offen, braucht erreichbaren Server: `PREFECT_API_URL` setzen,
     `prefect work-pool ls` abgleichen, `prefect deploy`, im Dashboard prüfen.

## Server-Setup (aus dem open-energy-data-server-Repo: compose.yml / requirements.txt)

- Prefect-Version: `prefect==3.6.17` (Server-Image `prefecthq/prefect:3-latest`)
  — lokal dieselbe Version nutzen (`.venv`).
- Der Worker (`prefect worker start --pool local-pool`) läuft **auf dem
  Server** (Docker). Flows laufen also dort; lokale Windows-Pfade
  (`CAPACITIES_DIR`, `MASTR_SQLITE_PATH`, ...) existieren dort nicht.
- Dashboard/API auf Port 4200. Die API kennt `PREFECT_SERVER_API_AUTH_STRING`;
  falls auf dem Server gesetzt, braucht `prefect deploy` lokal
  `PREFECT_API_AUTH_STRING` (Credential vom Server-Admin erfragen).
- Ungetestet bis zum ersten echten Deploy: Top-Level-`pull` in `prefect.yaml`,
  `job_variables.env.PREFECT_LOGGING_EXTRA_LOGGERS=common` (damit die Logs aus
  `common.logger` im Prefect-UI erscheinen), Auflösung von `flows.py` samt
  Geschwister-Imports nach dem `git_clone`.

## Entscheidungen (2026-09-20)

- Langfristig ziehen alle Flows ihre Daten direkt aus der Originalquelle auf
  dem Server; nichts läuft mehr über den lokalen Laptop. Die lokalen
  `transfer_*.py`-Skripte bleiben vorerst als Einmal-Upload, um Daten für Tests
  der Karte (co2map) auf den Server zu bringen.
- Schedules: mastr monatlich (1., 02:00 Europe/Berlin), capacities direkt
  danach, verkettet in einem Flow (mastr → capacities). demand_reg_factors
  jährlich oder manuell. shapefiles und generation_data ohne Schedule.
- Die Demand-Faktor-CSVs kommen vorerst NICHT ins öffentliche Repo (Herkunft
  der 2018-Basisdatei erst klären).
- Offen: interne Server-IP in `.env.example` und `common.py` vor dem ersten
  Push durch einen Platzhalter ersetzen (die Git-Historie behält sie sonst).
- Umbau für serverseitige Läufe nötig: mastr (`open_mastr` direkt gegen
  Postgres, Schattentabelle + Umbenennen), capacities (Berechnung aus
  Postgres, nur die aktuelle Periode ersetzen statt alles).
