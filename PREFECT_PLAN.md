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

1. `transfer_capacities.py`: Entscheidung treffen, ob die
   Kapazitätsberechnung aus co2map/cosema portiert oder importiert wird.
2. `transfer_demand_reg_factors.py`: niedrige Priorität, da kein echter
   Automatisierungsgewinn möglich (keine externe Quelle) — allenfalls
   jährlicher Lauf des Extrapolations-Skripts auf dem Server.
3. Prefect-Anbindung: `flows.py`/`prefect.yaml` stehen (4 Deployments auf
   `local-pool`), GitHub-Repo ist öffentlich. Offen: Schedules; woher
   `transfer_capacities`/`transfer_demand_reg_factors` ihre Quelldaten auf
   dem Server bekommen; MaStR-Flow-Status siehe unten. Braucht erreichbaren
   Server: `prefect deploy`, im Dashboard prüfen.

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
- Schedules: mastr monatlich (1., ab 05:00 Europe/Berlin, denn der MaStR-Export
  entsteht laut `open_mastr` zwischen 02:00 und 04:00), capacities direkt
  danach, verkettet in einem Flow (mastr → capacities). demand_reg_factors
  jährlich oder manuell. shapefiles und generation_data ohne Schedule.
- Die Demand-Faktor-CSVs kommen vorerst NICHT ins öffentliche Repo (Herkunft
  der 2018-Basisdatei erst klären).
- Erledigt: interne Server-IP und DB-Ports stehen nicht mehr in `.env.example`
  und `common.py` (vor dem ersten Push durch Platzhalter ersetzt).
- Umbau für serverseitige Läufe nötig: capacities (Berechnung aus Postgres,
  nur die aktuelle Periode ersetzen statt alles). Mastr-Umbau siehe
  MaStR-Flow-Sektion unten.

## MaStR-Flow: Entscheidungen und Umsetzungsplan (Stand 2026-09-22)

Grundlage sind Erkenntnisse aus dem Quellcode von `open_mastr` 0.17.4:

- `download(data=[...])` lädt gezielt nur die nötigen XML-Dateien per
  HTTP-Teildownload (Fallback: komplette ZIP).
- Beim ersten Datei-Teil jeder Tabelle macht `open_mastr` `DROP TABLE` und
  `CREATE TABLE` und befüllt sie dann. Die Tabelle ist während des Laufs leer
  bzw. unvollständig.
- Schreibfehler werden nur geloggt (`try/except` in `process_xml_file`), am Ende
  steht trotzdem "Bulk download was successful". Der Flow muss selbst prüfen.
- Jeder Schreibvorgang baut eine eigene Engine aus der URL (eigener Timeout von
  300 s): Die Timeouts aus `common.py` greifen dort nicht, ein Zielschema muss
  per URL-Parameter (`options=-csearch_path=…`) übergeben werden (noch zu
  testen), und Passwörter mit `@`, `/` oder `:` brechen dort.
- `Mastr(engine)` legt sofort alle ~38 ORM-Tabellen im Standardschema an.
- Der MaStR-Export entsteht zwischen 02:00 und 04:00; vorher gibt es "heute"
  nicht (Fallback auf gestern).
- co2map liest per `SELECT <Spalten> FROM <tech>_extended` mit `parse_dates` und
  ist bei Typunterschieden (date/timestamp) sowie fehlenden Spalten tolerant. Die
  Tabellen werden ohne Schema-Präfix gelesen: Bei Postgres muss der
  `search_path` auf `cosema_inputs` zeigen (oder die Namen müssen qualifiziert
  werden).
- Umgebungsvariablen von `open_mastr`: `OUTPUT_PATH` (Ablage der ZIP, Standard
  `~/.open-MaStR`), `NUMBER_OF_PROCESSES` bzw.
  `USE_RECOMMENDED_NUMBER_OF_PROCESSES` (Parallelisierung, Standard: sequentiell).

### Entscheidungen (2026-09-22)

| | Frage | Entscheidung |
|---|---|---|
| 1 | Wohin lädt der Flow? | Schattenschema `mastr_staging`, am Ende Tausch in `cosema_inputs` (`ALTER TABLE … SET SCHEMA` in einer Transaktion) |
| 2 | Prüfung vor dem Tausch? | Ja: jede Tabelle mindestens 98 % der bisherigen Zeilen (angepasst am 2026-10-03, ursprünglich 90 % -- MaStR wächst nur geringfügig, 10 % Toleranz hätte bei `solar_extended` bis zu 630.000 fehlende Zeilen unbemerkt durchgelassen), sonst kein Tausch und der Flow schlägt fehl. Zusätzlich: jeder Rückgang wird geloggt (Warnung), auch wenn er die 98-%-Schwelle nicht reißt. |
| 3 | Alte Tabellen behalten? | Ja, eine Generation Backup in `mastr_prev`. Begründung: die 90-%-Prüfung fängt abgebrochene/unvollständige Läufe schon ab (Tausch findet dann gar nicht statt); ein Backup lohnt sich für den Fall, dass ein Lauf die Prüfung besteht, aber inhaltlich fehlerhaft ist (z. B. `open_mastr` verschluckt einzelne Datei-Fehler nur als Log-Zeile, oder eine geänderte Bereinigungslogik verändert Werte, ohne die Zeilenzahl zu ändern) — das merkt man oft erst später (z. B. an auffälligen co2map-Kapazitätszahlen). Kostet fast nichts (Umbenennen statt Löschen). Grenze: nur eine Generation, also Sicherheitsnetz für ca. einen Monat, kein Archiv. |
| 4 | Einbindung | Vorerst **nebeneinander**: neues `mastr_flow.py` zusätzlich zum bestehenden `transfer-mastr`-Wrapper in `flows.py`/`prefect.yaml`. Alten Wrapper erst entfernen, wenn der neue Flow zuverlässig läuft. |
| 5 | Testen | Auf Staging, nicht in Produktion. |

### Umsetzungsplan mit Aufwandseinschätzung

| Schritt | Was passiert | Aufwand |
|---|---|---|
| A. Schema-Frage klären | Testen, ob `open_mastr` mit einer Engine, deren Connection-URL auf `mastr_staging` zeigt (`search_path`), tatsächlich dort schreibt statt im Standardschema — auch mit den Prozessen, die `open_mastr` intern selbst startet. Einziger Punkt, der sich nicht allein aus dem Code ableiten lässt, nur durch einen Testlauf. | Klein, aber mit Unsicherheit |
| B. `mastr_flow.py` schreiben | Schema anlegen, `open_mastr` aufrufen, die 8 Tabellen zählen, bei Erfolg tauschen (alte Tabellen → `mastr_prev`, neue aus `mastr_staging` → `cosema_inputs`, alles in einer Transaktion), bei Misserfolg abbrechen und `mastr_staging` verwerfen. | Mittel, geradlinig |
| C. Ende-zu-Ende-Test auf Staging | Echter Lauf: `open_mastr` lädt frisch von der Bundesnetzagentur (nicht aus lokaler SQLite). | **Größter Posten** — ein echter Download von 8 Technologien inkl. solar (6,3 Mio. Zeilen) kann je nach Verbindung Stunden dauern, ähnlich dem Einmal-Upload der letzten Wochen. Viel Wartezeit, wenig aktive Arbeit, läuft im Hintergrund. |
| D. In `flows.py` einhängen | Neuen Flow zusätzlich eintragen, alten Wrapper unverändert lassen. | Klein |
| E. Aufräumen (später) | Alten `transfer-mastr`-Wrapper entfernen, wenn der neue Flow sich bewährt hat. | Klein, erst später fällig, Zeitpunkt nach Entscheidung des Nutzers |


### Schritt A — erledigt (2026-10-02)

Erfolgreich auf Staging getestet. `open_mastr` schreibt zuverlässig in ein
vorgegebenes Schema, WENN der `search_path` als Query-Parameter in der
Connection-URL steht (nicht in `connect_args`) -- `open_mastr` baut beim
Schreiben intern eine neue Engine aus `str(engine.url)` (siehe
`utils_write_to_database.py::create_efficient_engine`), und `connect_args`
ist davon nicht Teil. Erster Versuch (search_path in `connect_args`) landete
sichtbar in `public` statt im Zielschema -- nach dem Fix (search_path in der
URL via `query={"options": "-csearch_path=..."}`) landete es korrekt im
Zielschema. Getestet mit `data=["nuclear"]` (kleinste Tabelle, 6 Zeilen).
Testartefakte (Schema `mastr_staging`, versehentliche `public.nuclear_extended`
aus dem ersten Versuch) wieder entfernt, Produktions-/Altbestand unberührt.

### Schritt B und D — erledigt (2026-10-02)

`mastr_flow.py` angelegt: Schema anlegen/leeren, bisherigen Zeilenstand
zählen, `open_mastr` mit der in Schritt A bestätigten search_path-URL
aufrufen, bei Download-Fehler `mastr_staging` verwerfen und Fehler
weiterreichen, neue Zeilenzahl gegen die 98-%-Schwelle prüfen (siehe
Entscheidung 2 oben), bei Erfolg Tausch in einer Transaktion
(`cosema_inputs`→`mastr_prev`, `mastr_staging`→`cosema_inputs`).

In `flows.py` als `mastr_refresh_flow` (ruft `mastr_flow.main()`) und in
`prefect.yaml` als Deployment `mastr-refresh` eingehängt, neben dem
unveränderten `transfer-mastr`-Wrapper (Entscheidung 4: vorerst
nebeneinander). Offline getestet (Import, YAML-Struktur,
Entrypoint-Ladetest wie der Worker ihn durchführen würde) -- noch nicht
committet.

### Schritt C — erster Versuch, hängen geblieben (2026-10-04)

Ganztägig gelaufen, nie vollständig durchgekommen:

- **Fund 1 (positiv):** `combustion_extended` traf beim echten Lauf auf einen
  kaputten Wert in der Quelle (`EinheitenVerbrennung.xml`:
  `Failed to parse string: '2467 2473' as a scalar of type int64`).
  `open_mastr` hat das nur geloggt, keinen Fehler geworfen -- genau der Fall,
  für den die 98-%-Prüfung gebaut wurde. Bestätigung aus der Praxis, nicht
  nur aus der Theorie. (Der Lauf kam allerdings nie bis zur Prüfung, siehe
  unten.)
- **Fund 2:** Mehrere Pausen von 20-60+ Minuten beim Verarbeiten der
  nummerierten Solar-/Speicher-Dateien (`EinheitenSolar_N`,
  `AnlagenEegSolar_N`, ...). Die ersten zwei haben sich von selbst erholt,
  die dritte (nach ~2,5h Laufzeit) war ein echter Hänger: CPU-Zeit zwischen
  zwei Messungen (15s Abstand) exakt gleich, TCP-Verbindungen zur DB
  `Established`, aber tot. Abgebrochen (`Stop-Process -Force`) -- kompletter
  Fortschritt verloren, da `mastr_flow.main()` beim Neustart `mastr_staging`
  immer komplett neu anlegt (keine Fortsetzungslogik wie beim alten
  `transfer_mastr.py`).
- **Fund 3:** Auch eine neue Diagnose-Verbindung zu Staging lief in denselben
  Timeout (30s) -- die Datenbank/der Pooler nahm zu dem Zeitpunkt generell
  keine Verbindungen mehr an, nicht nur unser hängender Prozess war betroffen.
- **Entscheidung danach:** `.env` auf Produktion umgestellt (Begründung: der
  riskante Teil -- der Tausch -- passiert ohnehin erst ganz am Ende und
  würde bis dahin nur in `mastr_staging` schreiben, unabhängig vom Ziel;
  Staging spiegelt zudem die übrigen Daten gar nicht vollständig). Vor dem
  nächsten Start aber: **Produktion zeigte denselben Timeout wie Staging**,
  und zusätzlich auch **SSH zum Server selbst** (`Connection timed out
  during banner exchange` -- TCP wird angenommen, aber kein
  Protokoll-Handschlag). Das betrifft also den Host insgesamt, nicht
  speziell unsere Staging-Last oder den Postgres-Pooler. Dem Server-Kollegen
  gemeldet, Antwort steht noch aus (Stand 2026-10-04, ~16:35).
- **Code-Verbesserung währenddessen:** `MIN_ROW_RATIO` von 0,9 auf 0,98
  gezogen (siehe Entscheidung 2), plus Warnung bei jedem Rückgang auch
  unterhalb der Schwelle. `swap()` aus `main()` herausgezogen in eine eigene
  Funktion, `main(do_swap: bool = True)` -- bei `do_swap=False` bricht der
  Lauf nach bestandener Prüfung ab, ohne zu tauschen, damit sich der Tausch
  bei einem künftigen Versuch separat testen lässt, statt ihn an einen
  mehrstündigen Download zu hängen.
- **Geprüft, nicht umgesetzt:** Ob sich die ungefragt mitgeladenen
  EEG-/KWK-Tabellen (`biomass_eeg`, `solar_eeg`, `kwk`, ...) vermeiden
  lassen -- `data_to_include_tables()` ist fest auf Technologie-Ebene
  verdrahtet, jede Technologie bringt immer sowohl die `einheiten...`- als
  auch die `anlageneeg...`-Dateien mit. Eine Umgehung wäre nur über
  `open_mastr`s private Interna möglich (zu fragil für den Zeitgewinn).
  Unproblematisch für die Korrektheit (die Zusatztabellen fliegen beim
  finalen `DROP SCHEMA ... CASCADE` ohnehin weg), kostet nur Laufzeit.

Noch offen, unverändert: **C ist nicht abgeschlossen**, kein vollständiger
Lauf bisher gelungen. Nächster Start erst, wenn die Serververbindung
(SSH + Postgres, beide Umgebungen) wieder stabil ist.

### Nach einem erfolgreichen Schritt C noch offen, bis es wirklich
automatisch auf dem Server läuft

Ein erfolgreicher Testlauf (Schritt C) prüft nur die Flow-*Logik* -- er
läuft dabei lokal/manuell gegen Staging, nicht über Prefect. Für echten
automatischen Server-Betrieb fehlen danach noch:

1. **Deploy** (`prefect deploy -n mastr-refresh`) -- noch nie durchgeführt,
   bewusst zurückgestellt bis ein funktionierender Flow steht.
2. **Zeitplan.** In `prefect.yaml` steht aktuell kein Schedule -- "1. jeden
   Monats, 05:00 Uhr" (siehe Entscheidungen 2026-09-20) ist bisher nur hier
   dokumentiert, nicht eingetragen.
3. **Zugangsdaten auf dem Server.** `mastr_flow.py`/`common.py` lesen
   Host/Port/Passwort aus der lokalen `.env`, die bewusst nicht im Git-Repo
   liegt. Wie der Worker auf dem Server an diese Werte kommt (Prefect
   Secret-Block, `job_variables.env`, o.ä.), ist noch nicht gebaut.
4. **Der `pull`-Schritt in `prefect.yaml` (`git_clone` +
   `pip_install_requirements`) ist noch nie echt durchlaufen** -- Schritt C
   testet das nicht, weil der Code lokal direkt ausgeführt wird, nicht über
   einen frischen Checkout.
5. **Ausgehender Internetzugriff** des Worker-Containers zu
   `marktstammdatenregister.de` -- wahrscheinlich okay (Präzedenzfall:
   ENTSO-E-Crawler läuft ja schon serverseitig gegen eine externe API),
   aber nie für genau diese Domain bestätigt.

### Offene Fragen

- **Erledigt/empirisch beantwortet (2026-10-02):** Der DB-Nutzer darf
  zusätzliche Schemas anlegen und Tabellen verschieben (`CREATE SCHEMA`,
  `ALTER TABLE ... SET SCHEMA`) -- das hat beim Test in Schritt A auf
  Staging funktioniert. Eine lokale Postgres-Instanz zum Testen des Tauschs
  wird dadurch auch nicht mehr gebraucht, Staging reicht.
- Gibt es eine unabhängige "richtige" Zeilenzahl für den heutigen
  MaStR-Export (z. B. von der Bundesnetzagentur selbst veröffentlicht), gegen
  die man zusätzlich zur relativen 98-%-Prüfung prüfen könnte? Nicht
  recherchiert (Stand 2026-10-03).
- RAM und Speicherplatz des Workers für den MaStR-Download.
- Zielumgebung des Einmal-Uploads: Stand 2026-09-21 ist die Produktion komplett
  (16 Tabellen, alle 8 MaStR-Tabellen stimmen mit der Quelle überein). Staging
  wurde seit dem 4.9. nicht erneut vollständig geprüft (nur die einzelnen
  Testläufe zu `nuclear_extended` am 3.10.).
