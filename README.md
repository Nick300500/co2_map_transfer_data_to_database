# Transfer data to database

Eigenständige, kleine Skripte, um die lokal vorliegenden CO2-Map-Inputs (MaStR,
Shapefiles, Kapazitäts-Parquets, Demand-Regionalisierungsfaktoren,
Technologie-Zuordnungstabellen) einmalig in ein eigenes Postgres-Schema
(`cosema_inputs`) auf dem OEDS-Server hochzuladen. Bewusst **nicht** Teil des
`open-energy-data-server`/`co2map`-Repos — eigenständig lauffähig, keine
Abhängigkeit von dessen Code.

Wetter-Cutouts (NetCDF) sind **nicht** dabei — die bleiben vorerst lokal
(Entscheidung Stand 2026-08-21).

## Setup

```
pip install -r requirements.txt
cp .env.example .env
```

Dann `.env` ausfüllen:
- `DB_HOST`, `DB_PORT`, `DB_PASSWORD` (Werte beim Server-Admin erfragen). Produktion und Staging sind getrennte Datenbanken auf unterschiedlichen Ports — der Port muss zur gewünschten Umgebung passen.
- die lokalen Quellpfade für die Skripte, die du tatsächlich laufen lassen willst (leer lassen, was du nicht brauchst)

## Nutzung

Jedes Skript ist unabhängig lauffähig, in beliebiger Reihenfolge:

```
python transfer_mastr.py                 # braucht MASTR_SQLITE_PATH
python transfer_shapefiles.py             # braucht SHAPEFILES_DIR
python transfer_capacities.py             # braucht CAPACITIES_DIR
python transfer_demand_reg_factors.py     # braucht DEMAND_REG_DATA_DIR
python transfer_generation_data.py        # braucht GENERATION_DATA_DIR
```

`transfer_mastr.py` ist der einzige, der länger dauern kann (~13 GB, läuft
tabellenweise + gechunkt, um nicht alles auf einmal in den Speicher zu laden).
Alle anderen sind klein genug für einen einzigen Durchlauf.

Alle Skripte legen das Zielschema (`cosema_inputs`, per `TARGET_SCHEMA` in
`.env` änderbar) automatisch an, falls es noch nicht existiert, und
überschreiben ihre jeweilige(n) Zieltabelle(n) bei erneutem Lauf
(`if_exists="replace"`) — außer `transfer_mastr.py`, das pro Quelltabelle
ebenfalls komplett ersetzt, aber intern in Chunks schreibt.

Beim Start loggt jedes Skript die Zeile `Target DB: host:port/db, schema '…'`
(ohne Passwort), damit klar ist, in welche Datenbank geschrieben wird. Der
Verbindungsaufbau bricht nach 30 s mit einem Fehler ab, statt endlos zu
hängen; `transfer_mastr.py` lässt sich danach einfach neu starten und macht
dort weiter, wo es aufgehört hat.

## Prefect (in Vorbereitung)

Langfristig sollen die Transfers regelmäßig und serverseitig über Prefect
laufen. Vorbereitet sind:

- `flows.py`: dünne `@flow`-Wrapper um `transfer_mastr.py`,
  `transfer_capacities.py` und `transfer_demand_reg_factors.py` (unverändert,
  brauchen Prefect nicht zum Laufen) plus `mastr_refresh_flow`, der stattdessen
  `mastr_flow.py` aufruft.
- `mastr_flow.py`: lädt MaStR direkt von der Bundesnetzagentur (`open-mastr`)
  statt aus einer lokalen Datei — braucht als einziger Flow keine lokalen
  Pfade. Schreibt in ein Schattenschema, prüft die Zeilenzahl, tauscht erst
  bei Erfolg (Details: `PREFECT_PLAN.md`). **Noch nicht erfolgreich
  Ende-zu-Ende getestet** (wiederholte Server-Hänger, siehe dort).
- `prefect.yaml`: vier Deployments auf `local-pool`; der Worker klont dieses
  Repo und installiert `requirements.txt`. Noch ohne Zeitpläne.

Stand: noch nichts deployt. Die drei alten Wrapper brauchen weiterhin lokale
Quelldateien, die auf dem Server nicht existieren. Details und Fahrplan:
[PREFECT_PLAN.md](PREFECT_PLAN.md).

Für die Flows lokal zusätzlich (gleiche Version wie auf dem Server):

```
pip install prefect==3.6.17
```

## Hinweise und bekannte Lücken

- `MASTR_SQLITE_PATH`: der Standardpfad von `open-mastr` ist
  `~/.open-MaStR/data/sqlite/open-mastr.db` (auf Windows bestätigt); bei
  abweichender Installation den echten Pfad in `.env` eintragen.
- `transfer_shapefiles.py`: die Dateiliste (`FILES`-Dict) geht von den
  Pfaden aus `co2map/config.yaml` aus (`states.geojson`, `plz/OSM_PLZ.shp`) —
  anpassen, falls die lokale Struktur abweicht.
