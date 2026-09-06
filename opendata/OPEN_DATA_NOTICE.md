# Open Food Facts – Daten- und Bildhinweis

ThomasFit enthält eine auf 100.000 Produkte reduzierte und technisch normalisierte
Produktdatenbank, die aus einem Open-Food-Facts-Datenexport erzeugt wurde.

- Datenquelle: Open Food Facts, `https://openfoodfacts.org`
- Urheber-/Datenbankhinweis: © Open Food Facts contributors
- Datenbanklizenz: Open Database License (ODbL) 1.0  
  `https://opendatacommons.org/licenses/odbl/1-0/`
- Einzelne Datenbankinhalte: Database Contents License (DbCL) 1.0  
  `https://opendatacommons.org/licenses/dbcl/1-0/`
- Online geladene Produktbilder: Creative Commons Attribution-ShareAlike
  (CC BY-SA) 3.0  
  `https://creativecommons.org/licenses/by-sa/3.0/`
- Nutzungs- und Wiederverwendungsbedingungen:  
  `https://world.openfoodfacts.org/terms-of-use`

## Änderungen durch ThomasFit

Der lokale Auszug wurde für die Offline-Suche gefiltert und in ein eigenes
SQLite-Schema überführt. Produktname, Marke, Barcode, Kategorie, Nährwerte und
Portionsangaben können normalisiert, gekürzt oder verworfen worden sein. Die
Ausgangsdaten wurden nicht von Open Food Facts für ThomasFit geprüft.

Die daraus entstandene Datenbank `ThomasFit/Resources/baseline-foods.sqlite`
wird unter ODbL 1.0 weitergegeben. Das reproduzierbare Import- und
Transformationsskript liegt unter `Scripts/build_massive_food_database.py`.
Vor dem öffentlichen Release muss zusätzlich ein dauerhaft erreichbares
Downloadangebot für die tatsächlich ausgelieferte Datenbankfassung veröffentlicht
und in der App beziehungsweise auf der Support-Seite verlinkt werden.

## Exakt zugeordnete Release-Ableitung

Die aktuell gebündelte Fassung wurde bytegenau geprüft:

- App-Ressource: `ThomasFit/Resources/baseline-foods.sqlite`
- Produkte: 100.000
- Dateigröße: 34.459.648 Byte
- Datenbank-Build-Zeitpunkt: 5. Mai 2026, 20:33:30 UTC
- SHA-256:
  `37f60ab122e7dfbab5e41127d47b8dbc26dac1000dbae025ac493483902314d7`
- erster Git-Commit dieser exakten Ressource:
  `cc188fb809ec180f75c6eb5af301728e67670f17`
- maschinenlesbares Manifest:
  `OpenData/OPEN_FOOD_FACTS_RELEASE_MANIFEST.json`

Das vollständige unkomprimierte Veröffentlichungspaket enthält die identische
SQLite-Datei, dieses Notice, das Transformationsskript, ein Manifest, README und
`SHA256SUMS`. Eine öffentliche Download-URL ist noch nicht vergeben; deshalb ist
das App-Store-Release-Gate im Manifest weiterhin `false`.

Bekannte Provenienzlücke: Der historische Open-Food-Facts-Quelldump wurde nicht
archiviert und hat daher keine belegbare historische Prüfsumme. Die tatsächlich
ausgelieferte Ableitung kann bytegenau geteilt und dem Binary zugeordnet werden;
der damalige Import kann jedoch nicht aus einem authentifizierten Dump-Snapshot
wiederholt werden. Künftige Builds müssen Quelldump-URL, Abrufzeit und SHA-256
vor der Transformation im Manifest sichern.

Produktbilder werden nur bei einer bewusst gestarteten Online-Produktsuche von
Open Food Facts geladen. Sie sind nicht Bestandteil der lokalen Datenbank.

Open Food Facts stellt die Daten gemeinschaftlich bereit und übernimmt keine
Gewähr für Richtigkeit, Vollständigkeit oder Verfügbarkeit. Die Nennung bedeutet
keine Unterstützung oder Empfehlung von ThomasFit durch Open Food Facts.
