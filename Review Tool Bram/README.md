Korte uitleg over de verschillende files/folders:

202509 -- > Data folder 

output --> Output folder voor iedere run die je doet via GUI

Review -->  functions.py --> semi colon seperated csv reader
            review_logic.py --> config file voor losse reviews

Review Comparison --> Output sheet voor scripts die output tool vs Dataiku checken

Static --> Euronext Logo voor GUI

templates --> index.html voor GUI

utils -->  Helper functions
           data_loader.py --> worden alle data sets geladen en daar zul je ook nieuwe datasets moeten toevoegen als ze er niet nog bijstaan. Hierbij goed opletten welke rij in excel de headers staan zodat de file juist wordt geimporteerd. Ook goed kijken naar Sheet names en of er meerdere Sheets in excel zijn. Kijk bij andere imports voor voorbeelden

           inclusion_exclusion.py --> Wat de naam zegt, functie zorgt dat inclusion en exclusion sheets in the output sheet worden gemaakt.

Algemene files:

app.py --> Flask applicatie om tool te draaien -> hiermee lanceer je GUI via terminal

batch_processor.py + enhanced_task_manager.py + task_manager.py --> zorgen er voor dat reviews sequentieel gedraaid kunnen worden door meerdere mensen vanaf meerdere computers. Zorgt er ook voor dat er "Multiple Reviews" gedraaid kunnen worden

compare_eia_files.py + compare.py --> vergelijken Dataiku met Output tool

Structure Review Tool.xlsx --> Overzicht van structuur van de tool

instructions.txt --> Geeft informatie over welke stappen je moet nemen voordat je een nieuwe review gaat maken om de deze mee te kunnen laten draaien in de tool/GUI


Bij vragen, let me know.
