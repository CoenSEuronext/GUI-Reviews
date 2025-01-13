@echo on
echo Starting batch file...
cd /d "C:\Users\CSonneveld\OneDrive - Euronext\Documents\Projects\Archive"
echo Changed directory
set "PYTHONPATH=%PYTHONPATH%;C:\Users\CSonneveld\OneDrive - Euronext\Documents\Projects\Archive"
echo Set Python path
echo Activated virtual environment
python archive_copy.py
echo Script finished
pause