@echo off
:: Daily scheduler batch file for Meta Catalog Feed Generator
:: To register with Windows Task Scheduler, run this command in an admin terminal:
::
::   schtasks /create /tn "MetaCatalogFeed" /tr "C:\Users\bos12\OneDrive\Desktop\Evolve Agentic Projects\Meta Catalog Feed\schedule_daily.bat" /sc daily /st 02:00 /ru SYSTEM
::
:: This will run the feed generator every day at 2:00 AM.

set SCRIPT_DIR=%~dp0
set LOG_FILE=%SCRIPT_DIR%feed_log.txt

echo [%DATE% %TIME%] Starting Meta Catalog Feed generation... >> "%LOG_FILE%"

cd /d "%SCRIPT_DIR%"

:: Run the Python scraper
python scraper.py >> "%LOG_FILE%" 2>&1

if %ERRORLEVEL% EQU 0 (
    echo [%DATE% %TIME%] Feed generated successfully. >> "%LOG_FILE%"
    :: Upload the XML to your hosting here, e.g. via FTP or aws s3 cp:
    :: aws s3 cp meta_catalog_feed.xml s3://your-bucket/meta_catalog_feed.xml --acl public-read
) else (
    echo [%DATE% %TIME%] ERROR: Feed generation failed. Check log above. >> "%LOG_FILE%"
)
