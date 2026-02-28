@echo off
REM Workaround for Windows Application Control blocking .exe in .venv/Scripts
REM Usage: run ingest init-db
REM        run ingest download
REM        run ingest all
REM        run serve

if "%1"=="ingest" (
    .venv\Scripts\python.exe -c "from app.ingestion.cli import app; app()" %2 %3 %4 %5 %6
) else if "%1"=="serve" (
    .venv\Scripts\python.exe -c "from app.api.run import main; main()" %2 %3 %4 %5 %6
) else (
    echo Usage: run ingest [command]  or  run serve
    echo.
    echo Examples:
    echo   run ingest init-db
    echo   run ingest download
    echo   run ingest all
    echo   run ingest status
    echo   run serve
)
