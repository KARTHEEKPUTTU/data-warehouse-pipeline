@echo off
setlocal

REM ensure logs folder exists
mkdir "C:\Kartheek_Space\Volume(F)\Full_Stack_&&_DS_ML\data-warehouse-pipeline\DE\phase2\logs" 2>nul

REM run and log output + errors
"C:\Python312\python.exe" "C:\Kartheek_Space\Volume(F)\Full_Stack_&&_DS_ML\data-warehouse-pipeline\DE\phase2\scripts\load_openmeteo_hourly.py" --lat 51.5074 --lon -0.1278 --name "London, UK" ^
  >> "C:\Kartheek_Space\Volume(F)\Full_Stack_&&_DS_ML\data-warehouse-pipeline\DE\phase2\logs\openmeteo_task.log" 2>&1

REM log exit code
echo ExitCode=%ERRORLEVEL% >> "C:\Kartheek_Space\Volume(F)\Full_Stack_&&_DS_ML\data-warehouse-pipeline\DE\phase2\logs\openmeteo_task.log"

endlocal
exit /b %ERRORLEVEL%
