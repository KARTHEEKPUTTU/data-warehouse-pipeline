@echo off
setlocal

REM ensure logs folder exists
mkdir "C:\Kartheek_Space\Volume(F)\Full_Stack_&&_DS_ML\data-warehouse-pipeline\DE\phase3\logs" 2>nul

REM run and log output + errors
"C:\Python312\python.exe" "C:\Kartheek_Space\Volume(F)\Full_Stack_&&_DS_ML\data-warehouse-pipeline\DE\phase3\scripts\load_openmeteo_hourly_incremental.py" --lat 40.712800 --lon -74.006000 --name "New York, NY" ^
  >> "C:\Kartheek_Space\Volume(F)\Full_Stack_&&_DS_ML\data-warehouse-pipeline\DE\phase3\logs\openmeteo_task_incremental.log" 2>&1

REM log exit code
echo ExitCode=%ERRORLEVEL% >> "C:\Kartheek_Space\Volume(F)\Full_Stack_&&_DS_ML\data-warehouse-pipeline\DE\phase3\logs\openmeteo_task_incremental.log"

endlocal
exit /b %ERRORLEVEL%
