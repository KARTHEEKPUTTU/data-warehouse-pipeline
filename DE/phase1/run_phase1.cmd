@echo off
setlocal
set "CONTAINER=de-postgres"
set "DB=de_db"
set "USER=de_user"
set "SQL_DIR=%~dp0sql"

type "%SQL_DIR%\01_create_insert_validate.sql" "%SQL_DIR%\02_staging_clean_load.sql" "%SQL_DIR%\03_dedup_upsert_dim_people.sql" "%SQL_DIR%\04_fact_events_star_schema.sql" "%SQL_DIR%\05_data_quality_quarantine.sql" "%SQL_DIR%\06_indexes_explain.sql" ^ | docker exec -i %CONTAINER% psql -U %USER% -d %DB% -v ON_ERROR_STOP=1