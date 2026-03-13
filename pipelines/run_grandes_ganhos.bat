@echo off
:: Wrapper do pipeline Grandes Ganhos — chamado pelo Task Scheduler
:: Log salvo em pipelines/logs/grandes_ganhos_YYYY-MM-DD.log

set PYTHON="C:\Users\NITRO\AppData\Local\Programs\Python\Python312\python.exe"
set SCRIPT="C:\Users\NITRO\OneDrive - PGX\MultiBet\pipelines\grandes_ganhos.py"
set LOG_DIR="C:\Users\NITRO\OneDrive - PGX\MultiBet\pipelines\logs"

:: Cria pasta de logs se não existir
if not exist %LOG_DIR% mkdir %LOG_DIR%

:: Nome do arquivo de log com data de hoje
for /f "tokens=2 delims==" %%I in ('wmic os get localdatetime /value') do set DT=%%I
set LOGFILE=%LOG_DIR%\grandes_ganhos_%DT:~0,4%-%DT:~4,2%-%DT:~6,2%.log

:: Executa o pipeline e salva o log
%PYTHON% %SCRIPT% >> %LOGFILE% 2>&1
