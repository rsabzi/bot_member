@echo off
setlocal
cd /d "%~dp0"

if not exist ".env" (
  echo .env پيدا نشد. ابتدا .env را تنظيم كنيد.
  pause
  exit /b 1
)

if not exist ".venv" (
  python -m venv .venv
)

call ".venv\Scripts\activate"
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python -m pip install paramiko
python deploy_full.py
pause
