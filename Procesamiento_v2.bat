@echo off
cd /d "%~dp0"
call venv\Scripts\activate
python run_post_processing_v2.py
pause
