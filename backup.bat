@echo off
echo ========================================
echo Auto Backup to GitHub
echo ========================================
echo.

git add .
git commit -m "Auto backup: %date% %time%"
git push origin main

echo.
echo ========================================
echo Backup completed!
echo ========================================
pause
