@echo off/on
set cmd1=C:\Users\Administrator\data\mid\eve\mid_eve_finance_fee.r
call Rscript %cmd1%
::Rscript mid_eve_finance_fee.r
echo %errorlevel%
pause