echo off
rem test.bat
rem 同时串行运行多个程序
set cmd1=C:\Users\Administrator\data\flow\test_flow.r
set cmd2=C:\Users\Administrator\data\mid\eve\mid_eve_finance_fee_property2.r
call Rscript %cmd1%
call Rscript %cmd2%

pause
