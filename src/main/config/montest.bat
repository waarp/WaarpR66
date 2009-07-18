@echo off
set FILEIN=%1
set FILEOUT=%2
set FINALNAME=%3
set ORIGNAME=%4

set /A INDEX=1
:loop
IF (%1)==() GOTO CLEANUP
echo arg %INDEX% %1 >> D:\GG\R66\conf\log.txt
shift
set /A INDEX=INDEX+1
goto loop
:CLEANUP

echo test on "%FINALNAME%" "%ORIGNAME%" >> D:\GG\R66\conf\log.txt
if "%FINALNAME%"=="%ORIGNAME%" goto ok

echo copy %FILEIN% %FILEOUT% >> D:\GG\R66\conf\log.txt
copy %FILEIN% %FILEOUT%
set MYERROR=%ERRORLEVEL%
IF %MYERROR% NEQ 0 goto ko

echo %FILEOUT%
exit /B 0

:ko
echo file copy in error >> D:\GG\R66\conf\log.txt
echo file copy in error
exit /B %MYERROR%

:ok
echo no file copy since source >> D:\GG\R66\conf\log.txt
echo no file copy since source
exit /B 1
