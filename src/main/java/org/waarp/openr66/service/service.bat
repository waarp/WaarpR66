@echo off

rem -- DO NOT CHANGE THIS ! OR YOU REALLY KNOW WHAT YOU ARE DOING ;)

rem -- Organization: 
rem -- EXEC_PATH is root (pid will be there)
rem -- EXEC_PATH/../logs/ will be the log place
rem -- EXEC_PATH/../bin/ is where jsvc is placed
rem -- DAEMON_ROOT is where all you jars are (even commons-daemon)
rem -- DAEMON_NAME will be the service name
rem -- SERVICE_DESCRIPTION will be the service description
rem -- MAIN_DAEMON_CLASS will be the start/stop class used

rem -- Root path where the executables are
set EXEC_PATH=C:\Waarp\Run

rem -- Change this by the path where all jars are
set DAEMON_ROOT=C:\Waarp\Classpath

rem -- Service description
set SERVICE_DESCRIPTION="Waarp R66 Server"

rem -- Service name
set SERVICE_NAME=WaarpR66

rem -- Service CLASSPATH
set SERVICE_CLASSPATH=%DAEMON_ROOT%\myjar.jar

rem -- Service main class
set MAIN_SERVICE_CLASS=org.waarp.openr66.service.R66ServiceLauncher

rem -- Path for log files
set LOG_PATH=%EXEC_PATH%\..\logs

rem -- STDERR log file
set ERR_LOG_FILE=%LOG_PATH%\stderr.txt

rem -- STDOUT log file
set OUT_LOG_FILE=%LOG_PATH%\stdout.txt

rem -- Startup mode (manual or auto)
set SERVICE_STARTUP=auto

rem -- Java memory options
set JAVAxMS=64m
set JAVAxMX=512m

rem -- JVM server option
set JAVASERVER=++JvmOptions=-server

rem -- Logback configuration file
set LOGBACK_CONF=%EXEC_PATH%\..\conf\logback.xml

rem -- Various Java options
set JAVA_OPTS=--JvmMs=%JAVAxMS% --JvmMx=%JAVAxMX% %JAVASERVER% ++JvmOptions=-Dlogback.configurationFile=%LOGBACK_CONF%

rem ---------------------------------------------------------------------------
set SERVICE_OPTIONS=%JAVA_OPTS% --Description=%SERVICE_DESCRIPTION% --Jvm=auto --Classpath=%SERVICE_CLASSPATH% --StartMode=jvm --StartClass=%MAIN_SERVICE_CLASS% --StartMethod=start --StopMode=jvm --StopClass=%MAIN_SERVICE_CLASS% --StopMethod=stop --LogPath=%LOG_PATH% --StdOutput=%OUT_LOG_FILE% --StdError=%ERR_LOG_FILE% --Startup=%SERVICE_STARTUP%

set RESTART=0

:GETOPTS
if /I "%1" == "start" ( goto START )
if /I "%1" == "stop" ( goto STOP )
if /I "%1" == "console" ( goto CONSOLE )
if /I "%1" == "restart" ( goto RESTART )
if /I "%1" == "install" ( goto INSTALL )
if /I "%1" == "remove" ( goto REMOVE )

goto HELP

rem -- START ------------------------------------------------------------------
:START

echo Start service %SERVICE_NAME%
%EXEC_PATH%\windows\prunsrv.exe //RS/%SERVICE_NAME% %SERVICE_OPTIONS%

goto FIN

rem -- INSTALL ----------------------------------------------------------------
:INSTALL

echo Install service %SERVICE_NAME%
%EXEC_PATH%\windows\prunsrv.exe //IS/%SERVICE_NAME% %SERVICE_OPTIONS%

goto FIN

rem -- STOP -------------------------------------------------------------------
:STOP

echo Stop service %SERVICE_NAME%
%EXEC_PATH%\windows\prunsrv.exe //SS/%SERVICE_NAME% %SERVICE_OPTIONS%

if "%RESTART%" == "1" ( goto START )
goto FIN

rem -- REMOVE -----------------------------------------------------------------
:REMOVE

echo Remove service %SERVICE_NAME%
%EXEC_PATH%\windows\prunsrv.exe //DS/%SERVICE_NAME% %SERVICE_OPTIONS%

goto FIN

rem -- CONSOLE ----------------------------------------------------------------
:CONSOLE

%EXEC_PATH%\windows\prunsrv.exe //TS/%SERVICE_NAME% %SERVICE_OPTIONS%

goto FIN

rem -- RESTART ----------------------------------------------------------------
:RESTART

set RESTART=1

goto STOP

rem -- HELP -------------------------------------------------------------------
:HELP

echo "service.bat install|remove|start|stop|restart"
goto FIN

:FIN
