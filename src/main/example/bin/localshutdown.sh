#!/bin/ksh
LASTPID=$1
if [[ "${LASTPID}x" != "x" ]]
then
   echo Will use PID ${LASTPID}
else
   LASTPID=`cat /appli/R66/log/lastpid`
fi
if [[ "${LASTPID}X" != "X" ]]
then
  echo try shutting down locally
  kill -s USR1 ${LASTPID}
else
  echo no process seems to be running
fi

