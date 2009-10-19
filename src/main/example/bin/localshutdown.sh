#!/bin/ksh
LASTPID=`cat /appli/R66/bin/lastpid`
if [[ "${LASTPID}X" != "X" ]]
then
  echo try shutting down locally
  kill -s USR1 ${LASTPID}
  rm /appli/R66/bin/lastpid
else
  echo no process seems to be running
fi

