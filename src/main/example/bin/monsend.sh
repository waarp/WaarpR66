#!/bin/ksh
SOURCEFILE=$1
ORIGINAL=$2
RULE=$3
REMOTEHOST=$4
INFO=$5
TRDATE=$6
TRHOUR=$7
#cp ${SOURCEFILE} /appli/R66/data/out/${ORIGINAL}
RESULT=0
nohup /appli/R66/bin/moncheck.sh ${SOURCEFILE} ${TRDATE} ${TRHOUR} ${INFO} 0</dev/null > /dev/null &
#cd /appli/R66/bin
#/appli/R66/bin/transfer.sh ${REMOTEHOST} ${ORIGINAL} ${RULE} ${SOURCEFILE}
/appli/R66/bin/transfersubmit.sh ${REMOTEHOST} ${ORIGINAL} ${RULE} ${ORIGINAL}-${TRDATE}-${TRHOUR}
RESULT=$?
echo ${TRDATE} ${TRHOUR} ${REMOTEHOST} ${ORIGINAL} from ${SOURCEFILE} ${RULE} ${INFO} >> /appli/R66/log/transfer.log
echo /appli/R66/data/out/${ORIGINAL}
exit ${RESULT}
