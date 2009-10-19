#!/bin/ksh
SOURCEFILE=$1
TRDATE=$2
TRHOUR=$3
INFO=$4
gunzip -t ${SOURCEFILE}
if [[ $? -ne 0 ]]
then
  echo ${TRDATE} ${TRHOUR} ${SOURCEFILE} ${INFO} not correct gzipped >> /appli/R66/log/transfer.log
else
  echo ${TRDATE} ${TRHOUR} ${SOURCEFILE} ${INFO} ok >> /appli/R66/log/transfer.log 
  rm ${SOURCEFILE}
fi

