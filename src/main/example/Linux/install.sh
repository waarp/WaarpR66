#!/bin/sh

if [[ $# -eq 0 ]]
then
	echo "$0 : You must give the main directory where WaarpR66 will be installed"
	echo "Optionnaly follow by JAVA_HOME path then HostID"
	exit 1
fi

# Home TOCHANGEHOMETOCHANGE
INSTALL=$1

if [[ $# -ge 2 ]]
then
	MYHOSTNAME=$2
else
	# Ask for JavaHome TOCHANGEJAVATOCHANGE
	echo What is the Java Path as parent of /jre
	read MYJAVA
fi

if [[ $# -ge 3 ]]
then
	MYHOSTNAME=$3
else
	# Ask for HostName TOCHANGEHOSTTOCHANGE
	echo What is the host name
	read MYHOSTNAME
fi


MYID=$$
sed s/TOCHANGEHOMETOCHANGE/${INSTALL}/g < ENV_R66.source > ENV_R66.${MYID}.1
sed s/TOCHANGEHOSTTOCHANGE/${MYHOSTNAME}/g < ENV_R66.${MYID}.1 > ENV_R66.${MYID}.2
sed s/TOCHANGEJAVATOCHANGE/${MYJAVA}/g < ENV_R66.${MYID}.2 > ENV_R66
rm ENV_R66.${MYID}.1 ENV_R66.${MYID}.2

