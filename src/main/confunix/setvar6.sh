#!/bin/sh
export AIXTHREAD_SCOPE=S
# JDK SUN
#export JAVABASE="/usr/local/jdk1.6.0_13"
#export JAVAJDK="${JAVABASE}"
#export JAVA_HOME="${JAVAJDK}/jre"
#export JAVA_OPTS1="-server"
#export JAVA_OPTS2="-Xms256m -Xmx2048m"
#export JAVA_OPTS3=""
#export JAVA_RUN="${JAVA_HOME}/bin/java ${JAVA_OPTS1} ${JAVA_OPTS2} ${JAVA_OPTS3}"
#export JAVA_RUNCLIENT="${JAVA_RUN} "
#export JAVA_RUNSERVER="${JAVA_RUN} "

# JDK IBM
#export JAVABASE="/usr/java6_64"
export JAVABASE="/appli/GGFTP/jdk6/sdk"
export JAVAJDK="${JAVABASE}"
#export JAVA_OPTS3="-Xshareclasses:cacheDir=${JAVABASE}/cache,nonpersistent,nonfatal,expire=480,name="
export JAVA_OPTS3=""
export JAVA_OPTSRESET="-Xshareclasses:cacheDir=${JAVABASE}/cache,reset"
export JAVA_OPTDESTROY="-Xshareclasses:cacheDir=${JAVABASE}/cache,nonpersistent,destroyAll"
export JAVA_OPTLIST="-Xshareclasses:cacheDir=${JAVABASE}/cache,nonpersistent,listAllCaches"
export JAVA_OPTSTAT="-Xshareclasses:cacheDir=${JAVABASE}/cache,printAllStats,nonpersistent"
export JAVA_HOME="${JAVAJDK}/jre"
export JAVA_OPTS1="-Xquickstart -Xgcpolicy:gencon -Xdisableexcessivegc"
export JAVA_OPTS2="-Xms256m -Xmx2048m"
export JAVA_RUN="${JAVA_HOME}/bin/java ${JAVA_OPTS1} ${JAVA_OPTS2} ${JAVA_OPTS3}"
#export JAVA_RUNCLIENT="${JAVA_RUN}GGCLIENT "
#export JAVA_RUNSERVER="${JAVA_RUN}GGSERVER "
export JAVA_RUNCLIENT="${JAVA_RUN} "
export JAVA_RUNSERVER="${JAVA_RUN} "
export JAVA_RUNTEST="${JAVA_RUN} "

export PATH=${JAVA_HOME}/bin:${JAVAJDK}/bin:$PATH
export LDR_CNTRL=LARGE_PAGE_DATA=Y

export GGHOME="/appli/GGFTP"
export GGBIN="${GGHOME}/lib"
# command for Client
export JAVARUNCLIENT="${JAVA_RUNCLIENT} -cp ${GGBIN}/OpenR66.jar:${GGBIN}/commons-exec-1.0.jar:${GGBIN}/GG-Common.jar:${GGBIN}/GG-MD5.jar:${GGBIN}/Netty-Jboss.jar:${GGBIN}/commons-io-1.4.jar:${GGBIN}/dom4j-1.6.1.jar:${GGBIN}/jaxen-1.1.1.jar:${GGBIN}/logback-access-0.9.15.jar:${GGBIN}/logback-classic-0.9.15.jar:${GGBIN}/logback-core-0.9.15.jar:${GGBIN}/slf4j-api-1.5.6.jar:${GGBIN}/h2-1.1.116.jar:${GGBIN}/commons-codec-1.3.jar "
# Command for Server
export JAVARUNSERVER="${JAVA_RUNSERVER} -cp ${GGBIN}/Commons-Compress.jar:${GGBIN}/Commons-Exec.jar:${GGBIN}/GG-Common.jar:${GGBIN}/GG-FTP-FilesystemBased.jar:${GGBIN}/GG-FTP-Implementation.jar:${GGBIN}/GG-FTP-Main.jar:${GGBIN}/GG-FTP-Test-Client.jar:${GGBIN}/Netty-Jboss.jar:${GGBIN}/apiviz-1.3.0.GA.jar:${GGBIN}/commons-io-1.4.jar:${GGBIN}/commons-net-2.0.jar:${GGBIN}/commons-net-ftp-2.0.jar:${GGBIN}/dom4j-1.6.1.jar:${GGBIN}/jaxen-1.1.1.jar:${GGBIN}/logback-access-0.9.15.jar:${GGBIN}/logback-classic-0.9.15.jar:${GGBIN}/logback-core-0.9.15.jar:${GGBIN}/slf4j-api-1.5.6.jar "


