#!/bin/sh
export AIXTHREAD_SCOPE=S
# JDK SUN
#export JAVABASE="/usr/local/jdk1.6.0_13"
#export JAVAJDK="${JAVABASE}"
#JAVA_HOME="${JAVAJDK}/jre"
#JAVA_OPTS1="-server"
#JAVA_OPTS2="-Xms256m -Xmx4096m"
#JAVA_OPTS3=""
#export JAVA_RUN="${JAVA_HOME}/bin/java ${JAVA_OPTS1} ${JAVA_OPTS2} ${JAVA_OPTS3}"

# JDK IBM
#export JAVABASE="/usr/java6_64"
export JAVABASE="/appli/R66/jdk6/sdk"
export JAVAJDK="${JAVABASE}"
#export JAVA_OPTS3="-Xshareclasses:cacheDir=${JAVABASE}/cache,nonpersistent,nonfatal,expire=480,name="
JAVA_OPTS3=""
JAVA_OPTSRESET="-Xshareclasses:cacheDir=${JAVABASE}/cache,reset"
JAVA_OPTDESTROY="-Xshareclasses:cacheDir=${JAVABASE}/cache,nonpersistent,destroyAll"
JAVA_OPTLIST="-Xshareclasses:cacheDir=${JAVABASE}/cache,nonpersistent,listAllCaches"
JAVA_OPTSTAT="-Xshareclasses:cacheDir=${JAVABASE}/cache,printAllStats,nonpersistent"
JAVA_HOME="${JAVAJDK}/jre"
JAVA_OPTS1="-Xquickstart -Xgcpolicy:gencon -Xdisableexcessivegc"
JAVA_OPTS2="-Xms256m -Xmx4096m"
export JAVA_RUN="${JAVA_HOME}/bin/java ${JAVA_OPTS1} ${JAVA_OPTS2} ${JAVA_OPTS3}"

export PATH=${JAVA_HOME}/bin:${JAVAJDK}/bin:$PATH
export LDR_CNTRL=LARGE_PAGE_DATA=Y

export GGHOME="/appli/R66"
export GGBIN="${GGHOME}/lib"
# command for Client
# Logger
loggerserver=" -Dlogback.configurationFile=${GGHOME}/conf/logback.xml "
loggerclient=" -Dlogback.configurationFile=${GGHOME}/conf/logback-client.xml "
libraries=" ${GGBIN}/slf4j-api-1.5.8.jar:${GGBIN}/logback-access-0.9.17.jar:\
${GGBIN}/logback-classic-0.9.17.jar:${GGBIN}/logback-core-0.9.17.jar:\
${GGBIN}/dom4j-1.6.1.jar:${GGBIN}/jaxen-1.1.1.jar:${GGBIN}/commons-io-1.4.jar:\
${GGBIN}/commons-codec-1.3.jar:${GGBIN}/commons-exec-1.0.jar:\
${GGBIN}/ojdbc5.jar:${GGBIN}/orai18n.jar:\
${GGBIN}/GoldenGateCommon-1.0.2.jar:${GGBIN}/GoldenGateDigest-1.0.0.jar:\
${GGBIN}/NettyExtension-1.1.1.jar:${GGBIN}/netty-3.1.3.GA.jar:\
${GGBIN}/GoldenGateR66-0.6.2.jar "
export JAVARUNCLIENT="${JAVA_RUN} -cp ${libraries} ${loggerclient} "
export JAVARUNSERVER="${JAVA_RUN} -cp ${libraries} ${loggerserver} "


