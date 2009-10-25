echo Start R66Server
. /appli/R66/ENV_R66
nohup ${JAVARUNSERVER} openr66.server.R66Server ${GGHOME}/conf/config`hostname`.xml 0</dev/null 2>&1 >> ${GGHOME}/log/R66Server.log &
echo $! > ${GGHOME}/log/lastpid
echo R66Server started
