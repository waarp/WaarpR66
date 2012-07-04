echo Start R66Server
. /appli/tower/R66/ENV_R66
nohup ${JAVARUNSERVER} openr66.server.R66Server ${R66HOME}/conf/config`hostname`.xml 0</dev/null 2>&1 >> ${R66HOME}/log/R66Server.log &
echo $! > ${R66HOME}/log/lastpid
echo R66Server started
