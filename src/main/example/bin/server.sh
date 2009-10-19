echo Start R66Server
. /appli/R66/bin/setvar6.sh
nohup ${JAVARUNSERVER} openr66.server.R66Server /appli/R66/conf/config`hostname`.xml 0</dev/null 2>&1 >> /appli/R66/log/R66Server.log &
echo $! > /appli/R66/bin/lastpid
echo R66Server started
