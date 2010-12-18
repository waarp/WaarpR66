/**
   This file is part of GoldenGate Project (named also GoldenGate or GG).

   Copyright 2009, Frederic Bregier, and individual contributors by the @author
   tags. See the COPYRIGHT.txt in the distribution for a full listing of
   individual contributors.

   All GoldenGate Project is free software: you can redistribute it and/or 
   modify it under the terms of the GNU General Public License as published 
   by the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   GoldenGate is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with GoldenGate .  If not, see <http://www.gnu.org/licenses/>.
 */
package openr66.protocol.http.adminssl;

import goldengate.common.database.DbAdmin;
import goldengate.common.database.DbPreparedStatement;
import goldengate.common.database.DbSession;
import goldengate.common.database.exception.OpenR66DatabaseException;
import goldengate.common.database.exception.OpenR66DatabaseNoConnectionError;
import goldengate.common.database.exception.OpenR66DatabaseSqlError;
import goldengate.common.exception.FileTransferException;
import goldengate.common.exception.InvalidArgumentException;
import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;
import goldengate.common.utility.GgStringUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.SocketAddress;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import openr66.client.Message;
import openr66.configuration.AuthenticationFileBasedConfiguration;
import openr66.configuration.RuleFileBasedConfiguration;
import openr66.context.ErrorCode;
import openr66.context.R66Result;
import openr66.context.R66Session;
import openr66.context.filesystem.R66Dir;
import openr66.database.DbConstant;
import openr66.database.data.DbHostAuth;
import openr66.database.data.DbRule;
import openr66.database.data.DbTaskRunner;
import openr66.protocol.configuration.Configuration;
import openr66.protocol.exception.OpenR66Exception;
import openr66.protocol.exception.OpenR66ExceptionTrappedFactory;
import openr66.protocol.exception.OpenR66ProtocolBusinessException;
import openr66.protocol.exception.OpenR66ProtocolBusinessNoWriteBackException;
import openr66.protocol.exception.OpenR66ProtocolSystemException;
import openr66.protocol.localhandler.LocalChannelReference;
import openr66.protocol.localhandler.packet.ErrorPacket;
import openr66.protocol.localhandler.packet.RequestPacket;
import openr66.protocol.localhandler.packet.RequestPacket.TRANSFERMODE;
import openr66.protocol.localhandler.packet.TestPacket;
import openr66.protocol.networkhandler.NetworkTransaction;
import openr66.protocol.utils.ChannelUtils;
import openr66.protocol.utils.FileUtils;
import openr66.protocol.utils.NbAndSpecialId;
import openr66.protocol.utils.R66Future;
import openr66.protocol.utils.TransferUtils;
import openr66.protocol.utils.Version;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.handler.codec.http.Cookie;
import org.jboss.netty.handler.codec.http.CookieDecoder;
import org.jboss.netty.handler.codec.http.CookieEncoder;
import org.jboss.netty.handler.codec.http.DefaultCookie;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.jboss.netty.handler.codec.http.QueryStringDecoder;
import org.jboss.netty.handler.ssl.SslHandler;
import org.jboss.netty.handler.traffic.TrafficCounter;

/**
 * @author Frederic Bregier
 *
 */
public class HttpSslHandler extends SimpleChannelUpstreamHandler {
    /**
     * Internal Logger
     */
    private static final GgInternalLogger logger = GgInternalLoggerFactory
            .getLogger(HttpSslHandler.class);
    /**
     * Waiter for SSL handshake is finished
     */
    private static final ConcurrentHashMap<Integer, R66Future> waitForSsl
        = new ConcurrentHashMap<Integer, R66Future>();
    /**
     * Session Management
     */
    private static final ConcurrentHashMap<String, R66Session> sessions
        = new ConcurrentHashMap<String, R66Session>();
    private static final ConcurrentHashMap<String, DbSession> dbSessions
        = new ConcurrentHashMap<String, DbSession>();
    private volatile R66Session authentHttp = new R66Session();

    private volatile HttpRequest request;
    private volatile boolean newSession = false;
    private volatile Cookie admin = null;
    private final StringBuilder responseContent = new StringBuilder();
    private volatile String uriRequest;
    private volatile Map<String, List<String>> params;
    private volatile QueryStringDecoder queryStringDecoder;
    private volatile boolean forceClose = false;
    private volatile boolean shutdown = false;

    private static final String R66SESSION = "R66SESSION";
    private static enum REQUEST {
        Logon("Logon.html"), 
        index("index.html"),
        error("error.html"),
        Transfers("Transfers.html"), 
        Listing("Listing_head.html","Listing_body0.html","Listing_body.html","Listing_body1.html","Listing_end.html"), 
        CancelRestart("CancelRestart_head.html","CancelRestart_body0.html","CancelRestart_body.html","CancelRestart_body1.html","CancelRestart_end.html"), 
        Export("Export.html"),
        Hosts("Hosts_head.html","Hosts_body0.html","Hosts_body.html","Hosts_body1.html","Hosts_end.html"), 
        Rules("Rules_head.html","Rules_body0.html","Rules_body.html","Rules_body1.html","Rules_end.html"), 
        System("System.html");
        
        private String header;
        private String headerBody;
        private String body;
        private String endBody;
        private String end;
        /**
         * Constructor for a unique file
         * @param uniquefile
         */
        private REQUEST(String uniquefile) {
            this.header = uniquefile;
            this.headerBody = null;
            this.body = null;
            this.endBody = null;
            this.end = null;
        }
        /**
         * @param header
         * @param headerBody
         * @param body
         * @param endBody
         * @param end
         */
        private REQUEST(String header, String headerBody, String body,
                String endBody, String end) {
            this.header = header;
            this.headerBody = headerBody;
            this.body = body;
            this.endBody = endBody;
            this.end = end;
        }
        
        /**
         * Reader for a unique file
         * @return the content of the unique file
         */
        public String readFileUnique(HttpSslHandler handler) {
            return handler.readFileHeader(Configuration.configuration.httpBasePath+this.header);
        }
        
        public String readHeader(HttpSslHandler handler) {
            return handler.readFileHeader(Configuration.configuration.httpBasePath+this.header);
        }
        public String readBodyHeader() {
            return GgStringUtils.readFile(Configuration.configuration.httpBasePath+this.headerBody);
        }
        public String readBody() {
            return GgStringUtils.readFile(Configuration.configuration.httpBasePath+this.body);
        }
        public String readBodyEnd() {
            return GgStringUtils.readFile(Configuration.configuration.httpBasePath+this.endBody);
        }
        public String readEnd() {
            return GgStringUtils.readFile(Configuration.configuration.httpBasePath+this.end);
        }
    }
    
    private static enum REPLACEMENT {
        XXXHOSTIDXXX, XXXADMINXXX, XXXVERSIONXXX, XXXBANDWIDTHXXX,
        XXXXSESSIONLIMITRXXX, XXXXSESSIONLIMITWXXX,
        XXXXCHANNELLIMITRXXX, XXXXCHANNELLIMITWXXX,
        XXXXDELAYCOMMDXXX, XXXXDELAYRETRYXXX,
        XXXLOCALXXX, XXXNETWORKXXX,
        XXXERRORMESGXXX;
    }
    public static final int LIMITROW = 48;// better if it can be divided by 4

    /**
     * The Database connection attached to this NetworkChannel
     * shared among all associated LocalChannels in the session
     */
    private volatile DbSession dbSession = null;
    /**
     * Does this dbSession is private and so should be closed
     */
    private volatile boolean isPrivateDbSession = false;

    /**
     * Remover from SSL HashMap
     */
    private static final ChannelFutureListener remover = new ChannelFutureListener() {
        public void operationComplete(ChannelFuture future) {
            logger.debug("SSL remover");
            waitForSsl.remove(future.getChannel().getId());
        }
    };

    private String readFileHeader(String filename) {
        String value;
        try {
            value = GgStringUtils.readFileException(filename);
        } catch (InvalidArgumentException e) {
            logger.error("Error while trying to open: "+filename,e);
            return "";
        } catch (FileTransferException e) {
            logger.error("Error while trying to read: "+filename,e);
            return "";
        }
        StringBuilder builder = new StringBuilder(value);
        FileUtils.replace(builder, REPLACEMENT.XXXLOCALXXX.toString(),
                Integer.toString(
                        Configuration.configuration.getLocalTransaction().
                        getNumberLocalChannel())+" "+Thread.activeCount());
        FileUtils.replace(builder, REPLACEMENT.XXXNETWORKXXX.toString(),
                Integer.toString(
                        DbAdmin.getNbConnection()));
        FileUtils.replace(builder, REPLACEMENT.XXXHOSTIDXXX.toString(),
                Configuration.configuration.HOST_ID);
        if (authentHttp.isAuthenticated()) {
            FileUtils.replace(builder, REPLACEMENT.XXXADMINXXX.toString(),
                "Connected");
        } else {
            FileUtils.replace(builder, REPLACEMENT.XXXADMINXXX.toString(),
                    "Not authenticated");
        }
        TrafficCounter trafficCounter =
            Configuration.configuration.getGlobalTrafficShapingHandler().getTrafficCounter();
        FileUtils.replace(builder, REPLACEMENT.XXXBANDWIDTHXXX.toString(),
                "IN:"+(trafficCounter.getLastReadThroughput()/131072)+
                "Mbits&nbsp;<br>&nbsp;OUT:"+
                (trafficCounter.getLastWriteThroughput()/131072)+"Mbits");
        return builder.toString();
    }

    private String getTrimValue(String varname) {
        String value = params.get(varname).get(0).trim();
        if (value.length() == 0) {
            value = null;
        }
        return value;
    }
    private String getValue(String varname) {
        return params.get(varname).get(0);
    }
    /**
     * Add the Channel as SSL handshake is over
     * @param channel
     */
    private static void addSslConnectedChannel(Channel channel) {
        R66Future futureSSL = new R66Future(true);
        waitForSsl.put(channel.getId(),futureSSL);
        channel.getCloseFuture().addListener(remover);
    }
    /**
     * Set the future of SSL handshake to status
     * @param channel
     * @param status
     */
    private static void setStatusSslConnectedChannel(Channel channel, boolean status) {
        R66Future futureSSL = waitForSsl.get(channel.getId());
        if (futureSSL != null) {
            if (status) {
                futureSSL.setSuccess();
            } else {
                futureSSL.cancel();
            }
        }
    }

    /* (non-Javadoc)
     * @see org.jboss.netty.channel.SimpleChannelUpstreamHandler#channelOpen(org.jboss.netty.channel.ChannelHandlerContext, org.jboss.netty.channel.ChannelStateEvent)
     */
    @Override
    public void channelOpen(ChannelHandlerContext ctx, ChannelStateEvent e)
            throws Exception {
        Channel channel = e.getChannel();
        logger.debug("Add channel to ssl");
        addSslConnectedChannel(channel);
        Configuration.configuration.getHttpChannelGroup().add(channel);
        super.channelOpen(ctx, e);
    }

    

    private String index() {
        String index = REQUEST.index.readFileUnique(this);
        StringBuilder builder = new StringBuilder(index);
        FileUtils.replaceAll(builder, REPLACEMENT.XXXHOSTIDXXX.toString(),
                Configuration.configuration.HOST_ID);
        FileUtils.replaceAll(builder, REPLACEMENT.XXXADMINXXX.toString(),
                "Administrator Connected");
        FileUtils.replace(builder, REPLACEMENT.XXXVERSIONXXX.toString(),
                Version.ID);
        return builder.toString();
    }
    private String error(String mesg) {
        String index = REQUEST.error.readFileUnique(this);
        return index.replaceAll(REPLACEMENT.XXXERRORMESGXXX.toString(),
                mesg);
    }
    private String Logon() {
        return REQUEST.Logon.readFileUnique(this);
    }
    private String Transfers() {
        return REQUEST.Transfers.readFileUnique(this);
    }

    private String resetOptionTransfer(String header, String startid, String stopid,
            String start, String stop, String rule, String req,
            boolean pending, boolean transfer, boolean error, boolean done, boolean all) {
        StringBuilder builder = new StringBuilder(header);
        FileUtils.replace(builder, "XXXSTARTIDXXX", startid);
        FileUtils.replace(builder, "XXXSTOPIDXXX", stopid);
        FileUtils.replace(builder, "XXXSTARTXXX", start);
        FileUtils.replace(builder, "XXXSTOPXXX", stop);
        FileUtils.replace(builder, "XXXRULEXXX", rule);
        FileUtils.replace(builder, "XXXREQXXX", req);
        FileUtils.replace(builder, "XXXPENDXXX", pending ? "checked":"");
        FileUtils.replace(builder, "XXXTRANSXXX", transfer ? "checked":"");
        FileUtils.replace(builder, "XXXERRXXX", error ? "checked":"");
        FileUtils.replace(builder, "XXXDONEXXX", done ? "checked":"");
        FileUtils.replace(builder, "XXXALLXXX", all ? "checked":"");
        return builder.toString();
    }
    private String Listing() {
        getParams();
        if (params == null) {
            String head = REQUEST.Listing.readHeader(this);
            head = resetOptionTransfer(head, "", "", "", "", "", "",
                    false, false, false, false, true);
            String end = REQUEST.Listing.readEnd();
            return head+end;
        }
        String head = REQUEST.Listing.readHeader(this);
        String body0, body, body1;
        body0 = body1 = body = "";
        List<String> parms = params.get("ACTION");
        if (parms != null) {
            body0 = REQUEST.Listing.readBodyHeader();
            String parm = parms.get(0);
            if ("Filter".equalsIgnoreCase(parm)) {
                String startid = getTrimValue("startid");
                String stopid = getTrimValue("stopid");
                if (startid != null && stopid == null) {
                    stopid = Long.toString(Long.parseLong(startid)+(LIMITROW/2));
                }
                if (stopid != null && startid == null) {
                    startid = Long.toString(Long.parseLong(stopid)-(LIMITROW/2));
                }
                String start = getValue("start");
                String stop = getValue("stop");
                String rule = getTrimValue("rule");
                String req = getTrimValue("req");
                boolean pending, transfer, error, done, all;
                pending = params.containsKey("pending");
                transfer = params.containsKey("transfer");
                error = params.containsKey("error");
                done = params.containsKey("done");
                all = params.containsKey("all");
                if (pending && transfer && error && done) {
                    all = true;
                } else if (!(pending || transfer || error || done)) {
                    all = true;
                }
                Timestamp tstart = GgStringUtils.fixDate(start);
                if (tstart != null) {
                    start = tstart.toString();
                }
                Timestamp tstop = GgStringUtils.fixDate(stop, tstart);
                if (tstop != null) {
                    stop = tstop.toString();
                }
                head = resetOptionTransfer(head, startid == null ? "":startid,
                        stopid == null ? "":stopid, start, stop,
                        rule == null ? "":rule, req == null ? "":req,
                        pending, transfer, error, done, all);
                body = REQUEST.Listing.readBody();
                DbPreparedStatement preparedStatement = null;
                try {
                    preparedStatement =
                        DbTaskRunner.getFilterPrepareStament(dbSession, LIMITROW, false,
                                startid, stopid, tstart, tstop, rule, req,
                                pending, transfer, error, done, all);
                    preparedStatement.executeQuery();
                    StringBuilder builder = new StringBuilder();
                    int i = 0;
                    while (preparedStatement.getNext()) {
                        try {
                            i++;
                            DbTaskRunner taskRunner = DbTaskRunner.getFromStatement(preparedStatement);
                            LocalChannelReference lcr =
                                Configuration.configuration.getLocalTransaction().
                                getFromRequest(taskRunner.getKey());
                            builder.append(taskRunner.toSpecializedHtml(authentHttp, body,
                                    lcr != null ? "Active" : "NotActive"));
                            if (i > LIMITROW) {
                                break;
                            }
                        } catch (OpenR66DatabaseException e) {
                            // try to continue if possible
                            logger.warn("An error occurs while accessing a Runner: {}",
                                    e.getMessage());
                            continue;
                        }
                    }
                    preparedStatement.realClose();
                    body = builder.toString();
                } catch (OpenR66DatabaseException e) {
                    if (preparedStatement != null) {
                        preparedStatement.realClose();
                    }
                    logger.warn("OpenR66 Web Error {}",e.getMessage());
                }
            } else {
                head = resetOptionTransfer(head, "", "", "", "", "", "",
                        false, false, false, false, true);
            }
            body1 = REQUEST.Listing.readBodyEnd();
        } else {
            head = resetOptionTransfer(head, "", "", "", "", "", "",
                    false, false, false, false, true);
        }
        String end;
        end = REQUEST.Listing.readEnd();
        return head+body0+body+body1+end;
    }

    private String CancelRestart() {
        getParams();
        if (params == null) {
            String head = REQUEST.CancelRestart.readHeader(this);
            head = resetOptionTransfer(head, "", "", "", "", "", "",
                    false, false, false, false, true);
            String end;
            end = REQUEST.CancelRestart.readEnd();
            return head+end;
        }
        String head = REQUEST.CancelRestart.readHeader(this);
        String body0, body, body1;
        body0 = body1 = body = "";
        List<String> parms = params.get("ACTION");
        if (parms != null) {
            body0 = REQUEST.CancelRestart.readBodyHeader();
            String parm = parms.get(0);
            if ("Filter".equalsIgnoreCase(parm)) {
                String startid = getTrimValue("startid");
                String stopid = getTrimValue("stopid");
                if (startid != null && stopid == null) {
                    stopid = Long.toString(Long.parseLong(startid)+(LIMITROW/2));
                }
                if (stopid != null && startid == null) {
                    startid = Long.toString(Long.parseLong(stopid)-(LIMITROW/2));
                }
                String start = getValue("start");
                String stop = getValue("stop");
                String rule = getTrimValue("rule");
                String req = getTrimValue("req");
                boolean pending, transfer, error, done, all;
                pending = params.containsKey("pending");
                transfer = params.containsKey("transfer");
                error = params.containsKey("error");
                done = params.containsKey("done");
                all = params.containsKey("all");
                if (pending && transfer && error && done) {
                    all = true;
                } else if (!(pending || transfer || error || done)) {
                    all = true;
                }
                Timestamp tstart = GgStringUtils.fixDate(start);
                if (tstart != null) {
                    start = tstart.toString();
                }
                Timestamp tstop = GgStringUtils.fixDate(stop, tstart);
                if (tstop != null) {
                    stop = tstop.toString();
                }
                head = resetOptionTransfer(head, startid == null ? "":startid,
                        stopid == null ? "":stopid, start, stop,
                        rule == null ? "":rule, req == null ? "":req,
                        pending, transfer, error, done, all);
                body = REQUEST.CancelRestart.readBody();
                DbPreparedStatement preparedStatement = null;
                try {
                    preparedStatement =
                        DbTaskRunner.getFilterPrepareStament(dbSession, LIMITROW, false,
                                startid, stopid, tstart, tstop, rule, req,
                                pending, transfer, error, done, all);
                    preparedStatement.executeQuery();
                    StringBuilder builder = new StringBuilder();
                    int i = 0;
                    while (preparedStatement.getNext()) {
                        try {
                            i++;
                            DbTaskRunner taskRunner = DbTaskRunner.getFromStatement(preparedStatement);
                            LocalChannelReference lcr =
                                Configuration.configuration.getLocalTransaction().
                                getFromRequest(taskRunner.getKey());
                            builder.append(taskRunner.toSpecializedHtml(authentHttp, body,
                                    lcr != null ? "Active" : "NotActive"));
                            if (i > LIMITROW) {
                                break;
                            }
                        } catch (OpenR66DatabaseException e) {
                            // try to continue if possible
                            logger.warn("An error occurs while accessing a Runner: {}",
                                    e.getMessage());
                            continue;
                        }
                    }
                    preparedStatement.realClose();
                    body = builder.toString();
                } catch (OpenR66DatabaseException e) {
                    if (preparedStatement != null) {
                        preparedStatement.realClose();
                    }
                    logger.warn("OpenR66 Web Error {}",e.getMessage());
                }
                body1 = REQUEST.CancelRestart.readBodyEnd();
            } else if ("RestartAll".equalsIgnoreCase(parm) ||
                    "StopAll".equalsIgnoreCase(parm)) {
                boolean stopcommand = "StopAll".equalsIgnoreCase(parm);
                String startid = getTrimValue("startid");
                String stopid = getTrimValue("stopid");
                String start = getValue("start");
                String stop = getValue("stop");
                String rule = getTrimValue("rule");
                String req = getTrimValue("req");
                boolean pending, transfer, error, done, all;
                pending = params.containsKey("pending");
                transfer = params.containsKey("transfer");
                error = params.containsKey("error");
                done = false;
                all = false;
                if (pending && transfer && error && done) {
                    all = true;
                } else if (!(pending || transfer || error || done)) {
                    all = true;
                }
                Timestamp tstart = GgStringUtils.fixDate(start);
                if (tstart != null) {
                    start = tstart.toString();
                }
                Timestamp tstop = GgStringUtils.fixDate(stop, tstart);
                if (tstop != null) {
                    stop = tstop.toString();
                }
                head = resetOptionTransfer(head, startid == null ? "":startid,
                        stopid == null ? "":stopid, start, stop,
                        rule == null ? "":rule, req == null ? "":req,
                        pending, transfer, error, done, all);
                body = REQUEST.CancelRestart.readBody();
                StringBuilder builder = new StringBuilder();
                if (stopcommand) {
                    builder = TransferUtils.stopSelectedTransfers(dbSession, LIMITROW, builder,
                            authentHttp, body, startid, stopid, tstart, tstop, rule, req,
                            pending, transfer, error);
                } else {
                    DbPreparedStatement preparedStatement = null;
                    try {
                        preparedStatement =
                            DbTaskRunner.getFilterPrepareStament(dbSession, LIMITROW, false,
                                    startid, stopid, tstart, tstop, rule, req,
                                    pending, transfer, error, done, all);
                        preparedStatement.executeQuery();
                        int i = 0;
                        while (preparedStatement.getNext()) {
                            try {
                                i++;
                                DbTaskRunner taskRunner = DbTaskRunner.getFromStatement(preparedStatement);
                                LocalChannelReference lcr =
                                    Configuration.configuration.getLocalTransaction().
                                    getFromRequest(taskRunner.getKey());
                                R66Result finalResult = TransferUtils.restartTransfer(taskRunner, lcr);
                                ErrorCode result = finalResult.code;
                                ErrorCode last = taskRunner.getErrorInfo();
                                taskRunner.setErrorExecutionStatus(result);
                                builder.append(taskRunner.toSpecializedHtml(authentHttp, body,
                                        lcr != null ? "Active" : "NotActive"));
                                taskRunner.setErrorExecutionStatus(last);
                                if (i > LIMITROW) {
                                    break;
                                }
                            } catch (OpenR66DatabaseException e) {
                                // try to continue if possible
                                logger.warn("An error occurs while accessing a Runner: {}",
                                        e.getMessage());
                                continue;
                            }                        }
                        preparedStatement.realClose();
                    } catch (OpenR66DatabaseException e) {
                        if (preparedStatement != null) {
                            preparedStatement.realClose();
                        }
                        logger.warn("OpenR66 Web Error {}",e.getMessage());
                    }
                }
                body = builder.toString();
                body1 = REQUEST.CancelRestart.readBodyEnd();
            } else if ("Cancel".equalsIgnoreCase(parm) || "Stop".equalsIgnoreCase(parm)) {
                // Cancel or Stop
                boolean stop = "Stop".equalsIgnoreCase(parm);
                String specid = getValue("specid");
                String reqd = getValue("reqd");
                String reqr = getValue("reqr");
                LocalChannelReference lcr =
                    Configuration.configuration.getLocalTransaction().
                    getFromRequest(reqd+" "+reqr+" "+specid);
                // stop the current transfer
                ErrorCode result;
                long lspecid = Long.parseLong(specid);
                DbTaskRunner taskRunner = null;
                try {
                    taskRunner = new DbTaskRunner(dbSession, authentHttp, null,
                            lspecid, reqr, reqd);
                } catch (OpenR66DatabaseException e) {
                }
                if (taskRunner == null) {
                    body = "";
                    body1 = REQUEST.CancelRestart.readBodyEnd();
                    body1 += "<br><b>"+parm+" aborted since Transfer is not found</b>";
                    String end;
                    end = REQUEST.CancelRestart.readEnd();
                    return head+body0+body+body1+end;
                }
                ErrorCode code = (stop) ?
                        ErrorCode.StoppedTransfer : ErrorCode.CanceledTransfer;
                if (lcr != null) {
                    int rank = taskRunner.getRank();
                    ErrorPacket error = new ErrorPacket("Transfer "+parm+" "+rank,
                            code.getCode(), ErrorPacket.FORWARDCLOSECODE);
                    try {
                        //XXX ChannelUtils.writeAbstractLocalPacket(lcr, error);
                        // inform local instead of remote
                        ChannelUtils.writeAbstractLocalPacketToLocal(lcr, error);
                    } catch (Exception e) {
                    }
                    result = ErrorCode.CompleteOk;
                } else {
                    // Transfer is not running
                    // But is the database saying the contrary
                    result = ErrorCode.TransferOk;
                    if (taskRunner != null) {
                        if (taskRunner.stopOrCancelRunner(code)) {
                            result = ErrorCode.CompleteOk;
                        }
                    }
                }
                if (taskRunner != null) {
                    body = REQUEST.CancelRestart.readBody();
                    body = taskRunner.toSpecializedHtml(authentHttp, body,
                            lcr != null ? "Active" : "NotActive");
                    String tstart = taskRunner.getStart().toString();
                    tstart = tstart.substring(0, tstart.length());
                    String tstop = taskRunner.getStop().toString();
                    tstop = tstop.substring(0, tstop.length());
                    head = resetOptionTransfer(head, (taskRunner.getSpecialId()-1)+"",
                            (taskRunner.getSpecialId()+1)+"", tstart, tstop,
                            taskRunner.getRuleId(), taskRunner.getRequested(),
                            false, false, false, false, true);
                }
                body1 = REQUEST.CancelRestart.readBodyEnd();
                body1 += "<br><b>"+(result == ErrorCode.CompleteOk ? parm+" transmitted":
                    parm+" aborted since Transfer is not running")+"</b>";
            } else if ("Restart".equalsIgnoreCase(parm)) {
                // Restart
                String specid = getValue("specid");
                String reqd = getValue("reqd");
                String reqr = getValue("reqr");
                long lspecid = Long.parseLong(specid);
                DbTaskRunner taskRunner;
                String comment;
                try {
                    taskRunner = new DbTaskRunner(dbSession, authentHttp, null,
                            lspecid, reqr, reqd);
                    LocalChannelReference lcr =
                        Configuration.configuration.getLocalTransaction().
                        getFromRequest(taskRunner.getKey());
                    R66Result finalResult = TransferUtils.restartTransfer(taskRunner, lcr);
                    comment = (String) finalResult.other;
                    body = REQUEST.CancelRestart.readBody();
                    body = taskRunner.toSpecializedHtml(authentHttp, body,
                            lcr != null ? "Active" : "NotActive");
                    String tstart = taskRunner.getStart().toString();
                    tstart = tstart.substring(0, tstart.length());
                    String tstop = taskRunner.getStop().toString();
                    tstop = tstop.substring(0, tstop.length());
                    head = resetOptionTransfer(head, (taskRunner.getSpecialId()-1)+"",
                            (taskRunner.getSpecialId()+1)+"", tstart, tstop,
                            taskRunner.getRuleId(), taskRunner.getRequested(),
                            false, false, false, false, true);
                } catch (OpenR66DatabaseException e) {
                    body = "";
                    comment = "Internal error";
                }
                body1 = REQUEST.CancelRestart.readBodyEnd();
                body1 += "<br><b>"+comment+"</b>";
            } else {
                head = resetOptionTransfer(head, "", "", "", "", "", "",
                        false, false, false, false, true);
            }
        } else {
            head = resetOptionTransfer(head, "", "", "", "", "", "",
                    false, false, false, false, true);
        }
        String end;
        end = REQUEST.CancelRestart.readEnd();
        return head+body0+body+body1+end;
    }
    private String Export() {
        getParams();
        if (params == null) {
            String body = REQUEST.Export.readFileUnique(this);
            body = resetOptionTransfer(body, "", "", "", "", "", "",
                    false, false, false, true, false);
            return body.replace("XXXRESULTXXX", "");
        }
        String body = REQUEST.Export.readFileUnique(this);
        String start = getValue("start");
        String stop = getValue("stop");
        String rule = getTrimValue("rule");
        String req = getTrimValue("req");
        boolean pending, transfer, error, done, all;
        pending = params.containsKey("pending");
        transfer = params.containsKey("transfer");
        error = params.containsKey("error");
        done = params.containsKey("done");
        all = params.containsKey("all");
        boolean toPurge = params.containsKey("purge");
        if (toPurge) {
            transfer = false;
        }
        if (pending && transfer && error && done) {
            all = true;
        } else if (!(pending || transfer || error || done)) {
            all = true;
        }
        Timestamp tstart = GgStringUtils.fixDate(start);
        if (tstart != null) {
            start = tstart.toString();
        }
        Timestamp tstop = GgStringUtils.fixDate(stop, tstart);
        if (tstop != null) {
            stop = tstop.toString();
        }
        body = resetOptionTransfer(body, "", "", start, stop,
                rule == null ? "":rule, req == null ? "":req,
                pending, transfer, error, done, all);
        boolean isexported = true;
        // clean a bit the database before exporting
        try {
            DbTaskRunner.changeFinishedToDone(dbSession);
        } catch (OpenR66DatabaseNoConnectionError e2) {
            // should not be
        }
        // create export of log and optionally purge them from database
        DbPreparedStatement getValid = null;
        NbAndSpecialId nbAndSpecialId = null;
        String filename = Configuration.configuration.baseDirectory+
            Configuration.configuration.archivePath+R66Dir.SEPARATOR+
            Configuration.configuration.HOST_ID+"_"+System.currentTimeMillis()+
            "_runners.xml";
        try {
            getValid =
                DbTaskRunner.getFilterPrepareStament(dbSession, 0,// 0 means no limit
                        true, null, null, tstart, tstop, rule, req,
                        pending, transfer, error, done, all);
            nbAndSpecialId = DbTaskRunner.writeXMLWriter(getValid, filename);
        } catch (OpenR66DatabaseNoConnectionError e1) {
            isexported = false;
            toPurge = false;
        } catch (OpenR66DatabaseSqlError e1) {
            isexported = false;
            toPurge = false;
        } catch (OpenR66ProtocolBusinessException e) {
            isexported = false;
            toPurge = false;
        } finally {
            if (getValid != null) {
                getValid.realClose();
            }
        }
        int purge = 0;
        if (isexported && nbAndSpecialId != null) {
            if (nbAndSpecialId.nb <= 0) {
                return body.replace("XXXRESULTXXX",
                        "Export unsuccessful since no records were found");
            }
            // in case of purge
            if (isexported && toPurge) {
                // purge with same filter all runners where globallasttep
                // is ALLDONE or ERROR
                // but getting the higher Special first
                String stopId = Long.toString(nbAndSpecialId.higherSpecialId);
                try {
                    purge =
                        DbTaskRunner.purgeLogPrepareStament(dbSession,
                                null,stopId, tstart, tstop, rule, req,
                                pending, transfer, error, done, all);
                } catch (OpenR66DatabaseNoConnectionError e) {
                } catch (OpenR66DatabaseSqlError e) {
                }
            }
        }
        return body.replace("XXXRESULTXXX", "Export "+(isexported?"successful into "+
                filename+" with "+nbAndSpecialId.nb+" exported and "+purge+" purged records":
                    "in error"));
    }
    private String resetOptionHosts(String header,
            String host, String addr, boolean ssl) {
        StringBuilder builder = new StringBuilder(header);
        FileUtils.replace(builder, "XXXFHOSTXXX", host);
        FileUtils.replace(builder, "XXXFADDRXXX", addr);
        FileUtils.replace(builder, "XXXFSSLXXX", ssl ? "checked":"");
        return builder.toString();
    }
    private String Hosts() {
        getParams();
        String head = REQUEST.Hosts.readHeader(this);
        String end;
        end = REQUEST.Hosts.readEnd();
        if (params == null) {
            head = resetOptionHosts(head, "", "", false);
            return head+end;
        }
        String body0, body, body1;
        body0 = body1 = body = "";
        List<String> parms = params.get("ACTION");
        if (parms != null) {
            body0 = REQUEST.Hosts.readBodyHeader();
            String parm = parms.get(0);
            if ("Create".equalsIgnoreCase(parm)) {
                String host = getTrimValue("host");
                String addr = getTrimValue("address");
                String port = getTrimValue("port");
                String key = getTrimValue("hostkey");
                boolean ssl, admin, isclient;
                ssl = params.containsKey("ssl");
                admin = params.containsKey("admin");
                isclient = params.containsKey("isclient");
                if (host == null || addr == null || port == null || key == null) {
                    body0 = body1 = body = "";
                    body = "<p><center><b>Not enough data to create a Host</b></center></p>";
                    head = resetOptionHosts(head, "", "", false);
                    return head+body0+body+body1+end;
                }
                head = resetOptionHosts(head, host, addr, ssl);
                int iport = Integer.parseInt(port);
                DbHostAuth dbhost = new DbHostAuth(dbSession, host, addr, iport,
                        ssl, key.getBytes(), admin, isclient);
                try {
                    dbhost.insert();
                } catch (OpenR66DatabaseException e) {
                    body0 = body1 = body = "";
                    body = "<p><center><b>Cannot create a Host: "+e.getMessage()+"</b></center></p>";
                    head = resetOptionHosts(head, "", "", false);
                    return head+body0+body+body1+end;
                }
                body = REQUEST.Hosts.readBody();
                body = dbhost.toSpecializedHtml(authentHttp, body, false);
            } else if ("Filter".equalsIgnoreCase(parm)) {
                String host = getTrimValue("host");
                String addr = getTrimValue("address");
                boolean ssl = params.containsKey("ssl");
                head = resetOptionHosts(head, host == null ? "":host,
                        addr == null ? "":addr, ssl);
                body = REQUEST.Hosts.readBody();
                DbPreparedStatement preparedStatement = null;
                try {
                    preparedStatement =
                        DbHostAuth.getFilterPrepareStament(dbSession,
                                host, addr, ssl);
                    preparedStatement.executeQuery();
                    StringBuilder builder = new StringBuilder();
                    int i = 0;
                    while (preparedStatement.getNext()) {
                        i++;
                        DbHostAuth dbhost = DbHostAuth.getFromStatement(preparedStatement);
                        builder.append(dbhost.toSpecializedHtml(authentHttp, body, false));
                        if (i > LIMITROW) {
                            break;
                        }
                    }
                    preparedStatement.realClose();
                    body = builder.toString();
                } catch (OpenR66DatabaseException e) {
                    if (preparedStatement != null) {
                        preparedStatement.realClose();
                    }
                    logger.warn("OpenR66 Web Error {}",e.getMessage());
                }
                body1 = REQUEST.Hosts.readBodyEnd();
            } else if ("Update".equalsIgnoreCase(parm)) {
                String host = getTrimValue("host");
                String addr = getTrimValue("address");
                String port = getTrimValue("port");
                String key = getTrimValue("hostkey");
                boolean ssl, admin, isclient;
                ssl = params.containsKey("ssl");
                admin = params.containsKey("admin");
                isclient = params.containsKey("isclient");
                if (host == null || addr == null || port == null || key == null) {
                    body0 = body1 = body = "";
                    body = "<p><center><b>Not enough data to update a Host</b></center></p>";
                    head = resetOptionHosts(head, "", "", false);
                    return head+body0+body+body1+end;
                }
                head = resetOptionHosts(head, host, addr, ssl);
                int iport = Integer.parseInt(port);
                DbHostAuth dbhost = new DbHostAuth(dbSession, host, addr, iport,
                        ssl, key.getBytes(), admin, isclient);
                try {
                    if (dbhost.exist()) {
                        dbhost.update();
                    } else {
                        dbhost.insert();
                    }
                } catch (OpenR66DatabaseException e) {
                    body0 = body1 = body = "";
                    body = "<p><center><b>Cannot update a Host: "+e.getMessage()+"</b></center></p>";
                    head = resetOptionHosts(head, "", "", false);
                    return head+body0+body+body1+end;
                }
                body = REQUEST.Hosts.readBody();
                body = dbhost.toSpecializedHtml(authentHttp, body, false);
            } else if ("TestConn".equalsIgnoreCase(parm)) {
                String host = getTrimValue("host");
                String addr = getTrimValue("address");
                String port = getTrimValue("port");
                String key = getTrimValue("hostkey");
                boolean ssl, admin, isclient;
                ssl = params.containsKey("ssl");
                admin = params.containsKey("admin");
                isclient = params.containsKey("isclient");
                head = resetOptionHosts(head, host, addr, ssl);
                int iport = Integer.parseInt(port);
                DbHostAuth dbhost = new DbHostAuth(dbSession, host, addr, iport,
                        ssl, key.getBytes(), admin, isclient);
                R66Future result = new R66Future(true);
                TestPacket packet = new TestPacket("MSG", "CheckConnection", 100);
                Message transaction = new Message(
                        Configuration.configuration.getInternalRunner().getNetworkTransaction(),
                        result, dbhost, packet);
                transaction.run();
                result.awaitUninterruptibly();
                body = REQUEST.Hosts.readBody();
                if (result.isSuccess()) {
                    body = dbhost.toSpecializedHtml(authentHttp, body, false);
                    body += "<p><center><b>Connection SUCCESSFUL</b></center></p>";
                } else {
                    boolean resultShutDown = false;
                    if (!dbhost.isClient()) {
                        SocketAddress socketAddress = dbhost.getSocketAddress();
                        resultShutDown =
                            NetworkTransaction.shuttingdownNetworkChannel(socketAddress, null);
                    }
                    resultShutDown = resultShutDown ||
                        NetworkTransaction.shuttingdownNetworkChannels(host);
                    if (resultShutDown) {
                        body = dbhost.toSpecializedHtml(authentHttp, body, false);
                        body += "<p><center><b>Connection FAILURE: Disconnection is on going due to "+
                        result.getResult().code.mesg+"</b></center></p>";
                    } else {
                        body = dbhost.toSpecializedHtml(authentHttp, body, false);
                        body += "<p><center><b>Connection FAILURE: "+
                        result.getResult().code.mesg+"</b></center></p>";
                    }
                }
            } else if ("CloseConn".equalsIgnoreCase(parm)) {
                String host = getTrimValue("host");
                String addr = getTrimValue("address");
                String port = getTrimValue("port");
                String key = getTrimValue("hostkey");
                boolean ssl, admin, isclient;
                ssl = params.containsKey("ssl");
                admin = params.containsKey("admin");
                isclient = params.containsKey("isclient");
                head = resetOptionHosts(head, host, addr, ssl);
                int iport = Integer.parseInt(port);
                DbHostAuth dbhost = new DbHostAuth(dbSession, host, addr, iport,
                        ssl, key.getBytes(), admin, isclient);
                body = REQUEST.Hosts.readBody();
                boolean resultShutDown = false;
                if (!dbhost.isClient()) {
                    SocketAddress socketAddress = dbhost.getSocketAddress();
                    resultShutDown =
                        NetworkTransaction.shuttingdownNetworkChannel(socketAddress, null);
                }
                resultShutDown = resultShutDown ||
                    NetworkTransaction.shuttingdownNetworkChannels(host);
                if (resultShutDown) {
                    body = dbhost.toSpecializedHtml(authentHttp, body, false);
                    body += "<p><center><b>Disconnection on going SUCCESSFUL</b></center></p>";
                } else {
                    body = dbhost.toSpecializedHtml(authentHttp, body, false);
                    body += "<p><center><b>Disconnection cannot be done</b></center></p>";
                }
            } else if ("Delete".equalsIgnoreCase(parm)) {
                String host = getTrimValue("host");
                if (host == null || host.length() == 0) {
                    body0 = body1 = body = "";
                    body = "<p><center><b>Not enough data to delete a Host</b></center></p>";
                    head = resetOptionHosts(head, "", "", false);
                    return head+body0+body+body1+end;
                }
                DbHostAuth dbhost;
                try {
                    dbhost = new DbHostAuth(dbSession, host);
                } catch (OpenR66DatabaseException e) {
                    body0 = body1 = body = "";
                    body = "<p><center><b>Cannot delete a Host: "+e.getMessage()+"</b></center></p>";
                    head = resetOptionHosts(head, "", "", false);
                    return head+body0+body+body1+end;
                }
                try {
                    dbhost.delete();
                } catch (OpenR66DatabaseException e) {
                    body0 = body1 = body = "";
                    body = "<p><center><b>Cannot delete a Host: "+e.getMessage()+"</b></center></p>";
                    head = resetOptionHosts(head, "", "", false);
                    return head+body0+body+body1+end;
                }
                body0 = body1 = body = "";
                body = "<p><center><b>Deleted Host: "+host+"</b></center></p>";
                head = resetOptionHosts(head, "", "", false);
                return head+body0+body+body1+end;
            } else {
                head = resetOptionHosts(head, "", "", false);
            }
            body1 = REQUEST.Hosts.readBodyEnd();
        } else {
            head = resetOptionHosts(head, "", "", false);
        }
        return head+body0+body+body1+end;
    }
    private void createExport(String body, StringBuilder builder, String rule, int mode, int limit) {
        DbPreparedStatement preparedStatement = null;
        try {
            preparedStatement =
                DbRule.getFilterPrepareStament(dbSession,
                        rule, mode);
            preparedStatement.executeQuery();
            int i = 0;
            while (preparedStatement.getNext()) {
                i++;
                DbRule dbrule = DbRule.getFromStatement(preparedStatement);
                builder.append(dbrule.toSpecializedHtml(authentHttp, body));
                if (i > limit) {
                    break;
                }
            }
            preparedStatement.realClose();
        } catch (OpenR66DatabaseException e) {
            if (preparedStatement != null) {
                preparedStatement.realClose();
            }
            logger.warn("OpenR66 Web Error {}",e.getMessage());
        }
    }
    private String resetOptionRules(String header,
            String rule, RequestPacket.TRANSFERMODE mode, int gmode) {
        StringBuilder builder = new StringBuilder(header);
        FileUtils.replace(builder, "XXXRULEXXX", rule);
        if (mode != null) {
            switch (mode) {
            case RECVMODE:
                FileUtils.replace(builder, "XXXRECVXXX", "checked");
                break;
            case SENDMODE:
                FileUtils.replace(builder, "XXXSENDXXX", "checked");
                break;
            case RECVMD5MODE:
                FileUtils.replace(builder, "XXXRECVMXXX", "checked");
                break;
            case SENDMD5MODE:
                FileUtils.replace(builder, "XXXSENDMXXX", "checked");
                break;
            case RECVTHROUGHMODE:
                FileUtils.replace(builder, "XXXRECVTXXX", "checked");
                break;
            case SENDTHROUGHMODE:
                FileUtils.replace(builder, "XXXSENDTXXX", "checked");
                break;
            case RECVMD5THROUGHMODE:
                FileUtils.replace(builder, "XXXRECVMTXXX", "checked");
                break;
            case SENDMD5THROUGHMODE:
                FileUtils.replace(builder, "XXXSENDMTXXX", "checked");
                break;
            }
        }
        if (gmode == -1) {// All Recv
            FileUtils.replace(builder, "XXXARECVXXX", "checked");
        } else if (gmode == -2) {// All Send
            FileUtils.replace(builder, "XXXASENDXXX", "checked");
        } else if (gmode == -3) {// All
            FileUtils.replace(builder, "XXXALLXXX", "checked");
        }
        return builder.toString();
    }
    private String Rules() {
        getParams();
        String head = REQUEST.Rules.readHeader(this);
        String end;
        end = REQUEST.Rules.readEnd();
        if (params == null) {
            head = resetOptionRules(head, "", null, -3);
            return head+end;
        }
        String body0, body, body1;
        body0 = body1 = body = "";
        List<String> parms = params.get("ACTION");
        if (parms != null) {
            body0 = REQUEST.Rules.readBodyHeader();
            String parm = parms.get(0);
            if ("Create".equalsIgnoreCase(parm) || "Update".equalsIgnoreCase(parm)) {
                String rule = getTrimValue("rule");
                String hostids = getTrimValue("hostids");
                String recvp = getTrimValue("recvp");
                String sendp = getTrimValue("sendp");
                String archp = getTrimValue("archp");
                String workp = getTrimValue("workp");
                String rpre = getTrimValue("rpre");
                String rpost = getTrimValue("rpost");
                String rerr = getTrimValue("rerr");
                String spre = getTrimValue("spre");
                String spost = getTrimValue("spost");
                String serr = getTrimValue("serr");
                String mode = getTrimValue("mode");
                if (rule == null || mode == null) {
                    body0 = body1 = body = "";
                    body = "<p><center><b>Not enough data to "+parm+" a Rule</b></center></p>";
                    head = resetOptionRules(head, "", null, -3);
                    return head+body0+body+body1+end;
                }
                int gmode = 0;

                TRANSFERMODE tmode = null;
                if (mode.equals("send")) {
                    tmode = RequestPacket.TRANSFERMODE.SENDMODE;
                    gmode= -2;
                } else if (mode.equals("recv")) {
                    tmode = RequestPacket.TRANSFERMODE.RECVMODE;
                    gmode= -1;
                } else if (mode.equals("sendmd5")) {
                    tmode = RequestPacket.TRANSFERMODE.SENDMD5MODE;
                    gmode= -2;
                } else if (mode.equals("recvmd5")) {
                    tmode = RequestPacket.TRANSFERMODE.RECVMD5MODE;
                    gmode= -1;
                } else if (mode.equals("sendth")) {
                    tmode = RequestPacket.TRANSFERMODE.SENDTHROUGHMODE;
                    gmode= -2;
                } else if (mode.equals("recvth")) {
                    tmode = RequestPacket.TRANSFERMODE.RECVTHROUGHMODE;
                    gmode= -1;
                } else if (mode.equals("sendthmd5")) {
                    tmode = RequestPacket.TRANSFERMODE.SENDMD5THROUGHMODE;
                    gmode= -2;
                } else if (mode.equals("recvthmd5")) {
                    tmode = RequestPacket.TRANSFERMODE.RECVMD5THROUGHMODE;
                    gmode= -1;
                }
                head = resetOptionRules(head, rule, tmode, gmode);
                DbRule dbrule = new DbRule(dbSession,rule,hostids,tmode.ordinal(),
                        recvp,sendp,archp,workp,rpre,rpost,rerr,spre,spost,serr);
                try {
                    if ("Create".equalsIgnoreCase(parm)) {
                        dbrule.insert();
                    } else {
                        if (dbrule.exist()) {
                            dbrule.update();
                        } else {
                            dbrule.insert();
                        }
                    }
                } catch (OpenR66DatabaseException e) {
                    body0 = body1 = body = "";
                    body = "<p><center><b>Cannot create a Rule: "+e.getMessage()+"</b></center></p>";
                    head = resetOptionRules(head, "", null, -3);
                    return head+body0+body+body1+end;
                }
                body = REQUEST.Rules.readBody();
                body = dbrule.toSpecializedHtml(authentHttp, body);
            } else if ("Filter".equalsIgnoreCase(parm)) {
                String rule = getTrimValue("rule");
                String mode = getTrimValue("mode");
                TRANSFERMODE tmode;
                int gmode = 0;
                if (mode.equals("all")) {
                    gmode = -3;
                } else if (mode.equals("send")) {
                    gmode = -2;
                } else if (mode.equals("recv")) {
                    gmode = -1;
                }
                head = resetOptionRules(head, rule == null ? "":rule,
                        null, gmode);
                body = REQUEST.Rules.readBody();
                StringBuilder builder = new StringBuilder();
                boolean specific = false;
                if (params.containsKey("send")) {
                    tmode = RequestPacket.TRANSFERMODE.SENDMODE;
                    head = resetOptionRules(head, rule == null ? "":rule,
                            tmode, gmode);
                    specific = true;
                    createExport(body, builder, rule,
                            RequestPacket.TRANSFERMODE.SENDMODE.ordinal(), LIMITROW/4);
                }
                if (params.containsKey("recv")) {
                    tmode = RequestPacket.TRANSFERMODE.RECVMODE;
                    head = resetOptionRules(head, rule == null ? "":rule,
                            tmode, gmode);
                    specific = true;
                    createExport(body, builder, rule,
                            RequestPacket.TRANSFERMODE.RECVMODE.ordinal(), LIMITROW/4);
                }
                if (params.containsKey("sendmd5")) {
                    tmode = RequestPacket.TRANSFERMODE.SENDMD5MODE;
                    head = resetOptionRules(head, rule == null ? "":rule,
                            tmode, gmode);
                    specific = true;
                    createExport(body, builder, rule,
                            RequestPacket.TRANSFERMODE.SENDMD5MODE.ordinal(), LIMITROW/4);
                }
                if (params.containsKey("recvmd5")) {
                    tmode = RequestPacket.TRANSFERMODE.RECVMD5MODE;
                    head = resetOptionRules(head, rule == null ? "":rule,
                            tmode, gmode);
                    specific = true;
                    createExport(body, builder, rule,
                            RequestPacket.TRANSFERMODE.RECVMD5MODE.ordinal(), LIMITROW/4);
                }
                if (params.containsKey("sendth")) {
                    tmode = RequestPacket.TRANSFERMODE.SENDTHROUGHMODE;
                    head = resetOptionRules(head, rule == null ? "":rule,
                            tmode, gmode);
                    specific = true;
                    createExport(body, builder, rule,
                            RequestPacket.TRANSFERMODE.SENDTHROUGHMODE.ordinal(), LIMITROW/4);
                }
                if (params.containsKey("recvth")) {
                    tmode = RequestPacket.TRANSFERMODE.RECVTHROUGHMODE;
                    head = resetOptionRules(head, rule == null ? "":rule,
                            tmode, gmode);
                    specific = true;
                    createExport(body, builder, rule,
                            RequestPacket.TRANSFERMODE.RECVTHROUGHMODE.ordinal(), LIMITROW/4);
                }
                if (params.containsKey("sendthmd5")) {
                    tmode = RequestPacket.TRANSFERMODE.SENDMD5THROUGHMODE;
                    head = resetOptionRules(head, rule == null ? "":rule,
                            tmode, gmode);
                    specific = true;
                    createExport(body, builder, rule,
                            RequestPacket.TRANSFERMODE.SENDMD5THROUGHMODE.ordinal(), LIMITROW/4);
                }
                if (params.containsKey("recvthmd5")) {
                    tmode = RequestPacket.TRANSFERMODE.RECVMD5THROUGHMODE;
                    head = resetOptionRules(head, rule == null ? "":rule,
                            tmode, gmode);
                    specific = true;
                    createExport(body, builder, rule,
                            RequestPacket.TRANSFERMODE.RECVMD5THROUGHMODE.ordinal(), LIMITROW/4);
                }
                if (!specific) {
                    if (gmode == -1) {
                        //recv
                        createExport(body, builder, rule,
                                RequestPacket.TRANSFERMODE.RECVMODE.ordinal(), LIMITROW/4);
                        createExport(body, builder, rule,
                                RequestPacket.TRANSFERMODE.RECVMD5MODE.ordinal(), LIMITROW/4);
                        createExport(body, builder, rule,
                                RequestPacket.TRANSFERMODE.RECVTHROUGHMODE.ordinal(), LIMITROW/4);
                        createExport(body, builder, rule,
                                RequestPacket.TRANSFERMODE.RECVMD5THROUGHMODE.ordinal(), LIMITROW/4);
                    } else if (gmode == -2) {
                        //send
                        createExport(body, builder, rule,
                                RequestPacket.TRANSFERMODE.SENDMODE.ordinal(), LIMITROW/4);
                        createExport(body, builder, rule,
                                RequestPacket.TRANSFERMODE.SENDMD5MODE.ordinal(), LIMITROW/4);
                        createExport(body, builder, rule,
                                RequestPacket.TRANSFERMODE.SENDTHROUGHMODE.ordinal(), LIMITROW/4);
                        createExport(body, builder, rule,
                                RequestPacket.TRANSFERMODE.SENDMD5THROUGHMODE.ordinal(), LIMITROW/4);
                    } else {
                        // all
                        createExport(body, builder, rule,
                                -1, LIMITROW);
                    }
                }
                body = builder.toString();
                body1 = REQUEST.Rules.readBodyEnd();
            } else if ("Delete".equalsIgnoreCase(parm)) {
                String rule = getTrimValue("rule");
                if (rule == null || rule.length() == 0) {
                    body0 = body1 = body = "";
                    body = "<p><center><b>Not enough data to delete a Rule</b></center></p>";
                    head = resetOptionRules(head, "", null, -3);
                    return head+body0+body+body1+end;
                }
                DbRule dbrule;
                try {
                    dbrule = new DbRule(dbSession,rule);
                } catch (OpenR66DatabaseException e) {
                    body0 = body1 = body = "";
                    body = "<p><center><b>Cannot delete a Rule: "+e.getMessage()+"</b></center></p>";
                    head = resetOptionRules(head, "", null, -3);
                    return head+body0+body+body1+end;
                }
                try {
                    dbrule.delete();
                } catch (OpenR66DatabaseException e) {
                    body0 = body1 = body = "";
                    body = "<p><center><b>Cannot delete a Rule: "+e.getMessage()+"</b></center></p>";
                    head = resetOptionRules(head, "", null, -3);
                    return head+body0+body+body1+end;
                }
                body0 = body1 = body = "";
                body = "<p><center><b>Deleted Rule: "+rule+"</b></center></p>";
                head = resetOptionRules(head, "", null, -3);
                return head+body0+body+body1+end;
            } else {
                head = resetOptionRules(head, "", null, -3);
            }
            body1 = REQUEST.Rules.readBodyEnd();
        } else {
            head = resetOptionRules(head, "", null, -3);
        }
        return head+body0+body+body1+end;
    }

    private String System() {
        getParams();
        if (params == null) {
            String system = REQUEST.System.readFileUnique(this);
            StringBuilder builder = new StringBuilder(system);
            FileUtils.replace(builder, REPLACEMENT.XXXXSESSIONLIMITWXXX.toString(),
                    Long.toString(Configuration.configuration.serverChannelWriteLimit));
            FileUtils.replace(builder, REPLACEMENT.XXXXSESSIONLIMITRXXX.toString(),
                    Long.toString(Configuration.configuration.serverChannelReadLimit));
            FileUtils.replace(builder, REPLACEMENT.XXXXDELAYCOMMDXXX.toString(),
                    Long.toString(Configuration.configuration.delayCommander));
            FileUtils.replace(builder, REPLACEMENT.XXXXDELAYRETRYXXX.toString(),
                    Long.toString(Configuration.configuration.delayRetry));
            FileUtils.replace(builder, REPLACEMENT.XXXXCHANNELLIMITWXXX.toString(),
                    Long.toString(Configuration.configuration.serverGlobalWriteLimit));
            FileUtils.replace(builder, REPLACEMENT.XXXXCHANNELLIMITRXXX.toString(),
                    Long.toString(Configuration.configuration.serverGlobalReadLimit));
            return builder.toString();
        }
        String extraInformation = null;
        if (params.containsKey("ACTION")) {
            List<String> action = params.get("ACTION");
            for (String act : action) {
                if (act.equalsIgnoreCase("ExportConfig")) {
                    String directory = Configuration.configuration.baseDirectory+
                        R66Dir.SEPARATOR+Configuration.configuration.archivePath;
                    extraInformation = "Export Directory: "+directory+"<br>";
                    try {
                        RuleFileBasedConfiguration.writeXml(directory,
                            Configuration.configuration.HOST_ID);
                        extraInformation += "-Rule are exported.<br>";
                    } catch (OpenR66DatabaseNoConnectionError e1) {
                    } catch (OpenR66DatabaseSqlError e1) {
                    } catch (OpenR66ProtocolSystemException e1) {
                    }
                    String filename =
                        directory+R66Dir.SEPARATOR+Configuration.configuration.HOST_ID+
                            "_Authentications.xml";
                    extraInformation += "-Authent are exported.<br>";
                    try {
                        AuthenticationFileBasedConfiguration.writeXML(filename);
                    } catch (OpenR66DatabaseNoConnectionError e) {
                    } catch (OpenR66DatabaseSqlError e) {
                    } catch (OpenR66ProtocolSystemException e) {
                    }
                } else if (act.equalsIgnoreCase("Disconnect")) {
                    String logon = Logon();
                    newSession = true;
                    clearSession();
                    forceClose = true;
                    return logon;
                } else if (act.equalsIgnoreCase("Shutdown")) {
                    String error = error("Shutdown in progress");
                    newSession = true;
                    clearSession();
                    forceClose = true;
                    shutdown = true;
                    return error;
                } else if (act.equalsIgnoreCase("Validate")) {
                    String bsessionr = getTrimValue("BSESSR");
                    long lsessionr = Configuration.configuration.serverChannelReadLimit;
                    if (bsessionr != null) {
                        lsessionr = Long.parseLong(bsessionr);
                    }
                    String bglobalr = getTrimValue("BGLOBR");
                    long lglobalr = Configuration.configuration.serverGlobalReadLimit;
                    if (bglobalr != null) {
                        lglobalr = Long.parseLong(bglobalr);
                    }
                    String bsessionw = getTrimValue("BSESSW");
                    long lsessionw = Configuration.configuration.serverChannelWriteLimit;
                    if (bsessionw !=null) {
                        lsessionw = Long.parseLong(bsessionw);
                    }
                    String bglobalw = getTrimValue("BGLOBW");
                    long lglobalw = Configuration.configuration.serverGlobalWriteLimit;
                    if (bglobalw != null) {
                        lglobalw = Long.parseLong(bglobalw);
                    }
                    Configuration.configuration.changeNetworkLimit(
                            lglobalw, lglobalr, lsessionw, lsessionr,
                            Configuration.configuration.delayLimit);
                    String dcomm = getTrimValue("DCOM");
                    if (dcomm != null) {
                        Configuration.configuration.delayCommander = Long.parseLong(dcomm);
                        if (Configuration.configuration.delayCommander <= 100) {
                            Configuration.configuration.delayCommander = 100;
                        }
                        Configuration.configuration.reloadCommanderDelay();
                    }
                    String dret = getTrimValue("DRET");
                    if (dret != null) {
                        Configuration.configuration.delayRetry = Long.parseLong(dret);
                        if (Configuration.configuration.delayRetry <= 1000) {
                            Configuration.configuration.delayRetry = 1000;
                        }
                    }
                    extraInformation = "Configuration Saved";
                }
            }
        }
        String system = REQUEST.System.readFileUnique(this);
        StringBuilder builder = new StringBuilder(system);
        FileUtils.replace(builder, REPLACEMENT.XXXXSESSIONLIMITWXXX.toString(),
                Long.toString(Configuration.configuration.serverChannelWriteLimit));
        FileUtils.replace(builder, REPLACEMENT.XXXXSESSIONLIMITRXXX.toString(),
                Long.toString(Configuration.configuration.serverChannelReadLimit));
        FileUtils.replace(builder, REPLACEMENT.XXXXDELAYCOMMDXXX.toString(),
                Long.toString(Configuration.configuration.delayCommander));
        FileUtils.replace(builder, REPLACEMENT.XXXXDELAYRETRYXXX.toString(),
                Long.toString(Configuration.configuration.delayRetry));
        FileUtils.replace(builder, REPLACEMENT.XXXXCHANNELLIMITWXXX.toString(),
                Long.toString(Configuration.configuration.serverGlobalWriteLimit));
        FileUtils.replace(builder, REPLACEMENT.XXXXCHANNELLIMITRXXX.toString(),
                Long.toString(Configuration.configuration.serverGlobalReadLimit));
        if (extraInformation != null) {
            builder.append(extraInformation);
        }
        return builder.toString();
    }

    private void getParams() {
        if (request.getMethod() == HttpMethod.GET) {
            params = null;
        } else if (request.getMethod() == HttpMethod.POST) {
            ChannelBuffer content = request.getContent();
            if (content.readable()) {
                String param = content.toString(GgStringUtils.UTF8);
                QueryStringDecoder queryStringDecoder2 = new QueryStringDecoder("/?"+param);
                params = queryStringDecoder2.getParameters();
            } else {
                params = null;
            }
        }
    }
    private void clearSession() {
        if (admin != null) {
            R66Session lsession = sessions.remove(admin.getValue());
            DbSession ldbsession = dbSessions.remove(admin.getValue());
            admin = null;
            if (lsession != null) {
                lsession.setStatus(75);
                lsession.clear();
            }
            if (ldbsession != null) {
                ldbsession.disconnect();
            }
        }
    }
    private void checkAuthent(MessageEvent e) {
        newSession = true;
        if (request.getMethod() == HttpMethod.GET) {
            String logon = Logon();
            responseContent.append(logon);
            clearSession();
            writeResponse(e.getChannel());
            return;
        } else if (request.getMethod() == HttpMethod.POST) {
            getParams();
            if (params == null) {
                String logon = Logon();
                responseContent.append(logon);
                clearSession();
                writeResponse(e.getChannel());
                return;
            }
        }
        boolean getMenu = false;
        if (params.containsKey("Logon")) {
            String name = null, password = null;
            List<String> values = null;
            if (!params.isEmpty()) {
                // get values
                if (params.containsKey("name")) {
                    values = params.get("name");
                    if (values != null) {
                        name = values.get(0);
                        if (name == null || name.length() == 0) {
                            getMenu = true;
                        }
                    }
                } else {
                    getMenu = true;
                }
                // search the nb param
                if ((!getMenu) && params.containsKey("passwd")) {
                    values = params.get("passwd");
                    if (values != null) {
                        password = values.get(0);
                        if (password == null || password.length() == 0) {
                            getMenu = true;
                        } else {
                            getMenu = false;
                        }
                    } else {
                        getMenu = true;
                    }
                } else {
                    getMenu = true;
                }
            } else {
                getMenu = true;
            }
            if (! getMenu) {
                logger.info("Name="+name+" vs "+name.equals(Configuration.configuration.ADMINNAME)+
                        " Passwd="+password+" vs "+Arrays.equals(password.getBytes(),
                                Configuration.configuration.getSERVERADMINKEY()));
                if (name.equals(Configuration.configuration.ADMINNAME) &&
                        Arrays.equals(password.getBytes(),
                                Configuration.configuration.getSERVERADMINKEY())) {
                    authentHttp.getAuth().specialNoSessionAuth(true, Configuration.configuration.HOST_ID);
                    authentHttp.setStatus(70);
                } else {
                    getMenu = true;
                }
                if (! authentHttp.isAuthenticated()) {
                    authentHttp.setStatus(71);
                    logger.debug("Still not authenticated: {}",authentHttp);
                    getMenu = true;
                }
                // load DbSession
                if (this.dbSession == null) {
                    try {
                        if (DbConstant.admin.isConnected) {
                            this.dbSession = new DbSession(DbConstant.admin, false);
                            this.isPrivateDbSession = true;
                        }
                    } catch (OpenR66DatabaseNoConnectionError e1) {
                        // Cannot connect so use default connection
                        logger.warn("Use default database connection");
                        this.dbSession = DbConstant.admin.session;
                    }
                }
            }
        } else {
            getMenu = true;
        }
        if (getMenu) {
            String logon = Logon();
            responseContent.append(logon);
            clearSession();
            writeResponse(e.getChannel());
        } else {
            String index = index();
            responseContent.append(index);
            clearSession();
            admin = new DefaultCookie(R66SESSION, Long.toHexString(new Random().nextLong()));
            sessions.put(admin.getValue(), this.authentHttp);
            authentHttp.setStatus(72);
            if (this.isPrivateDbSession) {
                dbSessions.put(admin.getValue(), dbSession);
            }
            logger.debug("CreateSession: "+uriRequest+":{}",admin);
            writeResponse(e.getChannel());
        }
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
            HttpRequest request = this.request = (HttpRequest) e.getMessage();
            queryStringDecoder = new QueryStringDecoder(request.getUri());
            uriRequest = queryStringDecoder.getPath();
            if (uriRequest.contains("gre/") || uriRequest.contains("img/") ||
                    uriRequest.contains("res/")) {
                writeFile(e.getChannel(), Configuration.configuration.httpBasePath+uriRequest);
                return;
            }
            checkSession(e.getChannel());
            if (! authentHttp.isAuthenticated()) {
                logger.info("Not Authent: "+uriRequest+":{}",authentHttp);
                checkAuthent(e);
                return;
            }
            String find = uriRequest;
            if (uriRequest.charAt(0) == '/') {
                find = uriRequest.substring(1);
            }
            find = find.substring(0, find.indexOf("."));
            REQUEST req = REQUEST.index;
            try {
                req = REQUEST.valueOf(find);
            } catch (IllegalArgumentException e1) {
                req = REQUEST.index;
                logger.debug("NotFound: "+find+":"+uriRequest);
            }
            switch (req) {
                case CancelRestart:
                    responseContent.append(CancelRestart());
                    break;
                case Export:
                    responseContent.append(Export());
                    break;
                case Hosts:
                    responseContent.append(Hosts());
                    break;
                case index:
                    responseContent.append(index());
                    break;
                case Listing:
                    responseContent.append(Listing());
                    break;
                case Logon:
                    responseContent.append(index());
                    break;
                case Rules:
                    responseContent.append(Rules());
                    break;
                case System:
                    responseContent.append(System());
                    break;
                case Transfers:
                    responseContent.append(Transfers());
                    break;
                default:
                    responseContent.append(index());
                    break;
            }
            writeResponse(e.getChannel());
    }

    /**
     * Write a File
     * @param e
     */
    private void writeFile(Channel channel, String filename) {
        // Convert the response content to a ChannelBuffer.
        HttpResponse response;
        File file = new File(filename);
        byte [] bytes = new byte[(int) file.length()];
        FileInputStream fileInputStream;
        try {
            fileInputStream = new FileInputStream(file);
        } catch (FileNotFoundException e) {
            response = new DefaultHttpResponse(HttpVersion.HTTP_1_1,
                    HttpResponseStatus.NOT_FOUND);
            channel.write(response);
            return;
        }
        try {
            fileInputStream.read(bytes);
        } catch (IOException e) {
            response = new DefaultHttpResponse(HttpVersion.HTTP_1_1,
                    HttpResponseStatus.NOT_FOUND);
            channel.write(response);
            return;
        }
        ChannelBuffer buf = ChannelBuffers.copiedBuffer(bytes);
        // Build the response object.
        response = new DefaultHttpResponse(HttpVersion.HTTP_1_1,
                HttpResponseStatus.OK);
        response.setContent(buf);
        response.setHeader(HttpHeaders.Names.CONTENT_LENGTH, String.valueOf(buf.readableBytes()));
        checkSession(channel);
        handleCookies(response);
        // Write the response.
        channel.write(response);
    }
    private void checkSession(Channel channel) {
        String cookieString = request.getHeader(HttpHeaders.Names.COOKIE);
        if (cookieString != null) {
            CookieDecoder cookieDecoder = new CookieDecoder();
            Set<Cookie> cookies = cookieDecoder.decode(cookieString);
            if(!cookies.isEmpty()) {
                for (Cookie elt : cookies) {
                    if (elt.getName().equalsIgnoreCase(R66SESSION)) {
                        admin = elt;
                        break;
                    }
                }
            }
        }
        if (admin != null) {
            R66Session session = sessions.get(admin.getValue());
            if (session != null) {
                authentHttp = session;
                authentHttp.setStatus(73);
            }
            DbSession dbSession = dbSessions.get(admin.getValue());
            if (dbSession != null) {
                this.dbSession = dbSession;
            }
        } else {
            logger.debug("NoSession: "+uriRequest+":{}",admin);
        }
    }
    private void handleCookies(HttpResponse response) {
        String cookieString = request.getHeader(HttpHeaders.Names.COOKIE);
        if (cookieString != null) {
            CookieDecoder cookieDecoder = new CookieDecoder();
            Set<Cookie> cookies = cookieDecoder.decode(cookieString);
            if(!cookies.isEmpty()) {
                // Reset the sessions if necessary.
                int nb = 0;
                CookieEncoder cookieEncoder = new CookieEncoder(true);
                boolean findSession = false;
                for (Cookie cookie : cookies) {
                    if (cookie.getName().equalsIgnoreCase(R66SESSION)) {
                        if (newSession) {
                            findSession = false;
                        } else {
                            findSession = true;
                            cookieEncoder.addCookie(cookie);
                            nb++;
                        }
                    } else {
                        cookieEncoder.addCookie(cookie);
                        nb++;
                    }
                }
                newSession = false;
                if (! findSession) {
                    if (admin != null) {
                        cookieEncoder.addCookie(admin);
                        nb++;
                        logger.debug("AddSession: "+uriRequest+":{}",admin);
                    }
                }
                if (nb > 0) {
                    response.addHeader(HttpHeaders.Names.SET_COOKIE, cookieEncoder.encode());
                }
            }
        } else if (admin != null) {
            CookieEncoder cookieEncoder = new CookieEncoder(true);
            cookieEncoder.addCookie(admin);
            logger.debug("AddSession: "+uriRequest+":{}",admin);
            response.addHeader(HttpHeaders.Names.SET_COOKIE, cookieEncoder.encode());
        }
    }
    /**
     * Write the response
     * @param e
     */
    private void writeResponse(Channel channel) {
        // Convert the response content to a ChannelBuffer.
        ChannelBuffer buf = ChannelBuffers.copiedBuffer(responseContent.toString(), GgStringUtils.UTF8);
        responseContent.setLength(0);

        // Decide whether to close the connection or not.
        boolean keepAlive = HttpHeaders.isKeepAlive(request);
        boolean close = HttpHeaders.Values.CLOSE.equalsIgnoreCase(request
                .getHeader(HttpHeaders.Names.CONNECTION)) ||
                (!keepAlive) || forceClose;

        // Build the response object.
        HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
        response.setContent(buf);
        response.setHeader(HttpHeaders.Names.CONTENT_TYPE, "text/html");
        if (keepAlive) {
            response.setHeader(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.KEEP_ALIVE);
        }
        if (!close) {
            // There's no need to add 'Content-Length' header
            // if this is the last response.
            response.setHeader(HttpHeaders.Names.CONTENT_LENGTH, String.valueOf(buf.readableBytes()));
        }

        handleCookies(response);

        // Write the response.
        ChannelFuture future = channel.write(response);
        // Close the connection after the write operation is done if necessary.
        if (close) {
            future.addListener(ChannelFutureListener.CLOSE);
        }
        if (shutdown) {
            Thread thread = new Thread(new ChannelUtils());
            thread.setDaemon(true);
            thread.setName("Shutdown Thread");
            thread.start();
        }
    }
    /**
     * Send an error and close
     * @param ctx
     * @param status
     */
    private void sendError(ChannelHandlerContext ctx, HttpResponseStatus status) {
        HttpResponse response = new DefaultHttpResponse(
                HttpVersion.HTTP_1_1, status);
        response.setHeader(
                HttpHeaders.Names.CONTENT_TYPE, "text/html");
        responseContent.setLength(0);
        responseContent.append(error(status.toString()));
        response.setContent(ChannelBuffers.copiedBuffer(responseContent.toString(), GgStringUtils.UTF8));
        clearSession();
        // Close the connection as soon as the error message is sent.
        ctx.getChannel().write(response).addListener(ChannelFutureListener.CLOSE);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
            throws Exception {
        OpenR66Exception exception = OpenR66ExceptionTrappedFactory
            .getExceptionFromTrappedException(e.getChannel(), e);
        if (exception != null) {
            if (!(exception instanceof OpenR66ProtocolBusinessNoWriteBackException)) {
                if (e.getCause() instanceof IOException) {
                    // Nothing to do
                    return;
                }
                logger.warn("Exception in HttpSslHandler {}", exception.getMessage());
            }
            if (e.getChannel().isConnected()) {
                sendError(ctx, HttpResponseStatus.BAD_REQUEST);
            }
        } else {
            // Nothing to do
            return;
        }
    }

    /* (non-Javadoc)
     * @see org.jboss.netty.channel.SimpleChannelUpstreamHandler#channelConnected(org.jboss.netty.channel.ChannelHandlerContext, org.jboss.netty.channel.ChannelStateEvent)
     */
    @Override
    public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e)
            throws Exception {
        // Get the SslHandler in the current pipeline.
        // We added it in NetworkSslServerPipelineFactory.
        final SslHandler sslHandler = ctx.getPipeline().get(SslHandler.class);
        if (sslHandler != null) {
            // Get the SslHandler and begin handshake ASAP.
            // Get notified when SSL handshake is done.
            ChannelFuture handshakeFuture;
            handshakeFuture = sslHandler.handshake();
            if (handshakeFuture != null) {
                handshakeFuture.addListener(new ChannelFutureListener() {
                    public void operationComplete(ChannelFuture future)
                            throws Exception {
                        logger.info("Handshake: "+future.isSuccess(),future.getCause());
                        if (future.isSuccess()) {
                            setStatusSslConnectedChannel(future.getChannel(), true);
                        } else {
                            setStatusSslConnectedChannel(future.getChannel(), false);
                            future.getChannel().close();
                        }
                    }
                });
            }
        } else {
            logger.warn("SSL Not found");
        }
        super.channelConnected(ctx, e);
        ChannelGroup group =
            Configuration.configuration.getHttpChannelGroup();
        if (group != null) {
            group.add(e.getChannel());
        }
    }
}
