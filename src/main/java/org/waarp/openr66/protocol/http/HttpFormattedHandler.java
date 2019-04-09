/**
 * This file is part of Waarp Project.
 *
 * Copyright 2009, Frederic Bregier, and individual contributors by the @author tags. See the
 * COPYRIGHT.txt in the distribution for a full listing of individual contributors.
 *
 * All Waarp Project is free software: you can redistribute it and/or modify it under the terms of
 * the GNU General Public License as published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Waarp is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even
 * the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
 * Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with Waarp . If not, see
 * <http://www.gnu.org/licenses/>.
 */
package org.waarp.openr66.protocol.http;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.group.ChannelGroup;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.handler.codec.http.cookie.Cookie;
import io.netty.handler.codec.http.cookie.DefaultCookie;
import io.netty.handler.codec.http.cookie.ServerCookieDecoder;
import io.netty.handler.codec.http.cookie.ServerCookieEncoder;
import io.netty.handler.traffic.TrafficCounter;

import org.waarp.common.database.DbAdmin;
import org.waarp.common.database.DbPreparedStatement;
import org.waarp.common.database.DbSession;
import org.waarp.common.database.data.AbstractDbData.UpdatedInfo;
import org.waarp.common.database.exception.WaarpDatabaseException;
import org.waarp.common.database.exception.WaarpDatabaseNoConnectionException;
import org.waarp.common.database.exception.WaarpDatabaseSqlException;
import org.waarp.common.exception.FileTransferException;
import org.waarp.common.exception.InvalidArgumentException;
import org.waarp.common.logging.WaarpLogger;
import org.waarp.common.logging.WaarpLoggerFactory;
import org.waarp.common.utility.WaarpStringUtils;
import org.waarp.gateway.kernel.http.HttpWriteCacheEnable;
import org.waarp.openr66.context.ErrorCode;
import org.waarp.openr66.context.R66Session;
import org.waarp.openr66.context.task.SpooledInformTask;
import org.waarp.openr66.database.DbConstant;
import org.waarp.openr66.database.data.DbTaskRunner;
import org.waarp.openr66.database.data.DbTaskRunner.TASKSTEP;
import org.waarp.openr66.protocol.configuration.Configuration;
import org.waarp.openr66.protocol.configuration.Messages;
import org.waarp.openr66.protocol.exception.OpenR66Exception;
import org.waarp.openr66.protocol.exception.OpenR66ExceptionTrappedFactory;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolBusinessNoWriteBackException;
import org.waarp.openr66.protocol.localhandler.LocalChannelReference;

/**
 * Handler for HTTP information support
 *
 * @author Frederic Bregier
 *
 */
public class HttpFormattedHandler extends SimpleChannelInboundHandler<FullHttpRequest> {
    /**
     * Internal Logger
     */
    private static final WaarpLogger logger = WaarpLoggerFactory
            .getLogger(HttpFormattedHandler.class);

    private static enum REQUEST {
        index("index.html"),
        active("monitoring_header.html", "monitoring_end.html"),
        error("monitoring_header.html", "monitoring_end.html"),
        done("monitoring_header.html", "monitoring_end.html"),
        all("monitoring_header.html", "monitoring_end.html"),
        status("monitoring_header.html", "monitoring_end.html"),
        statusxml(""),
        statusjson("");

        private String header;
        private String end;

        /**
         * Constructor for a unique file
         *
         * @param uniquefile
         */
        private REQUEST(String uniquefile) {
            this.header = uniquefile;
            this.end = uniquefile;
        }

        /**
         * @param header
         * @param end
         */
        private REQUEST(String header, String end) {
            this.header = header;
            this.end = end;
        }

        /**
         * Reader for a unique file
         *
         * @return the content of the unique file
         */
        public String readFileUnique(HttpFormattedHandler handler) {
            return handler.readFileHeader(Configuration.configuration.getHttpBasePath() + "monitor/"
                    + this.header);
        }

        public String readHeader(HttpFormattedHandler handler) {
            return handler.readFileHeader(Configuration.configuration.getHttpBasePath() + "monitor/"
                    + this.header);
        }

        public String readEnd() {
            return WaarpStringUtils.readFile(Configuration.configuration.getHttpBasePath() + "monitor/"
                    + this.end);
        }
    }

    private static enum REPLACEMENT {
        XXXHOSTIDXXX, XXXLOCACTIVEXXX, XXXNETACTIVEXXX, XXXBANDWIDTHXXX, XXXDATEXXX, XXXLANGXXX;
    }

    public static final int LIMITROW = 60; // better if it can be divided by 4
    private static final String I18NEXT = "i18next";

    private final R66Session authentHttp = new R66Session();

    private String lang = Messages.getSlocale();

    private FullHttpRequest request;

    private final StringBuilder responseContent = new StringBuilder();

    private HttpResponseStatus status;

    private String uriRequest;

    private static final String sINFO = "INFO",
            sNB = "NB", sDETAIL = "DETAIL";

    /**
     * The Database connection attached to this NetworkChannelReference shared among all associated
     * LocalChannels
     */
    private DbSession dbSession = DbConstant.admin.getSession();

    /**
     * Does this dbSession is private and so should be closed
     */
    private boolean isPrivateDbSession = false;
    private boolean isCurrentRequestXml = false;
    private boolean isCurrentRequestJson = false;

    private Map<String, List<String>> params = null;

    private String readFileHeader(String filename) {
        String value;
        try {
            value = WaarpStringUtils.readFileException(filename);
        } catch (InvalidArgumentException e) {
            logger.error("Error while trying to open: " + filename, e);
            return "";
        } catch (FileTransferException e) {
            logger.error("Error while trying to read: " + filename, e);
            return "";
        }
        StringBuilder builder = new StringBuilder(value);

        WaarpStringUtils.replace(builder, REPLACEMENT.XXXDATEXXX.toString(),
                (new Date()).toString());
        WaarpStringUtils.replace(builder, REPLACEMENT.XXXLOCACTIVEXXX.toString(),
                Integer.toString(
                        Configuration.configuration.getLocalTransaction().
                                getNumberLocalChannel()));
        WaarpStringUtils.replace(builder, REPLACEMENT.XXXNETACTIVEXXX.toString(),
                Integer.toString(
                        DbAdmin.getNbConnection()));
        WaarpStringUtils.replace(builder, REPLACEMENT.XXXHOSTIDXXX.toString(),
                Configuration.configuration.getHOST_ID());
        TrafficCounter trafficCounter =
                Configuration.configuration.getGlobalTrafficShapingHandler().trafficCounter();
        WaarpStringUtils.replace(builder, REPLACEMENT.XXXBANDWIDTHXXX.toString(),
                "IN:" + (trafficCounter.lastReadThroughput() / 131072) +
                        "Mbits&nbsp;&nbsp;OUT:" +
                        (trafficCounter.lastWriteThroughput() / 131072) + "Mbits");
        WaarpStringUtils.replace(builder, REPLACEMENT.XXXLANGXXX.toString(), lang);
        return builder.toString();
    }

    private String getTrimValue(String varname) {
        String value = null;
        try {
            value = params.get(varname).get(0).trim();
        } catch (NullPointerException e) {
            return null;
        }
        if (value.isEmpty()) {
            value = null;
        }
        return value;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest msg) throws Exception {
        isCurrentRequestXml = false;
        isCurrentRequestJson = false;
        status = HttpResponseStatus.OK;
        FullHttpRequest request = this.request = msg;
        QueryStringDecoder queryStringDecoder = new QueryStringDecoder(request.uri());
        uriRequest = queryStringDecoder.path();
        logger.debug("Msg: " + uriRequest);
        if (uriRequest.contains("gre/") || uriRequest.contains("img/") ||
                uriRequest.contains("res/") || uriRequest.contains("favicon.ico")) {
            HttpWriteCacheEnable.writeFile(request,
                    ctx, Configuration.configuration.getHttpBasePath() + uriRequest,
                    "XYZR66NOSESSION");
            return;
        }
        /*try {
        	if (DbConstant.admin.isActive) {
        		this.dbSession = new DbSession(DbConstant.admin, false);
        		DbAdmin.nbHttpSession++;
        		this.isPrivateDbSession = true;
        	}
        } catch (WaarpDatabaseNoConnectionException e1) {
        	// Cannot connect so use default connection
        	logger.warn("Use default database connection");
        	this.dbSession = DbConstant.admin.session;
        }*/
        try {
            char cval = 'z';
            long nb = LIMITROW;
            // check the URI
            if (uriRequest.equalsIgnoreCase("/active")) {
                cval = '0';
            } else if (uriRequest.equalsIgnoreCase("/error")) {
                cval = '1';
            } else if (uriRequest.equalsIgnoreCase("/done")) {
                cval = '2';
            } else if (uriRequest.equalsIgnoreCase("/all")) {
                cval = '3';
            } else if (uriRequest.equalsIgnoreCase("/status")) {
                cval = '4';
            } else if (uriRequest.equalsIgnoreCase("/statusxml")) {
                cval = '5';
                nb = 0; // since it could be the default or setup by request
                isCurrentRequestXml = true;
            } else if (uriRequest.toLowerCase().startsWith("/spooled")) {
                cval = '6';
            } else if (uriRequest.equalsIgnoreCase("/statusjson")) {
                cval = '7';
                nb = 0; // since it could be the default or setup by request
                isCurrentRequestJson = true;
            }
            // Get the params according to get or post
            if (request.method() == HttpMethod.GET) {
                params = queryStringDecoder.parameters();
            } else if (request.method() == HttpMethod.POST) {
                ByteBuf content = request.content();
                if (content.isReadable()) {
                    String param = content.toString(WaarpStringUtils.UTF8);
                    queryStringDecoder = new QueryStringDecoder("/?" + param);
                } else {
                    responseContent.append(REQUEST.index.readFileUnique(this));
                    writeResponse(ctx);
                    return;
                }
                params = queryStringDecoder.parameters();
            }
            boolean getMenu = (cval == 'z');
            boolean extraBoolean = false;
            if (!params.isEmpty()) {
                // if not uri, from get or post
                if (getMenu) {
                    String info = getTrimValue(sINFO);
                    if (info != null) {
                        getMenu = false;
                        cval = info.charAt(0);
                    } else {
                        getMenu = true;
                    }
                }
                // search the nb param
                String snb = getTrimValue(sNB);
                if (snb != null) {
                    try {
                        nb = Long.parseLong(snb);
                    } catch (Exception e1) {
                    }
                }
                // search the detail param
                String sdetail = getTrimValue(sDETAIL);
                if (sdetail != null) {
                    try {
                        if (Integer.parseInt(sdetail) > 0) {
                            extraBoolean = true;
                        }
                    } catch (Exception e1) {
                    }
                }
                String langarg = getTrimValue("setLng");
                if (langarg != null && !langarg.isEmpty()) {
                    lang = langarg;
                }
            }
            if (getMenu) {
                responseContent.append(REQUEST.index.readFileUnique(this));
            } else {
                // Use value 0=Active 1=Error 2=Done 3=All
                switch (cval) {
                    case '0':
                        active(ctx, (int) nb);
                        break;
                    case '1':
                        error(ctx, (int) nb);
                        break;
                    case '2':
                        done(ctx, (int) nb);
                        break;
                    case '3':
                        all(ctx, (int) nb);
                        break;
                    case '4':
                        status(ctx, (int) nb);
                        break;
                    case '5':
                        statusxml(ctx, nb, extraBoolean);
                        break;
                    case '6':
                        String name = null;
                        if (params.containsKey("name")) {
                            name = getTrimValue("name");
                        }
                        int istatus = 0;
                        if (params.containsKey("status")) {
                            String status = getTrimValue("status");
                            try {
                                istatus = Integer.parseInt(status);
                            } catch (NumberFormatException e1) {
                                istatus = 0;
                            }
                        }
                        if (uriRequest.toLowerCase().startsWith("/spooleddetail")) {
                            extraBoolean = true;
                        }
                        spooled(ctx, extraBoolean, name, istatus);
                        break;
                    case '7':
                        statusjson(ctx, nb, extraBoolean);
                        break;
                    default:
                        responseContent.append(REQUEST.index.readFileUnique(this));
                }
            }
            writeResponse(ctx);
        } finally {
            if (this.isPrivateDbSession && dbSession != null) {
                dbSession.forceDisconnect();
                DbAdmin.decHttpSession();
                dbSession = null;
            }
        }
    }

    /**
     * Add all runners from preparedStatement for type
     *
     * @param preparedStatement
     * @param type
     * @param nb
     * @throws WaarpDatabaseNoConnectionException
     * @throws WaarpDatabaseSqlException
     */
    private void addRunners(DbPreparedStatement preparedStatement, String type,
                            int nb) throws WaarpDatabaseNoConnectionException,
            WaarpDatabaseSqlException {
        try {
            preparedStatement.executeQuery();
            responseContent
                    .append("<style>td{font-size: 8pt;}</style><table border=\"2\">")
                    .append("<tr><td>").append(type).append("</td>")
                    .append(DbTaskRunner.headerHtml()).append("</tr>\r\n");
            int i = 0;
            while (preparedStatement.getNext()) {
                DbTaskRunner taskRunner = DbTaskRunner
                        .getFromStatement(preparedStatement);
                responseContent.append("<tr><td>").append(taskRunner.isSender() ? "S" : "R").append("</td>");
                LocalChannelReference lcr =
                        Configuration.configuration.getLocalTransaction().
                                getFromRequest(taskRunner.getKey());
                responseContent.append(
                        taskRunner.toHtml(
                                getAuthentHttp(),
                                lcr != null ? Messages.getString("HttpSslHandler.Active") : Messages
                                        .getString("HttpSslHandler.NotActive")))
                        .append("</tr>\r\n");
                if (nb > 0) {
                    i++;
                    if (i >= nb) {
                        break;
                    }
                }
            }
            responseContent.append("</table><br>\r\n");
        } finally {
            if (preparedStatement != null) {
                preparedStatement.realClose();
            }
        }
    }

    /**
     * print all active transfers
     *
     * @param ctx
     * @param nb
     */
    private void active(ChannelHandlerContext ctx, int nb) {
        responseContent.append(REQUEST.active.readHeader(this));
        DbPreparedStatement preparedStatement = null;
        try {
            preparedStatement = DbTaskRunner.getStatusPrepareStatement(dbSession,
                    ErrorCode.Running, nb);
            addRunners(preparedStatement, ErrorCode.Running.mesg, nb);
            preparedStatement = DbTaskRunner.getSelectFromInfoPrepareStatement(
                    dbSession, UpdatedInfo.INTERRUPTED, true, nb);
            DbTaskRunner.finishSelectOrCountPrepareStatement(preparedStatement);
            addRunners(preparedStatement, UpdatedInfo.INTERRUPTED.name(), nb);
            preparedStatement = DbTaskRunner.getSelectFromInfoPrepareStatement(
                    dbSession, UpdatedInfo.TOSUBMIT, true, nb);
            DbTaskRunner.finishSelectOrCountPrepareStatement(preparedStatement);
            addRunners(preparedStatement, UpdatedInfo.TOSUBMIT.name(), nb);
            preparedStatement = DbTaskRunner.getStatusPrepareStatement(dbSession,
                    ErrorCode.InitOk, nb);
            addRunners(preparedStatement, ErrorCode.InitOk.mesg, nb);
            preparedStatement = DbTaskRunner.getStatusPrepareStatement(dbSession,
                    ErrorCode.PreProcessingOk, nb);
            addRunners(preparedStatement, ErrorCode.PreProcessingOk.mesg, nb);
            preparedStatement = DbTaskRunner.getStatusPrepareStatement(dbSession,
                    ErrorCode.TransferOk, nb);
            addRunners(preparedStatement, ErrorCode.TransferOk.mesg, nb);
            preparedStatement = DbTaskRunner.getStatusPrepareStatement(dbSession,
                    ErrorCode.PostProcessingOk, nb);
            addRunners(preparedStatement, ErrorCode.PostProcessingOk.mesg, nb);
            preparedStatement = null;
        } catch (WaarpDatabaseException e) {
            if (preparedStatement != null) {
                preparedStatement.realClose();
            }
            logger.warn("OpenR66 Web Error {}", e.getMessage());
            sendError(ctx, HttpResponseStatus.SERVICE_UNAVAILABLE);
            return;
        }
        responseContent.append(REQUEST.active.readEnd());
    }

    /**
     * print all transfers in error
     *
     * @param ctx
     * @param nb
     */
    private void error(ChannelHandlerContext ctx, int nb) {
        responseContent.append(REQUEST.error.readHeader(this));
        DbPreparedStatement preparedStatement = null;
        try {
            preparedStatement = DbTaskRunner.getSelectFromInfoPrepareStatement(
                    dbSession, UpdatedInfo.INERROR, true, nb / 2);
            DbTaskRunner.finishSelectOrCountPrepareStatement(preparedStatement);
            addRunners(preparedStatement, UpdatedInfo.INERROR.name(), nb / 2);
            preparedStatement = DbTaskRunner.getSelectFromInfoPrepareStatement(
                    dbSession, UpdatedInfo.INTERRUPTED, true, nb / 2);
            DbTaskRunner.finishSelectOrCountPrepareStatement(preparedStatement);
            addRunners(preparedStatement, UpdatedInfo.INTERRUPTED.name(),
                    nb / 2);
            preparedStatement = DbTaskRunner.getStepPrepareStatement(dbSession,
                    TASKSTEP.ERRORTASK, nb / 4);
            addRunners(preparedStatement, TASKSTEP.ERRORTASK.name(), nb / 4);
        } catch (WaarpDatabaseException e) {
            if (preparedStatement != null) {
                preparedStatement.realClose();
            }
            logger.warn("OpenR66 Web Error {}", e.getMessage());
            sendError(ctx, HttpResponseStatus.SERVICE_UNAVAILABLE);
            return;
        }
        responseContent.append(REQUEST.error.readEnd());
    }

    /**
     * Print all done transfers
     *
     * @param ctx
     * @param nb
     */
    private void done(ChannelHandlerContext ctx, int nb) {
        responseContent.append(REQUEST.done.readHeader(this));
        DbPreparedStatement preparedStatement = null;
        try {
            preparedStatement = DbTaskRunner.getStatusPrepareStatement(dbSession,
                    ErrorCode.CompleteOk, nb);
            addRunners(preparedStatement, ErrorCode.CompleteOk.mesg, nb);
        } catch (WaarpDatabaseException e) {
            if (preparedStatement != null) {
                preparedStatement.realClose();
            }
            logger.warn("OpenR66 Web Error {}", e.getMessage());
            sendError(ctx, HttpResponseStatus.SERVICE_UNAVAILABLE);
            return;
        }
        responseContent.append(REQUEST.done.readEnd());
    }

    /**
     * Print all nb last transfers
     *
     * @param ctx
     * @param nb
     */
    private void all(ChannelHandlerContext ctx, int nb) {
        responseContent.append(REQUEST.all.readHeader(this));
        DbPreparedStatement preparedStatement = null;
        try {
            preparedStatement = DbTaskRunner.getStatusPrepareStatement(dbSession,
                    null, nb);// means all
            addRunners(preparedStatement, "ALL RUNNERS: " + nb, nb);
        } catch (WaarpDatabaseException e) {
            if (preparedStatement != null) {
                preparedStatement.realClose();
            }
            logger.warn("OpenR66 Web Error {}", e.getMessage());
            sendError(ctx, HttpResponseStatus.SERVICE_UNAVAILABLE);
            return;
        }
        responseContent.append(REQUEST.all.readEnd());
    }

    /**
     * print only status
     *
     * @param ctx
     * @param nb
     */
    private void status(ChannelHandlerContext ctx, int nb) {
        responseContent.append(REQUEST.status.readHeader(this));
        DbPreparedStatement preparedStatement = null;
        try {
            preparedStatement = DbTaskRunner.getSelectFromInfoPrepareStatement(
                    dbSession, UpdatedInfo.INERROR, true, 1);
            DbTaskRunner.finishSelectOrCountPrepareStatement(preparedStatement);
            try {
                preparedStatement.executeQuery();
                if (preparedStatement.getNext()) {
                    responseContent.append("<p>Some Transfers are in ERROR</p><br>");
                    status = HttpResponseStatus.INTERNAL_SERVER_ERROR;
                }
            } finally {
                if (preparedStatement != null) {
                    preparedStatement.realClose();
                }
            }
            preparedStatement = DbTaskRunner.getSelectFromInfoPrepareStatement(
                    dbSession, UpdatedInfo.INTERRUPTED, true, 1);
            DbTaskRunner.finishSelectOrCountPrepareStatement(preparedStatement);
            try {
                preparedStatement.executeQuery();
                if (preparedStatement.getNext()) {
                    responseContent.append("<p>Some Transfers are INTERRUPTED</p><br>");
                    status = HttpResponseStatus.INTERNAL_SERVER_ERROR;
                }
            } finally {
                if (preparedStatement != null) {
                    preparedStatement.realClose();
                }
            }
            preparedStatement = DbTaskRunner.getStepPrepareStatement(dbSession,
                    TASKSTEP.ERRORTASK, 1);
            try {
                preparedStatement.executeQuery();
                if (preparedStatement.getNext()) {
                    responseContent.append("<p>Some Transfers are in ERRORTASK</p><br>");
                    status = HttpResponseStatus.INTERNAL_SERVER_ERROR;
                }
            } finally {
                if (preparedStatement != null) {
                    preparedStatement.realClose();
                }
            }
            if (status != HttpResponseStatus.INTERNAL_SERVER_ERROR) {
                responseContent.append("<p>No problem is found in Transfers</p><br>");
            }
        } catch (WaarpDatabaseException e) {
            if (preparedStatement != null) {
                preparedStatement.realClose();
            }
            logger.warn("OpenR66 Web Error {}", e.getMessage());
            sendError(ctx, HttpResponseStatus.SERVICE_UNAVAILABLE);
            return;
        }
        responseContent.append(REQUEST.status.readEnd());
    }

    /**
     * print only status
     *
     * @param ctx
     * @param nb
     */
    private void statusxml(ChannelHandlerContext ctx, long nb, boolean detail) {
        Configuration.configuration.getMonitoring().run(nb, detail);
        responseContent.append(Configuration.configuration.getMonitoring().exportXml(detail));
    }

    /**
     * print only status
     *
     * @param ctx
     * @param nb
     */
    private void statusjson(ChannelHandlerContext ctx, long nb, boolean detail) {
        Configuration.configuration.getMonitoring().run(nb, detail);
        responseContent.append(Configuration.configuration.getMonitoring().exportJson(detail));
    }

    private void spooled(ChannelHandlerContext ctx, boolean detail, String name, int istatus) {
        responseContent
                .append(REQUEST.status.readHeader(this))
                .append("<p><table border='0' cellpadding='0' cellspacing='0' >")
                .append("<tr style='background-image:url(gre/gresm.png);background-repeat:repeat-x;background-position:left top;'><td class='col_MenuHaut'>")
                .append("<a data-i18n='menu2.sous-menu4a' href='Spooled.html' style='display:block;width:100%;height:100%;line-height:15px;'>")
                .append("SPOOLED DIRECTORY no detail</a></td><td></td><td><img src='gre/gre11.png' height='15' width='1' style='border: none; display: block;' alt='' /></td>")
                .append("<td></td><td class='col_MenuHaut'><a data-i18n='menu2.sous-menu4c' href='Spooled.html?status=-1' style='display:block;width:100%;height:100%;line-height:15px;'>")
                .append("SPOOLED DIRECTORY no detail KO</a></td><td></td><td><img src='gre/gre11.png' height='15' width='1' style='border: none; display: block;' alt='' /></td>")
                .append("<td></td><td class='col_MenuHaut'><a data-i18n='menu2.sous-menu4d' href='Spooled.html?status=1' style='display:block;width:100%;height:100%;line-height:15px;'>")
                .append("SPOOLED DIRECTORY no detail OK</a></td></tr><tr style='background-image:url(gre/gre11.png);background-repeat:repeat-x;background-position:left top;'>")
                .append("<td><img src='gre/gre11.png' height='1' width='100%' style='border: none; display: block;' alt='' /></td></tr>")
                .append("<tr style='background-image:url(gre/gresm.png);background-repeat:repeat-x;background-position:left top;'>")
                .append("<td class='col_MenuHaut'><a data-i18n='menu2.sous-menu4b' href='SpooledDetailed.html' style='display:block;width:100%;height:100%;line-height:15px;'>")
                .append("SPOOLED DIRECTORY detailed</a></td><td></td><td><img src='gre/gre11.png' height='15' width='1' style='border: none; display: block;' alt='' /></td>")
                .append("<td></td><td class='col_MenuHaut'><a data-i18n='menu2.sous-menu4e' href='SpooledDetailed.html?status=-1' style='display:block;width:100%;height:100%;line-height:15px;'>")
                .append("SPOOLED DIRECTORY detailed KO</a></td><td></td><td><img src='gre/gre11.png' height='15' width='1' style='border: none; display: block;' alt='' /></td>")
                .append("<td></td><td class='col_MenuHaut'><a data-i18n='menu2.sous-menu4f' href='SpooledDetailed.html?status=1' style='display:block;width:100%;height:100%;line-height:15px;'>")
                .append("SPOOLED DIRECTORY detailed OK</a></td></tr></table></p>");
        String uri = null;
        if (detail) {
            uri = "SpooledDetailed.html";
        } else {
            uri = "Spooled.html";
        }
        if (name != null && !name.isEmpty()) {
            // name is specified
            uri = request.uri();
            if (istatus != 0) {
                uri += "&status=" + istatus;
            }
            responseContent.append(SpooledInformTask.buildSpooledUniqueTable(uri, name));
        } else {
            if (istatus != 0) {
                uri += "&status=" + istatus;
            }
            responseContent.append(SpooledInformTask.buildSpooledTable(detail, istatus, uri));
        }
        responseContent.append(REQUEST.status.readEnd());
    }

    /**
     * Write the response
     *
     * @param ctx
     */
    private void writeResponse(ChannelHandlerContext ctx) {
        // Convert the response content to a ByteBuf.
        ByteBuf buf = Unpooled.copiedBuffer(responseContent.toString(), WaarpStringUtils.UTF8);
        responseContent.setLength(0);
        // Decide whether to close the connection or not.
        boolean keepAlive = HttpUtil.isKeepAlive(request);
        boolean close = HttpHeaderValues.CLOSE.contentEqualsIgnoreCase(request
                .headers().get(HttpHeaderNames.CONNECTION)) ||
                (!keepAlive);

        // Build the response object.
        FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status, buf);
        response.headers().add(HttpHeaderNames.CONTENT_LENGTH, response.content().readableBytes());
        if (isCurrentRequestXml) {
            response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/xml");
        } else if (isCurrentRequestJson) {
            response.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/json");
        } else {
            response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/html");
        }
        if (keepAlive) {
            response.headers().set(HttpHeaderNames.CONNECTION,
                    HttpHeaderValues.KEEP_ALIVE);
        }
        if (!close) {
            // There's no need to add 'Content-Length' header
            // if this is the last response.
            response.headers().set(HttpHeaderNames.CONTENT_LENGTH, String.valueOf(buf.readableBytes()));
        }

        String cookieString = request.headers().get(HttpHeaderNames.COOKIE);
        if (cookieString != null) {
            Set<Cookie> cookies = ServerCookieDecoder.LAX.decode(cookieString);
            boolean i18nextFound = false;
            if (!cookies.isEmpty()) {
                // Reset the cookies if necessary.
                for (Cookie cookie : cookies) {
                    if (cookie.name().equalsIgnoreCase(I18NEXT)) {
                        i18nextFound = true;
                        cookie.setValue(lang);
                        response.headers().add(HttpHeaderNames.SET_COOKIE, ServerCookieEncoder.LAX.encode(cookie));
                    } else {
                        response.headers().add(HttpHeaderNames.SET_COOKIE, ServerCookieEncoder.LAX.encode(cookie));
                    }
                }
                if (!i18nextFound) {
                    Cookie cookie = new DefaultCookie(I18NEXT, lang);
                    response.headers().add(HttpHeaderNames.SET_COOKIE, ServerCookieEncoder.LAX.encode(cookie));
                }
            }
            if (!i18nextFound) {
                Cookie cookie = new DefaultCookie(I18NEXT, lang);
                response.headers().add(HttpHeaderNames.SET_COOKIE, ServerCookieEncoder.LAX.encode(cookie));
            }
        }

        // Write the response.
        ChannelFuture future = ctx.writeAndFlush(response);
        // Close the connection after the write operation is done if necessary.
        if (close) {
            future.addListener(ChannelFutureListener.CLOSE);
            /*if (this.isPrivateDbSession && dbSession != null) {
            	dbSession.forceDisconnect();
            	DbAdmin.nbHttpSession--;
            	dbSession = null;
            }*/
        }
    }

    /**
     * Send an error and close
     *
     * @param ctx
     * @param status
     */
    private void sendError(ChannelHandlerContext ctx, HttpResponseStatus status) {
        responseContent.setLength(0);
        responseContent.append(REQUEST.error.readHeader(this)).append("OpenR66 Web Failure: ")
                .append(status.toString()).append(REQUEST.error.readEnd());
        ByteBuf buf = Unpooled.copiedBuffer(responseContent.toString(), WaarpStringUtils.UTF8);
        FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status, buf);
        response.headers().add(HttpHeaderNames.CONTENT_LENGTH, response.content().readableBytes());
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/html");
        responseContent.setLength(0);
        // Close the connection as soon as the error message is sent.
        ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
            throws Exception {
        OpenR66Exception exception = OpenR66ExceptionTrappedFactory
                .getExceptionFromTrappedException(ctx.channel(), cause);
        if (exception != null) {
            if (!(exception instanceof OpenR66ProtocolBusinessNoWriteBackException)) {
                if (cause instanceof IOException) {
                    if (this.isPrivateDbSession && dbSession != null) {
                        dbSession.forceDisconnect();
                        DbAdmin.decHttpSession();
                        dbSession = null;
                    }
                    // Nothing to do
                    return;
                }
                logger.warn("Exception in HttpHandler {}", exception.getMessage());
            }
            if (ctx.channel().isActive()) {
                sendError(ctx, HttpResponseStatus.BAD_REQUEST);
            }
        } else {
            if (this.isPrivateDbSession && dbSession != null) {
                dbSession.forceDisconnect();
                DbAdmin.decHttpSession();
                dbSession = null;
            }
            // Nothing to do
            return;
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
        logger.debug("Closed");
        if (this.isPrivateDbSession && dbSession != null) {
            dbSession.forceDisconnect();
            DbAdmin.decHttpSession();
            dbSession = null;
        }
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        logger.debug("Connected");
        getAuthentHttp().getAuth().specialNoSessionAuth(false, Configuration.configuration.getHOST_ID());
        super.channelActive(ctx);
        ChannelGroup group = Configuration.configuration.getHttpChannelGroup();
        if (group != null) {
            group.add(ctx.channel());
        }
    }

    /**
     * @return the authentHttp
     */
    public R66Session getAuthentHttp() {
        return authentHttp;
    }
}
