/**
 * Copyright 2009, Frederic Bregier, and individual contributors
 * by the @author tags. See the COPYRIGHT.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 3.0 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package openr66.protocol.http.adminssl;

import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.net.ssl.SSLException;

import openr66.context.ErrorCode;
import openr66.context.R66Session;
import openr66.database.DbConstant;
import openr66.database.DbPreparedStatement;
import openr66.database.DbSession;
import openr66.database.data.DbTaskRunner;
import openr66.database.data.DbTaskRunner.TASKSTEP;
import openr66.database.exception.OpenR66DatabaseException;
import openr66.database.exception.OpenR66DatabaseNoConnectionError;
import openr66.database.exception.OpenR66DatabaseSqlError;
import openr66.protocol.configuration.Configuration;
import openr66.protocol.exception.OpenR66Exception;
import openr66.protocol.exception.OpenR66ExceptionTrappedFactory;
import openr66.protocol.exception.OpenR66ProtocolBusinessNoWriteBackException;
import openr66.protocol.exception.OpenR66ProtocolNetworkException;
import openr66.protocol.utils.OpenR66SignalHandler;
import openr66.protocol.utils.R66Future;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipelineCoverage;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.handler.codec.http.Cookie;
import org.jboss.netty.handler.codec.http.CookieDecoder;
import org.jboss.netty.handler.codec.http.CookieEncoder;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.jboss.netty.handler.codec.http.QueryStringDecoder;
import org.jboss.netty.handler.ssl.SslHandler;

/**
 * Handler for HTTP information support
 * @author Frederic Bregier
 *
 */
@ChannelPipelineCoverage("one")
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

    private R66Session authentHttp = new R66Session();

    private volatile HttpRequest request;
    private final StringBuilder responseContent = new StringBuilder();
    private volatile String uriRequest;
    private static final String sINFO="INFO", sCOMMAND="COMMAND", sNB="NB";
    private static final String sAUTHENT="AUTHENT", sNAME="NAME", sPASSWORD="PASSWORD";

    /**
     * The Database connection attached to this NetworkChannel
     * shared among all associated LocalChannels
     */
    private DbSession dbSession;
    /**
     * Does this dbSession is private and so should be closed
     */
    private boolean isPrivateDbSession = false;

    /**
     * Remover from SSL HashMap
     */
    private static final ChannelFutureListener remover = new ChannelFutureListener() {
        public void operationComplete(ChannelFuture future) {
            logger.debug("SSL remover");
            waitForSsl.remove(future.getChannel().getId());
        }
    };
    /**
     * Add the Channel as SSL handshake is over
     * @param channel
     */
    public static void addSslConnectedChannel(Channel channel) {
        R66Future futureSSL = new R66Future(true);
        waitForSsl.put(channel.getId(),futureSSL);
        channel.getCloseFuture().addListener(remover);
    }
    /**
     * Set the future of SSL handshake to status
     * @param channel
     * @param status
     */
    public static void setStatusSslConnectedChannel(Channel channel, boolean status) {
        R66Future futureSSL = waitForSsl.get(channel.getId());
        if (status) {
            futureSSL.setSuccess();
        } else {
            futureSSL.cancel();
        }
    }
    /**
     *
     * @param channel
     * @return True if the SSL handshake is over and OK, else False
     */
    public static boolean isSslConnectedChannel(Channel channel) {
        R66Future futureSSL = waitForSsl.get(channel.getId());
        if (futureSSL == null) {
            logger.error("No wait For SSL found");
            return false;
        } else {
            futureSSL.awaitUninterruptibly(Configuration.configuration.TIMEOUTCON);
            if (futureSSL.isDone()) {
                logger.info("Wait For SSL: "+futureSSL.isSuccess());
                return futureSSL.isSuccess();
            }
            logger.error("Out of time for wait For SSL");
            return false;
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
        super.channelOpen(ctx, e);
    }

    /**
     * Set header
     */
    private void header() {
        responseContent.append("<html>");
        responseContent.append("<head>");
        responseContent.append("<title>OpenR66 Web Administration Server: ");
        if (authentHttp.isAuthenticated()) {
            responseContent.append(authentHttp.getAuth().getUser());
        } else {
            responseContent.append("Not Authenticated");
        }
        responseContent.append("</title>\r\n");
        responseContent.append("</head><body>\r\n");
    }
    /**
     * End Body
     */
    private void endBody() {
        responseContent.append("</body>");
        responseContent.append("</html>");
    }
    /**
     * Front of Body for valid requests
     */
    private void inlineBody() {
        responseContent.append("<table border=\"0\">");
        responseContent.append("<tr>");
        responseContent.append("<td>");
        responseContent.append("<A href='/'><h2>OpenR66 Administration Page: ");
        responseContent.append(authentHttp.getAuth().getUser());
        responseContent.append(" on ");
        responseContent.append(request.getHeader(HttpHeaders.Names.HOST));
        responseContent.append("</h2></A>");
        responseContent.append("</td>");
        responseContent.append("<td>");
        responseContent.append("</td>");
        responseContent.append("</tr>\r\n");
        responseContent.append("<tr>");
        responseContent.append("<td>"+(new Date()).toString());
        responseContent.append("</td>");
        responseContent.append("</tr>\r\n");
        responseContent.append("<tr>");
        responseContent.append("<td>");
        // Add number of connections
        responseContent.append("Number of local active connections: "+
                Configuration.configuration.getLocalTransaction().getNumberLocalChannel());
        responseContent.append("</td>");
        responseContent.append("<td>");
        // Add number of connections
        responseContent.append("Number of network active connections: "+
                OpenR66SignalHandler.getNbConnection());
        responseContent.append("</td>");
        responseContent.append("</tr>");
        responseContent.append("</table>\r\n");
        responseContent.append("<CENTER><HR WIDTH=\"75%\" NOSHADE color=\"blue\"></CENTER>");
    }
    /**
     * Create Menu
     */
    private void createMenu() {
        header();
        inlineBody();
        responseContent.append("<table border=\"0\">");
        responseContent.append("<tr>");
        responseContent.append("<td>");
        responseContent.append("You must fill all of the following fields.");
        responseContent.append("</td>");
        responseContent.append("</tr>");
        responseContent.append("</table>\r\n");

        responseContent.append("<CENTER><HR WIDTH=\"75%\" NOSHADE color=\"blue\"></CENTER>");
        responseContent.append("<A HREF='/active'>Active Runners</A><BR>");
        responseContent.append("<A HREF='/error'>Error Runners</A><BR>");
        responseContent.append("<A HREF='/done'>Done Runners</A><BR>");
        responseContent.append("<A HREF='/all'>All Runners</A><BR>");
        responseContent.append("<FORM ACTION=\""+uriRequest+"\" METHOD=\"GET\">");
        responseContent.append("<input type=hidden name="+sCOMMAND+" value=\"GET\">");
        responseContent.append("<table border=\"0\">");
        responseContent.append("<tr><td>One Choice 0=Active 1=Error 2=Done 3=All: <br> <input type=text name=\""+sINFO+"\" size=1></td></tr>");
        responseContent.append("<tr><td>Number of runners (0 for all): <br> <input type=text name=\""+sNB+"\" size=5>");
        responseContent.append("</td></tr>");
        responseContent.append("<tr><td><INPUT TYPE=\"submit\" NAME=\"Send\" VALUE=\"Send\"></INPUT></td>");
        responseContent.append("<td><INPUT TYPE=\"reset\" NAME=\"Clear\" VALUE=\"Clear\" ></INPUT></td></tr>");
        responseContent.append("</table></FORM>\r\n");
        responseContent.append("<CENTER><HR WIDTH=\"75%\" NOSHADE color=\"blue\"></CENTER>");
        endBody();
    }
    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
            HttpRequest request = this.request = (HttpRequest) e.getMessage();
            QueryStringDecoder queryStringDecoder = new QueryStringDecoder(request.getUri());
            uriRequest = queryStringDecoder.getPath();
            char cval = 'z';
            if (! authentHttp.isAuthenticated()) {
                Map<String, List<String>> params = null;
                if (request.getMethod() == HttpMethod.GET) {
                    params = queryStringDecoder.getParameters();
                } else if (request.getMethod() == HttpMethod.POST) {
                    ChannelBuffer content = request.getContent();
                    if (content.readable()) {
                        String param = content.toString("UTF-8");
                        queryStringDecoder = new QueryStringDecoder("/?"+param);
                    } else {
                        createMenu();
                        writeResponse(e.getChannel());
                        return;
                    }
                    params = queryStringDecoder.getParameters();
                }
                boolean getMenu = true;
                String name = null, password = null;
                if (!params.isEmpty()) {
                    // if not uri, from get or post
                    if (getMenu && params.containsKey(sAUTHENT)) {
                        List<String> values = params.get(sAUTHENT);
                        if (values != null && params.get(sAUTHENT).get(0).equals("POST")) {
                            // get values
                            if (params.containsKey(sNAME)) {
                                values = params.get(sNAME);
                                if (values != null) {
                                    name = values.get(0);
                                    if (name == null || name.length() == 0) {
                                        getMenu = true;
                                    } else {
                                        getMenu = false;
                                    }
                                }
                            }
                        }
                    }
                    // search the nb param
                    if (params.containsKey(sPASSWORD)) {
                        List<String> values = params.get(sPASSWORD);
                        if (values != null) {
                            password = values.get(0);
                            if (password == null || password.length() == 0) {
                                getMenu = true;
                            } else {
                                getMenu = false;
                            }
                        }
                    }
                }
                if (! getMenu) {
                    authentHttp.getAuth().connection(dbSession, name, password.getBytes());
                    if (! authentHttp.isAuthenticated()) {
                        getMenu = true;
                    }
                }
                if (getMenu) {
                    createLogon();
                } else {
                    createMenu();
                }
                writeResponse(e.getChannel());
                return;
            }
            // check the URI
            if (uriRequest.equalsIgnoreCase("/active")) {
                cval = '0';
            } else if (uriRequest.equalsIgnoreCase("/error")) {
                cval = '1';
            } else if (uriRequest.equalsIgnoreCase("/done")) {
                cval = '2';
            } else if (uriRequest.equalsIgnoreCase("/all")) {
                cval = '3';
            }
            // Get the params according to get or post
            Map<String, List<String>> params = null;
            if (request.getMethod() == HttpMethod.GET) {
                params = queryStringDecoder.getParameters();
            } else if (request.getMethod() == HttpMethod.POST) {
                ChannelBuffer content = request.getContent();
                if (content.readable()) {
                    String param = content.toString("UTF-8");
                    queryStringDecoder = new QueryStringDecoder("/?"+param);
                } else {
                    createMenu();
                    writeResponse(e.getChannel());
                    return;
                }
                params = queryStringDecoder.getParameters();
            }
            int nb = 100;
            boolean getMenu = (cval == 'z');
            String value = null;
            if (!params.isEmpty()) {
                // if not uri, from get or post
                if (getMenu && params.containsKey(sCOMMAND)) {
                    List<String> values = params.get(sCOMMAND);
                    if (values != null && params.get(sCOMMAND).get(0).equals("GET")) {
                        // get values
                        if (params.containsKey(sINFO)) {
                            values = params.get(sINFO);
                            if (values != null) {
                                value = values.get(0);
                                if (value == null || value.length() == 0) {
                                    getMenu = true;
                                } else {
                                    getMenu = false;
                                    cval = value.charAt(0);
                                }
                            }
                        }
                    }
                }
                // search the nb param
                if (params.containsKey(sNB)) {
                    List<String> values = params.get(sNB);
                    if (values != null) {
                        value = values.get(0);
                        if (value != null && value.length() != 0) {
                            nb = Integer.parseInt(value);
                        }
                    }
                }
            }
            if (getMenu) {
                createMenu();
            } else {
                // Use value 0=Active 1=Error 2=Done 3=All
                switch (cval) {
                    case '0':
                        active(ctx, nb);
                        break;
                    case '1':
                        error(ctx, nb);
                        break;
                    case '2':
                        done(ctx, nb);
                        break;
                    case '3':
                        all(ctx, nb);
                        break;
                    default:
                        createMenu();
                }
            }
            writeResponse(e.getChannel());
    }
    /**
     * Add all runners from preparedStatement for type
     * @param preparedStatement
     * @param type
     * @param nb
     * @throws OpenR66DatabaseNoConnectionError
     * @throws OpenR66DatabaseSqlError
     */
    private void addRunners(DbPreparedStatement preparedStatement, String type, int nb)
        throws OpenR66DatabaseNoConnectionError, OpenR66DatabaseSqlError {
        preparedStatement.executeQuery();
        responseContent.append("<style>td{font-size: 8pt;}</style><table border=\"2\">");
        responseContent.append("<tr><td>");
        responseContent.append(type);
        responseContent.append("</td>");
        responseContent.append(DbTaskRunner.headerHtml());
        responseContent.append("</tr>\r\n");
        int i = 0;
        while (preparedStatement.getNext()) {
            DbTaskRunner taskRunner = DbTaskRunner.getFromStatement(preparedStatement);
            responseContent.append("<tr><td>*</td>");
            responseContent.append(taskRunner.toHtml(authentHttp));
            responseContent.append("</tr>\r\n");
            if (nb > 0) {
                i++;
                if (i >= nb) {
                    break;
                }
            }
        }
        responseContent.append("</table><br>\r\n");
    }
    /**
     * print all active transfers
     * @param ctx
     * @param nb
     */
    private void active(ChannelHandlerContext ctx, int nb) {
        header();
        inlineBody();
        DbPreparedStatement preparedStatement;
        try {
            preparedStatement =
                DbTaskRunner.getStatusPrepareStament(dbSession, ErrorCode.InitOk);
            addRunners(preparedStatement, ErrorCode.InitOk.mesg,nb);
            preparedStatement.realClose();
            preparedStatement =
                DbTaskRunner.getStatusPrepareStament(dbSession, ErrorCode.PreProcessingOk);
            addRunners(preparedStatement, ErrorCode.PreProcessingOk.mesg, nb);
            preparedStatement.realClose();
            preparedStatement =
                DbTaskRunner.getStatusPrepareStament(dbSession, ErrorCode.TransferOk);
            addRunners(preparedStatement, ErrorCode.TransferOk.mesg, nb);
            preparedStatement.realClose();
            preparedStatement =
                DbTaskRunner.getStatusPrepareStament(dbSession, ErrorCode.PostProcessingOk);
            addRunners(preparedStatement, ErrorCode.PostProcessingOk.mesg, nb);
            preparedStatement.realClose();
            preparedStatement =
                DbTaskRunner.getStatusPrepareStament(dbSession, ErrorCode.Running);
            addRunners(preparedStatement, ErrorCode.Running.mesg, nb);
            preparedStatement.realClose();
        } catch (OpenR66DatabaseException e) {
            logger.warn("OpenR66 Web Error",e);
            sendError(ctx, HttpResponseStatus.SERVICE_UNAVAILABLE);
            return;
        }
        endBody();
    }
    /**
     * print all transfers in error
     * @param ctx
     * @param nb
     */
    private void error(ChannelHandlerContext ctx, int nb) {
        header();
        inlineBody();
        DbPreparedStatement preparedStatement;
        try {
            preparedStatement =
                DbTaskRunner.getStepPrepareStament(dbSession, TASKSTEP.ERRORTASK);
            addRunners(preparedStatement, TASKSTEP.ERRORTASK.name(), nb);
            preparedStatement.realClose();
        } catch (OpenR66DatabaseException e) {
            logger.warn("OpenR66 Web Error",e);
            sendError(ctx, HttpResponseStatus.SERVICE_UNAVAILABLE);
            return;
        }
        endBody();
    }
    /**
     * Print all done transfers
     * @param ctx
     * @param nb
     */
    private void done(ChannelHandlerContext ctx, int nb) {
        header();
        inlineBody();
        DbPreparedStatement preparedStatement;
        try {
            preparedStatement =
                DbTaskRunner.getStatusPrepareStament(dbSession, ErrorCode.CompleteOk);
            addRunners(preparedStatement, ErrorCode.CompleteOk.mesg, nb);
            preparedStatement.realClose();
        } catch (OpenR66DatabaseException e) {
            logger.warn("OpenR66 Web Error",e);
            sendError(ctx, HttpResponseStatus.SERVICE_UNAVAILABLE);
            return;
        }
        endBody();
    }
    /**
     * Print all nb last transfers
     * @param ctx
     * @param nb
     */
    private void all(ChannelHandlerContext ctx, int nb) {
        header();
        inlineBody();
        DbPreparedStatement preparedStatement;
        try {
            preparedStatement =
                DbTaskRunner.getStatusPrepareStament(dbSession, null);// means all
            addRunners(preparedStatement, "ALL RUNNERS: "+nb, nb);
            preparedStatement.realClose();
        } catch (OpenR66DatabaseException e) {
            logger.warn("OpenR66 Web Error",e);
            sendError(ctx, HttpResponseStatus.SERVICE_UNAVAILABLE);
            return;
        }
        endBody();
    }
    /**
     * Write the response
     * @param e
     */
    private void writeResponse(Channel channel) {
        // Convert the response content to a ChannelBuffer.
        ChannelBuffer buf = ChannelBuffers.copiedBuffer(responseContent.toString(), "UTF-8");
        responseContent.setLength(0);

        // Decide whether to close the connection or not.
        boolean close =
            HttpHeaders.Values.CLOSE.equalsIgnoreCase(request.getHeader(HttpHeaders.Names.CONNECTION)) ||
            request.getProtocolVersion().equals(HttpVersion.HTTP_1_0) &&
            !HttpHeaders.Values.KEEP_ALIVE.equalsIgnoreCase(request.getHeader(HttpHeaders.Names.CONNECTION));

        // Build the response object.
        HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
        response.setContent(buf);
        response.setHeader(HttpHeaders.Names.CONTENT_TYPE, "text/html");
        if (HttpHeaders.Values.KEEP_ALIVE.equalsIgnoreCase(request.getHeader(HttpHeaders.Names.CONNECTION))) {
            response.setHeader(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.KEEP_ALIVE);
        }
        if (!close) {
            // There's no need to add 'Content-Length' header
            // if this is the last response.
            response.setHeader(HttpHeaders.Names.CONTENT_LENGTH, String.valueOf(buf.readableBytes()));
        }

        String cookieString = request.getHeader(HttpHeaders.Names.COOKIE);
        if (cookieString != null) {
            CookieDecoder cookieDecoder = new CookieDecoder();
            Set<Cookie> cookies = cookieDecoder.decode(cookieString);
            if(!cookies.isEmpty()) {
                // Reset the cookies if necessary.
                CookieEncoder cookieEncoder = new CookieEncoder(true);
                for (Cookie cookie : cookies) {
                    cookieEncoder.addCookie(cookie);
                }
                response.addHeader(HttpHeaders.Names.SET_COOKIE, cookieEncoder.encode());
            }
        }

        // Write the response.
        ChannelFuture future = channel.write(response);

        // Close the connection after the write operation is done if necessary.
        if (close) {
            future.addListener(ChannelFutureListener.CLOSE);
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
        header();
        responseContent.append("OpenR66 Web Failure: ");
        responseContent.append(status.toString());
        endBody();
        response.setContent(ChannelBuffers.copiedBuffer(responseContent.toString(), "UTF-8"));
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
                logger.warn("Exception in HttpSslHandler", exception);
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
     * @see org.jboss.netty.channel.SimpleChannelUpstreamHandler#channelClosed(org.jboss.netty.channel.ChannelHandlerContext, org.jboss.netty.channel.ChannelStateEvent)
     */
    @Override
    public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e)
            throws Exception {
        super.channelClosed(ctx, e);
        if (this.isPrivateDbSession && dbSession != null) {
            try {
                dbSession.disconnect();
            } catch (OpenR66DatabaseSqlError e1) {
            }
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
            try {
                handshakeFuture = sslHandler.handshake(e.getChannel());
            } catch (SSLException e1) {
                setStatusSslConnectedChannel(e.getChannel(), false);
                throw new OpenR66ProtocolNetworkException("Bad SSL handshake",
                        e1);
            }
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
        } else {
            logger.warn("SSL Not found");
        }
        super.channelConnected(ctx, e);
        ChannelGroup group =
            Configuration.configuration.getHttpChannelGroup();
        if (group != null) {
            group.add(e.getChannel());
        }
        try {
            if (DbConstant.admin.isConnected) {
                this.dbSession = new DbSession(DbConstant.admin, false);
            }
            this.isPrivateDbSession = true;
        } catch (OpenR66DatabaseNoConnectionError e1) {
            // Cannot connect so use default connection
            logger.warn("Use default database connection");
            this.dbSession = DbConstant.admin.session;
        }
    }
    /**
     * Create Logon Menu
     */
    private void createLogon() {
        header();
        responseContent.append("<table border=\"0\">");
        responseContent.append("<tr>");
        responseContent.append("<td>");
        responseContent.append("<h1>OpenR66 Administration Page: "+
                request.getHeader(HttpHeaders.Names.HOST)+"</h1>");
        responseContent.append("You must fill all of the following fields.");
        responseContent.append("</td>");
        responseContent.append("</tr>");
        responseContent.append("</table>\r\n");

        responseContent.append("<CENTER><HR WIDTH=\"75%\" NOSHADE color=\"blue\"></CENTER>");
        responseContent.append("<FORM ACTION=\""+uriRequest+"\" METHOD=\"POST\">");
        responseContent.append("<input type=hidden name="+sAUTHENT+" value=\"POST\">");
        responseContent.append("<table border=\"0\">");
        responseContent.append("<tr><td>Name: <br> <input type=text name=\""+sNAME+"\" size=25></td></tr>");
        responseContent.append("<tr><td>Password: <br> <input type=PASSWORD name=\""+sPASSWORD+"\" size=8>");
        responseContent.append("</td></tr>");
        responseContent.append("<tr><td><INPUT TYPE=\"submit\" NAME=\"Send\" VALUE=\"Send\"></INPUT></td>");
        responseContent.append("<td><INPUT TYPE=\"reset\" NAME=\"Clear\" VALUE=\"Clear\" ></INPUT></td></tr>");
        responseContent.append("</table></FORM>\r\n");
        responseContent.append("<CENTER><HR WIDTH=\"75%\" NOSHADE color=\"blue\"></CENTER>");
        endBody();
    }
}
