/**
 * This file is part of Waarp Project (named also Waarp or GG).
 * 
 * Copyright 2009, Frederic Bregier, and individual contributors by the @author
 * tags. See the COPYRIGHT.txt in the distribution for a full listing of
 * individual contributors.
 * 
 * All Waarp Project is free software: you can redistribute it and/or
 * modify it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your
 * option) any later version.
 * 
 * Waarp is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
 * A PARTICULAR PURPOSE. See the GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License along with
 * Waarp . If not, see <http://www.gnu.org/licenses/>.
 */
package org.waarp.openr66.protocol.localhandler.rest;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.waarp.common.command.exception.Reply421Exception;
import org.waarp.common.command.exception.Reply530Exception;
import org.waarp.common.digest.FilesystemBasedDigest;
import org.waarp.common.exception.CryptoException;
import org.waarp.common.logging.WaarpInternalLogger;
import org.waarp.common.logging.WaarpInternalLoggerFactory;
import org.waarp.common.utility.WaarpStringUtils;
import org.waarp.gateway.kernel.exception.HttpInvalidAuthenticationException;
import org.waarp.gateway.kernel.rest.HttpRestHandler;
import org.waarp.openr66.context.R66Session;
import org.waarp.openr66.database.DbConstant;
import org.waarp.openr66.protocol.configuration.Configuration;
import org.waarp.openr66.protocol.localhandler.ServerActions;
import org.waarp.openr66.protocol.localhandler.rest.handler.DbHostAuthR66RestMethodHandler;
import org.waarp.openr66.protocol.localhandler.rest.handler.DbRuleR66RestMethodHandler;
import org.waarp.openr66.protocol.localhandler.rest.handler.DbTaskRunnerR66RestMethodHandler;
import org.waarp.openr66.protocol.localhandler.rest.handler.HttpRestBandwidthR66Handler;
import org.waarp.openr66.protocol.localhandler.rest.handler.HttpRestBusinessR66Handler;
import org.waarp.openr66.protocol.localhandler.rest.handler.HttpRestConfigR66Handler;
import org.waarp.openr66.protocol.localhandler.rest.handler.HttpRestInformationR66Handler;
import org.waarp.openr66.protocol.localhandler.rest.handler.HttpRestLogR66Handler;
import org.waarp.openr66.protocol.localhandler.rest.handler.HttpRestServerR66Handler;
import org.waarp.openr66.protocol.localhandler.rest.handler.HttpRestTransferR66Handler;

/**
 * Handler for HTTP support
 * 
 * @author Frederic Bregier
 * 
 */
public class HttpRestR66Handler extends HttpRestHandler {
	/**
     * Internal Logger
     */
    private static final WaarpInternalLogger logger = WaarpInternalLoggerFactory
            .getLogger(HttpRestR66Handler.class);

    public static HashMap<String, String> falseRepoPassword = new HashMap<String, String>();
    
    static {
    	restHashMap.put("transfers", new DbTaskRunnerR66RestMethodHandler("transfers", true));
    	restHashMap.put("hosts", new DbHostAuthR66RestMethodHandler("hosts", true));
    	restHashMap.put("rules", new DbRuleR66RestMethodHandler("rules", true));
    	restHashMap.put("hostconfigs", new DbTaskRunnerR66RestMethodHandler("hostconfigs", true));
    	restHashMap.put("configurations", new DbTaskRunnerR66RestMethodHandler("configurations", true));
    	restHashMap.put(HttpRestBandwidthR66Handler.BASEURI, new HttpRestBandwidthR66Handler());
    	restHashMap.put(HttpRestBusinessR66Handler.BASEURI, new HttpRestBusinessR66Handler());
    	restHashMap.put(HttpRestConfigR66Handler.BASEURI, new HttpRestConfigR66Handler());
    	restHashMap.put(HttpRestInformationR66Handler.BASEURI, new HttpRestInformationR66Handler());
    	restHashMap.put(HttpRestLogR66Handler.BASEURI, new HttpRestLogR66Handler());
    	restHashMap.put(HttpRestServerR66Handler.BASEURI, new HttpRestServerR66Handler());
    	restHashMap.put(HttpRestTransferR66Handler.BASEURI, new HttpRestTransferR66Handler());
    	//restHashMap.put("config", new Old_HttpRestMethodR66Handler("config", METHOD.GET, METHOD.POST));
    	falseRepoPassword.put("admin2", "test");
    }

    /**
     * If True: authentication is mandatory
     */
    public boolean checkAuthent = true;
    /**
     * If null, no time limit will be applied
     */
    public long checkTime = 0;
    /**
   	 * Server Actions handler
   	 */
   	public volatile ServerActions serverHandler = new ServerActions();
   	
	@Override
    protected void checkConnection(Channel channel) throws HttpInvalidAuthenticationException {
		logger.debug("Request: {} ### {}",arguments,response);
		String user = arguments.getXAuthUser();
		String key = user != null ? falseRepoPassword.get(user) : null;
		if (checkAuthent) {
			if (key == null) {
				status = HttpResponseStatus.UNAUTHORIZED;
				throw new HttpInvalidAuthenticationException("Wrong Authentication");
			}
			arguments.checkBaseAuthent(key, checkTime);
		} else {
			arguments.checkBaseAuthent(null, checkTime);
		}
		serverHandler.newSession();
		R66Session session = serverHandler.getSession();
		if (! checkAuthent) {
			user = Configuration.configuration.ADMINNAME;
			session.getAuth().specialNoSessionAuth(true, Configuration.configuration.HOST_SSLID);
		} else {
			// XXX FIXME Should we have one DbSession per connection ? Maybe only after authentication
			try {
				session.getAuth().connectionHttps(DbConstant.admin.session, user, 
						FilesystemBasedDigest.passwdCrypt(key.getBytes(WaarpStringUtils.UTF8)));
			} catch (Reply530Exception e) {
				status = HttpResponseStatus.UNAUTHORIZED;
				throw new HttpInvalidAuthenticationException("Wrong Authentication", e);
			} catch (Reply421Exception e) {
				status = HttpResponseStatus.SERVICE_UNAVAILABLE;
				throw new HttpInvalidAuthenticationException("Service unavailable", e);
			}
		}
		arguments.setXAuthRole(session.getAuth().getRole());
		arguments.methodFromUri();
		arguments.methodFromHeader();
    }

	
	@Override
	public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
		super.channelClosed(ctx, e);
		serverHandler.channelClosed(e);
	}


	/**
	 * Called at the beginning of every new request
	 * 
	 * Override if needed
	 */
	protected void initialize() {
		super.initialize();
	}

	public static void initializeService(long connectImeout, int port, String pathTemp, File keyFile) throws CryptoException, IOException {
        group = Configuration.configuration.getHttpChannelGroup();
		// Configure the server.
        NioServerSocketChannelFactory httpChannelFactory = new NioServerSocketChannelFactory(
				Configuration.configuration.getExecutorService(),
				Configuration.configuration.getExecutorService(),
				Configuration.configuration.SERVER_THREAD);
        ServerBootstrap httpBootstrap = new ServerBootstrap(httpChannelFactory);
		// Set up the event pipeline factory.
        HttpRestR66Handler.initialize(pathTemp, keyFile);

		httpBootstrap.setPipelineFactory(new HttpRestR66PipelineFactory(false, null));
		httpBootstrap.setOption("child.tcpNoDelay", true);
		httpBootstrap.setOption("child.keepAlive", true);
		httpBootstrap.setOption("child.reuseAddress", true);
		httpBootstrap.setOption("child.connectTimeoutMillis", connectImeout);
		httpBootstrap.setOption("tcpNoDelay", true);
		httpBootstrap.setOption("reuseAddress", true);
		httpBootstrap.setOption("connectTimeoutMillis", connectImeout);
		// Bind and start to accept incoming connections.
		group.add(httpBootstrap.bind(new InetSocketAddress(port)));
	}
}
