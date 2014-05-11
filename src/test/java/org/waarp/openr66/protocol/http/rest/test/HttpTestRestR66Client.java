/**
   This file is part of Waarp Project.

   Copyright 2009, Frederic Bregier, and individual contributors by the @author
   tags. See the COPYRIGHT.txt in the distribution for a full listing of
   individual contributors.

   All Waarp Project is free software: you can redistribute it and/or 
   modify it under the terms of the GNU General Public License as published 
   by the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   Waarp is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with Waarp .  If not, see <http://www.gnu.org/licenses/>.
 */
package org.waarp.openr66.protocol.http.rest.test;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.logging.InternalLoggerFactory;
import org.joda.time.DateTime;
import org.waarp.common.crypto.ssl.WaarpSslUtility;
import org.waarp.common.database.data.AbstractDbData;
import org.waarp.common.database.exception.WaarpDatabaseException;
import org.waarp.common.digest.FilesystemBasedDigest;
import org.waarp.common.exception.CryptoException;
import org.waarp.common.json.JsonHandler;
import org.waarp.common.logging.WaarpInternalLogger;
import org.waarp.common.logging.WaarpInternalLoggerFactory;
import org.waarp.common.logging.WaarpSlf4JLoggerFactory;
import org.waarp.common.utility.WaarpStringUtils;
import org.waarp.gateway.kernel.exception.HttpInvalidAuthenticationException;
import org.waarp.gateway.kernel.rest.DataModelRestMethodHandler;
import org.waarp.gateway.kernel.rest.HttpRestHandler;
import org.waarp.gateway.kernel.rest.RestArgument;
import org.waarp.gateway.kernel.rest.client.RestFuture;
import org.waarp.openr66.context.ErrorCode;
import org.waarp.openr66.context.task.test.TestExecJavaTask;
import org.waarp.openr66.database.DbConstant;
import org.waarp.openr66.database.data.DbConfiguration;
import org.waarp.openr66.database.data.DbHostAuth;
import org.waarp.openr66.database.data.DbHostConfiguration;
import org.waarp.openr66.database.data.DbRule;
import org.waarp.openr66.database.data.DbTaskRunner;
import org.waarp.openr66.database.data.DbTaskRunner.Columns;
import org.waarp.openr66.database.data.DbTaskRunner.TASKSTEP;
import org.waarp.openr66.protocol.configuration.Configuration;
import org.waarp.openr66.protocol.http.rest.HttpRestR66Handler;
import org.waarp.openr66.protocol.http.rest.HttpRestR66Handler.RESTHANDLERS;
import org.waarp.openr66.protocol.http.rest.client.HttpRestR66Client;
import org.waarp.openr66.protocol.http.rest.handler.DbConfigurationR66RestMethodHandler;
import org.waarp.openr66.protocol.http.rest.handler.DbHostAuthR66RestMethodHandler;
import org.waarp.openr66.protocol.http.rest.handler.DbHostConfigurationR66RestMethodHandler;
import org.waarp.openr66.protocol.http.rest.handler.DbRuleR66RestMethodHandler;
import org.waarp.openr66.protocol.http.rest.handler.DbTaskRunnerR66RestMethodHandler;
import org.waarp.openr66.protocol.localhandler.packet.InformationPacket;
import org.waarp.openr66.protocol.localhandler.packet.LocalPacketFactory;
import org.waarp.openr66.protocol.localhandler.packet.json.BandwidthJsonPacket;
import org.waarp.openr66.protocol.localhandler.packet.json.BusinessRequestJsonPacket;
import org.waarp.openr66.protocol.localhandler.packet.json.ConfigExportJsonPacket;
import org.waarp.openr66.protocol.localhandler.packet.json.InformationJsonPacket;
import org.waarp.openr66.protocol.localhandler.packet.json.JsonPacket;
import org.waarp.openr66.protocol.localhandler.packet.json.LogJsonPacket;
import org.waarp.openr66.protocol.localhandler.packet.json.ShutdownOrBlockJsonPacket;
import org.waarp.openr66.protocol.localhandler.packet.json.TransferRequestJsonPacket;

import com.fasterxml.jackson.databind.node.ObjectNode;


/**
 * @author "Frederic Bregier"
 *
 */
public class HttpTestRestR66Client  implements Runnable {
	private static WaarpInternalLogger logger;
	public static int NB = 2;

    public static int NBPERTHREAD = 100;
    
    private static HttpRestR66Client clientHelper;
	private static final String baseURI = "/";
	private static final String host = "127.0.0.1";
	private static final int port = 8080;
    
	private static final long limit = 10000000;
	private static final long delaylimit = 5000;
	private static final String hostid = "hostZZ";
	private static final String address = "10.10.10.10";
	private static final String key = "ABCDEFGH";
	private static final String business = "<business><businessid>hostas</businessid></business>";
	private static final String roles = "<roles><role><roleid>hostas</roleid><roleset>FULLADMIN</roleset></role></roles>";
	private static final String aliases = "<aliases><alias><realid>hostZZ</realid><aliasid>hostZZ2</aliasid></alias></aliases>";
	private static final String others = "<root><version>2.4.25</version></root>";
	private static final String idRule = "ruleZZ";
	private static final String ids = "<hostids><hostid>hosta</hostid><hostid>hostZZ</hostid></hostids>";
	private static final String tasks = "<tasks><task><type>LOG</type><path>log</path><delay>0</delay><comment></comment></task></tasks>";

	public static boolean DEBUG = false;
	
	public static AtomicLong count = new AtomicLong();
	/**
	 * @param args
	 */
	@SuppressWarnings("unused")
	public static void main(String[] args) {
		InternalLoggerFactory.setDefaultFactory(new WaarpSlf4JLoggerFactory(null));
		logger = WaarpInternalLoggerFactory.getLogger(HttpTestRestR66Client.class);
    	Configuration.configuration.HOST_ID = hostid;
		if (args.length > 0) {
			NB = Integer.parseInt(args[0]);
			if (Configuration.configuration.CLIENT_THREAD < NB) {
				Configuration.configuration.CLIENT_THREAD = NB+1;
			}
			if (args.length > 1) {
				NBPERTHREAD = Integer.parseInt(args[1]);
			}
		}
		if (NB == 1 && NBPERTHREAD == 1) {
			DEBUG = true;
		}
		String pathTemp = "J:/Temp/temp";
	    try {
			HttpRestHandler.initialize(pathTemp, new File("J:/Temp/temp/key.sha256"));
		} catch (CryptoException e2) {
			e2.printStackTrace();
			return;
		} catch (IOException e2) {
			e2.printStackTrace();
			return;
		}
		// Configure the client.
		clientHelper = new HttpRestR66Client(baseURI, Configuration.configuration.CLIENT_THREAD, Configuration.configuration.TIMEOUTCON, new HttpTestRestClientPipelineFactory(null));
		logger.warn("ClientHelper created");
		try {
	        try {
	        	long start = System.currentTimeMillis();
	        	for (int i = 0; i < NBPERTHREAD; i++) {
	        		options(null);
	        	}
	        	long stop = System.currentTimeMillis();
	        	logger.warn("Options: "+count.get()*1000/(stop-start)+" req/s "+NBPERTHREAD+"=?"+count.get());
			} catch (HttpInvalidAuthenticationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			count.set(0);
	    	long start = System.currentTimeMillis();
	    	if (false) {
		        for (RESTHANDLERS handler : HttpRestR66Handler.RESTHANDLERS.values()) {
			        try {
		        		deleteData(handler);
					} catch (HttpInvalidAuthenticationException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
		        }
	    	}
	    	long stop = System.currentTimeMillis();
	    	if (false) {
	    		logger.warn("Delete: "+count.get()*1000/(stop-start)+" req/s "+NBPERTHREAD+"=?"+count.get());
	    	}
	        count.set(0);
	    	start = System.currentTimeMillis();
	        for (RESTHANDLERS handler : HttpRestR66Handler.RESTHANDLERS.values()) {
	        	if (handler.getRestMethodHandler() instanceof DataModelRestMethodHandler<?>) {
			        try {
		        		multiDataRequests(handler);
					} catch (HttpInvalidAuthenticationException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
	        	}
	        }
	    	stop = System.currentTimeMillis();
	    	logger.warn("Create: "+count.get()*1000/(stop-start)+" req/s "+NBPERTHREAD+"=?"+count.get());
	        count.set(0);
	    	start = System.currentTimeMillis();
	        for (RESTHANDLERS handler : HttpRestR66Handler.RESTHANDLERS.values()) {
	        	if (handler.getRestMethodHandler() instanceof DataModelRestMethodHandler<?>) {
			        try {
		        		realAllData(handler);
					} catch (HttpInvalidAuthenticationException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
	        	}
	        }
	    	stop = System.currentTimeMillis();
	    	logger.warn("ReadAll: "+count.get()*1000/(stop-start)+" req/s "+NBPERTHREAD+"=?"+count.get());
	    	
	    	count.set(0);
	    	start = System.currentTimeMillis();
	    	for (int i = 0; i < NBPERTHREAD; i++) {
		        try {
		        	multiDataRequests(RESTHANDLERS.DbTaskRunner);
				} catch (HttpInvalidAuthenticationException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	        }
	    	stop = System.currentTimeMillis();
	    	logger.warn("CreateMultiple: "+count.get()*1000/(stop-start)+" req/s "+NBPERTHREAD+"=?"+count.get());
	    	
	        count.set(0);
	    	start = System.currentTimeMillis();
	        launchThreads();
	    	stop = System.currentTimeMillis();
	    	logger.warn("CreateMultipleThread: "+count.get()*1000/(stop-start)+" req/s "+NBPERTHREAD*NB+"=?"+count.get());
	
	    	// Set usefull item first
	    	{
	    		Channel channel = clientHelper.getChannel(host, port);
	    		Entry<String, String> id = HttpRestR66Handler.falseRepoPassword.entrySet().iterator().next();
		    	// Need business
		    	String buz = "<business><businessid>hostas</businessid><businessid>hosta2</businessid><businessid>hostas2</businessid>" +
		    			"<businessid>hosta</businessid><businessid>test</businessid><businessid>tests</businessid>" +
		    			"<businessid>"+id.getKey()+"</businessid></business>";
	    		ObjectNode node = JsonHandler.createObjectNode();
	    		node.put(DbHostConfiguration.Columns.BUSINESS.name(), buz);
	            RestFuture future = clientHelper.sendQuery(channel, HttpMethod.PUT, host, RESTHANDLERS.DbHostConfiguration.uri+"/hosta", id.getKey(), id.getValue(),
	            		null,
	            		JsonHandler.writeAsString(node));
	            try {
	    			future.await();
	    		} catch (InterruptedException e) {
	    		}
	            WaarpSslUtility.closingSslChannel(channel);
		    	// Need Hostzz
	            channel = clientHelper.getChannel(host, port);
	            AbstractDbData dbData;
				dbData = new DbHostAuth(null, hostid, address, port, false, key.getBytes(), true, false);
	            future = clientHelper.sendQuery(channel, HttpMethod.POST, host, RESTHANDLERS.DbHostAuth.uri, id.getKey(), id.getValue(),
	            		null,
	            		dbData.asJson());
	            try {
	    			future.await();
	    		} catch (InterruptedException e) {
	    		}
	       		WaarpSslUtility.closingSslChannel(channel);
	    	}
	    	
	    	// Other Command as actions
	    	count.set(0);
	    	start = System.currentTimeMillis();
	    	for (RESTHANDLERS handler : HttpRestR66Handler.RESTHANDLERS.values()) {
	    		if (!(handler.getRestMethodHandler() instanceof DataModelRestMethodHandler<?>)) {
		    		try {
		        		action(handler);
					} catch (HttpInvalidAuthenticationException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
	    		}
	        }
	        stop = System.currentTimeMillis();
	        logger.warn("Commands: "+count.get()*1000/(stop-start)+" req/s "+NBPERTHREAD*NB+"=?"+count.get());
	
	        // Clean
	    	{
	    		Channel channel = clientHelper.getChannel(host, port);
	    		Entry<String, String> id = HttpRestR66Handler.falseRepoPassword.entrySet().iterator().next();
		    	// Reset business
		    	String buz = "<business><businessid>hostas</businessid><businessid>hosta2</businessid><businessid>hostas2</businessid>" +
		    			"<businessid>hosta</businessid><businessid>test</businessid><businessid>tests</businessid></business>";
	    		ObjectNode node = JsonHandler.createObjectNode();
	    		node.put(DbHostConfiguration.Columns.BUSINESS.name(), buz);
	            RestFuture future = clientHelper.sendQuery(channel, HttpMethod.PUT, host, RESTHANDLERS.DbHostConfiguration.uri+"/hosta", id.getKey(), id.getValue(),
	            		null,
	            		JsonHandler.writeAsString(node));
	            try {
	    			future.await();
	    		} catch (InterruptedException e) {
	    		}
	            WaarpSslUtility.closingSslChannel(channel);
		    	// Remove Hostzz
	            channel = clientHelper.getChannel(host, port);
	            try {
					future = deleteData(channel, RESTHANDLERS.DbHostAuth);
		            try {
		    			future.await();
		    		} catch (InterruptedException e) {
		    		}
				} catch (HttpInvalidAuthenticationException e1) {
				}
	       		WaarpSslUtility.closingSslChannel(channel);
		    	// Shutdown
	            channel = clientHelper.getChannel(host, port);
	            ShutdownOrBlockJsonPacket shutd = new ShutdownOrBlockJsonPacket();
	            shutd.setRestartOrBlock(false);
	            shutd.setShutdownOrBlock(true);
	            shutd.setRequestUserPacket(LocalPacketFactory.SHUTDOWNPACKET);
	            String pwd = "pwdhttp";
	            byte [] bpwd = FilesystemBasedDigest.passwdCrypt(pwd.getBytes(WaarpStringUtils.UTF8));
	            shutd.setKey(bpwd);
	            future = action(channel, HttpMethod.PUT, RESTHANDLERS.Server.uri, shutd);
				try {
					future.await();
				} catch (InterruptedException e) {
				}
	       		WaarpSslUtility.closingSslChannel(channel);
	    	}
	        try {
				Thread.sleep(100);
			} catch (InterruptedException e1) {
			}
		} finally {
			logger.debug("ClientHelper closing");
			clientHelper.closeAll();
			logger.warn("ClientHelper closed");
		}
	}

	public static void launchThreads() {
        // init thread model
        ExecutorService pool = Executors.newFixedThreadPool(NB);
        HttpTestRestR66Client[] clients = new HttpTestRestR66Client[NB];
        for (int i = 0; i < NB; i ++) {
            clients[i] = new HttpTestRestR66Client();
        }
        for (int i = 0; i < NB; i ++) {
            pool.execute(clients[i]);
        }
        pool.shutdown();
        try {
            while (! pool.awaitTermination(100000, TimeUnit.SECONDS)) ;
        } catch (InterruptedException e) {
        }
    }

    public static void options(String uri) throws HttpInvalidAuthenticationException {
    	Channel channel = clientHelper.getChannel(host, port);
        if (channel == null) {
        	return;
        }
		RestFuture future = options(channel, uri);
    	try {
			future.await();
		} catch (InterruptedException e) {
		}
		logger.debug("Closing Channel");
   		WaarpSslUtility.closingSslChannel(channel);
		logger.debug("Channel closed");
    }

    protected static RestFuture options(Channel channel, String uri) throws HttpInvalidAuthenticationException {
		logger.debug("Send query");
        Entry<String, String> id = HttpRestR66Handler.falseRepoPassword.entrySet().iterator().next();
    	RestFuture future = clientHelper.sendQuery(channel, HttpMethod.OPTIONS, host, uri, id.getKey(), id.getValue(), null, null);
		logger.debug("Query sent");
		return future;
    }
    
    protected static AbstractDbData getItem(RESTHANDLERS data) throws HttpInvalidAuthenticationException {
    	switch (data) {
			case DbConfiguration:
				return new DbConfiguration(null, hostid, limit, limit, limit, limit, delaylimit);
			case DbHostAuth:
				return new DbHostAuth(null, hostid, address, port, false, key.getBytes(), false, false);
			case DbHostConfiguration:
				return new DbHostConfiguration(null, hostid, business, roles, aliases, others);
			case DbRule:
				return new DbRule(null, idRule, ids, 2, "/recv", "/send", "/arch", "/work", tasks, tasks, tasks, tasks, tasks, tasks);
			case DbTaskRunner:
				ObjectNode source = JsonHandler.createObjectNode();
				source.put(Columns.IDRULE.name(), idRule);
				source.put(Columns.RANK.name(), 0);
				source.put(Columns.BLOCKSZ.name(), 65536);
				source.put(Columns.FILEINFO.name(), "file info");
				source.put(Columns.FILENAME.name(), "filename");
				source.put(Columns.GLOBALLASTSTEP.name(), TASKSTEP.NOTASK.ordinal());
				source.put(Columns.GLOBALSTEP.name(), TASKSTEP.NOTASK.ordinal());
				source.put(Columns.INFOSTATUS.name(), ErrorCode.Unknown.ordinal());
				source.put(Columns.ISMOVED.name(), false);
				source.put(Columns.MODETRANS.name(), 2);
				source.put(Columns.ORIGINALNAME.name(), "original filename");
				source.put(Columns.OWNERREQ.name(), Configuration.configuration.HOST_ID);
				source.put(Columns.SPECIALID.name(), DbConstant.ILLEGALVALUE);
				source.put(Columns.REQUESTED.name(), hostid);
				source.put(Columns.REQUESTER.name(), hostid);
				source.put(Columns.RETRIEVEMODE.name(), true);
				source.put(Columns.STARTTRANS.name(), System.currentTimeMillis());
				source.put(Columns.STOPTRANS.name(), System.currentTimeMillis());
				source.put(Columns.STEP.name(), -1);
				source.put(Columns.STEPSTATUS.name(), ErrorCode.Unknown.ordinal());
				source.put(Columns.TRANSFERINFO.name(), "transfer info");
				source.put(DbTaskRunner.JSON_RESCHEDULE, false);
				source.put(DbTaskRunner.JSON_THROUGHMODE, false);
				source.put(DbTaskRunner.JSON_ORIGINALSIZE, 123L);
				try {
					return new DbTaskRunner(null, source);
				} catch (WaarpDatabaseException e) {
					throw new HttpInvalidAuthenticationException(e);
				}
			default:
				break;
    	}
    	return null;
    }
    
    protected static void realAllData(RESTHANDLERS data) throws HttpInvalidAuthenticationException {
    	Channel channel = clientHelper.getChannel(host, port);
        if (channel == null) {
        	return;
        }
        RestFuture future = realAllData(channel, data);
    	try {
			future.await();
		} catch (InterruptedException e) {
		}
   		WaarpSslUtility.closingSslChannel(channel);
		logger.debug("Channel closed");
    }
    
    protected static RestFuture realAllData(Channel channel, RESTHANDLERS data) throws HttpInvalidAuthenticationException {
		logger.debug("Send query");
        Entry<String, String> id = HttpRestR66Handler.falseRepoPassword.entrySet().iterator().next();
        ObjectNode answer = JsonHandler.createObjectNode();
        switch (data) {
			case DbConfiguration:
				answer.put(DbConfigurationR66RestMethodHandler.FILTER_ARGS.BANDWIDTH.name(), -1);
				break;
			case DbHostAuth:
				answer.put(DbHostAuthR66RestMethodHandler.FILTER_ARGS.ISSSL.name(), true);
				answer.put(DbHostAuthR66RestMethodHandler.FILTER_ARGS.ISACTIVE.name(), true);
				break;
			case DbHostConfiguration:
				answer.put(DbHostConfigurationR66RestMethodHandler.FILTER_ARGS.BUSINESS.name(), "hosta");
				break;
			case DbRule:
				answer.put(DbRuleR66RestMethodHandler.FILTER_ARGS.MODETRANS.name(), 2);
				break;
			case DbTaskRunner:
				answer.put(DbTaskRunnerR66RestMethodHandler.FILTER_ARGS.STOPTRANS.name(), new DateTime().toString());
				answer.put(Columns.OWNERREQ.name(), Configuration.configuration.HOST_ID);
				break;
			default:
				RestFuture future = (RestFuture) channel.getAttachment();
				future.cancel();
	        	WaarpSslUtility.closingSslChannel(channel);
	            return future;
        }
    	RestFuture future = clientHelper.sendQuery(channel, HttpMethod.GET, host, data.uri, id.getKey(), id.getValue(), null,
    			JsonHandler.writeAsString(answer));
		logger.debug("Query sent");
		return future;
    }

    protected static void deleteData(RESTHANDLERS data) throws HttpInvalidAuthenticationException {
    	Channel channel = clientHelper.getChannel(host, port);
        if (channel == null) {
        	return;
        }
        RestFuture future = deleteData(channel, data);
    	try {
			future.await();
		} catch (InterruptedException e) {
		}
   		WaarpSslUtility.closingSslChannel(channel);
		logger.debug("Channel closed");
    }
    
    protected static RestFuture deleteData(Channel channel, RESTHANDLERS data) throws HttpInvalidAuthenticationException {
		logger.debug("Send query");
        AbstractDbData dbData = getItem(data);
        if (dbData == null) {
			RestFuture future = (RestFuture) channel.getAttachment();
			future.cancel();
        	WaarpSslUtility.closingSslChannel(channel);
            return future;
        }
        DataModelRestMethodHandler<?> handler = (DataModelRestMethodHandler<?>) data.getRestMethodHandler();
        String item = dbData.getJson().path(handler.getPrimaryPropertyName()).asText();
        Entry<String, String> id = HttpRestR66Handler.falseRepoPassword.entrySet().iterator().next();
        Map<String, String> args = null;
        if (dbData instanceof DbTaskRunner) {
        	args = new HashMap<String, String>();
        	args.put(DbTaskRunner.Columns.REQUESTER.name(), hostid);
        	args.put(DbTaskRunner.Columns.REQUESTED.name(), hostid);
        	args.put(Columns.OWNERREQ.name(), Configuration.configuration.HOST_ID);
        }
        RestFuture future = clientHelper.sendQuery(channel, HttpMethod.DELETE, host, data.uri+"/"+item, id.getKey(), id.getValue(),
        		args,
        		null);
		logger.debug("Query sent");
        return future;
    }
    
    protected static void multiDataRequests(RESTHANDLERS data) throws HttpInvalidAuthenticationException {
    	Channel channel = clientHelper.getChannel(host, port);
        if (channel == null) {
        	return;
        }
        RestFuture future = createData(channel, data);
    	try {
			future.await();
		} catch (InterruptedException e) {
		}
   		WaarpSslUtility.closingSslChannel(channel);
		logger.debug("Channel closed");
    }
    
    protected static RestFuture createData(Channel channel, RESTHANDLERS data) throws HttpInvalidAuthenticationException {
		logger.debug("Send query");
        AbstractDbData dbData = getItem(data);
        if (dbData == null) {
			RestFuture future = (RestFuture) channel.getAttachment();
			future.cancel();
        	WaarpSslUtility.closingSslChannel(channel);
            return future;
        }
        Entry<String, String> id = HttpRestR66Handler.falseRepoPassword.entrySet().iterator().next();
        Map<String, String> args = null;
        RestFuture future = clientHelper.sendQuery(channel, HttpMethod.POST, host, data.uri, id.getKey(), id.getValue(),
        		args,
        		dbData.asJson());
		logger.debug("Query sent");
        return future;
    }

    protected static RestFuture readData(Channel channel, RestArgument arg) throws HttpInvalidAuthenticationException {
		logger.debug("Send query");
        String base = arg.getBaseUri();
        String item = clientHelper.getPrimaryProperty(arg);
        Entry<String, String> id = HttpRestR66Handler.falseRepoPassword.entrySet().iterator().next();
        Map<String, String> args = null;
        if (base.equals(HttpRestR66Handler.RESTHANDLERS.DbTaskRunner.uri)) {
        	args = new HashMap<String, String>();
        	args.put(DbTaskRunner.Columns.REQUESTER.name(), hostid);
        	args.put(DbTaskRunner.Columns.REQUESTED.name(), hostid);
        	args.put(Columns.OWNERREQ.name(), Configuration.configuration.HOST_ID);
        }
        RestFuture future = clientHelper.sendQuery(channel, HttpMethod.GET, host, base+"/"+item, id.getKey(), id.getValue(),
        		args,
        		null);
		logger.debug("Query sent");
        return future;
    }
    
    protected static RestFuture updateData(Channel channel, RestArgument arg) throws HttpInvalidAuthenticationException {
		logger.debug("Send query");
    	ObjectNode answer = arg.getAnswer();
        String base = arg.getBaseUri();
        String item = clientHelper.getPrimaryProperty(arg);
        RESTHANDLERS dbdata = clientHelper.getRestHandler(arg);
        Entry<String, String> id = HttpRestR66Handler.falseRepoPassword.entrySet().iterator().next();
        Map<String, String> args = null;
        if (base.equals(HttpRestR66Handler.RESTHANDLERS.DbTaskRunner.uri)) {
        	args = new HashMap<String, String>();
        	args.put(DbTaskRunner.Columns.REQUESTER.name(), hostid);
        	args.put(DbTaskRunner.Columns.REQUESTED.name(), hostid);
        	args.put(Columns.OWNERREQ.name(), Configuration.configuration.HOST_ID);
        }
        // update
        answer.removeAll();
        switch (dbdata) {
			case DbConfiguration:
				answer.put(DbConfiguration.Columns.READGLOBALLIMIT.name(), 0);
				break;
			case DbHostAuth:
				answer.put(DbHostAuth.Columns.PORT.name(), 100);
				break;
			case DbHostConfiguration:
				answer.put(DbHostConfiguration.Columns.OTHERS.name(), "");
				break;
			case DbRule:
				answer.put(DbRule.Columns.MODETRANS.name(), 4);
				break;
			case DbTaskRunner:
				answer.put(DbTaskRunner.Columns.FILEINFO.name(), "New Fileinfo");
				break;
			default:
				RestFuture future = (RestFuture) channel.getAttachment();
				future.cancel();
	        	WaarpSslUtility.closingSslChannel(channel);
	            return future;
        }
        RestFuture future = clientHelper.sendQuery(channel, HttpMethod.PUT, host, base+"/"+item, id.getKey(), id.getValue(),
        		args,
        		JsonHandler.writeAsString(answer));
		logger.debug("Query sent");
        return future;
    }
    
    protected static RestFuture deleteData(Channel channel, RestArgument arg) throws HttpInvalidAuthenticationException {
		logger.debug("Send query");
        String base = arg.getBaseUri();
        String item = clientHelper.getPrimaryProperty(arg);
        Entry<String, String> id = HttpRestR66Handler.falseRepoPassword.entrySet().iterator().next();
        Map<String, String> args = null;
        if (base.equals(HttpRestR66Handler.RESTHANDLERS.DbTaskRunner.uri)) {
        	args = new HashMap<String, String>();
        	args.put(DbTaskRunner.Columns.REQUESTER.name(), hostid);
        	args.put(DbTaskRunner.Columns.REQUESTED.name(), hostid);
        	args.put(Columns.OWNERREQ.name(), Configuration.configuration.HOST_ID);
        }
        RestFuture future = clientHelper.sendQuery(channel, HttpMethod.DELETE, host, base+"/"+item, id.getKey(), id.getValue(),
        		args,
        		null);
		logger.debug("Query sent");
        return future;
    }

    protected static RestFuture deleteData(Channel channel, String reqd, String reqr, long specid) throws HttpInvalidAuthenticationException {
		logger.debug("Send query");
        Entry<String, String> id = HttpRestR66Handler.falseRepoPassword.entrySet().iterator().next();
        Map<String, String> args = new HashMap<String, String>();
    	args.put(DbTaskRunner.Columns.REQUESTER.name(), reqr);
    	args.put(DbTaskRunner.Columns.REQUESTED.name(), reqd);
        RestFuture future = clientHelper.sendQuery(channel, HttpMethod.DELETE, host, RESTHANDLERS.DbTaskRunner.uri+"/"+specid, id.getKey(), id.getValue(),
        		args,
        		null);
		logger.debug("Query sent");
        return future;
    }
    
    protected static void action(RESTHANDLERS data) throws HttpInvalidAuthenticationException {
    	Channel channel = clientHelper.getChannel(host, port);
        if (channel == null) {
        	return;
        }
        RestFuture future = action(channel, data);
    	try {
			future.await();
		} catch (InterruptedException e) {
		}
   		WaarpSslUtility.closingSslChannel(channel);
		logger.debug("Channel closed");
    }
    
    protected static RestFuture action(Channel channel, RESTHANDLERS data) throws HttpInvalidAuthenticationException {
		logger.warn("Send query: "+data);
		JsonPacket packet = null;
		HttpMethod method = null;
		switch (data) {
			case Bandwidth: {
				BandwidthJsonPacket node = new BandwidthJsonPacket();
				node.setReadglobal(0);
				node.setReadsession(0);
				node.setSetter(true);
				node.setRequestUserPacket();
				packet = node;
				method = HttpMethod.PUT;
				break;
			}
			case Business: {
				BusinessRequestJsonPacket node = new BusinessRequestJsonPacket();
				node.setClassName(TestExecJavaTask.class.getName());
				node.setArguments("business 100 other arguments 0");
				node.setRequestUserPacket();
				node.setToApplied(true);
				packet = node;
				method = HttpMethod.GET;
				break;
			}
			case Config: {
				ConfigExportJsonPacket node = new ConfigExportJsonPacket();
				node.setHost(true);
				node.setRule(true);
				node.setBusiness(true);
				node.setAlias(true);
				node.setRoles(true);
				node.setRequestUserPacket();
				packet = node;
				method = HttpMethod.GET;
				break;
			}
			case Information: {
				InformationJsonPacket node = new InformationJsonPacket();
				node.setFilename("test*");
				node.setIdRequest(false);
				node.setRequest((byte) InformationPacket.ASKENUM.ASKLIST.ordinal());
				node.setRequestUserPacket();
				node.setRulename("rule4");
				packet = node;
				method = HttpMethod.GET;
				break;
			}
			case Log: {
				LogJsonPacket node = new LogJsonPacket();
				node.setStop(new Date());
				node.setRequestUserPacket();
				packet = node;
				method = HttpMethod.GET;
				break;
			}
			case Server: {
				ShutdownOrBlockJsonPacket node = new ShutdownOrBlockJsonPacket();
				node.setRestartOrBlock(true);
				node.setShutdownOrBlock(false);
	            String pwd = "pwdhttp";
	            byte [] bpwd = FilesystemBasedDigest.passwdCrypt(pwd.getBytes(WaarpStringUtils.UTF8));
	            node.setKey(bpwd);
				node.setRequestUserPacket(LocalPacketFactory.BLOCKREQUESTPACKET);
				packet = node;
				method = HttpMethod.PUT;
				break;
			}
			case Control: {
				TransferRequestJsonPacket node = new TransferRequestJsonPacket();
				node.setRequestUserPacket();
				node.setRulename("rule4");
				node.setRank(0);
				node.setBlocksize(65536);
				node.setFileInformation("file info");
				node.setFilename("filename");
				node.setMode(2);
				node.setSpecialId(DbConstant.ILLEGALVALUE);
				node.setRequested(hostid);
				node.setStart(new Date());
				node.setOriginalSize(123L);
				node.setRequestUserPacket();
				packet = node;
				method = HttpMethod.POST;
				break;
			}
			default:
				break;
		}
        return action(channel, method, data.uri, packet);
    }
    
    protected static RestFuture action(Channel channel, HttpMethod method, String uri, JsonPacket packet) {
        if (packet == null) {
			RestFuture future = (RestFuture) channel.getAttachment();
			future.cancel();
        	WaarpSslUtility.closingSslChannel(channel);
            return future;
        }
        Entry<String, String> id = HttpRestR66Handler.falseRepoPassword.entrySet().iterator().next();
        RestFuture future = clientHelper.sendQuery(channel, method, host, uri, id.getKey(), id.getValue(),
        		null,
        		packet.toString());
		logger.debug("Query sent");
        return future;
    }

	@Override
	public void run() {
        for (int i = 0; i < NBPERTHREAD; i ++) {
            try {
            	multiDataRequests(RESTHANDLERS.DbTaskRunner);
			} catch (HttpInvalidAuthenticationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        }
	}

}
