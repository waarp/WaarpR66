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
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.codec.http.CookieEncoder;
import org.jboss.netty.handler.codec.http.DefaultHttpRequest;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.jboss.netty.handler.codec.http.QueryStringEncoder;
import org.jboss.netty.logging.InternalLoggerFactory;
import org.joda.time.DateTime;
import org.waarp.common.crypto.ssl.WaarpSslUtility;
import org.waarp.common.database.data.AbstractDbData;
import org.waarp.common.database.exception.WaarpDatabaseException;
import org.waarp.common.exception.CryptoException;
import org.waarp.common.json.JsonHandler;
import org.waarp.common.logging.WaarpSlf4JLoggerFactory;
import org.waarp.common.utility.WaarpStringUtils;
import org.waarp.gateway.kernel.exception.HttpInvalidAuthenticationException;
import org.waarp.gateway.kernel.rest.DataModelRestMethodHandler;
import org.waarp.gateway.kernel.rest.HttpRestHandler;
import org.waarp.gateway.kernel.rest.RestArgument;
import org.waarp.openr66.context.ErrorCode;
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
import org.waarp.openr66.protocol.http.rest.handler.DbConfigurationR66RestMethodHandler;
import org.waarp.openr66.protocol.http.rest.handler.DbHostAuthR66RestMethodHandler;
import org.waarp.openr66.protocol.http.rest.handler.DbHostConfigurationR66RestMethodHandler;
import org.waarp.openr66.protocol.http.rest.handler.DbRuleR66RestMethodHandler;
import org.waarp.openr66.protocol.http.rest.handler.DbTaskRunnerR66RestMethodHandler;

import com.fasterxml.jackson.databind.node.ObjectNode;


/**
 * @author "Frederic Bregier"
 *
 */
public class HttpRestR66TestClient  implements Runnable {
	public static int NB = 2;

    public static int NBPERTHREAD = 100;
	private static final String baseURI = "/";
	private static final String host = "127.0.0.1";
	private static final int port = 8080;
	private static ClientBootstrap bootstrap = null;
	static URI uriSimple;
	private static List<Entry<String, String>> headers = null;

	public static boolean DEBUG = false;
	
	public static AtomicLong count = new AtomicLong();
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		InternalLoggerFactory.setDefaultFactory(new WaarpSlf4JLoggerFactory(null));
		// TODO Auto-generated method stub
		try {
            uriSimple = new URI(baseURI);
        } catch (URISyntaxException e) {
            System.err.println("Error: " + e.getMessage());
            return;
        }
		if (args.length > 0) {
			NB = Integer.parseInt(args[0]);
			if (Configuration.configuration.CLIENT_THREAD < NB) {
				Configuration.configuration.CLIENT_THREAD = NB+1;
			}
			if (args.length > 1) {
				NBPERTHREAD = Integer.parseInt(args[1]);
			}
		}
		// Configure the client.
        ExecutorService executorServiceBoss = Executors.newCachedThreadPool();
        ExecutorService executorServiceWorker = Executors.newCachedThreadPool();
        bootstrap = new ClientBootstrap(new NioClientSocketChannelFactory(
                executorServiceBoss,
                executorServiceWorker,
                Configuration.configuration.CLIENT_THREAD));
        // Set up the event pipeline factory.
        bootstrap.setPipelineFactory(new HttpClientPipelineFactory(null));
        try {
			initHeader();
		} catch (CryptoException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        try {
			Thread.sleep(100);
		} catch (InterruptedException e1) {
		}
		if (NB == 1 && NBPERTHREAD == 1) {
			DEBUG = true;
		}
        try {
        	long start = System.currentTimeMillis();
        	for (int i = 0; i < NBPERTHREAD; i++) {
        		options(null);
        	}
        	long stop = System.currentTimeMillis();
            try {
    			Thread.sleep(100);
    		} catch (InterruptedException e1) {
    		}
        	System.out.println("Options: "+count.get()*1000/(stop-start)+" req/s "+NBPERTHREAD+"=?"+count.get());
		} catch (HttpInvalidAuthenticationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		count.set(0);
    	long start = System.currentTimeMillis();
        for (RESTHANDLERS handler : HttpRestR66Handler.RESTHANDLERS.values()) {
	        try {
        		deleteData(handler);
	            try {
	    			Thread.sleep(100);
	    		} catch (InterruptedException e1) {
	    		}
			} catch (HttpInvalidAuthenticationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        }
    	long stop = System.currentTimeMillis();
    	System.out.println("Delete: "+count.get()*1000/(stop-start)+" req/s "+NBPERTHREAD+"=?"+count.get());
        count.set(0);
    	start = System.currentTimeMillis();
        for (RESTHANDLERS handler : HttpRestR66Handler.RESTHANDLERS.values()) {
	        try {
        		dataAccess(handler);
	            try {
	    			Thread.sleep(100);
	    		} catch (InterruptedException e1) {
	    		}
			} catch (HttpInvalidAuthenticationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        }
    	stop = System.currentTimeMillis();
    	System.out.println("Create: "+count.get()*1000/(stop-start)+" req/s "+NBPERTHREAD+"=?"+count.get());
        count.set(0);
    	start = System.currentTimeMillis();
        for (RESTHANDLERS handler : HttpRestR66Handler.RESTHANDLERS.values()) {
	        try {
        		realAllData(handler);
	            try {
	    			Thread.sleep(100);
	    		} catch (InterruptedException e1) {
	    		}
			} catch (HttpInvalidAuthenticationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        }
    	stop = System.currentTimeMillis();
    	System.out.println("ReadAll: "+count.get()*1000/(stop-start)+" req/s "+NBPERTHREAD+"=?"+count.get());
        /*count.set(0);
    	long start = System.currentTimeMillis();
        launchThreads();
    	long stop = System.currentTimeMillis();
        try {
			Thread.sleep(100);
		} catch (InterruptedException e1) {
		}
    	System.out.println("Options: "+count.get()*1000/(stop-start)+" req/s "+NBPERTHREAD*NB+"=?"+count.get());*/
        /*
        formget();
        formpost();
        Date date0 = new Date();
        isGet = true;
        launchThreads();
        Date date1 = new Date();
        isGet = false;
        launchThreads();
        Date date2 = new Date();
        System.err.println("Get: " + (NB * NBPERTHREAD * 1000) /
                ((double) (date1.getTime() - date0.getTime())));
        System.err.println("Post: " + (NB * NBPERTHREAD * 1000) /
                ((double) (date2.getTime() - date1.getTime())));
        */
        try {
			Thread.sleep(100);
		} catch (InterruptedException e1) {
		}
        bootstrap.releaseExternalResources();
	}

	public static void launchThreads() {
        // init thread model
        ExecutorService pool = Executors.newFixedThreadPool(NB);
        HttpRestR66TestClient[] clients = new HttpRestR66TestClient[NB];
        for (int i = 0; i < NB; i ++) {
            clients[i] = new HttpRestR66TestClient();
        }
        for (int i = 0; i < NB; i ++) {
            pool.execute(clients[i]);
        }
        pool.shutdown();
        try {
            pool.awaitTermination(100000, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    /**
     * @return the list of headers that will be used in every example after
     * @throws IOException 
     * @throws CryptoException 
     * 
     */
    private static void initHeader() throws CryptoException, IOException {
        // will ignore real request
        HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1,
                HttpMethod.GET, baseURI);
        request.headers().set(HttpHeaders.Names.HOST, host);
        /*request.headers().set(HttpHeaders.Names.CONNECTION,
                HttpHeaders.Values.KEEP_ALIVE);*/
        request.headers().set(HttpHeaders.Names.ACCEPT_ENCODING,
                HttpHeaders.Values.GZIP + "," + HttpHeaders.Values.DEFLATE);

        request.headers().set(HttpHeaders.Names.ACCEPT_CHARSET,
                "utf-8;q=0.7,*;q=0.7");
        request.headers().set(HttpHeaders.Names.ACCEPT_LANGUAGE, "fr,en");
        request.headers().set(HttpHeaders.Names.REFERER, uriSimple.toString());
        request.headers().set(HttpHeaders.Names.USER_AGENT,
                "Netty Simple Http Client side");
        request.headers().set(HttpHeaders.Names.ACCEPT,
                "text/html,text/plain,application/xhtml+xml,application/xml,application/json;q=0.9,*/*;q=0.8");
        // connection will not close but needed
        // request.setHeader("Connection","keep-alive");
        // request.setHeader("Keep-Alive","300");

        CookieEncoder httpCookieEncoder = new CookieEncoder(false);
        httpCookieEncoder.addCookie("my-cookie", "foo");
        httpCookieEncoder.addCookie("another-cookie", "bar");
        request.headers().set(HttpHeaders.Names.COOKIE, httpCookieEncoder.encode());

        headers = request.headers().entries();

        String pathTemp = "J:/Temp/temp";
        HttpRestHandler.initialize(pathTemp, new File("J:/Temp/temp/key.sha256"));
    }

    public static void options(String uri) throws HttpInvalidAuthenticationException {
        // Start the connection attempt.
        ChannelFuture future = bootstrap.connect(new InetSocketAddress(host,
                port));
        // Wait until the connection attempt succeeds or fails.
        Channel channel = WaarpSslUtility.waitforChannelReady(future);
        if (channel == null) {
        	return;
        }
        options(channel, uri);
        // Wait for the server to close the connection.
        if (! WaarpSslUtility.waitForClosingSslChannel(channel, 20000)) {
        	System.err.println("\nBad closing: ");
        }
    }

    protected static void options(Channel channel, String uri) throws HttpInvalidAuthenticationException {
        // Prepare the HTTP request.
        QueryStringEncoder encoder = null;
        if (uri != null) {
        	encoder = new QueryStringEncoder(baseURI+uri);
        } else {
        	encoder = new QueryStringEncoder(baseURI);
        }
        // add Form attribute
        Entry<String, String> id = HttpRestR66Handler.falseRepoPassword.entrySet().iterator().next();
        encoder.addParam(RestArgument.REST_ROOT_FIELD.ARG_X_AUTH_USER.field, id.getKey());
        RestArgument.getBaseAuthent(encoder, id.getValue());
        URI uriGet;
		try {
			uriGet = encoder.toUri();
		} catch (URISyntaxException e) {
            System.err.println("Error: " +e.getMessage());
            //bootstrap.releaseExternalResources();
            return;
        }

        HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1,
                HttpMethod.OPTIONS, uriGet.toASCIIString());
        // it is legal to add directly header or cookie into the request until
        // finalize
        for (Entry<String, String> entry: headers) {
            request.headers().set(entry.getKey(), entry.getValue());
        }
        // send request
        try {
			channel.write(request).await();
		} catch (InterruptedException e) {
		}
    }
    
    protected static void dataAccess(RESTHANDLERS data) throws HttpInvalidAuthenticationException {
    	Configuration.configuration.HOST_ID = hostid;
        // Start the connection attempt.
        ChannelFuture future = bootstrap.connect(new InetSocketAddress(host,
                port));
        // Wait until the connection attempt succeeds or fails.
        Channel channel = WaarpSslUtility.waitforChannelReady(future);
        if (channel == null) {
        	return;
        }
        createData(channel, data);
        // Wait for the server to close the connection.
        if (! WaarpSslUtility.waitForClosingSslChannel(channel, 20000)) {
        	System.err.println("Bad closing");
        }
    }
    
    public static long limit = 10000000;
    public static long delaylimit = 5000;
    public static String hostid = "hostZZ";
    public static String address = "10.10.10.10";
    public static String key = "ABCDEFGH";
    public static String business = "<business><businessid>hostas</businessid></business>";
    public static String roles = "<roles><role><roleid>hostas</roleid><roleset>FULLADMIN</roleset></role></roles>";
    public static String aliases = "<aliases><alias><realid>hostZZ</realid><aliasid>hostZZ2</aliasid></alias></aliases>";
    public static String others = "<root><version>2.4.25</version></root>";
    public static String idRule = "ruleZZ";
    public static String ids = "<hostids><hostid>hosta</hostid><hostid>hostZZ</hostid></hostids>";
    public static String tasks = "<tasks><task><type>LOG</type><path>log</path><delay>0</delay><comment></comment></task></tasks>";
    
    protected static AbstractDbData getItem(RESTHANDLERS data) throws HttpInvalidAuthenticationException {
    	switch (data) {
			case Bandwidth:
				break;
			case Business:
				break;
			case Config:
				break;
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
				/// FIXME
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
			case Information:
				break;
			case Log:
				break;
			case Server:
				break;
			case Transfer:
				break;
			default:
				break;
    		
    	}
    	// XXX FIXME
    	return null;
    }
    protected static void realAllData(RESTHANDLERS data) throws HttpInvalidAuthenticationException {
    	Configuration.configuration.HOST_ID = hostid;
        // Start the connection attempt.
        ChannelFuture future = bootstrap.connect(new InetSocketAddress(host,
                port));
        // Wait until the connection attempt succeeds or fails.
        Channel channel = WaarpSslUtility.waitforChannelReady(future);
        if (channel == null) {
        	return;
        }
        realAllData(channel, data);
        // Wait for the server to close the connection.
        if (! WaarpSslUtility.waitForClosingSslChannel(channel, 20000)) {
        	System.err.println("Bad closing");
        }
    }
    protected static void realAllData(Channel channel, RESTHANDLERS data) throws HttpInvalidAuthenticationException {
        // Prepare the HTTP request.
        QueryStringEncoder encoder = null;
       	encoder = new QueryStringEncoder(baseURI+data.uri);
        // add Form attribute
        Entry<String, String> id = HttpRestR66Handler.falseRepoPassword.entrySet().iterator().next();
        encoder.addParam(RestArgument.REST_ROOT_FIELD.ARG_X_AUTH_USER.field, id.getKey());
        RestArgument.getBaseAuthent(encoder, id.getValue());
        URI uriGet;
		try {
			uriGet = encoder.toUri();
		} catch (URISyntaxException e) {
            System.err.println("Error: " +e.getMessage());
        	WaarpSslUtility.closingSslChannel(channel);
            return;
        }
        HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1,
                HttpMethod.GET, uriGet.toASCIIString());
        // it is legal to add directly header or cookie into the request until
        // finalize
        for (Entry<String, String> entry: headers) {
            request.headers().set(entry.getKey(), entry.getValue());
        }
        ObjectNode answer = JsonHandler.createObjectNode();
        switch (data) {
			case DbConfiguration:
				answer.put(DbConfigurationR66RestMethodHandler.FILTER_ARGS.BANDWIDTH.name(), 1);
				break;
			case DbHostAuth:
				answer.put(DbHostAuthR66RestMethodHandler.FILTER_ARGS.ISSSL.name(), false);
				break;
			case DbHostConfiguration:
				answer.put(DbHostConfigurationR66RestMethodHandler.FILTER_ARGS.BUSINESS.name(), "hosta");
				break;
			case DbRule:
				answer.put(DbRuleR66RestMethodHandler.FILTER_ARGS.MODETRANS.name(), 4);
				break;
			case DbTaskRunner:
				answer.put(DbTaskRunnerR66RestMethodHandler.FILTER_ARGS.STOPTRANS.name(), new DateTime().toString());
				break;
			default:
	        	WaarpSslUtility.closingSslChannel(channel);
	            return;
        }
        ChannelBuffer buffer = ChannelBuffers.wrappedBuffer(JsonHandler.writeAsString(answer).getBytes(WaarpStringUtils.UTF8));
        request.setContent(buffer);
        request.headers().set(HttpHeaders.Names.CONTENT_LENGTH, buffer.readableBytes());
        // send request
        try {
			channel.write(request).await();
		} catch (InterruptedException e) {
		}
    }

    protected static void deleteData(RESTHANDLERS data) throws HttpInvalidAuthenticationException {
    	Configuration.configuration.HOST_ID = hostid;
        // Start the connection attempt.
        ChannelFuture future = bootstrap.connect(new InetSocketAddress(host,
                port));
        // Wait until the connection attempt succeeds or fails.
        Channel channel = WaarpSslUtility.waitforChannelReady(future);
        if (channel == null) {
        	return;
        }
        deleteData(channel, data);
        // Wait for the server to close the connection.
        if (! WaarpSslUtility.waitForClosingSslChannel(channel, 20000)) {
        	System.err.println("Bad closing");
        }
    }
    
    protected static void deleteData(Channel channel, RESTHANDLERS data) throws HttpInvalidAuthenticationException {
        // Prepare the HTTP request.
        QueryStringEncoder encoder = null;
        AbstractDbData dbData = getItem(data);
        if (dbData == null) {
        	WaarpSslUtility.closingSslChannel(channel);
            //bootstrap.releaseExternalResources();
            return;
        }
        DataModelRestMethodHandler<?> handler = (DataModelRestMethodHandler<?>) data.getRestMethodHandler();
        String item = dbData.getJson().path(handler.getPrimaryPropertyName()).asText();
       	encoder = new QueryStringEncoder(baseURI+data.uri+"/"+item);
        // add Form attribute
        Entry<String, String> id = HttpRestR66Handler.falseRepoPassword.entrySet().iterator().next();
        encoder.addParam(RestArgument.REST_ROOT_FIELD.ARG_X_AUTH_USER.field, id.getKey());
        if (dbData instanceof DbTaskRunner) {
        	encoder.addParam(DbTaskRunner.Columns.REQUESTER.name(), hostid);
        	encoder.addParam(DbTaskRunner.Columns.REQUESTED.name(), hostid);
        }
        RestArgument.getBaseAuthent(encoder, id.getValue());
        URI uriGet;
		try {
			uriGet = encoder.toUri();
		} catch (URISyntaxException e) {
            System.err.println("Error: " +e.getMessage());
        	WaarpSslUtility.closingSslChannel(channel);
            return;
        }
        HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1,
                HttpMethod.DELETE, uriGet.toASCIIString());
        // it is legal to add directly header or cookie into the request until
        // finalize
        for (Entry<String, String> entry: headers) {
            request.headers().set(entry.getKey(), entry.getValue());
        }
        ChannelBuffer buffer = ChannelBuffers.wrappedBuffer(dbData.asJson().getBytes(WaarpStringUtils.UTF8));
        request.setContent(buffer);
        request.headers().set(HttpHeaders.Names.CONTENT_LENGTH, buffer.readableBytes());
        // send request
        try {
			channel.write(request).await();
		} catch (InterruptedException e) {
		}
    }
    
    protected static void createData(Channel channel, RESTHANDLERS data) throws HttpInvalidAuthenticationException {
        // Prepare the HTTP request.
        QueryStringEncoder encoder = null;
        AbstractDbData dbData = getItem(data);
        if (dbData == null) {
        	WaarpSslUtility.closingSslChannel(channel);
            return;
        }
       	encoder = new QueryStringEncoder(baseURI+data.uri);
        // add Form attribute
        Entry<String, String> id = HttpRestR66Handler.falseRepoPassword.entrySet().iterator().next();
        encoder.addParam(RestArgument.REST_ROOT_FIELD.ARG_X_AUTH_USER.field, id.getKey());
        RestArgument.getBaseAuthent(encoder, id.getValue());
        URI uriGet;
		try {
			uriGet = encoder.toUri();
		} catch (URISyntaxException e) {
            System.err.println("Error: " +e.getMessage());
        	WaarpSslUtility.closingSslChannel(channel);
            return;
        }
        HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1,
                HttpMethod.POST, uriGet.toASCIIString());
        // it is legal to add directly header or cookie into the request until
        // finalize
        for (Entry<String, String> entry: headers) {
            request.headers().set(entry.getKey(), entry.getValue());
        }
        ChannelBuffer buffer = ChannelBuffers.wrappedBuffer(dbData.asJson().getBytes(WaarpStringUtils.UTF8));
        request.setContent(buffer);
        request.headers().set(HttpHeaders.Names.CONTENT_LENGTH, buffer.readableBytes());
        // send request
        try {
			channel.write(request).await();
		} catch (InterruptedException e) {
		}
    }

    protected static void readData(Channel channel, RestArgument arg) throws HttpInvalidAuthenticationException {
        // Prepare the HTTP request.
    	ObjectNode answer = arg.getAnswer();
        QueryStringEncoder encoder = null;
        String base = arg.getBaseUri();
        DataModelRestMethodHandler<?> handler = (DataModelRestMethodHandler<?>) HttpRestR66Handler.restHashMap.get(base);
        String item = answer.path(handler.getPrimaryPropertyName()).asText();
       	encoder = new QueryStringEncoder(baseURI+base+"/"+item);
        // add Form attribute
        Entry<String, String> id = HttpRestR66Handler.falseRepoPassword.entrySet().iterator().next();
        encoder.addParam(RestArgument.REST_ROOT_FIELD.ARG_X_AUTH_USER.field, id.getKey());
        if (base.equals(HttpRestR66Handler.RESTHANDLERS.DbTaskRunner.uri)) {
        	encoder.addParam(DbTaskRunner.Columns.REQUESTER.name(), hostid);
        	encoder.addParam(DbTaskRunner.Columns.REQUESTED.name(), hostid);
        }
        RestArgument.getBaseAuthent(encoder, id.getValue());
        URI uriGet;
		try {
			uriGet = encoder.toUri();
		} catch (URISyntaxException e) {
            System.err.println("Error: " +e.getMessage());
        	WaarpSslUtility.closingSslChannel(channel);
            return;
        }
		System.out.println(uriGet.toASCIIString());
        HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1,
                HttpMethod.GET, uriGet.toASCIIString());
        // it is legal to add directly header or cookie into the request until
        // finalize
        for (Entry<String, String> entry: headers) {
            request.headers().set(entry.getKey(), entry.getValue());
        }
        // send request
        try {
			channel.write(request).await();
		} catch (InterruptedException e) {
		}
    }
    protected static void updateData(Channel channel, RestArgument arg) throws HttpInvalidAuthenticationException {
        // Prepare the HTTP request.
    	ObjectNode answer = arg.getAnswer();
        QueryStringEncoder encoder = null;
        String base = arg.getBaseUri();
        String className = answer.get(AbstractDbData.JSON_MODEL).asText();
        RESTHANDLERS dbdata = RESTHANDLERS.valueOf(className);
        DataModelRestMethodHandler<?> handler = (DataModelRestMethodHandler<?>) dbdata.getRestMethodHandler();
        String item = answer.path(handler.getPrimaryPropertyName()).asText();
       	encoder = new QueryStringEncoder(baseURI+base+"/"+item);
        // add Form attribute
        Entry<String, String> id = HttpRestR66Handler.falseRepoPassword.entrySet().iterator().next();
        encoder.addParam(RestArgument.REST_ROOT_FIELD.ARG_X_AUTH_USER.field, id.getKey());
        if (base.equals(HttpRestR66Handler.RESTHANDLERS.DbTaskRunner.uri)) {
        	encoder.addParam(DbTaskRunner.Columns.REQUESTER.name(), hostid);
        	encoder.addParam(DbTaskRunner.Columns.REQUESTED.name(), hostid);
        }
        RestArgument.getBaseAuthent(encoder, id.getValue());
        URI uriGet;
		try {
			uriGet = encoder.toUri();
		} catch (URISyntaxException e) {
            System.err.println("Error: " +e.getMessage());
        	WaarpSslUtility.closingSslChannel(channel);
            return;
        }
        HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1,
                HttpMethod.PUT, uriGet.toASCIIString());
        // it is legal to add directly header or cookie into the request until
        // finalize
        for (Entry<String, String> entry: headers) {
            request.headers().set(entry.getKey(), entry.getValue());
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
	        	WaarpSslUtility.closingSslChannel(channel);
	            return;
        }
        ChannelBuffer buffer = ChannelBuffers.wrappedBuffer(JsonHandler.writeAsString(answer).getBytes(WaarpStringUtils.UTF8));
        request.setContent(buffer);
        request.headers().set(HttpHeaders.Names.CONTENT_LENGTH, buffer.readableBytes());
        // send request
        try {
			channel.write(request).await();
		} catch (InterruptedException e) {
		}
    }
    protected static void deleteData(Channel channel, RestArgument arg) throws HttpInvalidAuthenticationException {
        // Prepare the HTTP request.
    	ObjectNode answer = arg.getAnswer();
        QueryStringEncoder encoder = null;
        String base = arg.getBaseUri();
        String className = answer.get(AbstractDbData.JSON_MODEL).asText();
        RESTHANDLERS dbdata = RESTHANDLERS.valueOf(className);
        DataModelRestMethodHandler<?> handler = (DataModelRestMethodHandler<?>) dbdata.getRestMethodHandler();
        String item = answer.path(handler.getPrimaryPropertyName()).asText();
       	encoder = new QueryStringEncoder(baseURI+base+"/"+item);
        // add Form attribute
        Entry<String, String> id = HttpRestR66Handler.falseRepoPassword.entrySet().iterator().next();
        encoder.addParam(RestArgument.REST_ROOT_FIELD.ARG_X_AUTH_USER.field, id.getKey());
        if (base.equals(HttpRestR66Handler.RESTHANDLERS.DbTaskRunner.uri)) {
        	encoder.addParam(DbTaskRunner.Columns.REQUESTER.name(), hostid);
        	encoder.addParam(DbTaskRunner.Columns.REQUESTED.name(), hostid);
        }
        RestArgument.getBaseAuthent(encoder, id.getValue());
        URI uriGet;
		try {
			uriGet = encoder.toUri();
		} catch (URISyntaxException e) {
            System.err.println("Error: " +e.getMessage());
        	WaarpSslUtility.closingSslChannel(channel);
            return;
        }
        HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1,
                HttpMethod.DELETE, uriGet.toASCIIString());
        // it is legal to add directly header or cookie into the request until
        // finalize
        for (Entry<String, String> entry: headers) {
            request.headers().set(entry.getKey(), entry.getValue());
        }
        // send request
        try {
			channel.write(request).await();
		} catch (InterruptedException e) {
		}
    }
    
    protected void action(String uri) {
    	
    }

	@Override
	public void run() {
        for (int i = 0; i < NBPERTHREAD; i ++) {
            try {
				options(null);
			} catch (HttpInvalidAuthenticationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        }
	}

}
