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
package org.waarp.openr66.protocol.rest.packet.test;

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
import org.waarp.common.crypto.ssl.WaarpSslUtility;
import org.waarp.common.exception.CryptoException;
import org.waarp.common.json.JsonHandler;
import org.waarp.common.logging.WaarpSlf4JLoggerFactory;
import org.waarp.gateway.kernel.exception.HttpInvalidAuthenticationException;
import org.waarp.gateway.kernel.rest.HttpRestHandler;
import org.waarp.gateway.kernel.rest.RestArgument;
import org.waarp.openr66.protocol.configuration.Configuration;
import org.waarp.openr66.protocol.localhandler.packet.json.JsonPacket;
import org.waarp.openr66.protocol.localhandler.rest.HttpRestR66Handler;


/**
 * @author "Frederic Bregier"
 *
 */
public class HttpRestR66TestClient  implements Runnable {
	public static int NB = 2;

    public static int NBPERTHREAD = 100;
	private static final String baseURI = "/";
	private static JsonPacket packet = null;
	private static final String host = "127.0.0.1";
	private static final int port = 8080;
	private static ClientBootstrap bootstrap = null;
	static URI uriSimple;
	public static boolean init = true;
	public static boolean isGet = true;
	private static List<Entry<String, String>> headers = null;

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
        /*count.set(0);
    	long start = System.currentTimeMillis();
        launchThreads();
    	long stop = System.currentTimeMillis();
        try {
			Thread.sleep(100);
		} catch (InterruptedException e1) {
		}
    	System.out.println("Options: "+NBPERTHREAD*1000/(stop-start)+" req/s "+NBPERTHREAD*NB+"=?"+count.get());*/
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
        request.headers().set(HttpHeaders.Names.CONNECTION,
                HttpHeaders.Values.CLOSE);
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
        // XXX /formget
        // No use of HttpPostRequestEncoder since not a POST
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
        encoder.addParam(RestArgument.ARG_X_AUTH_USER, id.getKey());
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
    
    protected void formget() {
    	
    }
    
    protected void formpost() {
    	
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
