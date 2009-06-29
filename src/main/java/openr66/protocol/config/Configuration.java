/**
 * Copyright 2009, Frederic Bregier, and individual contributors by the @author
 * tags. See the COPYRIGHT.txt in the distribution for a full listing of
 * individual contributors. This is free software; you can redistribute it
 * and/or modify it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 3.0 of the License,
 * or (at your option) any later version. This software is distributed in the
 * hope that it will be useful, but WITHOUT ANY WARRANTY; without even the
 * implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See
 * the GNU Lesser General Public License for more details. You should have
 * received a copy of the GNU Lesser General Public License along with this
 * software; if not, write to the Free Software Foundation, Inc., 51 Franklin
 * St, Fifth Floor, Boston, MA 02110-1301 USA, or see the FSF site:
 * http://www.fsf.org.
 */
package openr66.protocol.config;

import goldengate.common.file.DataBlockSizeEstimator;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import openr66.protocol.localhandler.LocalTransaction;
import openr66.protocol.networkhandler.NetworkServerPipelineFactory;
import openr66.protocol.utils.OpenR66SignalHandler;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.execution.OrderedMemoryAwareThreadPoolExecutor;
import org.jboss.netty.handler.traffic.ChannelTrafficShapingHandler;
import org.jboss.netty.handler.traffic.GlobalTrafficShapingHandler;
import org.jboss.netty.logging.InternalLoggerFactory;
import org.jboss.netty.util.ObjectSizeEstimator;

/**
 * Configuration class
 * 
 * @author Frederic Bregier
 */
public class Configuration {
    // Static values
    /**
     * General Configuration object
     */
    public static final Configuration configuration = new Configuration();
    /**
     * Time elapse for retry in ms
     */
    public static final long RETRYINMS = 10;

    /**
     * Number of retry before error
     */
    public static final int RETRYNB = 3;

    /**
     * Hack to say Windows or Unix (USR1 not OK on Windows)
     */
    public static final boolean ISUNIX = !System.getProperty("os.name")
            .toLowerCase().startsWith("windows");
    /**
     * Default size for buffers (NIO)
     */
    public static final int BUFFERSIZEDEFAULT = 0x10000; // 64K
    // Global unique values
    /**
     * Default session limit 64Mbit, so up to 8 full simultaneous clients
     */
    public static long DEFAULT_SESSION_LIMIT = 0x800000L;

    /**
     * Default global limit 512Mbit
     */
    public static long DEFAULT_GLOBAL_LIMIT = 0x4000000L;
    /**
     * Default server port
     */
    public static int SERVER_PORT = 6666;
    /**
     * Default number of threads in pool for Server. The default value is for
     * client for Executor in the Pipeline for Business logic. Server will
     * change this value on startup if not set.
     */
    public static int SERVER_THREAD = 16;
    /**
     * Nb of milliseconds after connection is in timeout
     */
    public static int TIMEOUTCON = 30000;

    /**
     * Size by default of block size for receive/sending files. Should be a
     * multiple of 8192 (maximum = 64K due to block limitation to 2 bytes)
     */
    public static int BLOCKSIZE = 0x10000; // 64K
    /**
     * Max global memory limit: default is 4GB
     */
    public static long maxGlobalMemory = 0x100000000L;

    // Dynamic values
    /**
     * True if the service is going to shutdown
     */
    public volatile boolean isShutdown = false;
    /**
     * Limit in Write byte/s to apply globally to the FTP Server
     */
    protected long serverGlobalWriteLimit = DEFAULT_GLOBAL_LIMIT;

    /**
     * Limit in Read byte/s to apply globally to the FTP Server
     */
    protected long serverGlobalReadLimit = DEFAULT_GLOBAL_LIMIT;

    /**
     * Limit in Write byte/s to apply by session to the FTP Server
     */
    protected long serverChannelWriteLimit = DEFAULT_SESSION_LIMIT;

    /**
     * Limit in Read byte/s to apply by session to the FTP Server
     */
    protected long serverChannelReadLimit = DEFAULT_SESSION_LIMIT;

    /**
     * Delay in ms between two checks
     */
    protected long delayLimit = 10000;
    /**
     * List of all Server Channels to enable the close call on them using Netty
     * ChannelGroup
     */
    private ChannelGroup serverChannelGroup = null;

    /**
     * ExecutorService Server Boss
     */
    private final ExecutorService execServerBoss = Executors
            .newCachedThreadPool();

    /**
     * ExecutorService Server Worker
     */
    private final ExecutorService execServerWorker = Executors
            .newCachedThreadPool();

    /**
     * ChannelFactory for Server part
     */
    private ChannelFactory serverChannelFactory = null;
    /**
     * ThreadPoolExecutor for Server
     */
    private volatile OrderedMemoryAwareThreadPoolExecutor serverPipelineExecutor = new OrderedMemoryAwareThreadPoolExecutor(
            SERVER_THREAD + 1, maxGlobalMemory / 10, maxGlobalMemory, 200,
            TimeUnit.MILLISECONDS, Executors.defaultThreadFactory());
    /**
     * Bootstrap for server
     */
    private ServerBootstrap serverBootstrap = null;
    /**
     * ExecutorService for TrafficCounter
     */
    private final ExecutorService execTrafficCounter = Executors
            .newCachedThreadPool();
    /**
     * Global TrafficCounter (set from global configuration)
     */
    private volatile GlobalTrafficShapingHandler globalTrafficShapingHandler = null;
    /**
     * ObjectSizeEstimator
     */
    private ObjectSizeEstimator objectSizeEstimator = null;
    /**
     * LocalTransaction
     */
    private final LocalTransaction localTransaction = new LocalTransaction();

    public Configuration() {
        // Init signal handler
        OpenR66SignalHandler.initSignalHandler();
    }

    /**
     * Startup the server
     */
    public void serverStartup() {
        InternalLoggerFactory.setDefaultFactory(InternalLoggerFactory
                .getDefaultFactory());
        // Command
        serverChannelGroup = new DefaultChannelGroup("OpenR66");
        serverChannelFactory = new NioServerSocketChannelFactory(
                execServerBoss, execServerWorker, SERVER_THREAD);
        // Main Command server
        serverBootstrap = new ServerBootstrap(serverChannelFactory);
        serverBootstrap.setPipelineFactory(new NetworkServerPipelineFactory());
        serverBootstrap.setOption("child.tcpNoDelay", true);
        serverBootstrap.setOption("child.keepAlive", true);
        serverBootstrap.setOption("child.reuseAddress", true);
        serverBootstrap.setOption("child.connectTimeoutMillis", TIMEOUTCON);
        serverBootstrap.setOption("tcpNoDelay", true);
        serverBootstrap.setOption("reuseAddress", true);
        serverBootstrap.setOption("connectTimeoutMillis", TIMEOUTCON);

        serverChannelGroup.add(serverBootstrap.bind(new InetSocketAddress(
                SERVER_PORT)));

        // Factory for TrafficShapingHandler
        objectSizeEstimator = new DataBlockSizeEstimator();
        globalTrafficShapingHandler = new GlobalTrafficShapingHandler(
                objectSizeEstimator, execTrafficCounter,
                serverGlobalWriteLimit, serverGlobalReadLimit, delayLimit);
    }

    /**
     * Reset the global monitor for bandwidth limitation and change future
     * channel monitors with values divided by 10 (channel = global / 10)
     * 
     * @param writeLimit
     * @param readLimit
     */
    public void changeNetworkLimit(long writeLimit, long readLimit) {
        long newWriteLimit = writeLimit > 1024 ? writeLimit
                : serverGlobalWriteLimit;
        if (writeLimit <= 0) {
            newWriteLimit = 0;
        }
        long newReadLimit = readLimit > 1024 ? readLimit
                : serverGlobalReadLimit;
        if (readLimit <= 0) {
            newReadLimit = 0;
        }
        globalTrafficShapingHandler.configure(newWriteLimit, newReadLimit);
        serverChannelReadLimit = newReadLimit / 10;
        serverChannelWriteLimit = newWriteLimit / 10;
    }

    /**
     * Compute number of threads for both client and server from the real number
     * of available processors (double + 1) if the value is less than 64
     * threads.
     */
    public void computeNbThreads() {
        final int nb = Runtime.getRuntime().availableProcessors() * 2 + 1;
        if (SERVER_THREAD < nb) {
            SERVER_THREAD = nb;
        }
    }

    /**
     * @return a new ChannelTrafficShapingHandler
     */
    public ChannelTrafficShapingHandler newChannelTrafficShapingHandler() {
        return new ChannelTrafficShapingHandler(objectSizeEstimator,
                execTrafficCounter, serverChannelWriteLimit,
                serverChannelReadLimit, delayLimit);
    }

    /**
     * @return the serverChannelGroup
     */
    public ChannelGroup getServerChannelGroup() {
        return serverChannelGroup;
    }

    /**
     * @return the serverChannelFactory
     */
    public ChannelFactory getServerChannelFactory() {
        return serverChannelFactory;
    }

    /**
     * @return the serverPipelineExecutor
     */
    public OrderedMemoryAwareThreadPoolExecutor getServerPipelineExecutor() {
        return serverPipelineExecutor;
    }

    /**
     * @return the globalTrafficShapingHandler
     */
    public GlobalTrafficShapingHandler getGlobalTrafficShapingHandler() {
        return globalTrafficShapingHandler;
    }

    /**
     * @return the localTransaction
     */
    public LocalTransaction getLocalTransaction() {
        return localTransaction;
    }

}
