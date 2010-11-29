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
package openr66.protocol.configuration;

import goldengate.common.crypto.Des;
import goldengate.common.file.filesystembased.FilesystemBasedFileParameterImpl;
import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import openr66.commander.InternalRunner;
import openr66.context.task.localexec.LocalExecClient;
import openr66.database.DbSession;
import openr66.database.data.DbHostAuth;
import openr66.database.exception.OpenR66DatabaseException;
import openr66.database.exception.OpenR66DatabaseNoConnectionError;
import openr66.database.exception.OpenR66DatabaseSqlError;
import openr66.protocol.exception.OpenR66ProtocolNoDataException;
import openr66.protocol.exception.OpenR66ProtocolNoSslException;
import openr66.protocol.http.HttpPipelineFactory;
import openr66.protocol.http.adminssl.HttpSslPipelineFactory;
import openr66.protocol.localhandler.LocalTransaction;
import openr66.protocol.networkhandler.ChannelTrafficHandler;
import openr66.protocol.networkhandler.ConstraintLimitHandler;
import openr66.protocol.networkhandler.GlobalTrafficHandler;
import openr66.protocol.networkhandler.NetworkServerPipelineFactory;
import openr66.protocol.networkhandler.packet.NetworkPacketSizeEstimator;
import openr66.protocol.networkhandler.ssl.NetworkSslServerPipelineFactory;
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
    /**
     * Internal Logger
     */
    private static final GgInternalLogger logger = GgInternalLoggerFactory
            .getLogger(Configuration.class);

    // Static values
    /**
     * General Configuration object
     */
    public static final Configuration configuration = new Configuration();

    /**
     * True if JDK6 or upper, False if JDK5.
     */
    public static final boolean USEJDK6 = true;
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
    public static boolean ISUNIX;

    /**
     * Default size for buffers (NIO)
     */
    public static final int BUFFERSIZEDEFAULT = 0x10000; // 64K

    /**
     * Time elapse for WRITE OR CLOSE WAIT elaps in ms
     */
    public static final long WAITFORNETOP = 1000;

    /**
     * Extension of file during transfer
     */
    public static final String EXT_R66 = ".r66";

    /**
     * Rank to redo when a restart occurs
     */
    public static final long RANKRESTART = 30;

    /**
     * FileParameter
     */
    private static final FilesystemBasedFileParameterImpl fileParameter =
        new FilesystemBasedFileParameterImpl();

    // Global Dynamic values
    /**
     * Actual Host ID
     */
    public String HOST_ID;
    /**
     * Actual SSL Host ID
     */
    public String HOST_SSLID;

    /**
     * Server Administration user name
     */
    public String ADMINNAME = null;
    /**
     * Server Administration Key
     */
    private byte[] SERVERADMINKEY = null;
    /**
     * Server Actual Authentication
     */
    public DbHostAuth HOST_AUTH;
    /**
     * Server Actual SSL Authentication
     */
    public DbHostAuth HOST_SSLAUTH;

    /**
     * Default number of threads in pool for Server (true network listeners).
     * Server will change this value on startup if not set. The value should be
     * closed to the number of CPU.
     */
    public int SERVER_THREAD = 8;

    /**
     * Default number of threads in pool for Client. The value is for true
     * client for Executor in the Pipeline for Business logic. The value does
     * not indicate a limit of concurrent clients, but a limit on truly packet
     * concurrent actions.
     */
    public int CLIENT_THREAD = 80;

    /**
     * Default session limit 64Mbit, so up to 8 full simultaneous clients
     */
    public long DEFAULT_SESSION_LIMIT = 0x800000L;

    /**
     * Default global limit 512Mbit
     */
    public long DEFAULT_GLOBAL_LIMIT = 0x4000000L;

    /**
     * Default server port
     */
    public int SERVER_PORT = 6666;

    /**
     * Default SSL server port
     */
    public int SERVER_SSLPORT = 6667;

    /**
     * Default HTTP server port
     */
    public int SERVER_HTTPPORT = 8066;

    /**
     * Default HTTP server port
     */
    public int SERVER_HTTPSPORT = 8067;

    /**
     * Nb of milliseconds after connection is in timeout
     */
    public int TIMEOUTCON = 30000;

    /**
     * Size by default of block size for receive/sending files. Should be a
     * multiple of 8192 (maximum = 2^30K due to block limitation to 4 bytes)
     */
    public int BLOCKSIZE = 0x10000; // 64K

    /**
     * Max global memory limit: default is 4GB
     */
    public long maxGlobalMemory = 0x100000000L;

    /**
     * Base Directory
     */
    public String baseDirectory;

    /**
     * In path (receive)
     */
    public String inPath = null;

    /**
     * Out path (send, copy, pending)
     */
    public String outPath = null;

    /**
     * Archive path
     */
    public String archivePath = null;

    /**
     * Working path
     */
    public String workingPath = null;

    /**
     * Config path
     */
    public String configPath = null;

    /**
     * Http Admin base
     */
    public String httpBasePath = "src/main/admin/";

    /**
     * True if the service is going to shutdown
     */
    public volatile boolean isShutdown = false;

    /**
     * Limit in Write byte/s to apply globally to the FTP Server
     */
    public long serverGlobalWriteLimit = DEFAULT_GLOBAL_LIMIT;

    /**
     * Limit in Read byte/s to apply globally to the FTP Server
     */
    public long serverGlobalReadLimit = DEFAULT_GLOBAL_LIMIT;

    /**
     * Limit in Write byte/s to apply by session to the FTP Server
     */
    public long serverChannelWriteLimit = DEFAULT_SESSION_LIMIT;

    /**
     * Limit in Read byte/s to apply by session to the FTP Server
     */
    public long serverChannelReadLimit = DEFAULT_SESSION_LIMIT;

    /**
     * Delay in ms between two checks
     */
    public long delayLimit = 10000;

    /**
     * Does this OpenR66 server will use and accept SSL connections
     */
    public boolean useSSL = false;
    /**
     * Does this OpenR66 server will use and accept non SSL connections
     */
    public boolean useNOSSL = true;

    /**
     * Does this OpenR66 server will try to compress HTTP connections
     */
    public boolean useHttpCompression = false;

    /**
     * Does this OpenR66 server will use GoldenGate LocalExec Daemon for ExecTask and ExecMoveTask
     */
    public boolean useLocalExec = false;

    /**
     * Crypto Key
     */
    public Des cryptoKey = null;

    /**
     * List of all Server Channels to enable the close call on them using Netty
     * ChannelGroup
     */
    private ChannelGroup serverChannelGroup = null;

    private class R66ThreadFactory implements ThreadFactory {
        private String GlobalName;
        public R66ThreadFactory(String globalName) {
            GlobalName = globalName;
        }
        /* (non-Javadoc)
         * @see java.util.concurrent.ThreadFactory#newThread(java.lang.Runnable)
         */
        @Override
        public Thread newThread(Runnable arg0) {
            Thread thread = new Thread(arg0);
            thread.setName(GlobalName+thread.getName());
            return thread;
        }

    }
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
    private volatile OrderedMemoryAwareThreadPoolExecutor serverPipelineExecutor;

    /**
     * ThreadPoolExecutor for LocalServer
     */
    private volatile OrderedMemoryAwareThreadPoolExecutor localPipelineExecutor;

    /**
     * ThreadPoolExecutor for Http and Https Server
     */
    private volatile OrderedMemoryAwareThreadPoolExecutor httpPipelineExecutor;

    /**
     * Bootstrap for server
     */
    private ServerBootstrap serverBootstrap = null;

    /**
     * Bootstrap for SSL server
     */
    private ServerBootstrap serverSslBootstrap = null;
    /**
     * Bootstrap for Http server
     */
    private ServerBootstrap httpBootstrap = null;
    /**
     * Bootstrap for Https server
     */
    private ServerBootstrap httpsBootstrap = null;
    /**
     * ChannelFactory for HttpServer part
     */
    private ChannelFactory httpChannelFactory = null;
    /**
     * ChannelFactory for HttpsServer part
     */
    private ChannelFactory httpsChannelFactory = null;
    /**
     * List of all Http Channels to enable the close call on them using Netty
     * ChannelGroup
     */
    private ChannelGroup httpChannelGroup = null;

    /**
     * ExecutorService for TrafficCounter
     */
    private final ExecutorService execTrafficCounter = Executors
            .newCachedThreadPool();

    /**
     * Global TrafficCounter (set from global configuration)
     */
    private volatile GlobalTrafficHandler globalTrafficShapingHandler = null;

    /**
     * ObjectSizeEstimator
     */
    private ObjectSizeEstimator objectSizeEstimator = null;

    /**
     * LocalTransaction
     */
    private LocalTransaction localTransaction;
    /**
     * InternalRunner
     */
    private InternalRunner internalRunner;
    /**
     * Maximum number of concurrent active transfer by submission.
     */
    public int RUNNER_THREAD = 10000;
    /**
     * Delay in ms between two steps of Commander
     */
    public long delayCommander = 5000;
    /**
     * Delay in ms between two retries
     */
    public long delayRetry = 30000;
    
    public ConstraintLimitHandler constraintLimitHandler = 
        new ConstraintLimitHandler(true, false, 0.9, 100);

    public boolean checkRemoteAddress = false;
    public boolean checkClientAddress = false;
    
    private volatile boolean configured = false;

    public Configuration() {
        // Init signal handler
        OpenR66SignalHandler.initSignalHandler();
        computeNbThreads();
    }

    /**
     * Configure the pipeline for client (to be called ony once)
     */
    public void pipelineInit() {
        if (configured) {
            return;
        }
        localTransaction = new LocalTransaction();
        InternalLoggerFactory.setDefaultFactory(InternalLoggerFactory
                .getDefaultFactory());
        objectSizeEstimator = new NetworkPacketSizeEstimator();
        serverPipelineExecutor = new OrderedMemoryAwareThreadPoolExecutor(
                CLIENT_THREAD, maxGlobalMemory / 10, maxGlobalMemory, 500,
                TimeUnit.MILLISECONDS, new R66ThreadFactory("ServerExecutor"));
        localPipelineExecutor = new OrderedMemoryAwareThreadPoolExecutor(
                CLIENT_THREAD * 100, maxGlobalMemory / 10, maxGlobalMemory,
                500, TimeUnit.MILLISECONDS,
                new R66ThreadFactory("LocalExecutor"));
        httpPipelineExecutor = new OrderedMemoryAwareThreadPoolExecutor(
                CLIENT_THREAD, maxGlobalMemory / 10, maxGlobalMemory, 500,
                TimeUnit.MILLISECONDS, new R66ThreadFactory("HttpExecutor"));
        if (useLocalExec) {
            LocalExecClient.initialize();
        }
        configured = true;
    }

    /**
     * Startup the server
     * @throws OpenR66DatabaseSqlError
     * @throws OpenR66DatabaseNoConnectionError
     */
    public void serverStartup() throws OpenR66DatabaseNoConnectionError, OpenR66DatabaseSqlError {
        if ((!useNOSSL) && (!useSSL)) {
            logger.error("OpenR66 has neither NOSSL nor SSL support included! Stop here!");
            System.exit(-1);
        }
        pipelineInit();
        // FIXME add into configuration
        this.constraintLimitHandler.setServer(true);
        this.checkRemoteAddress = true;
        // Global Server
        serverChannelGroup = new DefaultChannelGroup("OpenR66");
        httpChannelGroup = new DefaultChannelGroup("HttpOpenR66");

        serverChannelFactory = new NioServerSocketChannelFactory(
                execServerBoss, execServerWorker, SERVER_THREAD);
        if (useNOSSL) {
            serverBootstrap = new ServerBootstrap(serverChannelFactory);
            serverBootstrap.setPipelineFactory(new NetworkServerPipelineFactory(true));
            serverBootstrap.setOption("child.tcpNoDelay", true);
            serverBootstrap.setOption("child.keepAlive", true);
            serverBootstrap.setOption("child.reuseAddress", true);
            serverBootstrap.setOption("child.connectTimeoutMillis", TIMEOUTCON);
            serverBootstrap.setOption("tcpNoDelay", true);
            serverBootstrap.setOption("reuseAddress", true);
            serverBootstrap.setOption("connectTimeoutMillis", TIMEOUTCON);

            serverChannelGroup.add(serverBootstrap.bind(new InetSocketAddress(
                    SERVER_PORT)));
        } else {
            logger.warn("NOSSL mode is desactivated");
        }

        if (useSSL && HOST_SSLID != null) {
            serverSslBootstrap = new ServerBootstrap(serverChannelFactory);
            serverSslBootstrap.setPipelineFactory(new NetworkSslServerPipelineFactory(false,
                    execServerWorker));
            serverSslBootstrap.setOption("child.tcpNoDelay", true);
            serverSslBootstrap.setOption("child.keepAlive", true);
            serverSslBootstrap.setOption("child.reuseAddress", true);
            serverSslBootstrap.setOption("child.connectTimeoutMillis", TIMEOUTCON);
            serverSslBootstrap.setOption("tcpNoDelay", true);
            serverSslBootstrap.setOption("reuseAddress", true);
            serverSslBootstrap.setOption("connectTimeoutMillis", TIMEOUTCON);

            serverChannelGroup.add(serverSslBootstrap.bind(new InetSocketAddress(
                    SERVER_SSLPORT)));
        } else {
            logger.warn("SSL mode is desactivated");
        }

        // Factory for TrafficShapingHandler
        globalTrafficShapingHandler = new GlobalTrafficHandler(
                objectSizeEstimator, execTrafficCounter,
                serverGlobalWriteLimit, serverGlobalReadLimit, delayLimit);

        // Now start the InternalRunner
        internalRunner = new InternalRunner();

        // Now start the HTTP support
        // Configure the server.
        httpChannelFactory = new NioServerSocketChannelFactory(
                Executors.newCachedThreadPool(),
                Executors.newCachedThreadPool(),
                SERVER_THREAD);
        httpBootstrap = new ServerBootstrap(
                httpChannelFactory);
        // Set up the event pipeline factory.
        httpBootstrap.setPipelineFactory(new HttpPipelineFactory(useHttpCompression));
        httpBootstrap.setOption("child.tcpNoDelay", true);
        httpBootstrap.setOption("child.keepAlive", true);
        httpBootstrap.setOption("child.reuseAddress", true);
        httpBootstrap.setOption("child.connectTimeoutMillis", TIMEOUTCON);
        httpBootstrap.setOption("tcpNoDelay", true);
        httpBootstrap.setOption("reuseAddress", true);
        httpBootstrap.setOption("connectTimeoutMillis", TIMEOUTCON);
        // Bind and start to accept incoming connections.
        httpChannelGroup.add(httpBootstrap.bind(new InetSocketAddress(SERVER_HTTPPORT)));

        // Now start the HTTPS support
        // Configure the server.
        httpsChannelFactory = new NioServerSocketChannelFactory(
                Executors.newCachedThreadPool(),
                Executors.newCachedThreadPool(),
                SERVER_THREAD);
        httpsBootstrap = new ServerBootstrap(
                httpsChannelFactory);
        // Set up the event pipeline factory.
        httpsBootstrap.setPipelineFactory(new HttpSslPipelineFactory(useHttpCompression,
                true, execServerWorker));
        httpsBootstrap.setOption("child.tcpNoDelay", true);
        httpsBootstrap.setOption("child.keepAlive", true);
        httpsBootstrap.setOption("child.reuseAddress", true);
        httpsBootstrap.setOption("child.connectTimeoutMillis", TIMEOUTCON);
        httpsBootstrap.setOption("tcpNoDelay", true);
        httpsBootstrap.setOption("reuseAddress", true);
        httpsBootstrap.setOption("connectTimeoutMillis", TIMEOUTCON);
        // Bind and start to accept incoming connections.
        httpChannelGroup.add(httpsBootstrap.bind(new InetSocketAddress(SERVER_HTTPSPORT)));

    }
    public InternalRunner getInternalRunner() {
        return internalRunner;
    }
    /**
     * Prepare the server to stop
     *
     * To be called early before other stuff will be closed
     */
    public void prepareServerStop() {
        if (internalRunner != null) {
            internalRunner.prepareStopInternalRunner();
        }
    }
    /**
     * Stops the server
     *
     * To be called after all other stuff are closed (channels, connections)
     */
    public void serverStop() {
        if (internalRunner != null) {
            internalRunner.stopInternalRunner();
        }
    }
    /**
     * Try to reload the Commander
     * @return True if reloaded, else in error
     */
    public boolean reloadCommanderDelay() {
        if (internalRunner != null) {
            try {
                internalRunner.reloadInternalRunner();
                return true;
            } catch (OpenR66DatabaseNoConnectionError e) {
            } catch (OpenR66DatabaseSqlError e) {
            }
        }
        return false;
    }
    /**
     * Reset the global monitor for bandwidth limitation and change future
     * channel monitors
     *
     * @param writeGlobalLimit
     * @param readGlobalLimit
     * @param writeSessionLimit
     * @param readSessionLimit
     * @param delayLimit
     */
    public void changeNetworkLimit(long writeGlobalLimit, long readGlobalLimit,
            long writeSessionLimit, long readSessionLimit, long delayLimit) {
        long newWriteLimit = writeGlobalLimit > 1024? writeGlobalLimit
                : serverGlobalWriteLimit;
        if (writeGlobalLimit <= 0) {
            newWriteLimit = 0;
        }
        long newReadLimit = readGlobalLimit > 1024? readGlobalLimit
                : serverGlobalReadLimit;
        if (readGlobalLimit <= 0) {
            newReadLimit = 0;
        }
        serverGlobalReadLimit = newReadLimit;
        serverGlobalWriteLimit = newWriteLimit;
        this.delayLimit = delayLimit;
        if (globalTrafficShapingHandler != null) {
            globalTrafficShapingHandler.configure(serverGlobalWriteLimit, serverGlobalReadLimit, delayLimit);
            logger.warn("Bandwidth limits change: {}", globalTrafficShapingHandler);
        }
        newWriteLimit = writeSessionLimit > 1024? writeSessionLimit
                : serverChannelWriteLimit;
        if (writeSessionLimit <= 0) {
            newWriteLimit = 0;
        }
        newReadLimit = readSessionLimit > 1024? readSessionLimit
                : serverChannelReadLimit;
        if (readSessionLimit <= 0) {
            newReadLimit = 0;
        }
        serverChannelReadLimit = newReadLimit;
        serverChannelWriteLimit = newWriteLimit;
    }

    /**
     * Compute number of threads for both client and server from the real number
     * of available processors (double + 1) if the value is less than 32
     * threads else (available +1).
     */
    public void computeNbThreads() {
        int nb = Runtime.getRuntime().availableProcessors() * 2 + 1;
        if (nb > 32) {
            nb = Runtime.getRuntime().availableProcessors() + 1;
        }
        if (SERVER_THREAD < nb) {
            logger.info("Change default number of threads to " + nb);
            SERVER_THREAD = nb;
            CLIENT_THREAD = SERVER_THREAD*10;
        }
    }

    /**
     * @return a new ChannelTrafficShapingHandler
     * @throws OpenR66ProtocolNoDataException
     */
    public ChannelTrafficShapingHandler newChannelTrafficShapingHandler() throws OpenR66ProtocolNoDataException {
        if (serverChannelReadLimit == 0 && serverChannelWriteLimit == 0) {
            throw new OpenR66ProtocolNoDataException("No limit for channel");
        }
        return new ChannelTrafficHandler(objectSizeEstimator,
                execTrafficCounter, serverChannelWriteLimit,
                serverChannelReadLimit, delayLimit);
    }

    /**
     * @return the globalTrafficShapingHandler
     */
    public GlobalTrafficShapingHandler getGlobalTrafficShapingHandler() {
        return globalTrafficShapingHandler;
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
     * @return the httpChannelGroup
     */
    public ChannelGroup getHttpChannelGroup() {
        return httpChannelGroup;
    }

    /**
     * @return the httpChannelFactory
     */
    public ChannelFactory getHttpChannelFactory() {
        return httpChannelFactory;
    }
    /**
     * @return the httpsChannelFactory
     */
    public ChannelFactory getHttpsChannelFactory() {
        return httpsChannelFactory;
    }
    /**
     * @return the serverPipelineExecutor
     */
    public OrderedMemoryAwareThreadPoolExecutor getServerPipelineExecutor() {
        return serverPipelineExecutor;
    }

    /**
     * @return the localPipelineExecutor
     */
    public OrderedMemoryAwareThreadPoolExecutor getLocalPipelineExecutor() {
        return localPipelineExecutor;
    }

    /**
     * @return the httpPipelineExecutor
     */
    public OrderedMemoryAwareThreadPoolExecutor getHttpPipelineExecutor() {
        return httpPipelineExecutor;
    }

    /**
     * @return the localTransaction
     */
    public LocalTransaction getLocalTransaction() {
        return localTransaction;
    }

    /**
     *
     * @return the FilesystemBasedFileParameterImpl
     */
    public static FilesystemBasedFileParameterImpl getFileParameter() {
        return fileParameter;
    }

    /**
     * @return the SERVERADMINKEY
     */
    public byte[] getSERVERADMINKEY() {
        return SERVERADMINKEY;
    }

    /**
     * Is the given key a valid one
     *
     * @param newkey
     * @return True if the key is valid (or any key is valid)
     */
    public boolean isKeyValid(byte[] newkey) {
        if (newkey == null) {
            return false;
        }
        return Arrays.equals(SERVERADMINKEY, newkey);
    }

    /**
     * @param serverkey
     *            the SERVERADMINKEY to set
     */
    public void setSERVERKEY(byte[] serverkey) {
        SERVERADMINKEY = serverkey;
    }
    /**
     *
     * @param isSSL
     * @return the HostId according to SSL
     * @throws OpenR66ProtocolNoSslException
     */
    public String getHostId(boolean isSSL) throws OpenR66ProtocolNoSslException {
        if (isSSL) {
            if (HOST_SSLID == null) {
                throw new OpenR66ProtocolNoSslException("No SSL support");
            }
            return HOST_SSLID;
        } else {
            return HOST_ID;
        }
    }
    /**
     *
     * @param dbSession
     * @param remoteHost
     * @return the HostId according to remoteHost (and its SSL status)
     * @throws OpenR66DatabaseException
     */
    public String getHostId(DbSession dbSession, String remoteHost) throws OpenR66DatabaseException {
        DbHostAuth hostAuth = new DbHostAuth(dbSession,remoteHost);
        try {
            return Configuration.configuration.getHostId(hostAuth.isSsl());
        } catch (OpenR66ProtocolNoSslException e) {
            throw new OpenR66DatabaseException(e);
        }
    }
}
