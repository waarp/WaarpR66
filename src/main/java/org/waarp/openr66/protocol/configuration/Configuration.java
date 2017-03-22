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
package org.waarp.openr66.protocol.configuration;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.handler.traffic.AbstractTrafficShapingHandler;
import io.netty.handler.traffic.ChannelTrafficShapingHandler;
import io.netty.handler.traffic.GlobalChannelTrafficShapingHandler;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timer;

import org.waarp.common.crypto.Des;
import org.waarp.common.crypto.ssl.WaarpSecureKeyStore;
import org.waarp.common.crypto.ssl.WaarpSslContextFactory;
import org.waarp.common.crypto.ssl.WaarpSslUtility;
import org.waarp.common.database.DbSession;
import org.waarp.common.database.exception.WaarpDatabaseException;
import org.waarp.common.database.exception.WaarpDatabaseNoConnectionException;
import org.waarp.common.database.exception.WaarpDatabaseSqlException;
import org.waarp.common.digest.FilesystemBasedDigest;
import org.waarp.common.digest.FilesystemBasedDigest.DigestAlgo;
import org.waarp.common.file.filesystembased.FilesystemBasedFileParameterImpl;
import org.waarp.common.future.WaarpFuture;
import org.waarp.common.logging.WaarpLogger;
import org.waarp.common.logging.WaarpLoggerFactory;
import org.waarp.common.role.RoleDefault;
import org.waarp.common.utility.SystemPropertyUtil;
import org.waarp.common.utility.WaarpNettyUtil;
import org.waarp.common.utility.WaarpShutdownHook;
import org.waarp.common.utility.WaarpShutdownHook.ShutdownConfiguration;
import org.waarp.common.utility.WaarpThreadFactory;
import org.waarp.gateway.kernel.rest.RestConfiguration;
import org.waarp.openr66.commander.ClientRunner;
import org.waarp.openr66.commander.InternalRunner;
import org.waarp.openr66.configuration.FileBasedConfiguration;
import org.waarp.openr66.context.R66BusinessFactoryInterface;
import org.waarp.openr66.context.R66DefaultBusinessFactory;
import org.waarp.openr66.context.R66FiniteDualStates;
import org.waarp.openr66.context.task.localexec.LocalExecClient;
import org.waarp.openr66.database.DbConstant;
import org.waarp.openr66.database.data.DbHostAuth;
import org.waarp.openr66.database.data.DbTaskRunner;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolNoDataException;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolNoSslException;
import org.waarp.openr66.protocol.http.HttpInitializer;
import org.waarp.openr66.protocol.http.adminssl.HttpReponsiveSslInitializer;
import org.waarp.openr66.protocol.http.adminssl.HttpSslHandler;
import org.waarp.openr66.protocol.http.adminssl.HttpSslInitializer;
import org.waarp.openr66.protocol.http.rest.HttpRestR66Handler;
import org.waarp.openr66.protocol.localhandler.LocalTransaction;
import org.waarp.openr66.protocol.localhandler.Monitoring;
import org.waarp.openr66.protocol.networkhandler.ChannelTrafficHandler;
import org.waarp.openr66.protocol.networkhandler.GlobalTrafficHandler;
import org.waarp.openr66.protocol.networkhandler.NetworkServerInitializer;
import org.waarp.openr66.protocol.networkhandler.NetworkTransaction;
import org.waarp.openr66.protocol.networkhandler.R66ConstraintLimitHandler;
import org.waarp.openr66.protocol.networkhandler.ssl.NetworkSslServerInitializer;
import org.waarp.openr66.protocol.snmp.R66PrivateMib;
import org.waarp.openr66.protocol.snmp.R66VariableFactory;
import org.waarp.openr66.protocol.utils.ChannelUtils;
import org.waarp.openr66.protocol.utils.R66ShutdownHook;
import org.waarp.openr66.protocol.utils.Version;
import org.waarp.openr66.thrift.R66ThriftServerService;
import org.waarp.snmp.WaarpMOFactory;
import org.waarp.snmp.WaarpSnmpAgent;

/**
 * Configuration class
 * 
 * @author Frederic Bregier
 */
public class Configuration {
    /**
     * Internal Logger
     */
    private static final WaarpLogger logger = WaarpLoggerFactory.getLogger(Configuration.class);

    // Static values
    /**
     * General Configuration object
     */
    public static Configuration configuration = new Configuration();

    public static final String SnmpName = "Waarp OpenR66 SNMP";
    public static final int SnmpPrivateId = 66666;
    public static final int SnmpR66Id = 66;
    public static final String SnmpDefaultAuthor = "Frederic Bregier";
    public static final String SnmpVersion = "Waarp OpenR66 "
            + Version.ID;
    public static final String SnmpDefaultLocalization = "Paris, France";
    public static final int SnmpService = 72;
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
    private static boolean ISUNIX;

    /**
     * Default size for buffers (NIO)
     */
    public static final int BUFFERSIZEDEFAULT = 0x10000; // 64K

    /**
     * Time elapse for WRITE OR CLOSE WAIT elaps in ms
     */
    public static final long WAITFORNETOP = 200;

    /**
     * Extension of file during transfer
     */
    public static final String EXT_R66 = ".r66";

    /**
     * Rank to redo when a restart occurs
     */
    private static int RANKRESTART = 30;
    /**
     * Number of DbSession for internal needs
     */
    private static int NBDBSESSION = 0;
    /**
     * FileParameter
     */
    private static final FilesystemBasedFileParameterImpl fileParameter = new FilesystemBasedFileParameterImpl();

    private R66BusinessFactoryInterface r66BusinessFactory = new R66DefaultBusinessFactory();
    // Global Dynamic values
    /**
     * Version validation
     */
    private boolean extendedProtocol = true;
    /**
     * Global digest
     */
    private boolean globalDigest = true;
    /**
     * White List of allowed Partners to use Business Requests
     */
    private final HashSet<String> businessWhiteSet = new HashSet<String>();
    /**
     * Roles list for identified partners
     */
    private final HashMap<String, RoleDefault> roles = new HashMap<String, RoleDefault>();
    /**
     * Aliases list for identified partners
     */
    private final HashMap<String, String> aliases = new HashMap<String, String>();
    /**
     * reverse Aliases list for identified partners
     */
    private final HashMap<String, String[]> reverseAliases = new HashMap<String, String[]>();
    /**
     * Versions for each HostID
     */
    private final ConcurrentHashMap<String, PartnerConfiguration> versions = new ConcurrentHashMap<String, PartnerConfiguration>();
    /**
     * Actual Host ID
     */
    private String HOST_ID;
    /**
     * Actual SSL Host ID
     */
    private String HOST_SSLID;

    /**
     * Server Administration user name
     */
    private String ADMINNAME = null;
    /**
     * Server Administration Key
     */
    private byte[] SERVERADMINKEY = null;
    /**
     * Server Administration Key file
     */
    private String serverKeyFile = null;
    /**
     * Server Actual Authentication
     */
    private DbHostAuth HOST_AUTH;
    /**
     * Server Actual SSL Authentication
     */
    private DbHostAuth HOST_SSLAUTH;

    /**
     * Default number of threads in pool for Server (true network listeners). Server will change
     * this value on startup if not set. The value should be closed to the number of CPU.
     */
    private int SERVER_THREAD = 0;

    /**
     * Default number of threads in pool for Client. The value is for true client for Executor in
     * the Pipeline for Business logic. The value does not indicate a limit of concurrent clients,
     * but a limit on truly packet concurrent actions.
     */
    private int CLIENT_THREAD = 10;

    /**
     * Default session limit 64Mbit, so up to 16 full simultaneous clients
     */
    private final long DEFAULT_SESSION_LIMIT = 0x800000L;

    /**
     * Default global limit 1024Mbit
     */
    private final long DEFAULT_GLOBAL_LIMIT = 0x8000000L;

    /**
     * Default server port
     */
    private int SERVER_PORT = 6666;

    /**
     * Default SSL server port
     */
    private int SERVER_SSLPORT = 6667;

    /**
     * Default HTTP server port
     */
    private int SERVER_HTTPPORT = 8066;

    /**
     * Default HTTP server port
     */
    private int SERVER_HTTPSPORT = 8067;

    /**
     * Nb of milliseconds after connection is in timeout
     */
    private long TIMEOUTCON = 30000;

    /**
     * Size by default of block size for receive/sending files. Should be a multiple of 8192
     * (maximum = 2^30K due to block limitation to 4 bytes)
     */
    private int BLOCKSIZE = 0x10000; // 64K

    /**
     * Max global memory limit: default is 4GB
     */
    private long maxGlobalMemory = 0x100000000L;

    /**
     * Rest configuration list
     */
    private final List<RestConfiguration> restConfigurations = new ArrayList<RestConfiguration>();

    /**
     * Base Directory
     */
    private String baseDirectory;

    /**
     * In path (receive)
     */
    private String inPath = null;

    /**
     * Out path (send, copy, pending)
     */
    private String outPath = null;

    /**
     * Archive path
     */
    private String archivePath = null;

    /**
     * Working path
     */
    private String workingPath = null;

    /**
     * Config path
     */
    private String configPath = null;

    /**
     * Http Admin base
     */
    private String httpBasePath = "src/main/admin/";

    /**
     * Model for Http Admin: 0 = standard (i18n only), 1 = responsive (i18n + bootstrap + dynamic table + refresh)
     */
    private int httpModel = 1;

    /**
     * True if the service is going to shutdown
     */
    private volatile boolean isShutdown = false;

    /**
     * Limit in Write byte/s to apply globally to the FTP Server
     */
    private long serverGlobalWriteLimit = getDEFAULT_GLOBAL_LIMIT();

    /**
     * Limit in Read byte/s to apply globally to the FTP Server
     */
    private long serverGlobalReadLimit = getDEFAULT_GLOBAL_LIMIT();

    /**
     * Limit in Write byte/s to apply by session to the FTP Server
     */
    private long serverChannelWriteLimit = getDEFAULT_SESSION_LIMIT();

    /**
     * Limit in Read byte/s to apply by session to the FTP Server
     */
    private long serverChannelReadLimit = getDEFAULT_SESSION_LIMIT();

    /**
     * Any limitation on bandwidth active?
     */
    private boolean anyBandwidthLimitation = false;
    /**
     * Delay in ms between two checks
     */
    private long delayLimit = AbstractTrafficShapingHandler.DEFAULT_CHECK_INTERVAL;

    /**
     * Does this OpenR66 server will use and accept SSL connections
     */
    private boolean useSSL = false;
    /**
     * Does this OpenR66 server will use and accept non SSL connections
     */
    private boolean useNOSSL = true;
    /**
     * Algorithm to use for Digest
     */
    private FilesystemBasedDigest.DigestAlgo digest = DigestAlgo.MD5;

    /**
     * Does this OpenR66 server will try to compress HTTP connections
     */
    private boolean useHttpCompression = false;

    /**
     * Does this OpenR66 server will use Waarp LocalExec Daemon for ExecTask and ExecMoveTask
     */
    private boolean useLocalExec = false;

    /**
     * Crypto Key
     */
    private Des cryptoKey = null;
    /**
     * Associated file for CryptoKey
     */
    private String cryptoFile = null;

    /**
     * List of all Server Channels to enable the close call on them using Netty ChannelGroup
     */
    protected ChannelGroup serverChannelGroup = null;
    /**
     * Main bind address in no ssl mode
     */
    protected Channel bindNoSSL = null;
    /**
     * Main bind address in ssl mode
     */
    protected Channel bindSSL = null;

    /**
     * Does the current program running as Server
     */
    private boolean isServer = false;

    /**
     * ExecutorService Other Worker
     */
    protected final ExecutorService execOtherWorker = Executors.newCachedThreadPool(new WaarpThreadFactory(
            "OtherWorker"));

    protected EventLoopGroup bossGroup;
    protected EventLoopGroup workerGroup;
    protected EventLoopGroup handlerGroup;
    protected EventLoopGroup subTaskGroup;
    protected EventLoopGroup localBossGroup;
    protected EventLoopGroup localWorkerGroup;
    protected EventLoopGroup httpBossGroup;
    protected EventLoopGroup httpWorkerGroup;

    /**
     * ExecutorService Scheduled tasks
     */
    protected final ScheduledExecutorService scheduledExecutorService;

    /**
     * Bootstrap for server
     */
    protected ServerBootstrap serverBootstrap = null;

    /**
     * Bootstrap for SSL server
     */
    protected ServerBootstrap serverSslBootstrap = null;
    /**
     * Factory for NON SSL Server
     */
    protected NetworkServerInitializer networkServerInitializer;
    /**
     * Factory for SSL Server
     */
    protected NetworkSslServerInitializer networkSslServerInitializer;

    /**
     * Bootstrap for Http server
     */
    protected ServerBootstrap httpBootstrap = null;
    /**
     * Bootstrap for Https server
     */
    protected ServerBootstrap httpsBootstrap = null;
    /**
     * List of all Http Channels to enable the close call on them using Netty ChannelGroup
     */
    protected ChannelGroup httpChannelGroup = null;

    /**
     * Timer for CloseOpertations
     */
    private final Timer timerCloseOperations =
            new HashedWheelTimer(
                    new WaarpThreadFactory(
                            "TimerClose"),
                    50,
                    TimeUnit.MILLISECONDS,
                    1024);
    /**
     * Global TrafficCounter (set from global configuration)
     */
    protected GlobalTrafficHandler globalTrafficShapingHandler = null;

    /**
     * LocalTransaction
     */
    protected LocalTransaction localTransaction;
    /**
     * InternalRunner
     */
    private InternalRunner internalRunner;
    /**
     * Maximum number of concurrent active transfer by submission.
     */
    private int RUNNER_THREAD = 1000;
    /**
     * Delay in ms between two steps of Commander
     */
    private long delayCommander = 5000;
    /**
     * Delay in ms between two retries
     */
    private long delayRetry = 30000;
    /**
     * Constraint Limit Handler on CPU usage and Connection limitation
     */
    private R66ConstraintLimitHandler constraintLimitHandler = new R66ConstraintLimitHandler();
    /**
     * Do we check Remote Address from DbHost
     */
    private boolean checkRemoteAddress = false;
    /**
     * Do we check address even for Client
     */
    private boolean checkClientAddress = false;
    /**
     * For No Db client, do we saved TaskRunner in a XML
     */
    private boolean saveTaskRunnerWithNoDb = false;
    /**
     * In case of Multiple OpenR66 monitor servers behing a load balancer (HA solution)
     */
    private int multipleMonitors = 1;
    /**
     * Monitoring object
     */
    private Monitoring monitoring = null;
    /**
     * Monitoring: how long in ms to get back in monitoring
     */
    private long pastLimit = 86400000; // 24H
    /**
     * Monitoring: minimal interval in ms before redo real monitoring
     */
    private long minimalDelay = 5000; // 5 seconds
    /**
     * Monitoring: snmp configuration file (empty means no snmp support)
     */
    private String snmpConfig = null;
    /**
     * SNMP Agent (if any)
     */
    private WaarpSnmpAgent agentSnmp = null;
    /**
     * Associated MIB
     */
    private R66PrivateMib r66Mib = null;

    protected volatile boolean configured = false;

    private static WaarpSecureKeyStore waarpSecureKeyStore;

    private static WaarpSslContextFactory waarpSslContextFactory;
    /**
     * Thrift support
     */
    private R66ThriftServerService thriftService;
    private int thriftport = -1;

    private boolean isExecuteErrorBeforeTransferAllowed = true;

    private final ShutdownConfiguration shutdownConfiguration = new ShutdownConfiguration();

    private boolean isHostProxyfied = false;

    private boolean warnOnStartup = true;

    private boolean chrootChecked = true;

    private boolean blacklistBadAuthent = false;

    private int maxfilenamelength = 255;

    private int timeStat = 0;

    private int limitCache = 20000;

    private long timeLimitCache = 180000;

    public Configuration() {
        // Init signal handler
        getShutdownConfiguration().timeout = getTIMEOUTCON();
        if (WaarpShutdownHook.shutdownHook == null) {
            new R66ShutdownHook(getShutdownConfiguration());
        }
        computeNbThreads();
        scheduledExecutorService = Executors.newScheduledThreadPool(this.getSERVER_THREAD(), new WaarpThreadFactory(
                "ScheduledTask"));
        // Init FiniteStates
        R66FiniteDualStates.initR66FiniteStates();
        if (!SystemPropertyUtil.isFileEncodingCorrect()) {
            logger.error("Issue while trying to set UTF-8 as default file encoding: use -Dfile.encoding=UTF-8 as java command argument");
            logger.warn("Currently file.encoding is: " + SystemPropertyUtil.get(SystemPropertyUtil.FILE_ENCODING));
        }
        setExecuteErrorBeforeTransferAllowed(SystemPropertyUtil.getBoolean(
                R66SystemProperties.OPENR66_EXECUTEBEFORETRANSFERRED, true));
        boolean useSpaceSeparator = SystemPropertyUtil.getBoolean(R66SystemProperties.OPENR66_USESPACESEPARATOR, false);
        if (useSpaceSeparator) {
            PartnerConfiguration.setSEPARATOR_FIELD(PartnerConfiguration.BLANK_SEPARATOR_FIELD);
        }
        setHostProxyfied(SystemPropertyUtil.getBoolean(R66SystemProperties.OPENR66_ISHOSTPROXYFIED, false));
        setWarnOnStartup(SystemPropertyUtil.getBoolean(R66SystemProperties.OPENR66_STARTUP_WARNING, true));
        FileBasedConfiguration.checkDatabase = SystemPropertyUtil.getBoolean(R66SystemProperties.OPENR66_STARTUP_DATABASE_CHECK, true);
        setChrootChecked(SystemPropertyUtil.getBoolean(R66SystemProperties.OPENR66_CHROOT_CHECKED, true));
        setBlacklistBadAuthent(SystemPropertyUtil.getBoolean(R66SystemProperties.OPENR66_BLACKLIST_BADAUTHENT, true));
        setMaxfilenamelength(SystemPropertyUtil.getInt(R66SystemProperties.OPENR66_FILENAME_MAXLENGTH, 255));
        setTimeStat(SystemPropertyUtil.getInt(R66SystemProperties.OPENR66_TRACE_STATS, 0));
        setLimitCache(SystemPropertyUtil.getInt(R66SystemProperties.OPENR66_CACHE_LIMIT, 20000));
        if (getLimitCache() <= 100) {
            setLimitCache(100);
        }
        setTimeLimitCache(SystemPropertyUtil.getLong(R66SystemProperties.OPENR66_CACHE_TIMELIMIT, 180000));
        if (getTimeLimitCache() < 1000) {
            setTimeLimitCache(1000);
        }
        DbTaskRunner.createLruCache(getLimitCache(), getTimeLimitCache());
        if (getLimitCache() > 0 && getTimeLimitCache() > 1000) {
            launchInFixedDelay(new CleanLruCache(), getTimeLimitCache(), TimeUnit.MILLISECONDS);
        }
        if (isHostProxyfied()) {
            setBlacklistBadAuthent(false);
        }
    }

    public String toString() {
        String rest = null;
        for (RestConfiguration config : getRestConfigurations()) {
            if (rest == null) {
                rest = (config.REST_ADDRESS != null ? "'" + config.REST_ADDRESS + ":" : "'All:") + config.REST_PORT
                        + "'";
            } else {
                rest += ", " + (config.REST_ADDRESS != null ? "'" + config.REST_ADDRESS + ":" : "'All:")
                        + config.REST_PORT + "'";
            }
        }
        return "Config: { ServerPort: " + getSERVER_PORT() + ", ServerSslPort: " + getSERVER_SSLPORT() + ", ServerView: "
                + getSERVER_HTTPPORT() + ", ServerAdmin: " + getSERVER_HTTPSPORT() +
                ", ThriftPort: " + (getThriftport() > 0 ? getThriftport() : "'NoThriftSupport'") + ", RestAddress: ["
                + (rest != null ? rest : "'NoRestSupport'") + "]" +
                ", TimeOut: " + getTIMEOUTCON() + ", BaseDir: '" + getBaseDirectory() + "', DigestAlgo: '" + getDigest().name
                + "', checkRemote: " + isCheckRemoteAddress() +
                ", checkClient: " + isCheckClientAddress() + ", snmpActive: " + (getAgentSnmp() != null) + ", chrootChecked: "
                + isChrootChecked() +
                ", blacklist: " + isBlacklistBadAuthent() + ", isHostProxified: " + isHostProxyfied() + "}";
    }

    /**
     * Configure the pipeline for client (to be called only once)
     */
    public void pipelineInit() {
        if (configured) {
            return;
        }
        workerGroup = new NioEventLoopGroup(getCLIENT_THREAD(), new WaarpThreadFactory("Worker"));
        handlerGroup = new NioEventLoopGroup(getCLIENT_THREAD(), new WaarpThreadFactory("Handler"));
        subTaskGroup = new NioEventLoopGroup(getCLIENT_THREAD(), new WaarpThreadFactory("SubTask"));
        localBossGroup = new NioEventLoopGroup(getCLIENT_THREAD(), new WaarpThreadFactory("LocalBoss"));
        localWorkerGroup = new NioEventLoopGroup(getCLIENT_THREAD(), new WaarpThreadFactory("LocalWorker"));
        localTransaction = new LocalTransaction();
        WaarpLoggerFactory.setDefaultFactory(WaarpLoggerFactory.getDefaultFactory());
        if (isWarnOnStartup()) {
            logger.warn("Server Thread: " + getSERVER_THREAD() + " Client Thread: " + getCLIENT_THREAD()
                    + " Runner Thread: " + getRUNNER_THREAD());
        } else {
            logger.info("Server Thread: " + getSERVER_THREAD() + " Client Thread: " + getCLIENT_THREAD()
                    + " Runner Thread: " + getRUNNER_THREAD());
        }
        logger.info("Current launched threads: " + ManagementFactory.getThreadMXBean().getThreadCount());
        if (isUseLocalExec()) {
            LocalExecClient.initialize();
        }
        configured = true;
    }

    public void serverPipelineInit() {
        bossGroup = new NioEventLoopGroup(getSERVER_THREAD(), new WaarpThreadFactory("Boss", false));
        httpBossGroup = new NioEventLoopGroup(getSERVER_THREAD(), new WaarpThreadFactory("HttpBoss"));
        httpWorkerGroup = new NioEventLoopGroup(getSERVER_THREAD() * 10, new WaarpThreadFactory("HttpWorker"));
    }

    /**
     * Startup the server
     * 
     * @throws WaarpDatabaseSqlException
     * @throws WaarpDatabaseNoConnectionException
     */
    public void serverStartup() throws WaarpDatabaseNoConnectionException,
            WaarpDatabaseSqlException {
        setServer(true);
        if (isBlacklistBadAuthent()) {
            setBlacklistBadAuthent(!DbHostAuth.hasProxifiedHosts(DbConstant.admin.getSession()));
        }
        getShutdownConfiguration().timeout = getTIMEOUTCON();
        if (getTimeLimitCache() < getTIMEOUTCON() * 10) {
            setTimeLimitCache(getTIMEOUTCON() * 10);
            DbTaskRunner.updateLruCacheTimeout(getTimeLimitCache());
        }
        R66ShutdownHook.addShutdownHook();
        logger.debug("Use NoSSL: " + isUseNOSSL() + " Use SSL: " + isUseSSL());
        if ((!isUseNOSSL()) && (!isUseSSL())) {
            logger.error(Messages.getString("Configuration.NoSSL")); //$NON-NLS-1$
            System.exit(-1);
        }
        pipelineInit();
        serverPipelineInit();
        r66Startup();
        startHttpSupport();
        startMonitoring();
        launchStatistics();
        startRestSupport();

        logger.info("Current launched threads: " + ManagementFactory.getThreadMXBean().getThreadCount());
    }

    /**
     * Used to log statistics information regularly
     */
    public void launchStatistics() {
        if (getTimeStat() > 0) {
            launchInFixedDelay(new UsageStatistic(), getTimeStat(), TimeUnit.SECONDS);
        }
    }

    public void r66Startup() throws WaarpDatabaseNoConnectionException, WaarpDatabaseSqlException {
        logger.info(Messages.getString("Configuration.Start") + getSERVER_PORT() + ":" + isUseNOSSL() + ":" + getHOST_ID() + //$NON-NLS-1$
                " " + getSERVER_SSLPORT() + ":" + isUseSSL() + ":" + getHOST_SSLID());
        // add into configuration
        this.getConstraintLimitHandler().setServer(true);
        // Global Server
        serverChannelGroup = new DefaultChannelGroup("OpenR66", subTaskGroup.next());
        if (isUseNOSSL()) {
            serverBootstrap = new ServerBootstrap();
            WaarpNettyUtil.setServerBootstrap(serverBootstrap, bossGroup, workerGroup, (int) getTIMEOUTCON());
            networkServerInitializer = new NetworkServerInitializer(true);
            serverBootstrap.childHandler(networkServerInitializer);
            ChannelFuture future = serverBootstrap.bind(new InetSocketAddress(getSERVER_PORT())).awaitUninterruptibly();
            if (future.isSuccess()) {
                bindNoSSL = future.channel();
                serverChannelGroup.add(bindNoSSL);
            } else {
                logger.warn(Messages.getString("Configuration.NOSSLDeactivated")); //$NON-NLS-1$
            }
        } else {
            networkServerInitializer = null;
            logger.warn(Messages.getString("Configuration.NOSSLDeactivated")); //$NON-NLS-1$
        }

        if (isUseSSL() && getHOST_SSLID() != null) {
            serverSslBootstrap = new ServerBootstrap();
            WaarpNettyUtil.setServerBootstrap(serverSslBootstrap, bossGroup, workerGroup, (int) getTIMEOUTCON());
            networkSslServerInitializer = new NetworkSslServerInitializer(false);
            serverSslBootstrap.childHandler(networkSslServerInitializer);
            ChannelFuture future = serverSslBootstrap.bind(new InetSocketAddress(getSERVER_SSLPORT()))
                    .awaitUninterruptibly();
            if (future.isSuccess()) {
                bindSSL = future.channel();
                serverChannelGroup.add(bindSSL);
            } else {
                logger.warn(Messages.getString("Configuration.SSLMODEDeactivated")); //$NON-NLS-1$
            }
        } else {
            networkSslServerInitializer = null;
            logger.warn(Messages.getString("Configuration.SSLMODEDeactivated")); //$NON-NLS-1$
        }

        // Factory for TrafficShapingHandler
        globalTrafficShapingHandler = new GlobalTrafficHandler(subTaskGroup, getServerGlobalWriteLimit(),
                getServerGlobalReadLimit(), getServerChannelWriteLimit(), getServerChannelReadLimit(), getDelayLimit());
        this.getConstraintLimitHandler().setHandler(globalTrafficShapingHandler);

        // Now start the InternalRunner
        internalRunner = new InternalRunner();

        if (getThriftport() > 0) {
            setThriftService(new R66ThriftServerService(new WaarpFuture(true), getThriftport()));
            execOtherWorker.execute(getThriftService());
            getThriftService().awaitInitialization();
        } else {
            setThriftService(null);
        }
    }

    public void startHttpSupport() {
        // Now start the HTTP support
        logger.info(Messages.getString("Configuration.HTTPStart") + getSERVER_HTTPPORT() + //$NON-NLS-1$
                " HTTPS: " + getSERVER_HTTPSPORT());
        httpChannelGroup = new DefaultChannelGroup("HttpOpenR66", subTaskGroup.next());
        // Configure the server.
        httpBootstrap = new ServerBootstrap();
        WaarpNettyUtil.setServerBootstrap(httpBootstrap, httpBossGroup, httpWorkerGroup, (int) getTIMEOUTCON());
        // Set up the event pipeline factory.
        httpBootstrap.childHandler(new HttpInitializer(isUseHttpCompression()));
        // Bind and start to accept incoming connections.
        if (getSERVER_HTTPPORT() > 0) {
            ChannelFuture future = httpBootstrap.bind(new InetSocketAddress(getSERVER_HTTPPORT())).awaitUninterruptibly();
            if (future.isSuccess()) {
                httpChannelGroup.add(future.channel());
            }
        }
        // Now start the HTTPS support
        // Configure the server.
        httpsBootstrap = new ServerBootstrap();
        // Set up the event pipeline factory.
        WaarpNettyUtil.setServerBootstrap(httpsBootstrap, httpBossGroup, httpWorkerGroup, (int) getTIMEOUTCON());
        if (getHttpModel() == 0) {
            httpsBootstrap.childHandler(new HttpSslInitializer(isUseHttpCompression()));
        } else {
            // Default
            httpsBootstrap.childHandler(new HttpReponsiveSslInitializer(isUseHttpCompression()));
        }
        // Bind and start to accept incoming connections.
        if (getSERVER_HTTPSPORT() > 0) {
            ChannelFuture future = httpsBootstrap.bind(new InetSocketAddress(getSERVER_HTTPSPORT())).awaitUninterruptibly();
            if (future.isSuccess()) {
                httpChannelGroup.add(future.channel());
            }
        }
    }

    public void startRestSupport() {
        HttpRestR66Handler.initialize(getBaseDirectory() + "/" + getWorkingPath() + "/httptemp");
        for (RestConfiguration config : getRestConfigurations()) {
            HttpRestR66Handler.initializeService(config);
            logger.info(Messages.getString("Configuration.HTTPStart") + " (REST Support) " + config.toString());
        }
    }

    public void startMonitoring() throws WaarpDatabaseSqlException {
        setMonitoring(new Monitoring(getPastLimit(), getMinimalDelay(), null));
        setNBDBSESSION(getNBDBSESSION() + 1);
        if (getSnmpConfig() != null) {
            int snmpPortShow = (isUseNOSSL() ? getSERVER_PORT() : getSERVER_SSLPORT());
            R66PrivateMib r66Mib =
                    new R66PrivateMib(SnmpName,
                            snmpPortShow,
                            SnmpPrivateId,
                            SnmpR66Id,
                            SnmpDefaultAuthor,
                            SnmpVersion,
                            SnmpDefaultLocalization,
                            SnmpService);
            WaarpMOFactory.setFactory(new R66VariableFactory());
            setAgentSnmp(new WaarpSnmpAgent(new File(getSnmpConfig()), getMonitoring(), r66Mib));
            try {
                getAgentSnmp().start();
            } catch (IOException e) {
                throw new WaarpDatabaseSqlException(Messages.getString("Configuration.SNMPError"), e); //$NON-NLS-1$
            }
            this.setR66Mib(r66Mib);
        }
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
        if (getThriftService() != null) {
            getThriftService().releaseResources();
        }
        if (internalRunner != null) {
            internalRunner.prepareStopInternalRunner();
        }
    }

    /**
     * Unbind network connectors
     */
    public void unbindServer() {
        if (bindNoSSL != null) {
            bindNoSSL.close();
            bindNoSSL = null;
        }
        if (bindSSL != null) {
            bindSSL.close();
            bindSSL = null;
        }
    }

    public void shutdownGracefully() {
        if (bossGroup != null && !bossGroup.isShuttingDown()) {
            bossGroup.shutdownGracefully();
        }
        if (workerGroup != null && !workerGroup.isShuttingDown()) {
            workerGroup.shutdownGracefully();
        }
        if (handlerGroup != null && !handlerGroup.isShuttingDown()) {
            handlerGroup.shutdownGracefully();
        }
        if (httpBossGroup != null && !httpBossGroup.isShuttingDown()) {
            httpBossGroup.shutdownGracefully();
        }
        if (httpWorkerGroup != null && !httpWorkerGroup.isShuttingDown()) {
            httpWorkerGroup.shutdownGracefully();
        }
        if (handlerGroup != null && !handlerGroup.isShuttingDown()) {
            handlerGroup.shutdownGracefully();
        }
        if (subTaskGroup != null && !subTaskGroup.isShuttingDown()) {
            subTaskGroup.shutdownGracefully();
        }
        if (localBossGroup != null && !localBossGroup.isShuttingDown()) {
            localBossGroup.shutdownGracefully();
        }
        if (localWorkerGroup != null && !localWorkerGroup.isShuttingDown()) {
            localWorkerGroup.shutdownGracefully();
        }
    }

    public void shutdownQuickly() {
        if (bossGroup != null && !bossGroup.isShuttingDown()) {
            bossGroup.shutdownGracefully(10, 10, TimeUnit.MILLISECONDS);
        }
        if (workerGroup != null && !workerGroup.isShuttingDown()) {
            workerGroup.shutdownGracefully(10, 10, TimeUnit.MILLISECONDS);
        }
        if (handlerGroup != null && !handlerGroup.isShuttingDown()) {
            handlerGroup.shutdownGracefully(10, 10, TimeUnit.MILLISECONDS);
        }
        if (httpBossGroup != null && !httpBossGroup.isShuttingDown()) {
            httpBossGroup.shutdownGracefully(10, 10, TimeUnit.MILLISECONDS);
        }
        if (httpWorkerGroup != null && !httpWorkerGroup.isShuttingDown()) {
            httpWorkerGroup.shutdownGracefully(10, 10, TimeUnit.MILLISECONDS);
        }
        if (handlerGroup != null && !handlerGroup.isShuttingDown()) {
            handlerGroup.shutdownGracefully(10, 10, TimeUnit.MILLISECONDS);
        }
        if (subTaskGroup != null && !subTaskGroup.isShuttingDown()) {
            subTaskGroup.shutdownGracefully(10, 10, TimeUnit.MILLISECONDS);
        }
        if (localBossGroup != null && !localBossGroup.isShuttingDown()) {
            localBossGroup.shutdownGracefully(10, 10, TimeUnit.MILLISECONDS);
        }
        if (localWorkerGroup != null && !localWorkerGroup.isShuttingDown()) {
            localWorkerGroup.shutdownGracefully(10, 10, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Stops the server
     * 
     * To be called after all other stuff are closed (channels, connections)
     */
    public void serverStop() {
        WaarpSslUtility.forceCloseAllSslChannels();
        if (internalRunner != null) {
            internalRunner.stopInternalRunner();
        }
        if (scheduledExecutorService != null) {
            scheduledExecutorService.shutdown();
        }
        if (getAgentSnmp() != null) {
            getAgentSnmp().stop();
        } else if (getMonitoring() != null) {
            getMonitoring().releaseResources();
            setMonitoring(null);
        }
        shutdownGracefully();
        if (execOtherWorker != null) {
            execOtherWorker.shutdownNow();
        }
        if (timerCloseOperations != null) {
            timerCloseOperations.stop();
        }
    }

    /**
     * To be called after all other stuff are closed for Client
     */
    public void clientStop() {
        clientStop(true);
    }
    /**
     * To be called after all other stuff are closed for Client
     * @param shutdownQuickly For client only, shall be true to speedup the end of the process
     */
    public void clientStop(boolean shutdownQuickly) {
        WaarpSslUtility.forceCloseAllSslChannels();
        if (!Configuration.configuration.isServer()) {
            ChannelUtils.stopLogger();
        }
        if (scheduledExecutorService != null) {
            scheduledExecutorService.shutdown();
        }
        if (localTransaction != null) {
            localTransaction.closeAll();
            localTransaction = null;
        }
        if (shutdownQuickly) {
            
        } else {
            shutdownGracefully();
        }
        if (isUseLocalExec()) {
            LocalExecClient.releaseResources();
        }
        if (timerCloseOperations != null) {
            timerCloseOperations.stop();
        }
        getR66BusinessFactory().releaseResources();
    }

    /**
     * Try to reload the Commander
     * 
     * @return True if reloaded, else in error
     */
    public boolean reloadCommanderDelay() {
        if (internalRunner != null) {
            try {
                internalRunner.reloadInternalRunner();
                return true;
            } catch (WaarpDatabaseNoConnectionException e) {
            } catch (WaarpDatabaseSqlException e) {
            }
        }
        return false;
    }

    /**
     * submit a task in a fixed delay
     * 
     * @param thread
     * @param delay
     * @param unit
     */
    public void launchInFixedDelay(Thread thread, long delay, TimeUnit unit) {
        scheduledExecutorService.schedule(thread, delay, unit);
    }

    /**
     * Reset the global monitor for bandwidth limitation and change future channel monitors
     * 
     * @param writeGlobalLimit
     * @param readGlobalLimit
     * @param writeSessionLimit
     * @param readSessionLimit
     * @param delayLimit
     */
    public void changeNetworkLimit(long writeGlobalLimit, long readGlobalLimit,
            long writeSessionLimit, long readSessionLimit, long delayLimit) {
        long newWriteLimit = writeGlobalLimit > 1024 ? writeGlobalLimit
                : getServerGlobalWriteLimit();
        if (writeGlobalLimit <= 0) {
            newWriteLimit = 0;
        }
        long newReadLimit = readGlobalLimit > 1024 ? readGlobalLimit : getServerGlobalReadLimit();
        if (readGlobalLimit <= 0) {
            newReadLimit = 0;
        }
        setServerGlobalReadLimit(newReadLimit);
        setServerGlobalWriteLimit(newWriteLimit);
        this.setDelayLimit(delayLimit);
        if (globalTrafficShapingHandler != null) {
            globalTrafficShapingHandler.configure(getServerGlobalWriteLimit(), getServerGlobalReadLimit(), delayLimit);
            logger.warn(Messages.getString("Configuration.BandwidthChange"), globalTrafficShapingHandler); //$NON-NLS-1$
        }
        newWriteLimit = writeSessionLimit > 1024 ? writeSessionLimit
                : getServerChannelWriteLimit();
        if (writeSessionLimit <= 0) {
            newWriteLimit = 0;
        }
        newReadLimit = readSessionLimit > 1024 ? readSessionLimit
                : getServerChannelReadLimit();
        if (readSessionLimit <= 0) {
            newReadLimit = 0;
        }
        setServerChannelReadLimit(newReadLimit);
        setServerChannelWriteLimit(newWriteLimit);
        if (globalTrafficShapingHandler != null && globalTrafficShapingHandler instanceof GlobalChannelTrafficShapingHandler) {
            ((GlobalChannelTrafficShapingHandler) globalTrafficShapingHandler).configureChannel(getServerChannelWriteLimit(), getServerChannelReadLimit());
        }
        setAnyBandwidthLimitation((getServerGlobalReadLimit() > 0 || getServerGlobalWriteLimit() > 0 ||
                getServerChannelReadLimit() > 0 || getServerChannelWriteLimit() > 0));
    }

    /**
     * Compute number of threads for both client and server from the real number of available
     * processors (double + 1) if the value is less than 32 threads else (available +1).
     */
    public void computeNbThreads() {
        int nb = Runtime.getRuntime().availableProcessors() * 2 + 1;
        if (nb > 32) {
            nb = Runtime.getRuntime().availableProcessors() + 1;
        }
        if (getSERVER_THREAD() <= 0 || getSERVER_THREAD() > nb) {
            logger.info(Messages.getString("Configuration.ThreadNumberChange") + nb); //$NON-NLS-1$
            setSERVER_THREAD(nb);
            setCLIENT_THREAD(getSERVER_THREAD() * 10);
        } else if (getCLIENT_THREAD() < nb) {
            setCLIENT_THREAD(nb);
        }
    }

    /**
     * @return a new ChannelTrafficShapingHandler
     * @throws OpenR66ProtocolNoDataException
     */
    public ChannelTrafficShapingHandler newChannelTrafficShapingHandler()
            throws OpenR66ProtocolNoDataException {
        if (getServerChannelReadLimit() == 0 && getServerChannelWriteLimit() == 0) {
            throw new OpenR66ProtocolNoDataException(Messages.getString("Configuration.ExcNoLimit")); //$NON-NLS-1$
        }
        if (globalTrafficShapingHandler instanceof GlobalChannelTrafficShapingHandler) {
            throw new OpenR66ProtocolNoDataException("Already included through GlobalChannelTSH");
        }
        return new ChannelTrafficHandler(getServerChannelWriteLimit(), getServerChannelReadLimit(), getDelayLimit());
    }

    /**
     * 
     * @return an executorService to be used for any thread
     */
    public ExecutorService getExecutorService() {
        return execOtherWorker;
    }

    public Timer getTimerClose() {
        return timerCloseOperations;
    }

    /**
     * @return the globalTrafficShapingHandler
     */
    public GlobalTrafficHandler getGlobalTrafficShapingHandler() {
        return globalTrafficShapingHandler;
    }

    /**
     * @return the serverChannelGroup
     */
    public ChannelGroup getServerChannelGroup() {
        return serverChannelGroup;
    }

    /**
     * @return the httpChannelGroup
     */
    public ChannelGroup getHttpChannelGroup() {
        return httpChannelGroup;
    }

    /**
     * @return the serverPipelineExecutor
     */
    public EventLoopGroup getNetworkWorkerGroup() {
        return workerGroup;
    }

    /**
     * @return the localBossGroup
     */
    public EventLoopGroup getLocalBossGroup() {
        return localBossGroup;
    }

    /**
     * @return the localWorkerGroup
     */
    public EventLoopGroup getLocalWorkerGroup() {
        return localWorkerGroup;
    }

    /**
     * @return the serverPipelineExecutor
     */
    public EventLoopGroup getHandlerGroup() {
        return handlerGroup;
    }

    /**
     * @return the subTaskGroup
     */
    public EventLoopGroup getSubTaskGroup() {
        return subTaskGroup;
    }

    /**
     * @return the httpBossGroup
     */
    public EventLoopGroup getHttpBossGroup() {
        return httpBossGroup;
    }

    /**
     * @return the httpWorkerGroup
     */
    public EventLoopGroup getHttpWorkerGroup() {
        return httpWorkerGroup;
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
        return FilesystemBasedDigest.equalPasswd(SERVERADMINKEY, newkey);
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
            if (getHOST_SSLID() == null) {
                throw new OpenR66ProtocolNoSslException(Messages.getString("Configuration.ExcNoSSL")); //$NON-NLS-1$
            }
            return getHOST_SSLID();
        } else {
            return getHOST_ID();
        }
    }

    /**
     * 
     * @param dbSession
     * @param remoteHost
     * @return the HostId according to remoteHost (and its SSL status)
     * @throws WaarpDatabaseException
     */
    public String getHostId(DbSession dbSession, String remoteHost) throws WaarpDatabaseException {
        DbHostAuth hostAuth = new DbHostAuth(dbSession, remoteHost);
        try {
            return Configuration.configuration.getHostId(hostAuth.isSsl());
        } catch (OpenR66ProtocolNoSslException e) {
            throw new WaarpDatabaseException(e);
        }
    }

    private static class UsageStatistic extends Thread {

        @Override
        public void run() {
            logger.warn(hashStatus());
            Configuration.configuration.launchInFixedDelay(this, 10, TimeUnit.SECONDS);
        }

    }

    public static String hashStatus() {
        String result = "\n";
        try {
            result += configuration.localTransaction.hashStatus() + "\n";
        } catch (Exception e) {
            logger.warn("Issue while debugging", e);
        }
        try {
            result += ClientRunner.hashStatus() + "\n";
        } catch (Exception e) {
            logger.warn("Issue while debugging", e);
        }
        try {
            result += DbTaskRunner.hashStatus() + "\n";
        } catch (Exception e) {
            logger.warn("Issue while debugging", e);
        }
        try {
            result += HttpSslHandler.hashStatus() + "\n";
        } catch (Exception e) {
            logger.warn("Issue while debugging", e);
        }
        try {
            result += NetworkTransaction.hashStatus();
        } catch (Exception e) {
            logger.warn("Issue while debugging", e);
        }
        return result;
    }

    /**
     * @return the nBDBSESSION
     */
    public static int getNBDBSESSION() {
        return NBDBSESSION;
    }

    /**
     * @param nBDBSESSION the nBDBSESSION to set
     */
    public static void setNBDBSESSION(int nBDBSESSION) {
        NBDBSESSION = nBDBSESSION;
    }

    /**
     * @return the rANKRESTART
     */
    public static int getRANKRESTART() {
        return RANKRESTART;
    }

    /**
     * @param rANKRESTART the rANKRESTART to set
     */
    public static void setRANKRESTART(int rANKRESTART) {
        RANKRESTART = rANKRESTART;
    }

    /**
     * @return the iSUNIX
     */
    public static boolean isISUNIX() {
        return ISUNIX;
    }

    /**
     * @param iSUNIX the iSUNIX to set
     */
    public static void setISUNIX(boolean iSUNIX) {
        ISUNIX = iSUNIX;
    }

    /**
     * @return the r66BusinessFactory
     */
    public R66BusinessFactoryInterface getR66BusinessFactory() {
        return r66BusinessFactory;
    }

    /**
     * @return the extendedProtocol
     */
    public boolean isExtendedProtocol() {
        return extendedProtocol;
    }

    /**
     * @param extendedProtocol the extendedProtocol to set
     */
    public void setExtendedProtocol(boolean extendedProtocol) {
        this.extendedProtocol = extendedProtocol;
    }

    /**
     * @return the globalDigest
     */
    public boolean isGlobalDigest() {
        return globalDigest;
    }

    /**
     * @param globalDigest the globalDigest to set
     */
    public void setGlobalDigest(boolean globalDigest) {
        this.globalDigest = globalDigest;
    }

    /**
     * @return the businessWhiteSet
     */
    public HashSet<String> getBusinessWhiteSet() {
        return businessWhiteSet;
    }

    /**
     * @return the roles
     */
    public HashMap<String, RoleDefault> getRoles() {
        return roles;
    }

    /**
     * @return the aliases
     */
    public HashMap<String, String> getAliases() {
        return aliases;
    }

    /**
     * @return the reverseAliases
     */
    public HashMap<String, String[]> getReverseAliases() {
        return reverseAliases;
    }

    /**
     * @return the versions
     */
    public ConcurrentHashMap<String, PartnerConfiguration> getVersions() {
        return versions;
    }

    /**
     * @return the hOST_ID
     */
    public String getHOST_ID() {
        return HOST_ID;
    }

    /**
     * @param hOST_ID the hOST_ID to set
     */
    public void setHOST_ID(String hOST_ID) {
        HOST_ID = hOST_ID;
    }

    /**
     * @return the hOST_SSLID
     */
    public String getHOST_SSLID() {
        return HOST_SSLID;
    }

    /**
     * @param hOST_SSLID the hOST_SSLID to set
     */
    public void setHOST_SSLID(String hOST_SSLID) {
        HOST_SSLID = hOST_SSLID;
    }

    /**
     * @return the aDMINNAME
     */
    public String getADMINNAME() {
        return ADMINNAME;
    }

    /**
     * @param aDMINNAME the aDMINNAME to set
     */
    public void setADMINNAME(String aDMINNAME) {
        ADMINNAME = aDMINNAME;
    }

    /**
     * @return the serverKeyFile
     */
    public String getServerKeyFile() {
        return serverKeyFile;
    }

    /**
     * @param serverKeyFile the serverKeyFile to set
     */
    public void setServerKeyFile(String serverKeyFile) {
        this.serverKeyFile = serverKeyFile;
    }

    /**
     * @return the hOST_AUTH
     */
    public DbHostAuth getHOST_AUTH() {
        return HOST_AUTH;
    }

    /**
     * @param hOST_AUTH the hOST_AUTH to set
     */
    public void setHOST_AUTH(DbHostAuth hOST_AUTH) {
        HOST_AUTH = hOST_AUTH;
    }

    /**
     * @return the hOST_SSLAUTH
     */
    public DbHostAuth getHOST_SSLAUTH() {
        return HOST_SSLAUTH;
    }

    /**
     * @param hOST_SSLAUTH the hOST_SSLAUTH to set
     */
    public void setHOST_SSLAUTH(DbHostAuth hOST_SSLAUTH) {
        HOST_SSLAUTH = hOST_SSLAUTH;
    }

    /**
     * @return the sERVER_THREAD
     */
    public int getSERVER_THREAD() {
        return SERVER_THREAD;
    }

    /**
     * @param sERVER_THREAD the sERVER_THREAD to set
     */
    public void setSERVER_THREAD(int sERVER_THREAD) {
        SERVER_THREAD = sERVER_THREAD;
    }

    /**
     * @return the cLIENT_THREAD
     */
    public int getCLIENT_THREAD() {
        return CLIENT_THREAD;
    }

    /**
     * @param cLIENT_THREAD the cLIENT_THREAD to set
     */
    public void setCLIENT_THREAD(int cLIENT_THREAD) {
        CLIENT_THREAD = cLIENT_THREAD;
    }

    /**
     * @return the dEFAULT_SESSION_LIMIT
     */
    public long getDEFAULT_SESSION_LIMIT() {
        return DEFAULT_SESSION_LIMIT;
    }

    /**
     * @return the dEFAULT_GLOBAL_LIMIT
     */
    public long getDEFAULT_GLOBAL_LIMIT() {
        return DEFAULT_GLOBAL_LIMIT;
    }

    /**
     * @return the sERVER_PORT
     */
    public int getSERVER_PORT() {
        return SERVER_PORT;
    }

    /**
     * @param sERVER_PORT the sERVER_PORT to set
     */
    public void setSERVER_PORT(int sERVER_PORT) {
        SERVER_PORT = sERVER_PORT;
    }

    /**
     * @return the sERVER_SSLPORT
     */
    public int getSERVER_SSLPORT() {
        return SERVER_SSLPORT;
    }

    /**
     * @param sERVER_SSLPORT the sERVER_SSLPORT to set
     */
    public void setSERVER_SSLPORT(int sERVER_SSLPORT) {
        SERVER_SSLPORT = sERVER_SSLPORT;
    }

    /**
     * @return the sERVER_HTTPPORT
     */
    public int getSERVER_HTTPPORT() {
        return SERVER_HTTPPORT;
    }

    /**
     * @param sERVER_HTTPPORT the sERVER_HTTPPORT to set
     */
    public void setSERVER_HTTPPORT(int sERVER_HTTPPORT) {
        SERVER_HTTPPORT = sERVER_HTTPPORT;
    }

    /**
     * @return the sERVER_HTTPSPORT
     */
    public int getSERVER_HTTPSPORT() {
        return SERVER_HTTPSPORT;
    }

    /**
     * @param sERVER_HTTPSPORT the sERVER_HTTPSPORT to set
     */
    public void setSERVER_HTTPSPORT(int sERVER_HTTPSPORT) {
        SERVER_HTTPSPORT = sERVER_HTTPSPORT;
    }

    /**
     * @return the tIMEOUTCON
     */
    public long getTIMEOUTCON() {
        return TIMEOUTCON;
    }

    /**
     * @param tIMEOUTCON the tIMEOUTCON to set
     */
    public void setTIMEOUTCON(long tIMEOUTCON) {
        TIMEOUTCON = tIMEOUTCON;
    }

    /**
     * @return the bLOCKSIZE
     */
    public int getBLOCKSIZE() {
        return BLOCKSIZE;
    }

    /**
     * @param bLOCKSIZE the bLOCKSIZE to set
     */
    public void setBLOCKSIZE(int bLOCKSIZE) {
        BLOCKSIZE = bLOCKSIZE;
    }

    /**
     * @return the maxGlobalMemory
     */
    public long getMaxGlobalMemory() {
        return maxGlobalMemory;
    }

    /**
     * @param maxGlobalMemory the maxGlobalMemory to set
     */
    public void setMaxGlobalMemory(long maxGlobalMemory) {
        this.maxGlobalMemory = maxGlobalMemory;
    }

    /**
     * @return the restConfigurations
     */
    public List<RestConfiguration> getRestConfigurations() {
        return restConfigurations;
    }

    /**
     * @return the baseDirectory
     */
    public String getBaseDirectory() {
        return baseDirectory;
    }

    /**
     * @param baseDirectory the baseDirectory to set
     */
    public void setBaseDirectory(String baseDirectory) {
        this.baseDirectory = baseDirectory;
    }

    /**
     * @return the inPath
     */
    public String getInPath() {
        return inPath;
    }

    /**
     * @param inPath the inPath to set
     */
    public void setInPath(String inPath) {
        this.inPath = inPath;
    }

    /**
     * @return the outPath
     */
    public String getOutPath() {
        return outPath;
    }

    /**
     * @param outPath the outPath to set
     */
    public void setOutPath(String outPath) {
        this.outPath = outPath;
    }

    /**
     * @return the archivePath
     */
    public String getArchivePath() {
        return archivePath;
    }

    /**
     * @param archivePath the archivePath to set
     */
    public void setArchivePath(String archivePath) {
        this.archivePath = archivePath;
    }

    /**
     * @return the workingPath
     */
    public String getWorkingPath() {
        return workingPath;
    }

    /**
     * @param workingPath the workingPath to set
     */
    public void setWorkingPath(String workingPath) {
        this.workingPath = workingPath;
    }

    /**
     * @return the configPath
     */
    public String getConfigPath() {
        return configPath;
    }

    /**
     * @param configPath the configPath to set
     */
    public void setConfigPath(String configPath) {
        this.configPath = configPath;
    }

    /**
     * @return the httpBasePath
     */
    public String getHttpBasePath() {
        return httpBasePath;
    }

    /**
     * @param httpBasePath the httpBasePath to set
     */
    public void setHttpBasePath(String httpBasePath) {
        this.httpBasePath = httpBasePath;
    }

    /**
     * @return the httpModel
     */
    public int getHttpModel() {
        return httpModel;
    }

    /**
     * @param httpModel the httpModel to set
     */
    public void setHttpModel(int httpModel) {
        this.httpModel = httpModel;
    }

    /**
     * @return the isShutdown
     */
    public boolean isShutdown() {
        return isShutdown;
    }

    /**
     * @param isShutdown the isShutdown to set
     */
    public void setShutdown(boolean isShutdown) {
        this.isShutdown = isShutdown;
    }

    /**
     * @return the serverGlobalWriteLimit
     */
    public long getServerGlobalWriteLimit() {
        return serverGlobalWriteLimit;
    }

    /**
     * @param serverGlobalWriteLimit the serverGlobalWriteLimit to set
     */
    public void setServerGlobalWriteLimit(long serverGlobalWriteLimit) {
        this.serverGlobalWriteLimit = serverGlobalWriteLimit;
    }

    /**
     * @return the serverGlobalReadLimit
     */
    public long getServerGlobalReadLimit() {
        return serverGlobalReadLimit;
    }

    /**
     * @param serverGlobalReadLimit the serverGlobalReadLimit to set
     */
    public void setServerGlobalReadLimit(long serverGlobalReadLimit) {
        this.serverGlobalReadLimit = serverGlobalReadLimit;
    }

    /**
     * @return the serverChannelWriteLimit
     */
    public long getServerChannelWriteLimit() {
        return serverChannelWriteLimit;
    }

    /**
     * @param serverChannelWriteLimit the serverChannelWriteLimit to set
     */
    public void setServerChannelWriteLimit(long serverChannelWriteLimit) {
        this.serverChannelWriteLimit = serverChannelWriteLimit;
    }

    /**
     * @return the serverChannelReadLimit
     */
    public long getServerChannelReadLimit() {
        return serverChannelReadLimit;
    }

    /**
     * @param serverChannelReadLimit the serverChannelReadLimit to set
     */
    public void setServerChannelReadLimit(long serverChannelReadLimit) {
        this.serverChannelReadLimit = serverChannelReadLimit;
    }

    /**
     * @return the anyBandwidthLimitation
     */
    public boolean isAnyBandwidthLimitation() {
        return anyBandwidthLimitation;
    }

    /**
     * @param anyBandwidthLimitation the anyBandwidthLimitation to set
     */
    public void setAnyBandwidthLimitation(boolean anyBandwidthLimitation) {
        this.anyBandwidthLimitation = anyBandwidthLimitation;
    }

    /**
     * @return the delayLimit
     */
    public long getDelayLimit() {
        return delayLimit;
    }

    /**
     * @param delayLimit the delayLimit to set
     */
    public void setDelayLimit(long delayLimit) {
        this.delayLimit = delayLimit;
    }

    /**
     * @return the useSSL
     */
    public boolean isUseSSL() {
        return useSSL;
    }

    /**
     * @param useSSL the useSSL to set
     */
    public void setUseSSL(boolean useSSL) {
        this.useSSL = useSSL;
    }

    /**
     * @return the useNOSSL
     */
    public boolean isUseNOSSL() {
        return useNOSSL;
    }

    /**
     * @param useNOSSL the useNOSSL to set
     */
    public void setUseNOSSL(boolean useNOSSL) {
        this.useNOSSL = useNOSSL;
    }

    /**
     * @return the digest
     */
    public FilesystemBasedDigest.DigestAlgo getDigest() {
        return digest;
    }

    /**
     * @param digest the digest to set
     */
    public void setDigest(FilesystemBasedDigest.DigestAlgo digest) {
        this.digest = digest;
    }

    /**
     * @return the useHttpCompression
     */
    public boolean isUseHttpCompression() {
        return useHttpCompression;
    }

    /**
     * @param useHttpCompression the useHttpCompression to set
     */
    public void setUseHttpCompression(boolean useHttpCompression) {
        this.useHttpCompression = useHttpCompression;
    }

    /**
     * @return the cryptoKey
     */
    public Des getCryptoKey() {
        return cryptoKey;
    }

    /**
     * @param cryptoKey the cryptoKey to set
     */
    public void setCryptoKey(Des cryptoKey) {
        this.cryptoKey = cryptoKey;
    }

    /**
     * @return the cryptoFile
     */
    public String getCryptoFile() {
        return cryptoFile;
    }

    /**
     * @param cryptoFile the cryptoFile to set
     */
    public void setCryptoFile(String cryptoFile) {
        this.cryptoFile = cryptoFile;
    }

    /**
     * @return the useLocalExec
     */
    public boolean isUseLocalExec() {
        return useLocalExec;
    }

    /**
     * @param useLocalExec the useLocalExec to set
     */
    public void setUseLocalExec(boolean useLocalExec) {
        this.useLocalExec = useLocalExec;
    }

    /**
     * @return the isServer
     */
    public boolean isServer() {
        return isServer;
    }

    /**
     * @param isServer the isServer to set
     */
    protected void setServer(boolean isServer) {
        this.isServer = isServer;
    }

    /**
     * @return the rUNNER_THREAD
     */
    public int getRUNNER_THREAD() {
        return RUNNER_THREAD;
    }

    /**
     * @param rUNNER_THREAD the rUNNER_THREAD to set
     */
    public void setRUNNER_THREAD(int rUNNER_THREAD) {
        RUNNER_THREAD = rUNNER_THREAD;
    }

    /**
     * @return the delayCommander
     */
    public long getDelayCommander() {
        return delayCommander;
    }

    /**
     * @param delayCommander the delayCommander to set
     */
    public void setDelayCommander(long delayCommander) {
        this.delayCommander = delayCommander;
    }

    /**
     * @return the delayRetry
     */
    public long getDelayRetry() {
        return delayRetry;
    }

    /**
     * @param delayRetry the delayRetry to set
     */
    public void setDelayRetry(long delayRetry) {
        this.delayRetry = delayRetry;
    }

    /**
     * @return the constraintLimitHandler
     */
    public R66ConstraintLimitHandler getConstraintLimitHandler() {
        return constraintLimitHandler;
    }

    /**
     * @param constraintLimitHandler the constraintLimitHandler to set
     */
    public void setConstraintLimitHandler(R66ConstraintLimitHandler constraintLimitHandler) {
        this.constraintLimitHandler = constraintLimitHandler;
    }

    /**
     * @return the checkRemoteAddress
     */
    public boolean isCheckRemoteAddress() {
        return checkRemoteAddress;
    }

    /**
     * @param checkRemoteAddress the checkRemoteAddress to set
     */
    public void setCheckRemoteAddress(boolean checkRemoteAddress) {
        this.checkRemoteAddress = checkRemoteAddress;
    }

    /**
     * @return the checkClientAddress
     */
    public boolean isCheckClientAddress() {
        return checkClientAddress;
    }

    /**
     * @param checkClientAddress the checkClientAddress to set
     */
    public void setCheckClientAddress(boolean checkClientAddress) {
        this.checkClientAddress = checkClientAddress;
    }

    /**
     * @return the saveTaskRunnerWithNoDb
     */
    public boolean isSaveTaskRunnerWithNoDb() {
        return saveTaskRunnerWithNoDb;
    }

    /**
     * @param saveTaskRunnerWithNoDb the saveTaskRunnerWithNoDb to set
     */
    public void setSaveTaskRunnerWithNoDb(boolean saveTaskRunnerWithNoDb) {
        this.saveTaskRunnerWithNoDb = saveTaskRunnerWithNoDb;
    }

    /**
     * @return the multipleMonitors
     */
    public int getMultipleMonitors() {
        return multipleMonitors;
    }

    /**
     * @param multipleMonitors the multipleMonitors to set
     */
    public void setMultipleMonitors(int multipleMonitors) {
        this.multipleMonitors = multipleMonitors;
    }

    /**
     * @return the monitoring
     */
    public Monitoring getMonitoring() {
        return monitoring;
    }

    /**
     * @param monitoring the monitoring to set
     */
    public void setMonitoring(Monitoring monitoring) {
        this.monitoring = monitoring;
    }

    /**
     * @return the pastLimit
     */
    public long getPastLimit() {
        return pastLimit;
    }

    /**
     * @param pastLimit the pastLimit to set
     */
    public void setPastLimit(long pastLimit) {
        this.pastLimit = pastLimit;
    }

    /**
     * @return the minimalDelay
     */
    public long getMinimalDelay() {
        return minimalDelay;
    }

    /**
     * @param minimalDelay the minimalDelay to set
     */
    public void setMinimalDelay(long minimalDelay) {
        this.minimalDelay = minimalDelay;
    }

    /**
     * @return the snmpConfig
     */
    public String getSnmpConfig() {
        return snmpConfig;
    }

    /**
     * @param snmpConfig the snmpConfig to set
     */
    public void setSnmpConfig(String snmpConfig) {
        this.snmpConfig = snmpConfig;
    }

    /**
     * @return the agentSnmp
     */
    public WaarpSnmpAgent getAgentSnmp() {
        return agentSnmp;
    }

    /**
     * @param agentSnmp the agentSnmp to set
     */
    public void setAgentSnmp(WaarpSnmpAgent agentSnmp) {
        this.agentSnmp = agentSnmp;
    }

    /**
     * @return the r66Mib
     */
    public R66PrivateMib getR66Mib() {
        return r66Mib;
    }

    /**
     * @param r66Mib the r66Mib to set
     */
    public void setR66Mib(R66PrivateMib r66Mib) {
        this.r66Mib = r66Mib;
    }

    /**
     * @return the waarpSecureKeyStore
     */
    public static WaarpSecureKeyStore getWaarpSecureKeyStore() {
        return waarpSecureKeyStore;
    }

    /**
     * @param waarpSecureKeyStore the waarpSecureKeyStore to set
     */
    public static void setWaarpSecureKeyStore(WaarpSecureKeyStore waarpSecureKeyStore) {
        Configuration.waarpSecureKeyStore = waarpSecureKeyStore;
    }

    /**
     * @return the waarpSslContextFactory
     */
    public static WaarpSslContextFactory getWaarpSslContextFactory() {
        return waarpSslContextFactory;
    }

    /**
     * @param waarpSslContextFactory the waarpSslContextFactory to set
     */
    public static void setWaarpSslContextFactory(WaarpSslContextFactory waarpSslContextFactory) {
        Configuration.waarpSslContextFactory = waarpSslContextFactory;
    }

    /**
     * @return the thriftService
     */
    public R66ThriftServerService getThriftService() {
        return thriftService;
    }

    /**
     * @param thriftService the thriftService to set
     */
    public void setThriftService(R66ThriftServerService thriftService) {
        this.thriftService = thriftService;
    }

    /**
     * @return the thriftport
     */
    public int getThriftport() {
        return thriftport;
    }

    /**
     * @param thriftport the thriftport to set
     */
    public void setThriftport(int thriftport) {
        this.thriftport = thriftport;
    }

    /**
     * @return the isExecuteErrorBeforeTransferAllowed
     */
    public boolean isExecuteErrorBeforeTransferAllowed() {
        return isExecuteErrorBeforeTransferAllowed;
    }

    /**
     * @param isExecuteErrorBeforeTransferAllowed the isExecuteErrorBeforeTransferAllowed to set
     */
    public void setExecuteErrorBeforeTransferAllowed(boolean isExecuteErrorBeforeTransferAllowed) {
        this.isExecuteErrorBeforeTransferAllowed = isExecuteErrorBeforeTransferAllowed;
    }

    /**
     * @return the shutdownConfiguration
     */
    public ShutdownConfiguration getShutdownConfiguration() {
        return shutdownConfiguration;
    }

    /**
     * @return the isHostProxyfied
     */
    public boolean isHostProxyfied() {
        return isHostProxyfied;
    }

    /**
     * @param isHostProxyfied the isHostProxyfied to set
     */
    public void setHostProxyfied(boolean isHostProxyfied) {
        this.isHostProxyfied = isHostProxyfied;
    }

    /**
     * @return the warnOnStartup
     */
    public boolean isWarnOnStartup() {
        return warnOnStartup;
    }

    /**
     * @param warnOnStartup the warnOnStartup to set
     */
    public void setWarnOnStartup(boolean warnOnStartup) {
        this.warnOnStartup = warnOnStartup;
    }

    /**
     * @return the chrootChecked
     */
    public boolean isChrootChecked() {
        return chrootChecked;
    }

    /**
     * @param chrootChecked the chrootChecked to set
     */
    public void setChrootChecked(boolean chrootChecked) {
        this.chrootChecked = chrootChecked;
    }

    /**
     * @return the blacklistBadAuthent
     */
    public boolean isBlacklistBadAuthent() {
        return blacklistBadAuthent;
    }

    /**
     * @param blacklistBadAuthent the blacklistBadAuthent to set
     */
    public void setBlacklistBadAuthent(boolean blacklistBadAuthent) {
        this.blacklistBadAuthent = blacklistBadAuthent;
    }

    /**
     * @return the maxfilenamelength
     */
    public int getMaxfilenamelength() {
        return maxfilenamelength;
    }

    /**
     * @param maxfilenamelength the maxfilenamelength to set
     */
    public void setMaxfilenamelength(int maxfilenamelength) {
        this.maxfilenamelength = maxfilenamelength;
    }

    /**
     * @return the timeStat
     */
    public int getTimeStat() {
        return timeStat;
    }

    /**
     * @param timeStat the timeStat to set
     */
    public void setTimeStat(int timeStat) {
        this.timeStat = timeStat;
    }

    /**
     * @return the limitCache
     */
    public int getLimitCache() {
        return limitCache;
    }

    /**
     * @param limitCache the limitCache to set
     */
    public void setLimitCache(int limitCache) {
        this.limitCache = limitCache;
    }

    /**
     * @return the timeLimitCache
     */
    public long getTimeLimitCache() {
        return timeLimitCache;
    }

    /**
     * @param timeLimitCache the timeLimitCache to set
     */
    public void setTimeLimitCache(long timeLimitCache) {
        this.timeLimitCache = timeLimitCache;
    }

    /**
     * @param r66BusinessFactory the r66BusinessFactory to set
     */
    public void setR66BusinessFactory(R66BusinessFactoryInterface r66BusinessFactory) {
        this.r66BusinessFactory = r66BusinessFactory;
    }

    private static class CleanLruCache extends Thread {

        @Override
        public void run() {
            int nb = DbTaskRunner.clearCache();
            logger.info("Clear Cache: " + nb);
            Configuration.configuration.launchInFixedDelay(this, Configuration.configuration.getTimeLimitCache(),
                    TimeUnit.MILLISECONDS);
        }

    }
}
