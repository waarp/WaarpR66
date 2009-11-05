/**
 *
 */
package openr66.protocol.networkhandler;

import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;

import java.net.ConnectException;
import java.net.SocketAddress;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantLock;

import openr66.context.ErrorCode;
import openr66.context.R66Result;
import openr66.context.R66Session;
import openr66.protocol.configuration.Configuration;
import openr66.protocol.exception.OpenR66Exception;
import openr66.protocol.exception.OpenR66ProtocolNetworkException;
import openr66.protocol.exception.OpenR66ProtocolNoConnectionException;
import openr66.protocol.exception.OpenR66ProtocolNoDataException;
import openr66.protocol.exception.OpenR66ProtocolNoSslException;
import openr66.protocol.exception.OpenR66ProtocolPacketException;
import openr66.protocol.exception.OpenR66ProtocolRemoteShutdownException;
import openr66.protocol.exception.OpenR66ProtocolSystemException;
import openr66.protocol.localhandler.LocalChannelReference;
import openr66.protocol.localhandler.RetrieveRunner;
import openr66.protocol.localhandler.packet.AuthentPacket;
import openr66.protocol.localhandler.packet.ConnectionErrorPacket;
import openr66.protocol.networkhandler.packet.NetworkPacket;
import openr66.protocol.networkhandler.ssl.NetworkSslServerHandler;
import openr66.protocol.networkhandler.ssl.NetworkSslServerPipelineFactory;
import openr66.protocol.utils.ChannelUtils;
import openr66.protocol.utils.OpenR66SignalHandler;
import openr66.protocol.utils.R66Future;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

/**
 * This class handles Network Transaction connections
 *
 * @author frederic bregier
 */
public class NetworkTransaction {
    /**
     * Internal Logger
     */
    private static final GgInternalLogger logger = GgInternalLoggerFactory
            .getLogger(NetworkTransaction.class);

    /**
     * Hashmap for Currently Shutdown remote host
     */
    private static final ConcurrentHashMap<Integer, NetworkChannel> networkChannelShutdownOnSocketAddressConcurrentHashMap = new ConcurrentHashMap<Integer, NetworkChannel>();

    /**
     * Hashmap for currently active remote host
     */
    private static final ConcurrentHashMap<Integer, NetworkChannel> networkChannelOnSocketAddressConcurrentHashMap = new ConcurrentHashMap<Integer, NetworkChannel>();

    private static final ConcurrentHashMap<String, NetworkChannel> remoteClient = new ConcurrentHashMap<String, NetworkChannel>();
    /**
     * Hashmap for currently active Retrieve Runner (sender)
     */
    private static final ConcurrentHashMap<Integer, RetrieveRunner> retrieveRunnerConcurrentHashMap =
        new ConcurrentHashMap<Integer, RetrieveRunner>();

    /**
     * Lock for NetworkChannel operations
     */
    private static final ReentrantLock lock = new ReentrantLock();

    /**
     * ExecutorService for RetrieveOperation
     */
    private static final ExecutorService retrieveExecutor = Executors
            .newCachedThreadPool();

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

    private final ChannelFactory channelClientFactory = new NioClientSocketChannelFactory(
            execServerBoss, execServerWorker,
            Configuration.configuration.SERVER_THREAD);

    private final ClientBootstrap clientBootstrap = new ClientBootstrap(
            channelClientFactory);
    private final ClientBootstrap clientSslBootstrap = new ClientBootstrap(
            channelClientFactory);
    private final ChannelGroup networkChannelGroup = new DefaultChannelGroup(
            "NetworkChannels");

    public NetworkTransaction() {
        clientBootstrap.setPipelineFactory(new NetworkServerPipelineFactory());
        clientBootstrap.setOption("tcpNoDelay", true);
        clientBootstrap.setOption("reuseAddress", true);
        clientBootstrap.setOption("connectTimeoutMillis",
                Configuration.configuration.TIMEOUTCON);
        if (Configuration.configuration.HOST_SSLID != null) {
            clientSslBootstrap.setPipelineFactory(new NetworkSslServerPipelineFactory(true,
                    execServerWorker));
            clientSslBootstrap.setOption("tcpNoDelay", true);
            clientSslBootstrap.setOption("reuseAddress", true);
            clientSslBootstrap.setOption("connectTimeoutMillis", Configuration.configuration.TIMEOUTCON);
        }
    }

    /**
     * Create a connection to the specified socketAddress with multiple retries
     * @param socketAddress
     * @param isSSL
     * @param futureRequest
     * @return the LocalChannelReference
     */
    public LocalChannelReference createConnectionWithRetry(SocketAddress socketAddress,
            boolean isSSL, R66Future futureRequest) {
        LocalChannelReference localChannelReference = null;
        OpenR66Exception lastException = null;
        for (int i = 0; i < Configuration.RETRYNB; i ++) {
            try {
                localChannelReference =
                        createConnection(socketAddress, isSSL, futureRequest);
                break;
            } catch (OpenR66ProtocolNetworkException e1) {
                // Can retry
                lastException = e1;
                localChannelReference = null;
                try {
                    Thread.sleep(Configuration.WAITFORNETOP);
                } catch (InterruptedException e) {
                    break;
                }
            } catch (OpenR66ProtocolRemoteShutdownException e1) {
                lastException = e1;
                localChannelReference = null;
                break;
            } catch (OpenR66ProtocolNoConnectionException e1) {
                lastException = e1;
                localChannelReference = null;
                break;
            }
        }
        if (localChannelReference == null) {
            logger.info("Cannot connect : {}", lastException.getMessage());
        } else if (lastException != null) {
            logger.info("Connection retried since {}", lastException.getMessage());
        }
        return localChannelReference;
    }
    /**
     * Create a connection to the specified socketAddress
     * @param socketAddress
     * @param isSSL
     * @param futureRequest
     * @return the LocalChannelReference
     * @throws OpenR66ProtocolNetworkException
     * @throws OpenR66ProtocolRemoteShutdownException
     * @throws OpenR66ProtocolNoConnectionException
     */
    public LocalChannelReference createConnection(SocketAddress socketAddress, boolean isSSL,
            R66Future futureRequest)
            throws OpenR66ProtocolNetworkException,
            OpenR66ProtocolRemoteShutdownException,
            OpenR66ProtocolNoConnectionException {
        NetworkChannel networkChannel = null;
        LocalChannelReference localChannelReference = null;
        boolean ok = false;
        lock.lock();
        try {
            networkChannel = createNewConnection(socketAddress, isSSL);
            localChannelReference = createNewClient(networkChannel, futureRequest);
            ok = true;
        } finally {
            if (!ok) {
                if (networkChannel != null) {
                    removeNetworkChannel(networkChannel.channel, null);
                }
            }
            lock.unlock();
        }
        if (localChannelReference.getFutureValidateStartup().isSuccess()) {
            sendValidationConnection(localChannelReference);
        } else {
            OpenR66ProtocolNetworkException exc =
                new OpenR66ProtocolNetworkException("Startup is invalid");
            logger.warn("Startup is Invalid", exc);
            R66Result finalValue = new R66Result(
                    exc, null, true, ErrorCode.ConnectionImpossible, null);
            localChannelReference.invalidateRequest(finalValue);
            Channels.close(localChannelReference.getLocalChannel());
            throw exc;
        }
        return localChannelReference;
    }

    /**
     *
     * @param socketServerAddress
     * @param isSSL
     * @return the NetworkChannel
     * @throws OpenR66ProtocolNetworkException
     * @throws OpenR66ProtocolRemoteShutdownException
     * @throws OpenR66ProtocolNoConnectionException
     */
    private NetworkChannel createNewConnection(SocketAddress socketServerAddress, boolean isSSL)
            throws OpenR66ProtocolNetworkException,
            OpenR66ProtocolRemoteShutdownException,
            OpenR66ProtocolNoConnectionException {
        if (!isAddressValid(socketServerAddress)) {
            throw new OpenR66ProtocolRemoteShutdownException(
                    "Cannot connect to remote server since it is shutting down");
        }
        NetworkChannel networkChannel;
        try {
            networkChannel = getRemoteChannel(socketServerAddress);
        } catch (OpenR66ProtocolNoDataException e1) {
            networkChannel = null;
        }
        if (networkChannel != null) {
            logger.info("Already Connected: {}", networkChannel);
            networkChannel.count++;
            return networkChannel;
        }
        ChannelFuture channelFuture = null;
        for (int i = 0; i < Configuration.RETRYNB; i ++) {
            if (isSSL) {
                if (Configuration.configuration.HOST_SSLID != null) {
                    channelFuture = clientSslBootstrap.connect(socketServerAddress);
                } else {
                    throw new OpenR66ProtocolNoConnectionException("No SSL support");
                }
            } else {
                channelFuture = clientBootstrap.connect(socketServerAddress);
            }
            try {
                channelFuture.await();
            } catch (InterruptedException e1) {
            }
            if (channelFuture.isSuccess()) {
                final Channel channel = channelFuture.getChannel();
                if (isSSL) {
                    if (! NetworkSslServerHandler.isSslConnectedChannel(channel)) {
                        logger.error("KO CONNECT since SSL handshake is over");
                        Channels.close(channel);
                        throw new OpenR66ProtocolNoConnectionException(
                                "Cannot finish connect to remote server");
                    }
                }
                networkChannelGroup.add(channel);
                return putRemoteChannel(channel);
            } else {
                try {
                    Thread.sleep(Configuration.WAITFORNETOP);
                } catch (InterruptedException e) {
                }
                if (! channelFuture.isDone()) {
                    throw new OpenR66ProtocolNoConnectionException(
                            "Cannot connect to remote server due to interruption");
                }
                if (channelFuture.getCause() instanceof ConnectException) {
                    logger.info("KO CONNECT:" +
                            channelFuture.getCause().getMessage());
                    throw new OpenR66ProtocolNoConnectionException(
                            "Cannot connect to remote server", channelFuture
                                    .getCause());
                } else {
                    logger.info("KO CONNECT but retry", channelFuture
                            .getCause());
                }
            }
            try {
                Thread.sleep(Configuration.WAITFORNETOP);
            } catch (InterruptedException e) {
                throw new OpenR66ProtocolNetworkException(
                        "Cannot connect to remote server", e);
            }
        }
        throw new OpenR66ProtocolNetworkException(
                "Cannot connect to remote server", channelFuture.getCause());
    }
   /**
     *
     * @param channel
     * @param futureRequest
     * @return the LocalChannelReference
     * @throws OpenR66ProtocolNetworkException
     * @throws OpenR66ProtocolRemoteShutdownException
     */
    private LocalChannelReference createNewClient(NetworkChannel networkChannel,
            R66Future futureRequest)
            throws OpenR66ProtocolNetworkException,
            OpenR66ProtocolRemoteShutdownException {
        if (!networkChannel.channel.isConnected()) {
            throw new OpenR66ProtocolNetworkException(
                    "Network channel no more connected");
        }
        LocalChannelReference localChannelReference = null;
        try {
            localChannelReference = Configuration.configuration
                .getLocalTransaction().createNewClient(networkChannel.channel,
                ChannelUtils.NOCHANNEL, futureRequest);
        } catch (OpenR66ProtocolSystemException e) {
            // check if the channel has other attached local channels
            logger.warn("Try to Close Network");
            removeNetworkChannel(networkChannel.channel, null);
            throw new OpenR66ProtocolNetworkException(
                    "Cannot connect to local channel", e);
        }
        return localChannelReference;
    }
    /**
     * Create the LocalChannelReference when a remote local channel starts its connection
     * @param channel
     * @param packet
     * @return the LocalChannelReference
     * @throws OpenR66ProtocolRemoteShutdownException
     * @throws OpenR66ProtocolSystemException
     */
    public static LocalChannelReference createConnectionFromNetworkChannelStartup(Channel channel,
            NetworkPacket packet)
    throws OpenR66ProtocolRemoteShutdownException, OpenR66ProtocolSystemException {
        lock.lock();
        try {
            NetworkTransaction.addNetworkChannel(channel);
            LocalChannelReference localChannelReference = Configuration.configuration
                    .getLocalTransaction().createNewClient(channel,
                            packet.getRemoteId(), null);
            return localChannelReference;
        } finally {
            lock.unlock();
        }
    }
    /**
     * Send a validation of connection with Authentication
     * @param localChannelReference
     * @throws OpenR66ProtocolNetworkException
     * @throws OpenR66ProtocolRemoteShutdownException
     */
    private void sendValidationConnection(
            LocalChannelReference localChannelReference)
            throws OpenR66ProtocolNetworkException,
            OpenR66ProtocolRemoteShutdownException {
        AuthentPacket authent;
        try {
            authent = new AuthentPacket(
                    Configuration.configuration.getHostId(
                            localChannelReference.getNetworkServerHandler().isSsl()),
                    Configuration.configuration.HOST_AUTH.getHostkey(),
                    localChannelReference.getLocalId());
        } catch (OpenR66ProtocolNoSslException e1) {
            R66Result finalValue = new R66Result(
                    new OpenR66ProtocolSystemException("No SSL support", e1),
                    null, true, ErrorCode.ConnectionImpossible, null);
            logger.warn("Authent is Invalid due to no SSL: {}", e1.getMessage());
            if (localChannelReference.getRemoteId() != ChannelUtils.NOCHANNEL) {
                ConnectionErrorPacket error = new ConnectionErrorPacket(
                        "Cannot connect to localChannel since no SSL is supported", null);
                try {
                    ChannelUtils.writeAbstractLocalPacket(localChannelReference, error)
                        .awaitUninterruptibly();
                } catch (OpenR66ProtocolPacketException e) {
                }
            }
            localChannelReference.invalidateRequest(finalValue);
            Channels.close(localChannelReference.getLocalChannel());
            throw new OpenR66ProtocolNetworkException(e1);
        }
        logger.info("Will send request of connection validation");
        try {
            ChannelUtils.writeAbstractLocalPacket(localChannelReference, authent)
            .awaitUninterruptibly();
        } catch (OpenR66ProtocolPacketException e) {
            R66Result finalValue = new R66Result(
                    new OpenR66ProtocolSystemException("Wrong Authent Protocol",e),
                    null, true, ErrorCode.ConnectionImpossible, null);
            logger.warn("Authent is Invalid due to protocol: {}", e.getMessage());
            localChannelReference.invalidateRequest(finalValue);
            if (localChannelReference.getRemoteId() != ChannelUtils.NOCHANNEL) {
                ConnectionErrorPacket error = new ConnectionErrorPacket(
                        "Cannot connect to localChannel since Authent Protocol is invalid", null);
                try {
                    ChannelUtils.writeAbstractLocalPacket(localChannelReference, error)
                        .awaitUninterruptibly();
                } catch (OpenR66ProtocolPacketException e1) {
                }
            }
            Channels.close(localChannelReference.getLocalChannel());
            throw new OpenR66ProtocolNetworkException("Bad packet", e);
        }
        R66Future future = localChannelReference.getFutureValidateConnection();
        if (future.isFailed()) {
            logger.info("Will close NETWORK channel since Future cancelled: {}",
                    future);
            R66Result finalValue = new R66Result(
                    new OpenR66ProtocolSystemException("Out of time during Authentication"),
                    null, true, ErrorCode.ConnectionImpossible, null);
            logger.warn("Authent is Invalid due to out of time: {}", finalValue.exception.getMessage());
            localChannelReference.invalidateRequest(finalValue);
            if (localChannelReference.getRemoteId() != ChannelUtils.NOCHANNEL) {
                ConnectionErrorPacket error = new ConnectionErrorPacket(
                        "Cannot connect to localChannel with Out of Time", null);
                try {
                    ChannelUtils.writeAbstractLocalPacket(localChannelReference, error)
                        .awaitUninterruptibly();
                } catch (OpenR66ProtocolPacketException e) {
                }
            }
            Channels.close(localChannelReference.getLocalChannel());
            throw new OpenR66ProtocolNetworkException(
                    "Cannot validate connection: " + future.getResult(), future
                            .getCause());
        }
    }
    /**
     * Start retrieve operation
     * @param session
     * @param channel
     */
    public static void runRetrieve(R66Session session, Channel channel) {
        RetrieveRunner retrieveRunner = new RetrieveRunner(session, channel);
        retrieveRunnerConcurrentHashMap.put(session.getLocalChannelReference().getLocalId(),
                retrieveRunner);
        retrieveRunner.setDaemon(true);
        retrieveExecutor.execute(retrieveRunner);
    }
    /**
     * Stop a retrieve operation
     * @param localChannelReference
     */
    public static void stopRetrieve(LocalChannelReference localChannelReference) {
        RetrieveRunner retrieveRunner =
            retrieveRunnerConcurrentHashMap.remove(localChannelReference.getLocalId());
        if (retrieveRunner != null) {
            retrieveRunner.stopRunner();
        }
    }
    /**
     * Normal end of a Retrieve Operation
     * @param localChannelReference
     */
    public static void normalEndRetrieve(LocalChannelReference localChannelReference) {
        retrieveRunnerConcurrentHashMap.remove(localChannelReference.getLocalId());
    }
    /**
     * Stop all Retrieve Executors
     */
    public static void closeRetrieveExecutors() {
        retrieveExecutor.shutdownNow();
    }
    /**
     * Close all Network Ttransaction
     */
    public void closeAll() {
        logger.info("close All Network Channels");
        closeRetrieveExecutors();
        networkChannelGroup.close().awaitUninterruptibly();
        clientBootstrap.releaseExternalResources();
        clientSslBootstrap.releaseExternalResources();
        channelClientFactory.releaseExternalResources();
        try {
            Thread.sleep(Configuration.WAITFORNETOP);
        } catch (InterruptedException e) {
        }
        OpenR66SignalHandler.closeAllConnection();
        logger.info("Last action before exit");
        ChannelUtils.stopLogger();
    }
    /**
     *
     * @param channel
     * @throws OpenR66ProtocolRemoteShutdownException
     */
    public static void addNetworkChannel(Channel channel)
            throws OpenR66ProtocolRemoteShutdownException {
        lock.lock();
        try {
            if (!isAddressValid(channel.getRemoteAddress())) {
                throw new OpenR66ProtocolRemoteShutdownException(
                        "Channel is already in shutdown");
            }
            try {
                putRemoteChannel(channel);
            } catch (OpenR66ProtocolNoConnectionException e) {
                throw new OpenR66ProtocolRemoteShutdownException(
                    "Channel is already in shutdown");
            }
        } finally {
            lock.unlock();
        }
    }
    /**
     * Add a LocalChannel to a NetworkChannel
     * @param networkChannel
     * @param localChannel
     * @throws OpenR66ProtocolRemoteShutdownException
     */
    public static void addLocalChannelToNetworkChannel(Channel networkChannel,
            Channel localChannel) throws OpenR66ProtocolRemoteShutdownException {
        SocketAddress address = networkChannel.getRemoteAddress();
        if (address != null) {
            NetworkChannel networkChannelObject = networkChannelOnSocketAddressConcurrentHashMap
                    .get(address.hashCode());
            if (networkChannelObject != null) {
                networkChannelObject.add(localChannel);
            }
        }
    }
    /**
     * Shutdown one Network Channel
     * @param channel
     */
    public static void shuttingdownNetworkChannel(Channel channel) {
        SocketAddress address = channel.getRemoteAddress();
        shuttingdownNetworkChannel(address, channel);
    }
    /**
     * Shutdown one Network Channel according to its SocketAddress
     * @param address
     * @param channel (can be null)
     * @return True if the shutdown is starting, False if cannot be done (can be already done before)
     */
    public static boolean shuttingdownNetworkChannel(SocketAddress address, Channel channel) {
        if (address != null) {
            lock.lock();
            try {
                NetworkChannel networkChannel =
                    networkChannelShutdownOnSocketAddressConcurrentHashMap
                        .get(address.hashCode());
                if (networkChannel != null) {
                    // already done
                    logger.debug("Already set as shutdown");
                    return false;
                }
                networkChannel = networkChannelOnSocketAddressConcurrentHashMap
                        .get(address.hashCode());
                if (networkChannel != null) {
                    logger.debug("Set as shutdown");
                } else {
                    if (channel != null) {
                        logger.debug("Newly Set as shutdown");
                        networkChannel = new NetworkChannel(channel);
                    }
                }
                if (networkChannel != null) {
                    networkChannelShutdownOnSocketAddressConcurrentHashMap.put(address
                            .hashCode(), networkChannel);
                    if (networkChannel.isShuttingDown) {
                        return false;
                    }
                    networkChannel.shutdownAllLocalChannels();
                    Timer timer = new Timer(true);
                    final R66TimerTask timerTask = new R66TimerTask(address.hashCode());
                    timer.schedule(timerTask,
                            Configuration.configuration.TIMEOUTCON * 2);
                    return true;
                }
            } finally {
                lock.unlock();
            }
        }
        return false;
    }
    /**
     *
     * @param channel
     * @return True if this channel is currently in shutdown
     */
    public static boolean isShuttingdownNetworkChannel(Channel channel) {
        lock.lock();
        try {
            return !isAddressValid(channel.getRemoteAddress());
        } finally {
            lock.unlock();
        }
    }
    /**
     * Remove of requester
     * @param requester
     */
    public static void removeClient(String requester) {
        remoteClient.remove(requester);
    }
    /**
     * Get NetworkChannel as client
     * @param requester
     * @return NetworkChannel associated with this host as client (only 1 allow even if more are available)
     */
    public static NetworkChannel getClient(String requester) {
        return remoteClient.get(requester);
    }
    /**
     * Add a requester
     * @param channel (network channel)
     * @param requester
     */
    public static void addClient(Channel channel, String requester) {
        SocketAddress address = channel.getRemoteAddress();
        if (address != null) {
            NetworkChannel networkChannel =
                networkChannelOnSocketAddressConcurrentHashMap.get(address.hashCode());
            if (networkChannel != null) {
                remoteClient.put(requester, networkChannel);
            }
        }
    }
    /**
     * Force remove of NetworkChannel when it is closed
     * @param channel
     * @return the number of still connected Local Channels
     */
    public static int removeForceNetworkChannel(Channel channel) {
        lock.lock();
        try {
            SocketAddress address = channel.getRemoteAddress();
            if (address != null) {
                NetworkChannel networkChannel = networkChannelOnSocketAddressConcurrentHashMap
                    .get(address.hashCode());
                if (networkChannel != null) {
                    if (networkChannel.isShuttingDown) {
                        return networkChannel.count;
                    }
                    logger.debug("NC left: {}", networkChannel);
                    int count = networkChannel.count;
                    networkChannel.shutdownAllLocalChannels();
                    networkChannelOnSocketAddressConcurrentHashMap
                        .remove(address.hashCode());
                    return count;
                } else {
                    logger.info("Network not registered");
                }
            }
            return 0;
        } finally {
            lock.unlock();
        }
    }
    /**
     *
     * @param channel networkChannel
     * @param localChannel localChannel
     * @return the number of local channel still connected to this channel
     */
    public static int removeNetworkChannel(Channel channel, Channel localChannel) {
        lock.lock();
        try {
            SocketAddress address = channel.getRemoteAddress();
            if (address != null) {
                NetworkChannel networkChannel = networkChannelOnSocketAddressConcurrentHashMap
                        .get(address.hashCode());
                if (networkChannel != null) {
                    networkChannel.count--;
                    if (localChannel != null) {
                        networkChannel.remove(localChannel);
                    }
                    if (networkChannel.count <= 0) {
                        networkChannelOnSocketAddressConcurrentHashMap
                                .remove(address.hashCode());
                        logger
                                .info("Will close NETWORK channel");
                        Channels.close(channel).awaitUninterruptibly();
                        return 0;
                    }
                    logger.debug("NC left: {}", networkChannel);
                    return networkChannel.count;
                } else {
                    if (channel.isConnected()) {
                        logger.info("Should not be here",
                                new OpenR66ProtocolSystemException());
                    }
                }
            }
            return 0;
        } finally {
            lock.unlock();
        }
    }
    /**
     *
     * @param address
     * @param host
     * @return True if a connection is still active on this socket or for this host
     */
    public static boolean existConnection(SocketAddress address, String host) {
        return networkChannelOnSocketAddressConcurrentHashMap.containsKey(address.hashCode())
            || remoteClient.containsKey(host);
    }
    /**
     *
     * @param channel
     * @return the number of local channel associated with this channel
     */
    public static int getNbLocalChannel(Channel channel) {
        SocketAddress address = channel.getRemoteAddress();
        if (address != null) {
            NetworkChannel networkChannel = networkChannelOnSocketAddressConcurrentHashMap
                    .get(address.hashCode());
            if (networkChannel != null) {
                return networkChannel.count;
            }
        }
        return -1;
    }

    /**
     *
     * @param address
     * @return True if this socket Address is currently valid for connection
     */
    private static boolean isAddressValid(SocketAddress address) {
        if (OpenR66SignalHandler.isInShutdown()) {
            logger.info("IS IN SHUTDOWN");
            return false;
        }
        if (address == null) {
            logger.info("ADDRESS IS NULL");
            return false;
        }
        try {
            NetworkChannel networkChannel = getRemoteChannel(address);
            logger.info("IS IN SHUTDOWN: " + networkChannel.isShuttingDown);
            return !networkChannel.isShuttingDown;
        } catch (OpenR66ProtocolRemoteShutdownException e) {
            logger.info("ALREADY IN SHUTDOWN");
            return false;
        } catch (OpenR66ProtocolNoDataException e) {
            logger.info("NOT FOUND SO NO SHUTDOWN");
            return true;
        }
    }
    /**
     *
     * @param address
     * @return NetworkChannel
     * @throws OpenR66ProtocolRemoteShutdownException
     * @throws OpenR66ProtocolNoDataException
     */
    private static NetworkChannel getRemoteChannel(SocketAddress address)
            throws OpenR66ProtocolRemoteShutdownException,
            OpenR66ProtocolNoDataException {
        if (OpenR66SignalHandler.isInShutdown()) {
            logger.info("IS IN SHUTDOWN");
            throw new OpenR66ProtocolRemoteShutdownException(
                "Local Host already in shutdown");
        }
        if (address == null) {
            throw new OpenR66ProtocolRemoteShutdownException(
                    "Remote Host already in shutdown");
        }
        NetworkChannel nc = networkChannelShutdownOnSocketAddressConcurrentHashMap
                .get(address.hashCode());
        if (nc != null) {
            throw new OpenR66ProtocolRemoteShutdownException(
                    "Remote Host already in shutdown");
        }
        nc = networkChannelOnSocketAddressConcurrentHashMap.get(address
                .hashCode());
        if (nc == null) {
            throw new OpenR66ProtocolNoDataException("Channel not found");
        }
        return nc;
    }
    /**
     *
     * @param channel
     * @return the associated NetworkChannel
     * @throws OpenR66ProtocolRemoteShutdownException
     * @throws OpenR66ProtocolNoDataException
     */
    private static NetworkChannel putRemoteChannel(Channel channel)
            throws OpenR66ProtocolRemoteShutdownException, OpenR66ProtocolNoConnectionException {
        SocketAddress address = channel.getRemoteAddress();
        if (address != null) {
            NetworkChannel networkChannel;
            try {
                networkChannel = getRemoteChannel(address);
                networkChannel.count ++;
                logger.debug("NC active: {}", networkChannel);
                return networkChannel;
            } catch (OpenR66ProtocolRemoteShutdownException e) {
                throw e;
            } catch (OpenR66ProtocolNoDataException e) {
                networkChannel = new NetworkChannel(channel);
                logger.debug("NC new active: {}", networkChannel);
                networkChannelOnSocketAddressConcurrentHashMap.put(address
                        .hashCode(), networkChannel);
                return networkChannel;
            }
        }
        throw new OpenR66ProtocolNoConnectionException("Address is not correct");
    }

    /**
     * Remover of Shutdown Remote Host
     *
     * @author Frederic Bregier
     *
     */
    private static class R66TimerTask extends TimerTask {
        /**
         * href to remove
         */
        private final int href;

        /**
         * Constructor from type
         *
         * @param href
         */
        public R66TimerTask(int href) {
            super();
            this.href = href;
        }

        @Override
        public void run() {
            NetworkChannel networkChannel =
                networkChannelShutdownOnSocketAddressConcurrentHashMap.remove(href);
            if (networkChannel != null && networkChannel.channel != null
                    && networkChannel.channel.isConnected()) {
                Channels.close(networkChannel.channel);
            }
        }
    }
}
