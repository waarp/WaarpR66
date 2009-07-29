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

import openr66.context.R66Session;
import openr66.context.authentication.R66Auth;
import openr66.protocol.config.Configuration;
import openr66.protocol.exception.OpenR66ProtocolNetworkException;
import openr66.protocol.exception.OpenR66ProtocolNoConnectionException;
import openr66.protocol.exception.OpenR66ProtocolNoDataException;
import openr66.protocol.exception.OpenR66ProtocolPacketException;
import openr66.protocol.exception.OpenR66ProtocolRemoteShutdownException;
import openr66.protocol.exception.OpenR66ProtocolSystemException;
import openr66.protocol.localhandler.LocalChannelReference;
import openr66.protocol.localhandler.RetrieveRunner;
import openr66.protocol.localhandler.packet.AuthentPacket;
import openr66.protocol.networkhandler.packet.NetworkPacket;
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

    private final ChannelGroup networkChannelGroup = new DefaultChannelGroup(
            "NetworkChannels");

    public NetworkTransaction() {
        logger.warn("THREAD: " + Configuration.configuration.SERVER_THREAD);
        clientBootstrap.setPipelineFactory(new NetworkServerPipelineFactory());
    }
    /**
     * Create a connection to the specified socketAddress
     * @param socketAddress
     * @return the LocalChannelReference
     * @throws OpenR66ProtocolNetworkException
     * @throws OpenR66ProtocolRemoteShutdownException
     * @throws OpenR66ProtocolNoConnectionException
     */
    public LocalChannelReference createConnection(SocketAddress socketAddress)
            throws OpenR66ProtocolNetworkException,
            OpenR66ProtocolRemoteShutdownException,
            OpenR66ProtocolNoConnectionException {
        lock.lock();
        try {
            Channel channel = createNewConnection(socketAddress);
            LocalChannelReference localChannelReference = createNewClient(channel);
            sendValidationConnection(localChannelReference);
            return localChannelReference;
        } finally {
            lock.unlock();
        }
    }
    /**
     *
     * @param socketServerAddress
     * @return the new channel
     * @throws OpenR66ProtocolNetworkException
     * @throws OpenR66ProtocolRemoteShutdownException
     * @throws OpenR66ProtocolNoConnectionException
     */
    private Channel createNewConnection(SocketAddress socketServerAddress)
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
            logger.info("Already Connected: " + networkChannel.toString());
            return networkChannel.channel;
        }
        ChannelFuture channelFuture = null;
        for (int i = 0; i < Configuration.RETRYNB; i ++) {
            channelFuture = clientBootstrap.connect(socketServerAddress);
            channelFuture.awaitUninterruptibly();
            if (channelFuture.isSuccess()) {
                final Channel channel = channelFuture.getChannel();
                networkChannelGroup.add(channel);
                if (networkChannel != null) {
                    networkChannel.channel = channel;
                }
                return channel;
            } else {
                if (channelFuture.getCause() instanceof ConnectException) {
                    logger.error("KO CONNECT:" +
                            channelFuture.getCause().getMessage());
                    throw new OpenR66ProtocolNoConnectionException(
                            "Cannot connect to remote server", channelFuture
                                    .getCause());
                }
            }
            try {
                Thread.sleep(Configuration.RETRYINMS);
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
     * @return the LocalChannelReference
     * @throws OpenR66ProtocolNetworkException
     * @throws OpenR66ProtocolRemoteShutdownException
     */
    private LocalChannelReference createNewClient(Channel channel)
            throws OpenR66ProtocolNetworkException,
            OpenR66ProtocolRemoteShutdownException {
        // FIXME WARNING
        if (!channel.isConnected()) {
            throw new OpenR66ProtocolNetworkException(
                    "Network channel no more connected");
        }
        LocalChannelReference localChannelReference = null;
        try {
            localChannelReference = Configuration.configuration
                    .getLocalTransaction().createNewClient(channel,
                            ChannelUtils.NOCHANNEL);
        } catch (OpenR66ProtocolSystemException e) {
            throw new OpenR66ProtocolNetworkException(
                    "Cannot connect to local channel", e);
        }
        NetworkTransaction.addNetworkChannel(channel);
        return localChannelReference;
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
        AuthentPacket authent = new AuthentPacket(
                Configuration.configuration.HOST_ID, R66Auth.getServerAuth(),
                localChannelReference.getLocalId());
        NetworkPacket packet;
        try {
            packet = new NetworkPacket(localChannelReference.getLocalId(),
                    localChannelReference.getRemoteId(), authent);
        } catch (OpenR66ProtocolPacketException e) {
            throw new OpenR66ProtocolNetworkException("Bad packet", e);
        }
        Channels.write(localChannelReference.getNetworkChannel(), packet)
                .awaitUninterruptibly();
        R66Future future = localChannelReference.getFutureValidateConnection();
        if (future.isCancelled()) {
            logger.info("Will close NETWORK channel since Future cancelled: " +
                    future.toString());
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
        retrieveExecutor.execute(retrieveRunner);
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
        logger.warn("close All Network Channels");
        closeRetrieveExecutors();
        networkChannelGroup.close().awaitUninterruptibly();
        clientBootstrap.releaseExternalResources();
        channelClientFactory.releaseExternalResources();
        OpenR66SignalHandler.closeAllConnection();
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
            putRemoteChannel(channel);
        } finally {
            lock.unlock();
        }
    }
    /**
     *
     * @param channel
     */
    public static void shuttingdownNetworkChannel(Channel channel) {
        SocketAddress address = channel.getRemoteAddress();
        if (address != null) {
            NetworkChannel networkChannel = networkChannelShutdownOnSocketAddressConcurrentHashMap
                    .get(address.hashCode());
            if (networkChannel != null) {
                // already done
                logger.info("Already set as shutdown");
                return;
            }
            networkChannel = networkChannelOnSocketAddressConcurrentHashMap
                    .get(address.hashCode());
            if (networkChannel != null) {
                logger.info("Set as shutdown");
            } else {
                logger.info("Newly Set as shutdown");
                networkChannel = new NetworkChannel(channel);
            }
            networkChannel.isShuttingDown = true;
            networkChannelShutdownOnSocketAddressConcurrentHashMap.put(address
                    .hashCode(), networkChannel);
            logger.info("Add NC to shutdown hashmap");
            Timer timer = new Timer(true);
            final R66TimerTask timerTask = new R66TimerTask(address.hashCode());
            timer.schedule(timerTask,
                    Configuration.configuration.TIMEOUTCON * 2);
        }
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
     *
     * @param channel
     * @return the number of local channel still connected to this channel
     */
    public static int removeNetworkChannel(Channel channel) {
        lock.lock();
        try {
            SocketAddress address = channel.getRemoteAddress();
            if (address != null) {
                NetworkChannel networkChannel = networkChannelOnSocketAddressConcurrentHashMap
                        .get(address.hashCode());
                if (networkChannel != null) {
                    if (networkChannel.count -- <= 0) {
                        networkChannelOnSocketAddressConcurrentHashMap
                                .remove(address.hashCode());
                        logger
                                .warn("Will close NETWORK channel Close network channel");
                        Channels.close(channel).awaitUninterruptibly();
                        return 0;
                    }
                    logger.info("NC left: " + networkChannel.toString());
                    return networkChannel.count;
                } else {
                    if (channel.isConnected()) {
                        logger.error("Should not be here",
                                new OpenR66ProtocolSystemException());
                        // Channels.close(channel);
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
    public static NetworkChannel getRemoteChannel(SocketAddress address)
            throws OpenR66ProtocolRemoteShutdownException,
            OpenR66ProtocolNoDataException {
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
     * @throws OpenR66ProtocolRemoteShutdownException
     */
    public static void putRemoteChannel(Channel channel)
            throws OpenR66ProtocolRemoteShutdownException {
        SocketAddress address = channel.getRemoteAddress();
        if (address != null) {
            NetworkChannel networkChannel;
            try {
                networkChannel = getRemoteChannel(address);
                networkChannel.count ++;
                logger.info("NC active: " + networkChannel.toString());
            } catch (OpenR66ProtocolRemoteShutdownException e) {
                throw e;
            } catch (OpenR66ProtocolNoDataException e) {
                networkChannel = new NetworkChannel(channel);
                networkChannelOnSocketAddressConcurrentHashMap.put(address
                        .hashCode(), networkChannel);
            }
        }
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
            logger.warn("Remove NC from shutdown hashmap");
            networkChannelShutdownOnSocketAddressConcurrentHashMap.remove(href);
        }
    }
}
