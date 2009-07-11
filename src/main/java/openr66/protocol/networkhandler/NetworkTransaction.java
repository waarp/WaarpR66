/**
 *
 */
package openr66.protocol.networkhandler;

import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;

import java.net.SocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import openr66.authentication.R66Auth;
import openr66.protocol.config.Configuration;
import openr66.protocol.exception.OpenR66ProtocolNetworkException;
import openr66.protocol.exception.OpenR66ProtocolPacketException;
import openr66.protocol.exception.OpenR66ProtocolRemoteShutdownException;
import openr66.protocol.exception.OpenR66ProtocolSystemException;
import openr66.protocol.localhandler.LocalChannelReference;
import openr66.protocol.localhandler.packet.AuthentPacket;
import openr66.protocol.localhandler.packet.LocalPacketFactory;
import openr66.protocol.localhandler.packet.ValidPacket;
import openr66.protocol.networkhandler.packet.NetworkPacket;
import openr66.protocol.utils.ChannelUtils;
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

    static class NetworkChannel {
        AtomicInteger count = new AtomicInteger(1);
        AtomicBoolean isShuttingDown = new AtomicBoolean(false);
        Channel channel;

        public NetworkChannel(Channel channel) {
            this.channel = channel;
        }

        @Override
        public String toString() {
            return "NC: " + channel.isConnected() + " " +
                    channel.getRemoteAddress() + " Count: " + count;
        }
    }

    private static ConcurrentHashMap<Integer, NetworkChannel> networkChannelOnSocketAddressConcurrentHashMap = new ConcurrentHashMap<Integer, NetworkChannel>();

    private static ReentrantLock lock = new ReentrantLock();

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
            execServerBoss, execServerWorker, Configuration.configuration.SERVER_THREAD);

    private final ClientBootstrap clientBootstrap = new ClientBootstrap(
            channelClientFactory);

    private final ChannelGroup networkChannelGroup = new DefaultChannelGroup(
            "NetworkChannels");

    public NetworkTransaction() {
        logger.warn("THREAD: " + Configuration.configuration.SERVER_THREAD);
        clientBootstrap.setPipelineFactory(new NetworkServerPipelineFactory());
    }

    public LocalChannelReference createConnection(SocketAddress socketAddress)
            throws OpenR66ProtocolNetworkException, OpenR66ProtocolRemoteShutdownException {
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

    private Channel createNewConnection(SocketAddress socketServerAddress)
            throws OpenR66ProtocolNetworkException, OpenR66ProtocolRemoteShutdownException {
        NetworkChannel networkChannel = networkChannelOnSocketAddressConcurrentHashMap
                .get(socketServerAddress.hashCode());
        if (networkChannel != null) {
            if (networkChannel.isShuttingDown.get()) {
                throw new OpenR66ProtocolRemoteShutdownException(
                        "Cannot connect to remote server since it is shutting down");
            }
            if (networkChannel.channel.isConnected()) {
                logger.info("Already Connected: " + networkChannel.toString());
                return networkChannel.channel;
            }
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

    private LocalChannelReference createNewClient(Channel channel)
            throws OpenR66ProtocolNetworkException, OpenR66ProtocolRemoteShutdownException {
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

    private void sendValidationConnection(
            LocalChannelReference localChannelReference)
            throws OpenR66ProtocolNetworkException, OpenR66ProtocolRemoteShutdownException {
        AuthentPacket authent = new AuthentPacket(Configuration.configuration.HOST_ID,
                R66Auth.getServerAuth(),
                localChannelReference.getLocalId());
        NetworkPacket packet;
        try {
            packet = new NetworkPacket(localChannelReference.getLocalId(),
                    localChannelReference.getRemoteId(), authent);
        } catch (OpenR66ProtocolPacketException e) {
            throw new OpenR66ProtocolNetworkException("Bad packet", e);
        }
        Channel channel = localChannelReference.getNetworkChannel();
        if (!channel.isConnected()) {
            throw new OpenR66ProtocolNetworkException(
                    "Cannot validate connection since connection closed");
        }
        ChannelUtils.write(localChannelReference.getNetworkChannel(), packet)
                .awaitUninterruptibly();
        R66Future future = localChannelReference.getValidateFuture();
        if (future.isCancelled()) {
            Channels.close(channel);
            logger.warn("Future cancelled: "+future.toString());
            throw new OpenR66ProtocolNetworkException(
                    "Cannot validate connection");
        }
    }

    public void closeAll() {
        logger.warn("close All Network Channels");
        networkChannelGroup.close().awaitUninterruptibly();
        clientBootstrap.releaseExternalResources();
        channelClientFactory.releaseExternalResources();
    }

    public static void addNetworkChannel(Channel channel) throws OpenR66ProtocolRemoteShutdownException {
        lock.lock();
        try {
            SocketAddress address = channel.getRemoteAddress();
            if (address != null){
                NetworkChannel networkChannel = networkChannelOnSocketAddressConcurrentHashMap
                        .get(address.hashCode());
                if (networkChannel != null) {
                    /*if (networkChannel.isShuttingDown.get()) {
                        throw new OpenR66ProtocolRemoteShutdownException(
                                "Cannot valid connection to remote server since it is shutting down");
                    }*/
                    networkChannel.count.incrementAndGet();
                    logger.info("NC active: " + networkChannel.toString());
                } else {
                    networkChannel = new NetworkChannel(channel);
                    networkChannelOnSocketAddressConcurrentHashMap.put(
                            address.hashCode(), networkChannel);
                }
            }
        } finally {
            lock.unlock();
        }
    }

    public static void shuttingdownNetworkChannel(Channel channel) {
        lock.lock();
        try {
            SocketAddress address = channel.getRemoteAddress();
            if (address != null){
                NetworkChannel networkChannel = networkChannelOnSocketAddressConcurrentHashMap
                    .get(address.hashCode());
                if (networkChannel != null) {
                    networkChannel.isShuttingDown.set(true);
                }
            }
        } finally {
            lock.unlock();
        }
    }

    public static boolean isShuttingdownNetworkChannel(Channel channel) {
        lock.lock();
        try {
            SocketAddress address = channel.getRemoteAddress();
            if (address != null){
                NetworkChannel networkChannel = networkChannelOnSocketAddressConcurrentHashMap
                    .get(address.hashCode());
                if (networkChannel != null) {
                    return networkChannel.isShuttingDown.get();
                }
            }
            return true;
        } finally {
            lock.unlock();
        }
    }

    public static int removeNetworkChannel(Channel channel) {
        lock.lock();
        try {
            SocketAddress address = channel.getRemoteAddress();
            if (address != null){
                NetworkChannel networkChannel = networkChannelOnSocketAddressConcurrentHashMap
                    .get(address.hashCode());
                if (networkChannel != null) {
                    if (networkChannel.count.decrementAndGet() <= 0) {
                        // networkChannel.count.set(0);
                        networkChannelOnSocketAddressConcurrentHashMap
                                .remove(address.hashCode());
                        logger.info("Close network channel");
                        Channels.close(channel).awaitUninterruptibly();
                        return 0;
                    }
                    logger.info("NC left: " + networkChannel.toString());
                    return networkChannel.count.get();
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

    public static int getNbLocalChannel(Channel channel) {
        SocketAddress address = channel.getRemoteAddress();
        if (address != null){
            NetworkChannel networkChannel = networkChannelOnSocketAddressConcurrentHashMap
                    .get(address.hashCode());
            if (networkChannel != null) {
                return networkChannel.count.get();
            }
        }
        return -1;
    }
}
