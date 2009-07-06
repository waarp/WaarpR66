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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import openr66.protocol.config.Configuration;
import openr66.protocol.exception.OpenR66ProtocolNetworkException;
import openr66.protocol.exception.OpenR66ProtocolSystemException;
import openr66.protocol.localhandler.LocalChannelReference;
import openr66.protocol.utils.ChannelUtils;

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
        Channel channel;
        public NetworkChannel(Channel channel) {
            this.channel = channel;
        }
        public String toString() {
            return "NC: "+channel.isConnected()+" "+channel.getRemoteAddress()+" Count: "+count;
        }
    }
    private static ConcurrentHashMap<Integer, NetworkChannel> networkChannelOnSocketAddressConcurrentHashMap =
        new ConcurrentHashMap<Integer, NetworkChannel>();
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
            execServerBoss, execServerWorker, Configuration.SERVER_THREAD);

    private final ClientBootstrap clientBootstrap = new ClientBootstrap(
            channelClientFactory);

    private final ChannelGroup networkChannelGroup = new DefaultChannelGroup(
            "NetworkChannels");

    public NetworkTransaction() {
        logger.warn("THREAD: "+Configuration.SERVER_THREAD);
        clientBootstrap.setPipelineFactory(new NetworkServerPipelineFactory());
    }
    
    public LocalChannelReference createConnection(SocketAddress socketAddress) throws OpenR66ProtocolNetworkException {
        lock.lock();
        try {
            Channel channel = createNewConnection(socketAddress);
            return createNewClient(channel);
        } finally {
            lock.unlock();
        }
    }
    private Channel createNewConnection(SocketAddress socketServerAddress) throws OpenR66ProtocolNetworkException {
        NetworkChannel networkChannel = networkChannelOnSocketAddressConcurrentHashMap.get(socketServerAddress.hashCode());
        if (networkChannel != null) {
            if (networkChannel.channel.isConnected()) {
                logger.warn("Already Connected: "+networkChannel.toString());
                return networkChannel.channel;
            }
        }
        ChannelFuture channelFuture = null;
        for (int i = 0; i < Configuration.RETRYNB; i++) {
            channelFuture = clientBootstrap
                    .connect(socketServerAddress);
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
                throw new OpenR66ProtocolNetworkException("Cannot connect to remote server",e);
            }
        }
        throw new OpenR66ProtocolNetworkException("Cannot connect to remote server", channelFuture.getCause());
    }

    private LocalChannelReference createNewClient(Channel channel) throws OpenR66ProtocolNetworkException {
        if (! channel.isConnected()) {
            throw new OpenR66ProtocolNetworkException("Network channel no more connected");            
        }
        LocalChannelReference localChannelReference = null;
        try {
            localChannelReference = 
                Configuration.configuration.getLocalTransaction().createNewClient(channel, ChannelUtils.NOCHANNEL);
        } catch (OpenR66ProtocolSystemException e) {
            throw new OpenR66ProtocolNetworkException("Cannot connect to local channel",e);
        }
        NetworkTransaction.addNetworkChannel(channel);
        return localChannelReference;
    }

    public void closeAll() {
        networkChannelGroup.close().awaitUninterruptibly();
        clientBootstrap.releaseExternalResources();
        channelClientFactory.releaseExternalResources();
    }
    public static void addNetworkChannel(Channel channel) {
        lock.lock();
        try {
            NetworkChannel networkChannel = 
                networkChannelOnSocketAddressConcurrentHashMap.get(channel.getRemoteAddress().hashCode());
            if (networkChannel != null) {
                networkChannel.count.incrementAndGet();
                logger.warn("NC active: "+networkChannel.toString());
            } else {
                networkChannel = new NetworkChannel(channel);
                networkChannelOnSocketAddressConcurrentHashMap.put(channel.getRemoteAddress().hashCode(), networkChannel);
            }
        } finally {
            lock.unlock();
        }
    }
    public static int removeNetworkChannel(Channel channel) {
        lock.lock();
        try {
            NetworkChannel networkChannel = networkChannelOnSocketAddressConcurrentHashMap.get(channel.getRemoteAddress().hashCode());
            if (networkChannel != null) {
                if (networkChannel.count.decrementAndGet() <= 0) {
                    //networkChannel.count.set(0);
                    networkChannelOnSocketAddressConcurrentHashMap.remove(channel.getRemoteAddress().hashCode());
                    logger.info("Close network channel");
                    Channels.close(channel);
                    return 0;
                }
                logger.warn("NC left: "+networkChannel.toString());
                return networkChannel.count.get();
            } else {
                if (channel.isConnected()) {
                    logger.error("Should not be here",new OpenR66ProtocolSystemException());
                    //Channels.close(channel);
                }
            }
            return 0;
        } finally {
            lock.unlock();
        }
    }
    public static int getNbLocalChannel(Channel channel) {
        NetworkChannel networkChannel = networkChannelOnSocketAddressConcurrentHashMap.get(channel.getRemoteAddress().hashCode());
        if (networkChannel != null) {
            return networkChannel.count.get();
        }
        return -1;
    }
}
