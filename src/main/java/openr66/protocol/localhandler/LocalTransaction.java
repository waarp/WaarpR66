/**
 * 
 */
package openr66.protocol.localhandler;

import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

import openr66.protocol.config.Configuration;
import openr66.protocol.exception.OpenR66ProtocolSystemException;
import openr66.protocol.localhandler.packet.StartupPacket;
import openr66.protocol.utils.ChannelUtils;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.local.DefaultLocalClientChannelFactory;
import org.jboss.netty.channel.local.DefaultLocalServerChannelFactory;
import org.jboss.netty.channel.local.LocalAddress;

/**
 * This class handles Local Transaction connections
 * 
 * @author frederic bregier
 */
public class LocalTransaction {
    /**
     * Internal Logger
     */
    private static final GgInternalLogger logger = GgInternalLoggerFactory
            .getLogger(LocalTransaction.class);
    
    final ConcurrentHashMap<Integer, LocalChannelReference> localChannelHashMap = new ConcurrentHashMap<Integer, LocalChannelReference>();
    private final ChannelFutureListener remover =
        new ChannelFutureListener() {
        public void operationComplete(ChannelFuture future) {
            localChannelHashMap.remove(future.getChannel().getId());
        }
    };

    private final ChannelFactory channelServerFactory = new DefaultLocalServerChannelFactory();
    private final ServerBootstrap serverBootstrap = new ServerBootstrap(
            channelServerFactory);

    private final Channel serverChannel;
    private final LocalAddress socketServerAddress = new LocalAddress("0");

    private final ChannelFactory channelClientFactory = new DefaultLocalClientChannelFactory();
    private final ClientBootstrap clientBootstrap = new ClientBootstrap(
            channelClientFactory);

    private final ChannelGroup localChannelGroup = new DefaultChannelGroup(
            "LocalChannels");

    public LocalTransaction() {
        serverBootstrap.setPipelineFactory(new LocalServerPipelineFactory());
        serverBootstrap.setOption("connectTimeoutMillis", Configuration.TIMEOUTCON);
        serverChannel = serverBootstrap.bind(socketServerAddress);
        localChannelGroup.add(serverChannel);
        serverChannel.getCloseFuture().addListener(ChannelUtils.channelClosedLogger);
        clientBootstrap.setPipelineFactory(new LocalClientPipelineFactory());
    }

    public LocalChannelReference getClient(Integer remoteId, Integer localId) throws OpenR66ProtocolSystemException {
        LocalChannelReference localChannelReference = getFromId(localId);
        if (localChannelReference != null) {
            if (localChannelReference.getRemoteId() != remoteId) {
                localChannelReference.setRemoteId(remoteId);
            }
            return localChannelReference;
        }
        throw new OpenR66ProtocolSystemException("Cannot find LocalChannelReference");
    }
    public LocalChannelReference createNewClient(Channel networkChannel,
            Integer remoteId) throws OpenR66ProtocolSystemException {
        ChannelFuture channelFuture = null;
        logger.info("Status LocalChannelServer: "+serverChannel.getClass().getName()+" "+
                serverChannel.getConfig().getConnectTimeoutMillis()+" "+serverChannel.isBound());
        for (int i = 0; i < Configuration.RETRYNB*2; i++) {
            channelFuture = clientBootstrap
                    .connect(socketServerAddress);
            channelFuture.awaitUninterruptibly();
            if (channelFuture.isSuccess()) {
                final Channel channel = channelFuture.getChannel();
                localChannelGroup.add(channel);
                final LocalChannelReference localChannelReference = new LocalChannelReference(
                        channel, networkChannel, remoteId);
                logger.info("Create LocalChannel entry: "+localChannelReference);
                localChannelHashMap.put(channel.getId(), localChannelReference);
                channel.getCloseFuture().addListener(remover);
                // Now send first a Startup message
                StartupPacket startup = new StartupPacket(localChannelReference.getLocalId());
                ChannelUtils.write(channel, startup).awaitUninterruptibly();
                return localChannelReference;
            }
            try {
                Thread.sleep(Configuration.RETRYINMS*2);
            } catch (InterruptedException e) {
                throw new OpenR66ProtocolSystemException("Cannot connect to local handler", e);
            }
        }
        logger.error("LocalChannelServer: "+serverChannel.getClass().getName()+" "+
                serverChannel.getConfig().getConnectTimeoutMillis()+" "+serverChannel.isBound());
        throw new OpenR66ProtocolSystemException("Cannot connect to local handler: "+socketServerAddress+
                " "+serverChannel.isBound()+" "+serverChannel, 
                channelFuture.getCause());
    }
    public LocalChannelReference getFromId(Integer id) {
        return localChannelHashMap.get(id);
    }
    public void closeLocalChannelsFromNetworkChannel(Channel networkChannel) {
        Collection<LocalChannelReference> collection = localChannelHashMap.values();
        Iterator<LocalChannelReference> iterator = collection.iterator();
        for (; iterator.hasNext() ;) { 
            LocalChannelReference localChannelReference = iterator.next();
            if (localChannelReference.getNetworkChannel().compareTo(networkChannel) == 0) {
                logger.warn("Will close local channel");
                Channels.close(localChannelReference.getLocalChannel());
            }
        }
    }
    public void closeAll() {
        logger.warn("close All Local Channels");
        localChannelGroup.close().awaitUninterruptibly();
        clientBootstrap.releaseExternalResources();
        channelClientFactory.releaseExternalResources();
        serverBootstrap.releaseExternalResources();
        channelServerFactory.releaseExternalResources();
    }

}
