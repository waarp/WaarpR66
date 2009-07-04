/**
 * 
 */
package openr66.protocol.localhandler;

import java.util.concurrent.ConcurrentHashMap;

import openr66.protocol.exception.OpenR66ProtocolSystemException;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
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
    final ConcurrentHashMap<Integer, LocalChannelReference> localChannelHashMap = new ConcurrentHashMap<Integer, LocalChannelReference>();
    private final ChannelFutureListener remover =
        new ChannelFutureListener() {
        public void operationComplete(ChannelFuture future) {
            localChannelHashMap.remove(future.getChannel().getId());
        }
    };
    private final ChannelFactory channelClientFactory = new DefaultLocalClientChannelFactory();

    private final ChannelFactory channelServerFactory = new DefaultLocalServerChannelFactory();

    private final ClientBootstrap clientBootstrap = new ClientBootstrap(
            channelClientFactory);

    private final ServerBootstrap serverBootstrap = new ServerBootstrap(
            channelServerFactory);

    private final Channel serverChannel;

    private final LocalAddress socketServerAddress = new LocalAddress("0");

    private final ChannelGroup localChannelGroup = new DefaultChannelGroup(
            "LocalChannels");

    public LocalTransaction() {
        serverBootstrap.setPipelineFactory(new LocalServerPipelineFactory());
        serverChannel = serverBootstrap.bind(socketServerAddress);
        localChannelGroup.add(serverChannel);
        clientBootstrap.setPipelineFactory(new LocalClientPipelineFactory());
    }

    public LocalChannelReference createNewClient(Channel networkChannel,
            Integer remoteId) throws OpenR66ProtocolSystemException {
        final ChannelFuture channelFuture = clientBootstrap
                .connect(socketServerAddress);
        channelFuture.awaitUninterruptibly();
        if (channelFuture.isSuccess()) {
            final Channel channel = channelFuture.getChannel();
            localChannelGroup.add(channel);
            final LocalChannelReference localChannelReference = new LocalChannelReference(
                    channel, networkChannel, remoteId);
            localChannelHashMap.put(channel.getId(), localChannelReference);
            channel.getCloseFuture().addListener(remover);
            return localChannelReference;
        } else {
            throw new OpenR66ProtocolSystemException("Cannot connect to local handler", channelFuture.getCause());
        }
    }

    public void closeAll() {
        localChannelGroup.close().awaitUninterruptibly();
        clientBootstrap.releaseExternalResources();
        channelClientFactory.releaseExternalResources();
        serverBootstrap.releaseExternalResources();
        channelServerFactory.releaseExternalResources();
    }

    public LocalChannelReference getFromId(Integer id) {
        return localChannelHashMap.get(id);
    }
}
