/**
 * 
 */
package openr66.protocol.networkhandler;

import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;
import openr66.protocol.config.Configuration;
import openr66.protocol.exception.OpenR66ExceptionTrappedFactory;
import openr66.protocol.exception.OpenR66ProtocolBusinessNoWriteBackException;
import openr66.protocol.exception.OpenR66ProtocolException;
import openr66.protocol.exception.OpenR66ProtocolPacketException;
import openr66.protocol.exception.OpenR66ProtocolSystemException;
import openr66.protocol.localhandler.LocalChannelReference;
import openr66.protocol.localhandler.packet.ConnectionErrorPacket;
import openr66.protocol.localhandler.packet.LocalPacketFactory;
import openr66.protocol.networkhandler.packet.NetworkPacket;
import openr66.protocol.utils.ChannelUtils;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipelineCoverage;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;

/**
 * @author frederic bregier
 */
@ChannelPipelineCoverage("one")
public class NetworkServerHandler extends SimpleChannelHandler {
    /**
     * Internal Logger
     */
    private static final GgInternalLogger logger = GgInternalLoggerFactory
            .getLogger(NetworkServerHandler.class);

    /*
     * (non-Javadoc)
     * @see
     * org.jboss.netty.channel.SimpleChannelHandler#channelClosed(org.jboss.
     * netty.channel.ChannelHandlerContext,
     * org.jboss.netty.channel.ChannelStateEvent)
     */
    @Override
    public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e) {
        logger.warn("Network Channel Closed: " + e.getChannel().getId()
                +" LocalChannels Left: "+NetworkTransaction.getNbLocalChannel(e.getChannel()));
        // FIXME close if necessary the local channel
        Configuration.configuration
            .getLocalTransaction().closeLocalChannelsFromNetworkChannel(e.getChannel());
    }

    /*
     * (non-Javadoc)
     * @see
     * org.jboss.netty.channel.SimpleChannelHandler#channelConnected(org.jboss
     * .netty.channel.ChannelHandlerContext,
     * org.jboss.netty.channel.ChannelStateEvent)
     */
    @Override
    public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) {
        logger.info("Network Channel Connected: " + e.getChannel().getId());
    }

    /*
     * (non-Javadoc)
     * @see
     * org.jboss.netty.channel.SimpleChannelHandler#messageReceived(org.jboss
     * .netty.channel.ChannelHandlerContext,
     * org.jboss.netty.channel.MessageEvent)
     */
    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
        final NetworkPacket packet = (NetworkPacket) e.getMessage();
        logger.info("Network Channel Recv: " + e.getChannel().getId()+" "+packet.toString());
        if (packet.getCode() == LocalPacketFactory.CONNECTERRORPACKET) {
            // Special code to stop here
            if (packet.getLocalId() == ChannelUtils.NOCHANNEL) {
                // No way to know what is wrong: close all connections with remote host
                logger.error("Cannot continue connection with remote Host: "+packet.toString()+
                        " : "+e.getChannel().getRemoteAddress());
                Channels.close(e.getChannel());
                return;
            }
        }
        LocalChannelReference localChannelReference = null;
        if (packet.getLocalId() == ChannelUtils.NOCHANNEL) {
            try {
                localChannelReference = Configuration.configuration
                    .getLocalTransaction().createNewClient(e.getChannel(), packet.getRemoteId());
                NetworkTransaction.addNetworkChannel(e.getChannel());
                logger.info("Create LocalChannel: "+localChannelReference.getLocalId());
            } catch (OpenR66ProtocolSystemException e1) {
                logger.error("Cannot create LocalChannel: "+packet,e1);
                final ConnectionErrorPacket error = 
                    new ConnectionErrorPacket("Cannot connect to localChannel", null);
                this.writeError(e.getChannel(), packet.getRemoteId(), packet.getLocalId(), error);
                return;
            }
        } else {
            try {
                localChannelReference = Configuration.configuration.
                    getLocalTransaction().getClient(packet.getRemoteId(), packet.getLocalId());
                logger.info("Get LocalChannel: "+localChannelReference.getLocalId());
            } catch (OpenR66ProtocolSystemException e1) {
                logger.error("Cannot get LocalChannel: "+packet,e1);
                final ConnectionErrorPacket error = 
                    new ConnectionErrorPacket("Cannot get localChannel", null);
                this.writeError(e.getChannel(), packet.getRemoteId(), packet.getLocalId(), error);
                return;
            }
        }
        ChannelUtils.write(localChannelReference.getLocalChannel(), packet
                .getBuffer());
    }

    /*
     * (non-Javadoc)
     * @see
     * org.jboss.netty.channel.SimpleChannelHandler#exceptionCaught(org.jboss
     * .netty.channel.ChannelHandlerContext,
     * org.jboss.netty.channel.ExceptionEvent)
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
        logger.info("Network Channel Exception: " + e.getChannel().getId(), e
                .getCause());
        OpenR66ProtocolException exception = 
            OpenR66ExceptionTrappedFactory.getExceptionFromTrappedException(e.getChannel(), e);
        if (exception != null) {
            logger.error("Network Channel Exception: " + e.getChannel().getId(), exception);
            if (exception instanceof OpenR66ProtocolBusinessNoWriteBackException) {
                logger.error("Will close channel",exception);
                Channels.close(e.getChannel());
                return;
            }
            final ConnectionErrorPacket errorPacket = new ConnectionErrorPacket(exception
                    .getMessage(), null);
            this.writeError(e.getChannel(), ChannelUtils.NOCHANNEL, ChannelUtils.NOCHANNEL, errorPacket);
        } else {
            // Nothing to do
            return;
        }
        logger.info("Will close channel");
        Channels.close(e.getChannel());
    }

    private void writeError(Channel channel, Integer remoteId, Integer localId, ConnectionErrorPacket error) {
        NetworkPacket networkPacket = null;;
        try {
            networkPacket = new NetworkPacket(
                    localId, remoteId, error.getType(), error.getLocalPacket());
        } catch (OpenR66ProtocolPacketException e) {
        }
        ChannelUtils.write(channel, networkPacket).awaitUninterruptibly();
    }
}
