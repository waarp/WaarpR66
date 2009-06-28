/**
 * 
 */
package openr66.protocol.networkhandler;

import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;
import openr66.protocol.localhandler.LocalChannelReference;
import openr66.protocol.packet.ErrorPacket;
import openr66.protocol.packet.NetworkPacket;

import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipelineCoverage;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;

/**
 * @author frederic bregier
 * 
 */
@ChannelPipelineCoverage("one")
public class NetworkServerHandler extends SimpleChannelHandler {
    /**
     * Internal Logger
     */
    private static final GgInternalLogger logger = GgInternalLoggerFactory
            .getLogger(NetworkServerHandler.class);
    
    private LocalChannelReference localChannelReference = null;
    /*
     * (non-Javadoc)
     * 
     * @see
     * org.jboss.netty.channel.SimpleChannelHandler#channelClosed(org.jboss.
     * netty.channel.ChannelHandlerContext,
     * org.jboss.netty.channel.ChannelStateEvent)
     */
    @Override
    public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e)
            throws Exception {
        logger.info("Network Channel Closed: "+e.getChannel().getId());
        // FIXME close if necessary the local channel
        if (localChannelReference != null) {
            Channels.close(localChannelReference.getLocalChannel());
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.jboss.netty.channel.SimpleChannelHandler#channelConnected(org.jboss
     * .netty.channel.ChannelHandlerContext,
     * org.jboss.netty.channel.ChannelStateEvent)
     */
    @Override
    public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e)
            throws Exception {
        logger.info("Network Channel Connected: "+e.getChannel().getId());
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.jboss.netty.channel.SimpleChannelHandler#messageReceived(org.jboss
     * .netty.channel.ChannelHandlerContext,
     * org.jboss.netty.channel.MessageEvent)
     */
    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
            throws Exception {
        logger.info("Network Channel Recv: "+e.getChannel().getId());
        NetworkPacket packet = (NetworkPacket) e.getMessage();
        if (localChannelReference == null) {
            localChannelReference = NetworkTransaction.configuration.getLocalTransaction().
                createNewClient(e.getChannel(), packet.getRemoteId());
        }
        Channels.write(localChannelReference.getLocalChannel(), packet.getBuffer());
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.jboss.netty.channel.SimpleChannelHandler#exceptionCaught(org.jboss
     * .netty.channel.ChannelHandlerContext,
     * org.jboss.netty.channel.ExceptionEvent)
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
            throws Exception {
        logger.error("Network Channel Exception: "+e.getChannel().getId(), e.getCause());
        if (localChannelReference != null) {
            ErrorPacket packet = new ErrorPacket(e.getCause().getMessage(),null,null);
            NetworkPacket networkPacket = new NetworkPacket(localChannelReference.getId(),
                    localChannelReference.getRemoteId(),packet.getLocalPacket());
            Channels.write(e.getChannel(), networkPacket);
        }
    }

}
