/**
 * 
 */
package openr66.protocol.localhandler;

import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;
import openr66.protocol.config.Configuration;
import openr66.protocol.exception.OpenR66ExceptionTrappedFactory;
import openr66.protocol.exception.OpenR66ProtocolException;
import openr66.protocol.exception.OpenR66ProtocolShutdownException;
import openr66.protocol.localhandler.packet.AbstractLocalPacket;
import openr66.protocol.localhandler.packet.ErrorPacket;
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
public class LocalClientHandler extends SimpleChannelHandler {
    /**
     * Internal Logger
     */
    private static final GgInternalLogger logger = GgInternalLoggerFactory
            .getLogger(LocalClientHandler.class);

    private LocalChannelReference localChannelReference = null;

    /*
     * (non-Javadoc)
     * @see
     * org.jboss.netty.channel.SimpleChannelHandler#channelClosed(org.jboss.
     * netty.channel.ChannelHandlerContext,
     * org.jboss.netty.channel.ChannelStateEvent)
     */
    @Override
    public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e)
            throws Exception {
        // FIXME nothing to do ?
        logger.info("Local Client Channel Closed: " + e.getChannel().getId());
        if (localChannelReference != null) {
            Configuration.configuration.getLocalTransaction().removeFromId(
                    localChannelReference.getId());
            localChannelReference = null;
        }
    }

    /*
     * (non-Javadoc)
     * @see
     * org.jboss.netty.channel.SimpleChannelHandler#channelConnected(org.jboss
     * .netty.channel.ChannelHandlerContext,
     * org.jboss.netty.channel.ChannelStateEvent)
     */
    @Override
    public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e)
            throws Exception {
        // FIXME once connected, first message should create the
        // LocalChannelReference (not now)
        logger
                .info("Local Client Channel Connected: "
                        + e.getChannel().getId());
    }

    private void initLocalClientHandler(Channel channel)
            throws InterruptedException {
        while (localChannelReference == null) {
            Thread.sleep(Configuration.RETRYINMS);
            localChannelReference = Configuration.configuration
                    .getLocalTransaction().getFromId(channel.getId());
        }
    }

    /*
     * (non-Javadoc)
     * @see
     * org.jboss.netty.channel.SimpleChannelHandler#messageReceived(org.jboss
     * .netty.channel.ChannelHandlerContext,
     * org.jboss.netty.channel.MessageEvent)
     */
    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
            throws Exception {
        if (localChannelReference == null) {
            initLocalClientHandler(e.getChannel());
        }
        // FIXME now handle message from local server and write back them the
        // network channel
        final AbstractLocalPacket packet = (AbstractLocalPacket) e.getMessage();
        logger.info("Local Client Channel Recv: " + e.getChannel().getId());
        // FIXME write back to the network channel
        final NetworkPacket networkPacket = new NetworkPacket(
                localChannelReference.getId(), localChannelReference
                        .getRemoteId(), packet.getLocalPacket());
        Channels
                .write(localChannelReference.getNetworkChannel(), networkPacket);
    }

    /*
     * (non-Javadoc)
     * @see
     * org.jboss.netty.channel.SimpleChannelHandler#exceptionCaught(org.jboss
     * .netty.channel.ChannelHandlerContext,
     * org.jboss.netty.channel.ExceptionEvent)
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
            throws Exception {
        // FIXME informs network of the problem
        logger.error("Local Client Channel Exception: "
                + e.getChannel().getId(), e.getCause());
        if (localChannelReference != null) {
            OpenR66ProtocolException exception = 
                OpenR66ExceptionTrappedFactory.getExceptionFromTrappedException(e.getChannel(), e);
            if (exception != null) {
                if (exception instanceof OpenR66ProtocolShutdownException) {
                    new Thread(new ChannelUtils()).start();
                    Channels.close(e.getChannel());
                    return;
                }
                final ErrorPacket errorPacket = new ErrorPacket(exception
                        .getMessage(), null, null);
                final NetworkPacket networkPacket = new NetworkPacket(
                        localChannelReference.getId(), localChannelReference
                                .getRemoteId(), errorPacket.getLocalPacket());
                Channels.write(localChannelReference.getNetworkChannel(),
                        networkPacket).awaitUninterruptibly(Configuration.TIMEOUTCON);
            } else {
                // Nothing to do
                return;
            }
        }
        Channels.close(e.getChannel());
    }

}
