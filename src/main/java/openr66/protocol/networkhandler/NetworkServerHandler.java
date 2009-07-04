/**
 * 
 */
package openr66.protocol.networkhandler;

import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;
import openr66.protocol.config.Configuration;
import openr66.protocol.exception.OpenR66ExceptionTrappedFactory;
import openr66.protocol.exception.OpenR66ProtocolException;
import openr66.protocol.exception.OpenR66ProtocolSystemException;
import openr66.protocol.localhandler.LocalChannelReference;
import openr66.protocol.localhandler.packet.ErrorPacket;
import openr66.protocol.networkhandler.packet.NetworkPacket;
import openr66.protocol.utils.ChannelUtils;

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
        logger.info("Network Channel Closed: " + e.getChannel().getId());
        // FIXME close if necessary the local channel
        if (localChannelReference != null) {
            logger.info("Will close Local channel");
            Channels.close(localChannelReference.getLocalChannel());
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
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
            throws Exception {
        logger.info("Network Channel Recv: " + e.getChannel().getId());
        final NetworkPacket packet = (NetworkPacket) e.getMessage();
        if (localChannelReference == null) {
            OpenR66ProtocolSystemException ebak = null;
            for (int i = 0; i < Configuration.RETRYNB*2; i++) {
                try {
                    localChannelReference = Configuration.configuration
                        .getLocalTransaction().createNewClient(e.getChannel(),
                                packet.getRemoteId());
                    ebak = null;
                    break;
                } catch (OpenR66ProtocolSystemException e1) {
                    Thread.sleep(Configuration.RETRYINMS);
                    ebak = e1;
                }
            }
            if (ebak != null) {
                throw ebak;
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
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
            throws Exception {
        logger.error("Network Channel Exception: " + e.getChannel().getId(), e
                .getCause());
        if (localChannelReference != null) {
            OpenR66ProtocolException exception = 
                OpenR66ExceptionTrappedFactory.getExceptionFromTrappedException(e.getChannel(), e);
            if (exception != null) {
                final ErrorPacket errorPacket = new ErrorPacket(exception
                        .getMessage(), null, ErrorPacket.FORWARDCLOSECODE);
                final NetworkPacket networkPacket = new NetworkPacket(
                        localChannelReference.getId(), localChannelReference
                                .getRemoteId(), errorPacket.getLocalPacket());
                ChannelUtils.write(e.getChannel(), networkPacket)
                    .awaitUninterruptibly();
            } else {
                // Nothing to do
                return;
            }
        }
        logger.info("Will close channel");
        Channels.close(e.getChannel());
    }

}
