/**
 *
 */
package openr66.protocol.networkhandler;

import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;
import openr66.protocol.exception.OpenR66ProtocolNetworkException;
import openr66.protocol.exception.OpenR66ProtocolRemoteShutdownException;

import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipelineCoverage;
import org.jboss.netty.channel.ChannelStateEvent;

/**
 * Network Server Handler for Passive connection (Requested side)
 * @author frederic bregier
 */
@ChannelPipelineCoverage("one")
public class NetworkServerPassiveHandler extends NetworkServerHandler {
    /**
     * Internal Logger
     */
    private static final GgInternalLogger logger = GgInternalLoggerFactory
            .getLogger(NetworkServerPassiveHandler.class);

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
        throws OpenR66ProtocolNetworkException {
        //Fix to prevent passive channel to close itself alone
        try {
            NetworkTransaction.setPassiveNetworkChannel(e.getChannel());
        } catch (OpenR66ProtocolRemoteShutdownException e1) {
            logger.warn("Passive Connectionin error",e1);
        }
        super.channelConnected(ctx, e);
    }
}
