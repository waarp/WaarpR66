/**
 *
 */
package openr66.protocol.localhandler;

import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;
import openr66.context.ErrorCode;
import openr66.context.R66Result;
import openr66.protocol.configuration.Configuration;
import openr66.protocol.exception.OpenR66Exception;
import openr66.protocol.exception.OpenR66ExceptionTrappedFactory;
import openr66.protocol.exception.OpenR66ProtocolBusinessNoWriteBackException;
import openr66.protocol.exception.OpenR66ProtocolNetworkException;
import openr66.protocol.exception.OpenR66ProtocolNoConnectionException;
import openr66.protocol.exception.OpenR66ProtocolShutdownException;
import openr66.protocol.exception.OpenR66ProtocolSystemException;
import openr66.protocol.localhandler.packet.AbstractLocalPacket;
import openr66.protocol.localhandler.packet.ErrorPacket;
import openr66.protocol.localhandler.packet.LocalPacketFactory;
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

    /**
     * Local Channel Reference
     */
    private volatile LocalChannelReference localChannelReference = null;

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
        logger.debug("Local Client Channel Closed: {}", e.getChannel().getId());
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
        logger
                .debug("Local Client Channel Connected: " +
                        e.getChannel().getId());
    }
    /**
     * Initiate the LocalChannelReference
     * @param channel
     * @throws InterruptedException
     * @throws OpenR66ProtocolNetworkException
     */
    private void initLocalClientHandler(Channel channel)
            throws InterruptedException, OpenR66ProtocolNetworkException {
        int i = 0;
        if (localChannelReference == null) {
            for (i = 0; i < Configuration.RETRYNB; i ++) {
                localChannelReference = Configuration.configuration
                        .getLocalTransaction().getFromId(channel.getId());
                if (localChannelReference != null) {
                    return;
                }
            }
            throw new OpenR66ProtocolNetworkException(
                    "Cannot find local connection");
        }
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
        if (localChannelReference == null) {
            initLocalClientHandler(e.getChannel());
        }
        // only Startup Packet should arrived here !
        final AbstractLocalPacket packet = (AbstractLocalPacket) e.getMessage();
        if (packet.getType() != LocalPacketFactory.STARTUPPACKET) {
            logger.error("Local Client Channel Recv wrong packet: " +
                    e.getChannel().getId() + " : " + packet.toString());
            throw new OpenR66ProtocolSystemException("Should not be here");
        }
        logger.debug("LocalClientHandler initialized: " +
                (localChannelReference != null));
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
        // informs network of the problem
        logger.debug(
                "Local Client Channel Exception: {}",e.getChannel().getId(), e
                        .getCause());
        if (localChannelReference == null) {
            initLocalClientHandler(e.getChannel());
        }
        if (localChannelReference != null) {
            OpenR66Exception exception = OpenR66ExceptionTrappedFactory
                    .getExceptionFromTrappedException(e.getChannel(), e);
            if (exception != null) {
                if (exception instanceof OpenR66ProtocolShutdownException) {
                    Thread thread = new Thread(new ChannelUtils());
                    thread.setDaemon(true);
                    thread.start();
                    logger.info("Will close channel");
                    Channels.close(e.getChannel());
                    return;
                } else if (exception instanceof OpenR66ProtocolBusinessNoWriteBackException) {
                    logger.error("Will close channel", exception);
                    Channels.close(e.getChannel());
                    return;
                } else if (exception instanceof OpenR66ProtocolNoConnectionException) {
                    logger.error("Will close channel", exception);
                    Channels.close(e.getChannel());
                    return;
                }
                final ErrorPacket errorPacket = new ErrorPacket(exception
                        .getMessage(),
                        ErrorCode.RemoteError.getCode(), ErrorPacket.FORWARDCLOSECODE);
                ChannelUtils.writeAbstractLocalPacket(localChannelReference, errorPacket)
                    .awaitUninterruptibly();
                if (!localChannelReference.getFutureRequest().isDone()) {
                    localChannelReference.invalidateRequest(new R66Result(
                            exception, null, true, ErrorCode.Internal, null));
                }
            } else {
                // Nothing to do
                return;
            }
        }
        logger.info("Will close channel");
        ChannelUtils.close(e.getChannel());
    }

}
