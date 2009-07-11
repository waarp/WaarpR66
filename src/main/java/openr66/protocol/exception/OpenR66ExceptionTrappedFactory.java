/**
 *
 */
package openr66.protocol.exception;

import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;

import java.io.IOException;
import java.net.BindException;
import java.net.ConnectException;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedChannelException;

import openr66.protocol.networkhandler.NetworkTransaction;
import openr66.protocol.utils.OpenR66SignalHandler;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelException;
import org.jboss.netty.channel.ExceptionEvent;

/**
 * @author frederic bregier
 */
public class OpenR66ExceptionTrappedFactory {
    /**
     * Internal Logger
     */
    private static final GgInternalLogger logger = GgInternalLoggerFactory
            .getLogger(OpenR66ExceptionTrappedFactory.class);

    /**
     * @param channel
     * @param e
     * @return the OpenR66ProtocolException corresponding to the ExceptionEvent,
     *         or null if the exception should be ignored
     */
    public static OpenR66ProtocolException getExceptionFromTrappedException(
            Channel channel, ExceptionEvent e) {
        final Throwable e1 = e.getCause();
        if (e1 instanceof ConnectException) {
            final ConnectException e2 = (ConnectException) e1;
            logger.warn("Connection impossible since {} with Channel {}", e2
                    .getMessage(), channel);
            return new OpenR66ProtocolNetworkException("Connection impossible",
                    e2);
        } else if (e1 instanceof ChannelException) {
            final ChannelException e2 = (ChannelException) e1;
            logger
                    .warn(
                            "Connection (example: timeout) impossible since {} with Channel {}",
                            e2.getMessage(), channel);
            return new OpenR66ProtocolNetworkException(
                    "Connection (example: timeout) impossible", e2);
        } else if (e1 instanceof CancelledKeyException) {
            final CancelledKeyException e2 = (CancelledKeyException) e1;
            logger.warn("Connection aborted since {}", e2.getMessage());
            // XXX TODO FIXME is it really what we should do ?
            // No action
            return null;
        } else if (e1 instanceof ClosedChannelException) {
            logger.warn("Connection closed before end");
            if (NetworkTransaction.isShuttingdownNetworkChannel(channel) ||
                    OpenR66SignalHandler.isInShutdown()) {
                // no action
                return null;
            }
            return new OpenR66ProtocolBusinessNoWriteBackException(
                    "Connection closed before end", e1);
        } else if (e1 instanceof OpenR66ProtocolBusinessNoWriteBackException) {
            final OpenR66ProtocolBusinessNoWriteBackException e2 = (OpenR66ProtocolBusinessNoWriteBackException) e1;
            logger.error("Command Error Reply", e2);
            return e2;
        } else if (e1 instanceof OpenR66ProtocolShutdownException) {
            final OpenR66ProtocolShutdownException e2 = (OpenR66ProtocolShutdownException) e1;
            logger.info("Command Shutdown", e2);
            return e2;
        } else if (e1 instanceof OpenR66ProtocolException) {
            final OpenR66ProtocolException e2 = (OpenR66ProtocolException) e1;
            logger.warn("Command Error Reply", e2);
            return e2;
        } else if (e1 instanceof BindException) {
            final BindException e2 = (BindException) e1;
            logger.warn("Address already in use {}", e2.getMessage());
            return new OpenR66ProtocolNetworkException(
                    "Address already in use", e2);
        } else if (e1 instanceof ConnectException) {
            final ConnectException e2 = (ConnectException) e1;
            logger.warn("Timeout occurs {}", e2.getMessage());
            return new OpenR66ProtocolNetworkException("Timeout occurs", e2);
        } else if (e1 instanceof NullPointerException) {
            final NullPointerException e2 = (NullPointerException) e1;
            logger.warn("Null pointer Exception", e2);
            return new OpenR66ProtocolSystemException("Null Pointer Exception",
                    e2);
        } else if (e1 instanceof IOException) {
            final IOException e2 = (IOException) e1;
            logger.warn("Connection aborted since {} with Channel {}", e2
                    .getMessage(), channel);
            return new OpenR66ProtocolSystemException("Connection aborted", e2);
        } else {
            logger.warn("Unexpected exception from downstream" +
                    " Ref Channel: " + channel.toString(), e1);
        }
        return new OpenR66ProtocolSystemException("Unexpected exception", e1);
    }
}
