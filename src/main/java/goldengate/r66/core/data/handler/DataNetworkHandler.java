/**
 * Copyright 2009, Frederic Bregier, and individual contributors by the @author
 * tags. See the COPYRIGHT.txt in the distribution for a full listing of
 * individual contributors. This is free software; you can redistribute it
 * and/or modify it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 3.0 of the License,
 * or (at your option) any later version. This software is distributed in the
 * hope that it will be useful, but WITHOUT ANY WARRANTY; without even the
 * implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See
 * the GNU Lesser General Public License for more details. You should have
 * received a copy of the GNU Lesser General Public License along with this
 * software; if not, write to the Free Software Foundation, Inc., 51 Franklin
 * St, Fifth Floor, Boston, MA 02110-1301 USA, or see the FSF site:
 * http://www.fsf.org.
 */
package goldengate.r66.core.data.handler;

import goldengate.common.exception.FileEndOfTransferException;
import goldengate.common.exception.FileTransferException;
import goldengate.common.exception.InvalidArgumentException;
import goldengate.common.file.DataBlock;
import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;
import goldengate.r66.core.control.NetworkHandler;
import goldengate.r66.core.data.R66TransferControl;

import java.io.IOException;
import java.net.BindException;
import java.net.ConnectException;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.NotYetConnectedException;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelException;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineCoverage;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;

/**
 * Network handler for Data connections
 * 
 * @author Frederic Bregier
 */
@ChannelPipelineCoverage("one")
public class DataNetworkHandler extends SimpleChannelHandler {
    /**
     * Internal Logger
     */
    private static final GgInternalLogger logger = GgInternalLoggerFactory
            .getLogger(DataNetworkHandler.class);

    /**
     * Business Data Handler
     */
    private DataBusinessHandler dataBusinessHandler = null;

    /**
     * Configuration
     */
    private final FtpConfiguration configuration;

    /**
     * Is this Data Connection an Active or Passive one
     */
    private final boolean isActive;

    /**
     * Internal store for the SessionInterface
     */
    private FtpSession session = null;

    /**
     * The associated Channel
     */
    private Channel dataChannel = null;

    /**
     * Pipeline
     */
    private ChannelPipeline channelPipeline = null;

    /**
     * True when the DataNetworkHandler is fully ready (to prevent action before
     * ready)
     */
    private boolean isReady = false;

    /**
     * Constructor from DataBusinessHandler
     * 
     * @param configuration
     * @param handler
     * @param active
     */
    public DataNetworkHandler(FtpConfiguration configuration,
            DataBusinessHandler handler, boolean active) {
        super();
        this.configuration = configuration;
        dataBusinessHandler = handler;
        dataBusinessHandler.setDataNetworkHandler(this);
        isActive = active;
    }

    /**
     * @return the dataBusinessHandler
     * @throws FtpNoConnectionException
     */
    public DataBusinessHandler getDataBusinessHandler()
            throws FtpNoConnectionException {
        if (dataBusinessHandler == null) {
            throw new FtpNoConnectionException("No Data Connection active");
        }
        return dataBusinessHandler;
    }

    /**
     * @return the session
     */
    public FtpSession getFtpSession() {
        return session;
    }

    /**
     * @return the NetworkHandler associated with the control connection
     */
    public NetworkHandler getNetworkHandler() {
        return session.getBusinessHandler().getNetworkHandler();
    }

    /**
     * Run firstly executeChannelClosed.
     * 
     * @throws Exception
     * @see org.jboss.netty.channel.SimpleChannelHandler#channelClosed(org.jboss.netty.channel.ChannelHandlerContext,
     *      org.jboss.netty.channel.ChannelStateEvent)
     */
    @Override
    public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e)
            throws Exception {
        if (session != null) {
            // logger.debug("Channel closed, about to set it down");
            session.getDataConn().getFtpTransferControl().setPreEndOfTransfer();
            session.getDataConn().unbindPassive();
            try {
                getDataBusinessHandler().executeChannelClosed();
                // release file and other permanent objects
                getDataBusinessHandler().clear();
            } catch (final FtpNoConnectionException e1) {
            }
            // logger.debug("Channel closed inform closed");
            session.getDataConn().getFtpTransferControl()
                    .setClosedDataChannel();
            dataBusinessHandler = null;
            channelPipeline = null;
            dataChannel = null;
            // logger.debug("Channel closed: finish");
        }
        super.channelClosed(ctx, e);
    }

    /**
     * Initialize the Handler.
     * 
     * @see org.jboss.netty.channel.SimpleChannelHandler#channelConnected(org.jboss.netty.channel.ChannelHandlerContext,
     *      org.jboss.netty.channel.ChannelStateEvent)
     */
    @Override
    public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) {
        final Channel channel = e.getChannel();
        // First get the ftpSession from inetaddresses
        for (int i = 0; i < FtpInternalConfiguration.RETRYNB; i++) {
            session = configuration.getFtpSession(channel, isActive);
            if (session == null) {
                logger.warn("Session not found at try " + i);
                try {
                    Thread.sleep(FtpInternalConfiguration.RETRYINMS);
                } catch (final InterruptedException e1) {
                    break;
                }
            } else {
                break;
            }
        }
        if (session == null) {
            // Not found !!!
            logger.error("Session not found!");
            Channels.close(channel);
            // Problem: control connection could not be directly informed!!!
            // Only timeout will occur
            return;
        }
        // logger.debug("Start DataNetwork");
        channelPipeline = ctx.getPipeline();
        dataChannel = channel;
        dataBusinessHandler.setFtpSession(getFtpSession());
        FtpChannelUtils.addDataChannel(channel, session.getConfiguration());
        if (isStillAlive()) {
            setCorrectCodec();
            session.getDataConn().getFtpTransferControl().setOpenedDataChannel(
                    channel, this);
        } else {
            // Cannot continue
            session.getDataConn().getFtpTransferControl().setOpenedDataChannel(
                    null, this);
            return;
        }
        isReady = true;
        // logger.debug("End of Start DataNetwork");
    }

    /**
     * Set the CODEC according to the mode. Must be called after each call of
     * MODE, STRU or TYPE
     */
    public void setCorrectCodec() {
        final R66DataModeCodec modeCodec = (R66DataModeCodec) channelPipeline
                .get(R66DataPipelineFactory.CODEC_MODE);
        final R66DataTypeCodec typeCodec = (R66DataTypeCodec) channelPipeline
                .get(R66DataPipelineFactory.CODEC_TYPE);
        final FtpDataStructureCodec structureCodec = (FtpDataStructureCodec) channelPipeline
                .get(R66DataPipelineFactory.CODEC_STRUCTURE);
        modeCodec.setMode(session.getDataConn().getMode());
        modeCodec.setStructure(session.getDataConn().getStructure());
        typeCodec.setFullType(session.getDataConn().getType(), session
                .getDataConn().getSubType());
        structureCodec.setStructure(session.getDataConn().getStructure());
        // logger.debug("Set Correct Codec: {}", session.getDataConn());
    }

    /**
     * Unlock the Mode Codec from openConnection of {@link R66TransferControl}
     */
    public void unlockModeCodec() {
        final R66DataModeCodec modeCodec = (R66DataModeCodec) channelPipeline
                .get("MODE");
        modeCodec.setCodecReady();
    }

    /**
     * Default exception task: close the current connection after calling
     * exceptionLocalCaught.
     * 
     * @see org.jboss.netty.channel.SimpleChannelHandler#exceptionCaught(org.jboss.netty.channel.ChannelHandlerContext,
     *      org.jboss.netty.channel.ExceptionEvent)
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
        if (session == null) {
            logger.warn("Error without any session active {}", e.getCause());
            return;
        }
        final Throwable e1 = e.getCause();
        if (e1 instanceof ConnectException) {
            final ConnectException e2 = (ConnectException) e1;
            logger.warn("Connection impossible since {}", e2.getMessage());
        } else if (e1 instanceof ChannelException) {
            final ChannelException e2 = (ChannelException) e1;
            logger.warn("Connection (example: timeout) impossible since {}", e2
                    .getMessage());
        } else if (e1 instanceof ClosedChannelException) {
            logger.warn("Connection closed before end");
        } else if (e1 instanceof InvalidArgumentException) {
            final InvalidArgumentException e2 = (InvalidArgumentException) e1;
            logger.warn("Bad configuration in Codec in " + e2.getMessage(), e2);
        } else if (e1 instanceof NullPointerException) {
            final NullPointerException e2 = (NullPointerException) e1;
            logger.warn("Null pointer Exception", e2);
            try {
                if (dataBusinessHandler != null) {
                    dataBusinessHandler.exceptionLocalCaught(e);
                    if (session.getDataConn() != null) {
                        session.getDataConn().getFtpTransferControl()
                                .setTransferAbortedFromInternal(true);
                    }
                }
            } catch (final NullPointerException e3) {
            }
            return;
        } else if (e1 instanceof CancelledKeyException) {
            final CancelledKeyException e2 = (CancelledKeyException) e1;
            logger.warn("Connection aborted since {}", e2.getMessage());
            // XXX TODO FIXME is it really what we should do ?
            // No action
            return;
        } else if (e1 instanceof IOException) {
            final IOException e2 = (IOException) e1;
            logger.warn("Connection aborted since {}", e2.getMessage());
        } else if (e1 instanceof NotYetConnectedException) {
            final NotYetConnectedException e2 = (NotYetConnectedException) e1;
            logger.info("Ignore this exception {}", e2.getMessage());
            return;
        } else if (e1 instanceof BindException) {
            final BindException e2 = (BindException) e1;
            logger.warn("Address already in use {}", e2.getMessage());
        } else if (e1 instanceof ConnectException) {
            final ConnectException e2 = (ConnectException) e1;
            logger.warn("Timeout occurs {}", e2.getMessage());
        } else {
            logger.warn("Unexpected exception from downstream:", e1);
        }
        if (dataBusinessHandler != null) {
            dataBusinessHandler.exceptionLocalCaught(e);
        }
        session.getDataConn().getFtpTransferControl()
                .setTransferAbortedFromInternal(true);
    }

    /**
     * To enable continues of Retrieve operation (prevent OOM)
     * 
     * @see org.jboss.netty.channel.SimpleChannelHandler#channelInterestChanged(org.jboss.netty.channel.ChannelHandlerContext,
     *      org.jboss.netty.channel.ChannelStateEvent)
     */
    @Override
    public void channelInterestChanged(ChannelHandlerContext arg0,
            ChannelStateEvent arg1) {
        final int op = arg1.getChannel().getInterestOps();
        if ((op == Channel.OP_NONE) || (op == Channel.OP_READ)) {
            if (isReady) {
                session.getDataConn().getFtpTransferControl().runTrueRetrieve();
            }
        }
    }

    /**
     * Act as needed according to the receive DataBlock message
     * 
     * @see org.jboss.netty.channel.SimpleChannelHandler#messageReceived(org.jboss.netty.channel.ChannelHandlerContext,
     *      org.jboss.netty.channel.MessageEvent)
     */
    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
        if (isStillAlive()) {
            final DataBlock dataBlock = (DataBlock) e.getMessage();
            try {
                session.getDataConn().getFtpTransferControl()
                        .getExecutingFtpTransfer().getFtpFile().writeDataBlock(
                                dataBlock);
            } catch (final FtpNoFileException e1) {
                // logger.debug("NoFile", e1);
                session.getDataConn().getFtpTransferControl()
                        .setTransferAbortedFromInternal(true);
                return;
            } catch (final FtpNoTransferException e1) {
                // logger.debug("NoTransfer", e1);
                session.getDataConn().getFtpTransferControl()
                        .setTransferAbortedFromInternal(true);
                return;
            } catch (final FileEndOfTransferException e1) {
                if (dataBlock.isEOF()) {
                    session.getDataConn().getFtpTransferControl()
                            .setPreEndOfTransfer();
                }
            } catch (final FileTransferException e1) {
                // logger.debug("TransferException", e1);
                session.getDataConn().getFtpTransferControl()
                        .setTransferAbortedFromInternal(true);
            }
        } else {
            // Shutdown
            session.getDataConn().getFtpTransferControl()
                    .setTransferAbortedFromInternal(true);
        }
    }

    /**
     * Write a simple message (like LIST) and wait for it
     * 
     * @param message
     * @return True if the message is correctly written
     */
    public boolean writeMessage(String message) {
        final DataBlock dataBlock = new DataBlock();
        dataBlock.setEOF(true);
        final ChannelBuffer buffer = ChannelBuffers.wrappedBuffer(message
                .getBytes());
        dataBlock.setBlock(buffer);
        // logger.debug("Message to be sent: {}", message);
        return Channels.write(dataChannel, dataBlock).awaitUninterruptibly()
                .isSuccess();
    }

    /**
     * If the service is going to shutdown, it sends back a 421 message to the
     * connection
     * 
     * @return True if the service is alive, else False if the system is going
     *         down
     */
    private boolean isStillAlive() {
        if (session.getConfiguration().isShutdown) {
            session.setExitErrorCode("Service is going down: disconnect");
            return false;
        }
        return true;
    }
}
