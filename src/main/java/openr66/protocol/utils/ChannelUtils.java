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
package openr66.protocol.utils;

import goldengate.common.file.DataBlock;
import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;
import goldengate.common.logging.GgSlf4JLoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.sql.Timestamp;

import openr66.context.ErrorCode;
import openr66.context.R66Session;
import openr66.database.DbConstant;
import openr66.database.DbPreparedStatement;
import openr66.database.DbSession;
import openr66.database.data.DbTaskRunner;
import openr66.database.exception.OpenR66DatabaseException;
import openr66.protocol.configuration.Configuration;
import openr66.protocol.exception.OpenR66ProtocolPacketException;
import openr66.protocol.localhandler.LocalChannelReference;
import openr66.protocol.localhandler.packet.AbstractLocalPacket;
import openr66.protocol.localhandler.packet.DataPacket;
import openr66.protocol.localhandler.packet.EndTransferPacket;
import openr66.protocol.localhandler.packet.ErrorPacket;
import openr66.protocol.localhandler.packet.LocalPacketFactory;
import openr66.protocol.localhandler.packet.RequestPacket;
import openr66.protocol.networkhandler.NetworkTransaction;
import openr66.protocol.networkhandler.packet.NetworkPacket;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.group.ChannelGroupFuture;
import org.jboss.netty.channel.group.ChannelGroupFutureListener;
import org.jboss.netty.handler.execution.OrderedMemoryAwareThreadPoolExecutor;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.LoggerContext;

/**
 * Channel Utils
 * @author Frederic Bregier
 */
public class ChannelUtils extends Thread {
    /**
     * Internal Logger
     */
    private static final GgInternalLogger logger = GgInternalLoggerFactory
            .getLogger(ChannelUtils.class);

    public static final Integer NOCHANNEL = Integer.MIN_VALUE;

    /**
     * Get the Remote InetAddress
     *
     * @param channel
     * @return the remote InetAddress
     */
    public static InetAddress getRemoteInetAddress(Channel channel) {
        InetSocketAddress socketAddress = (InetSocketAddress) channel
                .getRemoteAddress();
        if (socketAddress == null) {
            socketAddress = new InetSocketAddress(20);
        }
        return socketAddress.getAddress();
    }

    /**
     * Get the Local InetAddress
     *
     * @param channel
     * @return the local InetAddress
     */
    public static InetAddress getLocalInetAddress(Channel channel) {
        final InetSocketAddress socketAddress = (InetSocketAddress) channel
                .getLocalAddress();
        return socketAddress.getAddress();
    }

    /**
     * Get the Remote InetSocketAddress
     *
     * @param channel
     * @return the remote InetSocketAddress
     */
    public static InetSocketAddress getRemoteInetSocketAddress(Channel channel) {
        return (InetSocketAddress) channel.getRemoteAddress();
    }

    /**
     * Get the Local InetSocketAddress
     *
     * @param channel
     * @return the local InetSocketAddress
     */
    public static InetSocketAddress getLocalInetSocketAddress(Channel channel) {
        return (InetSocketAddress) channel.getLocalAddress();
    }

    /**
     * Finalize resources attached to handlers
     *
     * @author Frederic Bregier
     */
    private static class R66ChannelGroupFutureListener implements
            ChannelGroupFutureListener {
        OrderedMemoryAwareThreadPoolExecutor pool;
        String name;
        ChannelFactory channelFactory;

        public R66ChannelGroupFutureListener(
                String name,
                OrderedMemoryAwareThreadPoolExecutor pool,
                ChannelFactory channelFactory) {
            this.name = name;
            this.pool = pool;
            this.channelFactory = channelFactory;
        }

        public void operationComplete(ChannelGroupFuture future)
                throws Exception {
            if (pool != null) {
                pool.shutdownNow();
            }
            if (channelFactory != null) {
                channelFactory.releaseExternalResources();
            }
            logger.info("Done with shutdown "+name);
        }
    }

    /**
     * Terminate all registered channels
     *
     * @return the number of previously registered network channels
     */
    private static int terminateCommandChannels() {
        final int result = Configuration.configuration.getServerChannelGroup()
                .size();
        logger.debug("ServerChannelGroup: " + result);
        Configuration.configuration.getServerChannelGroup().close()
                .addListener(
                        new R66ChannelGroupFutureListener(
                                "ServerChannelGroup",
                                Configuration.configuration
                                        .getServerPipelineExecutor(),
                                Configuration.configuration
                                        .getServerChannelFactory()));
        return result;
    }
    /**
     * Terminate all registered Http channels
     *
     * @return the number of previously registered http network channels
     */
    private static int terminateHttpChannels() {
        final int result = Configuration.configuration.getHttpChannelGroup()
                .size();
        logger.debug("HttpChannelGroup: " + result);
        Configuration.configuration.getHttpChannelGroup().close()
                .addListener(
                        new R66ChannelGroupFutureListener(
                                "HttpChannelGroup",
                                null,
                                Configuration.configuration
                                        .getHttpChannelFactory()));
        Configuration.configuration.getHttpsChannelFactory().releaseExternalResources();
        return result;
    }
    /**
     * Return the current number of network connections
     *
     * @param configuration
     * @return the current number of network connections
     */
    public static int nbCommandChannels(Configuration configuration) {
        return configuration.getServerChannelGroup().size();
    }

    /**
     *
     * @param channel
     */
    public static void close(Channel channel) {
        try {
            Thread.sleep(Configuration.WAITFORNETOP);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        Channels.close(channel);
    }

    /**
     *
     * @param localChannelReference
     * @param block
     * @return the ChannelFuture of this write operation
     * @throws OpenR66ProtocolPacketException
     */
    public static ChannelFuture writeBackDataBlock(
            LocalChannelReference localChannelReference, DataBlock block)
            throws OpenR66ProtocolPacketException {
        ChannelBuffer md5 = ChannelBuffers.EMPTY_BUFFER;
        DbTaskRunner runner = localChannelReference.getSession().getRunner();
        if (RequestPacket.isMD5Mode(runner.getMode())) {
            md5 = FileUtils.getHash(block.getBlock());
        }
        DataPacket data = new DataPacket(runner.getRank(), block.getBlock()
                .copy(), md5);
        ChannelFuture future = writeAbstractLocalPacket(localChannelReference, data);
        runner.incrementRank();
        return future;
    }

    /**
     * Write the EndTransfer
     *
     * @param localChannelReference
     * @throws OpenR66ProtocolPacketException
     */
    public static void writeEndTransfer(
            LocalChannelReference localChannelReference)
    throws OpenR66ProtocolPacketException {
        EndTransferPacket packet = new EndTransferPacket(
                LocalPacketFactory.REQUESTPACKET);
        writeAbstractLocalPacket(localChannelReference, packet);
    }
    /**
     * Write an AbstractLocalPacket to the network Channel
     * @param localChannelReference
     * @param packet
     * @return the ChannelFuture on write operation
     * @throws OpenR66ProtocolPacketException
     */
    public static ChannelFuture writeAbstractLocalPacket(
            LocalChannelReference localChannelReference, AbstractLocalPacket packet)
    throws OpenR66ProtocolPacketException {
        NetworkPacket networkPacket;
        try {
            networkPacket = new NetworkPacket(localChannelReference
                    .getLocalId(), localChannelReference.getRemoteId(), packet);
        } catch (OpenR66ProtocolPacketException e) {
            logger.error("Cannot construct message from " + packet.toString(),
                    e);
            throw e;
        }
        return Channels.write(localChannelReference.getNetworkChannel(), networkPacket);
    }

    /**
     * Write an AbstractLocalPacket to the Local Channel
     * @param localChannelReference
     * @param packet
     * @return the ChannelFuture on write operation
     * @throws OpenR66ProtocolPacketException
     */
    public static ChannelFuture writeAbstractLocalPacketToLocal(
            LocalChannelReference localChannelReference, AbstractLocalPacket packet)
    throws OpenR66ProtocolPacketException {
        return Channels.write(localChannelReference.getLocalChannel(), packet);
    }
    /**
     * Stop all selected transfers
     * @param dbSession
     * @param limit
     * @param builder
     * @param session
     * @param body
     * @param startid
     * @param stopid
     * @param tstart
     * @param tstop
     * @param rule
     * @param req
     * @param pending
     * @param transfer
     * @param error
     * @return
     */
    public static StringBuilder StopAllTransfer(DbSession dbSession, int limit,
            StringBuilder builder, R66Session session, String body,
            String startid, String stopid, Timestamp tstart, Timestamp tstop, String rule,
            String req, boolean pending, boolean transfer, boolean error) {
        DbPreparedStatement preparedStatement = null;
        try {
            preparedStatement =
                DbTaskRunner.getFilterPrepareStament(dbSession, limit, true,
                        startid, stopid, tstart, tstop, rule, req,
                        pending, transfer, error, false, false);
            preparedStatement.executeQuery();
            int i = 0;
            while (preparedStatement.getNext()) {
                i++;
                DbTaskRunner taskRunner = DbTaskRunner.getFromStatement(preparedStatement);
                LocalChannelReference lcr =
                    Configuration.configuration.getLocalTransaction().
                    getFromRequest(taskRunner.getKey());
                ErrorCode result;
                ErrorCode code = ErrorCode.StoppedTransfer;
                if (lcr != null) {
                    int rank = taskRunner.getRank();
                    ErrorPacket perror = new ErrorPacket("Transfer Stopped at "+rank,
                            code.getCode(), ErrorPacket.FORWARDCLOSECODE);
                    try {
                        //XXX ChannelUtils.writeAbstractLocalPacket(lcr, perror);
                        // inform local instead of remote
                        ChannelUtils.writeAbstractLocalPacketToLocal(lcr, perror);
                    } catch (Exception e) {
                    }
                    result = ErrorCode.StoppedTransfer;
                } else {
                    // Transfer is not running
                    // But is the database saying the contrary
                    result = ErrorCode.TransferError;
                    if (taskRunner != null) {
                        if (taskRunner.stopOrCancelRunner(code)) {
                            result = ErrorCode.StoppedTransfer;
                        }
                    }
                }
                ErrorCode last = taskRunner.getErrorInfo();
                taskRunner.setErrorExecutionStatus(result);
                if (builder != null) {
                    builder.append(taskRunner.toSpecializedHtml(session, body,
                        lcr != null ? "Active" : "NotActive"));
                }
                taskRunner.setErrorExecutionStatus(last);
            }
            preparedStatement.realClose();
            return builder;
        } catch (OpenR66DatabaseException e) {
            if (preparedStatement != null) {
                preparedStatement.realClose();
            }
            logger.warn("OpenR66 Error",e);
            return null;
        }
    }
    /**
     * Exit global ChannelFactory
     */
    public static void exit() {
        // First try to StopAll
        StopAllTransfer(DbConstant.admin.session, 0,
                null, null, null, null, null, null, null, null, null, true, true, true);
        Configuration.configuration.isShutdown = true;
        Configuration.configuration.prepareServerStop();
        final long delay = Configuration.configuration.TIMEOUTCON;
        // Inform others that shutdown
        Configuration.configuration.getLocalTransaction()
                .shutdownLocalChannels();
        logger.warn("Exit: Give a delay of " + delay + " ms");
        try {
            Thread.sleep(delay);
        } catch (final InterruptedException e) {
        }
        NetworkTransaction.closeRetrieveExecutors();
        Configuration.configuration.getLocalTransaction().debugPrintActiveLocalChannels();
        Configuration.configuration.getGlobalTrafficShapingHandler()
                .releaseExternalResources();
        logger.info("Exit Shutdown Command");
        terminateCommandChannels();
        logger.info("Exit Shutdown Local");
        Configuration.configuration.getLocalTransaction().closeAll();
        logger.info("Exit Shutdown Http");
        terminateHttpChannels();
        OpenR66SignalHandler.closeAllConnection();
        Configuration.configuration.serverStop();
        System.err.println("Exit end of Shutdown");
        Thread.currentThread().interrupt();
    }

    public static void stopLogger() {
        if (GgInternalLoggerFactory.getDefaultFactory() instanceof GgSlf4JLoggerFactory) {
            LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
            lc.stop();
        }
    }
    /**
     * This function is the top function to be called when the server is to be
     * shutdown.
     */
    @Override
    public void run() {
        OpenR66SignalHandler.terminate(false);
    }
}
