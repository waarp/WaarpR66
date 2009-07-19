/**
 *
 */
package openr66.protocol.localhandler;

import goldengate.common.command.exception.Reply421Exception;
import goldengate.common.command.exception.Reply530Exception;
import goldengate.common.exception.FileTransferException;
import goldengate.common.file.DataBlock;
import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;
import openr66.filesystem.R66File;
import openr66.filesystem.R66Rule;
import openr66.filesystem.R66Session;
import openr66.protocol.config.Configuration;
import openr66.protocol.exception.OpenR66ExceptionTrappedFactory;
import openr66.protocol.exception.OpenR66ProtocolBusinessException;
import openr66.protocol.exception.OpenR66ProtocolBusinessNoWriteBackException;
import openr66.protocol.exception.OpenR66ProtocolException;
import openr66.protocol.exception.OpenR66ProtocolNoConnectionException;
import openr66.protocol.exception.OpenR66ProtocolNoDataException;
import openr66.protocol.exception.OpenR66ProtocolNotAuthenticatedException;
import openr66.protocol.exception.OpenR66ProtocolPacketException;
import openr66.protocol.exception.OpenR66ProtocolShutdownException;
import openr66.protocol.exception.OpenR66ProtocolSystemException;
import openr66.protocol.localhandler.packet.AbstractLocalPacket;
import openr66.protocol.localhandler.packet.AuthentPacket;
import openr66.protocol.localhandler.packet.ConnectionErrorPacket;
import openr66.protocol.localhandler.packet.DataPacket;
import openr66.protocol.localhandler.packet.ErrorPacket;
import openr66.protocol.localhandler.packet.LocalPacketFactory;
import openr66.protocol.localhandler.packet.RequestPacket;
import openr66.protocol.localhandler.packet.ShutdownPacket;
import openr66.protocol.localhandler.packet.StartupPacket;
import openr66.protocol.localhandler.packet.TestPacket;
import openr66.protocol.localhandler.packet.ValidPacket;
import openr66.protocol.networkhandler.NetworkTransaction;
import openr66.protocol.networkhandler.packet.NetworkPacket;
import openr66.protocol.utils.ChannelUtils;
import openr66.task.TaskRunner;
import openr66.task.exception.OpenR66RunnerErrorException;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipelineCoverage;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;

/**
 * The local server handler handles real end file operations.
 *
 * @author frederic bregier
 */
@ChannelPipelineCoverage("one")
public class LocalServerHandler extends SimpleChannelHandler {
    /**
     * Internal Logger
     */
    private static final GgInternalLogger logger = GgInternalLoggerFactory
            .getLogger(LocalServerHandler.class);

    private R66Session session;

    private boolean status = false;

    private LocalChannelReference localChannelReference;

    /*
     * (non-Javadoc)
     *
     * @see
     * org.jboss.netty.channel.SimpleChannelHandler#channelClosed(org.jboss.
     * netty.channel.ChannelHandlerContext,
     * org.jboss.netty.channel.ChannelStateEvent)
     */
    @Override
    public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e) {
        logger.info("Local Server Channel Closed: " +
                status +
                " " +
                (localChannelReference != null? localChannelReference
                        .toString() : "no LocalChannelReference"));
        // FIXME clean session objects like files
        if (localChannelReference != null &&
                !localChannelReference.getFutureAction().isDone()) {
            if (!status) {
                logger.error("Finalize BUT SHOULD NOT");
            }
            setFinalize(status, null);
        }
        if (localChannelReference != null) {
            NetworkTransaction.removeNetworkChannel(localChannelReference
                    .getNetworkChannel());
        } else {
            logger
                    .error("Local Server Channel Closed but no LocalChannelReference: " +
                            e.getChannel().getId());
        }
        session.clear();
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
    public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) {
        logger
                .info("Local Server Channel Connected: " +
                        e.getChannel().getId());
        session = new R66Session();
        // FIXME prepare session objects
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
            throws OpenR66ProtocolException {
        // FIXME action as requested and answer if necessary
        final AbstractLocalPacket packet = (AbstractLocalPacket) e.getMessage();
        logger.info("Local Server Channel Recv: " + e.getChannel().getId() +
                " : " + packet.getClass().getSimpleName());
        if (packet.getType() == LocalPacketFactory.STARTUPPACKET) {
            startup(e.getChannel(), (StartupPacket) packet);
        } else {
            if (localChannelReference == null) {
                logger.error("No LocalChannelReference at " +
                        packet.getClass().getName());
                setFinalize(false, null);
                final ErrorPacket errorPacket = new ErrorPacket(
                        "No LocalChannelReference at " +
                                packet.getClass().getName(), null,
                        ErrorPacket.FORWARDCLOSECODE);
                Channels.write(e.getChannel(), errorPacket)
                        .awaitUninterruptibly();
                ChannelUtils.close(e.getChannel());
                return;
            }
            switch (packet.getType()) {
                case LocalPacketFactory.AUTHENTPACKET: {
                    authent(e.getChannel(), (AuthentPacket) packet);
                    break;
                }
                    // Already done case LocalPacketFactory.STARTUPPACKET:
                case LocalPacketFactory.DATAPACKET: {
                    data(e.getChannel(), (DataPacket) packet);
                    break;
                }
                case LocalPacketFactory.VALIDPACKET: {
                    valid(e.getChannel(), (ValidPacket) packet);
                    break;
                }
                case LocalPacketFactory.ERRORPACKET: {
                    error(e.getChannel(), (ErrorPacket) packet);
                    break;
                }
                case LocalPacketFactory.CONNECTERRORPACKET: {
                    connectionError(e.getChannel(),
                            (ConnectionErrorPacket) packet);
                    break;
                }
                case LocalPacketFactory.REQUESTPACKET: {
                    request(e.getChannel(), (RequestPacket) packet);
                    break;
                }
                case LocalPacketFactory.SHUTDOWNPACKET: {
                    shutdown(e.getChannel(), (ShutdownPacket) packet);
                    break;
                }
                case LocalPacketFactory.STATUSPACKET:
                case LocalPacketFactory.CANCELPACKET:
                case LocalPacketFactory.CONFIGSENDPACKET:
                case LocalPacketFactory.CONFIGRECVPACKET: {
                    logger.error("Unimplemented Mesg: " +
                            packet.getClass().getName());
                    setFinalize(false, null);
                    final ErrorPacket errorPacket = new ErrorPacket(
                            "Unkown Mesg: " + packet.getClass().getName(),
                            null, ErrorPacket.FORWARDCLOSECODE);
                    writeBack(errorPacket, true);
                    ChannelUtils.close(e.getChannel());
                    break;
                }
                case LocalPacketFactory.TESTPACKET: {
                    test(e.getChannel(), (TestPacket) packet);
                    break;
                }
                default: {
                    logger
                            .error("Unknown Mesg: " +
                                    packet.getClass().getName());
                    setFinalize(false, null);
                    final ErrorPacket errorPacket = new ErrorPacket(
                            "Unkown Mesg: " + packet.getClass().getName(),
                            null, ErrorPacket.FORWARDCLOSECODE);
                    writeBack(errorPacket, true);
                    ChannelUtils.close(e.getChannel());
                }
            }
        }
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
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
        // inform clients
        OpenR66ProtocolException exception = OpenR66ExceptionTrappedFactory
                .getExceptionFromTrappedException(e.getChannel(), e);
        if (exception != null) {
            if (exception instanceof OpenR66ProtocolShutdownException) {
                logger.warn("Shutdown order received and going from: " +
                        session.getAuth().getUser());
                setFinalize(true, null);
                // XXX dont'close
                new Thread(new ChannelUtils()).start();
                // set global shutdown info and before close, send a valid
                // shutdown to all
                return;
            } else {
                setFinalize(false, null);
            }
            if (exception instanceof OpenR66ProtocolBusinessNoWriteBackException) {
                logger.error("Will close channel", exception);
                Channels.close(e.getChannel());
                return;
            } else if (exception instanceof OpenR66ProtocolNoConnectionException) {
                logger.error("Will close channel", exception);
                Channels.close(e.getChannel());
                return;
            }
            final ErrorPacket errorPacket = new ErrorPacket(exception
                    .getMessage(), null, ErrorPacket.FORWARDCLOSECODE);
            try {
                writeBack(errorPacket, true);
            } catch (OpenR66ProtocolPacketException e1) {
                // should not be
            }
            ChannelUtils.close(e.getChannel());
        } else {
            // Nothing to do
            return;
        }
    }

    private void startup(Channel channel, StartupPacket packet)
            throws OpenR66ProtocolPacketException {
        logger.info("Recv: " + packet.toString());
        localChannelReference = Configuration.configuration
                .getLocalTransaction().getFromId(packet.getLocalId());
        if (localChannelReference == null) {
            logger.error("Cannot startup");
            setFinalize(false, null);
            ErrorPacket error = new ErrorPacket("Cannot startup connection",
                    null, ErrorPacket.FORWARDCLOSECODE);
            Channels.write(channel, error).awaitUninterruptibly();
            // Cannot do writeBack(error, true);
            ChannelUtils.close(channel);
            return;
        }
        session.setLocalChannelReference(localChannelReference);
        Channels.write(channel, packet);
        logger.info("Get LocalChannel: " + localChannelReference.getLocalId());
    }

    private void authent(Channel channel, AuthentPacket packet)
            throws OpenR66ProtocolPacketException {
        try {
            session.getAuth().connection(packet.getHostId(), packet.getKey());
        } catch (Reply530Exception e1) {
            logger.error("Cannot connect: " + packet.getHostId(), e1);
            setFinalize(false, e1);
            ErrorPacket error = new ErrorPacket("Connection not allowed", null,
                    ErrorPacket.FORWARDCLOSECODE);
            writeBack(error, true);
            localChannelReference.validateConnection(false, error);
            ChannelUtils.close(channel);
            return;
        } catch (Reply421Exception e1) {
            logger.error("Service unavailable: " + packet.getHostId(), e1);
            setFinalize(false, e1);
            ErrorPacket error = new ErrorPacket("Service unavailable", null,
                    ErrorPacket.FORWARDCLOSECODE);
            writeBack(error, true);
            localChannelReference.validateConnection(false, error);
            ChannelUtils.close(channel);
            return;
        }
        localChannelReference.validateConnection(true, packet);
        if (packet.isToValidate()) {
            packet.validate();
            writeBack(packet, false);
        }
    }

    private void connectionError(Channel channel, ConnectionErrorPacket packet) {
        // FIXME do something according to the error
        logger.error(channel.getId() + ": " + packet.toString());
        setFinalize(false, null);
        Channels.close(channel);
    }

    private void error(Channel channel, ErrorPacket packet)
            throws OpenR66ProtocolBusinessNoWriteBackException {
        // FIXME do something according to the error
        logger.error(channel.getId() + ": " + packet.toString());
        throw new OpenR66ProtocolBusinessNoWriteBackException(packet.toString());
    }

    private void request(Channel channel, RequestPacket packet)
            throws OpenR66ProtocolNotAuthenticatedException,
            OpenR66ProtocolNoDataException, OpenR66ProtocolPacketException {
        // FIXME do something
        if (!session.isAuthenticated()) {
            throw new OpenR66ProtocolNotAuthenticatedException(
                    "Not authenticated");
        }
        R66Rule rule;
        try {
            rule = R66Rule.getHash(packet.getRulename());
        } catch (OpenR66ProtocolNoDataException e) {
            logger.error("Rule is unknown: " + packet.getRulename(), e);
            throw e;
        }
        if (packet.isToValidate()) {
            if (!rule.checkHostAllow(session.getAuth().getUser())) {
                throw new OpenR66ProtocolNotAuthenticatedException(
                        "Rule is not allowed for the remote host");
            }
        }
        session.setRequest(packet);
        session.setBlockSize(packet.getBlocksize());
        TaskRunner runner;
        if (packet.getSpecialId() > 0) {
            // Reload
            runner = new TaskRunner(session, rule, packet.getSpecialId());
        } else {
            boolean isRetrieve = packet.getMode() == RequestPacket.RECVMD5MODE ||
                    packet.getMode() == RequestPacket.RECVMODE;
            if (!packet.isToValidate()) {
                isRetrieve = !isRetrieve;
            }
            runner = new TaskRunner(session, rule, isRetrieve);
            packet.setSpecialId(runner.getSpecialId());
        }
        // FIXME should be load from database or from remote status and/or saved
        // to database
        try {
            session.setRunner(runner);
        } catch (OpenR66RunnerErrorException e) {
            logger.error("PresTask in error", e);
            setFinalize(false, e);
            ErrorPacket error = new ErrorPacket("PreTask in error", e
                    .getMessage(), ErrorPacket.FORWARDCLOSECODE);
            writeBack(error, true);
            ChannelUtils.close(channel);
            return;
        }
        session.setReady(true);
        // inform back
        if (packet.isToValidate()) {
            if (runner.isRetrieve()) {
                // In case Wildcard was used
                logger.warn("New filename: " + runner.getFilename());
                packet.setFilename(R66File.getBasename(runner.getFilename()));
            }
            packet.validate();
            writeBack(packet, true);
        }
        // FIXME if retrieve => start the retrieve operation
        if (runner.isRetrieve()) {
            NetworkTransaction.runRetrieve(session, channel);
        }
    }

    private void data(Channel channel, DataPacket packet)
            throws OpenR66ProtocolNotAuthenticatedException,
            OpenR66ProtocolBusinessException, OpenR66ProtocolPacketException {
        // FIXME do something
        if (!session.isAuthenticated()) {
            throw new OpenR66ProtocolNotAuthenticatedException(
                    "Not authenticated");
        }
        if (!session.isReady()) {
            throw new OpenR66ProtocolBusinessException("No request prepared");
        }
        if (session.getRunner().isRetrieve()) {
            throw new OpenR66ProtocolBusinessException(
                    "Not in receive mode but receive a packet");
        }
        if (packet.getPacketRank() != session.getRunner().getRank()) {
            logger.warn("Bad rank: " + packet.getPacketRank() + " : " +
                    session.getRunner().getRank());
        }
        DataBlock dataBlock = new DataBlock();
        // FIXME if MD5 check MD5
        dataBlock.setBlock(packet.getData());
        try {
            session.getFile().writeDataBlock(dataBlock);
            session.getRunner().incrementRank();
        } catch (FileTransferException e) {
            try {
                session.setFinalizeTransfer(false, e);
            } catch (OpenR66RunnerErrorException e1) {
            } catch (OpenR66ProtocolSystemException e1) {
            }
            ErrorPacket error = new ErrorPacket("Transfer in error",
                    localChannelReference.getFutureAction().getResult()
                            .toString(), ErrorPacket.FORWARDCLOSECODE);
            writeBack(error, true);
            ChannelUtils.close(channel);
            return;
        }
    }

    private void test(Channel channel, TestPacket packet)
            throws OpenR66ProtocolNotAuthenticatedException,
            OpenR66ProtocolPacketException {
        if (!session.isAuthenticated()) {
            throw new OpenR66ProtocolNotAuthenticatedException(
                    "Not authenticated");
        }
        logger.info(channel.getId() + ": " + packet.toString());
        // simply write back after+1
        packet.update();
        if (packet.getType() == LocalPacketFactory.VALIDPACKET) {
            logger.info(packet.toString());
            ValidPacket validPacket = new ValidPacket(packet.toString(), null,
                    LocalPacketFactory.TESTPACKET);
            setFinalize(true, validPacket);
            writeBack(validPacket, true);
            Channels.close(channel);
        } else {
            writeBack(packet, false);
        }
    }

    private void valid(Channel channel, ValidPacket packet)
            throws OpenR66ProtocolNotAuthenticatedException,
            OpenR66RunnerErrorException, OpenR66ProtocolSystemException {
        // FIXME do something
        if (packet.getTypeValid() != LocalPacketFactory.SHUTDOWNPACKET &&
                !session.isAuthenticated()) {
            throw new OpenR66ProtocolNotAuthenticatedException(
                    "Not authenticated");
        }
        switch (packet.getTypeValid()) {
            case LocalPacketFactory.REQUESTPACKET:
                logger.info("Valid Request " +
                        localChannelReference.toString() + " " +
                        packet.toString());
                // int rank = Integer.parseInt(packet.getSmiddle());
                session.setFinalizeTransfer(true, session.getFile());
                Channels.close(channel);
                break;
            case LocalPacketFactory.SHUTDOWNPACKET:
                logger.warn("Shutdown received so Will close channel" +
                        localChannelReference.toString());
                NetworkTransaction
                        .shuttingdownNetworkChannel(localChannelReference
                                .getNetworkChannel());
                setFinalize(false, packet);
                Channels.close(channel);
                break;
            case LocalPacketFactory.TESTPACKET:
                logger.warn("Valid TEST so Will close channel" +
                        localChannelReference.toString());
                setFinalize(true, packet);
                Channels.close(channel);
                break;
            default:
                logger.warn("Validation is ignored: " + packet.getTypeValid());
        }
    }

    private void shutdown(Channel channel, ShutdownPacket packet)
            throws OpenR66ProtocolShutdownException,
            OpenR66ProtocolNotAuthenticatedException,
            OpenR66ProtocolBusinessException {
        if (!session.isAuthenticated()) {
            throw new OpenR66ProtocolNotAuthenticatedException(
                    "Not authenticated");
        }
        if (session.getAuth().isAdmin() &&
                Configuration.configuration.isKeyValid(packet.getKey())) {
            throw new OpenR66ProtocolShutdownException("Shutdown Type received");
        }
        logger.error("Invalid Shutdown command");
        throw new OpenR66ProtocolBusinessException("Invalid Shutdown comand");
    }

    public void setFinalize(boolean status, Object finalValue) {
        this.status = status;
        if (localChannelReference != null) {
            localChannelReference.validateAction(status, finalValue);
        }
    }

    private void writeBack(AbstractLocalPacket packet, boolean await)
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
        if (await) {
            Channels.write(localChannelReference.getNetworkChannel(),
                    networkPacket).awaitUninterruptibly();
        } else {
            Channels.write(localChannelReference.getNetworkChannel(),
                    networkPacket);
        }
    }
}
