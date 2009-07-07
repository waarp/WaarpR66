/**
 * 
 */
package openr66.protocol.localhandler;

import goldengate.common.command.exception.Reply421Exception;
import goldengate.common.command.exception.Reply530Exception;
import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;
import openr66.filesystem.R66Session;
import openr66.protocol.config.Configuration;
import openr66.protocol.exception.OpenR66ExceptionTrappedFactory;
import openr66.protocol.exception.OpenR66ProtocolBusinessException;
import openr66.protocol.exception.OpenR66ProtocolBusinessNoWriteBackException;
import openr66.protocol.exception.OpenR66ProtocolException;
import openr66.protocol.exception.OpenR66ProtocolNotAuthenticatedException;
import openr66.protocol.exception.OpenR66ProtocolPacketException;
import openr66.protocol.exception.OpenR66ProtocolShutdownException;
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
import openr66.protocol.localhandler.packet.ValidateConnectionPacket;
import openr66.protocol.networkhandler.NetworkTransaction;
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
     * @see
     * org.jboss.netty.channel.SimpleChannelHandler#channelClosed(org.jboss.
     * netty.channel.ChannelHandlerContext,
     * org.jboss.netty.channel.ChannelStateEvent)
     */
    @Override
    public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e) {
        logger.warn("Local Server Channel Closed: " +status+" "+ ((this.localChannelReference != null) ? this.localChannelReference.toString() : "no LocalChannelReference"));
        // FIXME clean session objects like files
        if ((this.localChannelReference != null) && (! this.localChannelReference.getFuture().isDone())) {
            if (!status) {
                logger.error("Finalize BUT SHOULD NOT");
            }
            this.setFinalize(status, null);
        }
        if (localChannelReference != null) {
            NetworkTransaction.removeNetworkChannel(localChannelReference.getNetworkChannel());
        } else {
            logger.error("Local Server Channel Closed but no LocalChannelReference: " + e.getChannel().getId());
        }
        session.clear();
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
        logger
                .info("Local Server Channel Connected: "
                        + e.getChannel().getId());
        this.session = new R66Session();
        // FIXME prepare session objects
    }

    /*
     * (non-Javadoc)
     * @see
     * org.jboss.netty.channel.SimpleChannelHandler#messageReceived(org.jboss
     * .netty.channel.ChannelHandlerContext,
     * org.jboss.netty.channel.MessageEvent)
     */
    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws OpenR66ProtocolException {
        // FIXME action as requested and answer if necessary
        final AbstractLocalPacket packet = (AbstractLocalPacket) e.getMessage();
        logger.info("Local Server Channel Recv: " + e.getChannel().getId()+" : "+packet.getClass().getSimpleName());
        if (packet instanceof StartupPacket) {
            startup(e.getChannel(), (StartupPacket) packet);
        } else {
            if (localChannelReference == null) {
                logger.error("No LocalChannelReference at "+packet.getClass().getName());
                setFinalize(false, null);
                final ErrorPacket errorPacket = new ErrorPacket("No LocalChannelReference at "+packet.getClass().getName(), 
                        null, ErrorPacket.FORWARDCLOSECODE);
                ChannelUtils.write(e.getChannel(), errorPacket).awaitUninterruptibly();
                ChannelUtils.close(e.getChannel());
                return;
            }
            if (packet instanceof DataPacket) {
                data(e.getChannel(), (DataPacket) packet);
            } else if (packet instanceof ValidPacket) {
                valid(e.getChannel(),(ValidPacket) packet);
            } else if (packet instanceof RequestPacket) {
                request(e.getChannel(),(RequestPacket) packet);
            } else if (packet instanceof AuthentPacket) {
                authent(e.getChannel(), (AuthentPacket) packet);
            } else if (packet instanceof ErrorPacket) {
                error(e.getChannel(), (ErrorPacket) packet);
            } else if (packet instanceof TestPacket) {
                test(e.getChannel(), (TestPacket) packet);
            } else if (packet instanceof ShutdownPacket) {
                shutdown(e.getChannel(), (ShutdownPacket) packet);
            } else if (packet instanceof ConnectionErrorPacket) {
                connectionError(e.getChannel(), (ConnectionErrorPacket) packet);
            } else if (packet instanceof ValidateConnectionPacket) {
                validateConnection(e.getChannel(), (ValidateConnectionPacket) packet);
            } else {
                logger.error("Unknown Mesg: "+packet.getClass().getName());
                setFinalize(false, null);
                final ErrorPacket errorPacket = new ErrorPacket("Unkown Mesg: "+packet.getClass().getName(), 
                        null, ErrorPacket.FORWARDCLOSECODE);
                writeBack(errorPacket, true);
                ChannelUtils.close(e.getChannel());
            }
        }
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
        // FIXME inform clients
        OpenR66ProtocolException exception = 
            OpenR66ExceptionTrappedFactory.getExceptionFromTrappedException(e.getChannel(), e);
        if (exception != null) {
            if (exception instanceof OpenR66ProtocolShutdownException) {
                setFinalize(true, null);
                new Thread(new ChannelUtils()).start();
            } else {
                setFinalize(false, null);
            }
            if (exception instanceof OpenR66ProtocolBusinessNoWriteBackException) {
                logger.error("Will close channel", exception);
                Channels.close(e.getChannel());
                return;
            }
            final ErrorPacket errorPacket = new ErrorPacket(exception
                    .getMessage(), null, ErrorPacket.FORWARDCLOSECODE);
            // XXX FIXME Do not close, client will close
            try {
                writeBack(errorPacket, true);
            } catch (OpenR66ProtocolPacketException e1) {
                // should not be
            }
            ChannelUtils.close(e.getChannel());
            //ChannelUtils.write(e.getChannel(), errorPacket);
        } else {
            // Nothing to do
            return;
        }
    }

    private void startup(Channel channel, StartupPacket packet) throws OpenR66ProtocolPacketException {
        logger.info("Recv: "+packet.toString());
        localChannelReference = 
            Configuration.configuration.getLocalTransaction().getFromId(packet.getLocalId());
        if (localChannelReference == null) {
            logger.error("Cannot startup");
            setFinalize(false, null);
            ErrorPacket error = new ErrorPacket("Cannot startup connection",null,ErrorPacket.FORWARDCLOSECODE);
            // XXX FIXME  Do not close, client will close
            ChannelUtils.write(channel, error).awaitUninterruptibly();
            //Cannot do writeBack(error, true);
            ChannelUtils.close(channel);
            return;
        }
        ChannelUtils.write(channel, packet);
        logger.info("Get LocalChannel: "+localChannelReference.getLocalId());
    }
    private void validateConnection(Channel channel, ValidateConnectionPacket packet) throws OpenR66ProtocolPacketException {
        logger.info("Recv: "+packet.toString());
        this.localChannelReference.validateConnection(true);
        if (packet.isToValidate()) {
            packet.validate();
            writeBack(packet, false);
        }
    }
    
    
    private void authent(Channel channel, AuthentPacket packet) throws OpenR66ProtocolPacketException {
        try {
            this.session.getAuth().connection(packet.getHostId(), packet.getKey());
        } catch (Reply530Exception e1) {
            logger.error("Cannot connect: "+packet.getHostId(),e1);
            setFinalize(false, e1);
            ErrorPacket error = new ErrorPacket("Connection not allowed",null,ErrorPacket.FORWARDCLOSECODE);
            // XXX FIXME Do not close, client will close
            writeBack(error, true);
            ChannelUtils.close(channel);
            //ChannelUtils.write(channel, error);
            return;
        } catch (Reply421Exception e1) {
            logger.error("Service unavailable: "+packet.getHostId(),e1);
            setFinalize(false, e1);
            ErrorPacket error = new ErrorPacket("Service unavailable",null,ErrorPacket.FORWARDCLOSECODE);
            // XXX FIXME Do not close, client will close
            writeBack(error, true);
            ChannelUtils.close(channel);
            //ChannelUtils.write(channel, error);
            return;
        }
        ValidPacket validPacket = new ValidPacket("Authenticated",null,packet.getType());
        writeBack(validPacket, false);
        //ChannelUtils.write(channel, validPacket);
    }
    private void data(Channel channel, DataPacket packet) throws OpenR66ProtocolNotAuthenticatedException {
        // FIXME do something
        if (! this.session.isAuthenticated()) {
            throw new OpenR66ProtocolNotAuthenticatedException("Not authenticated");
        }
    }
    private void connectionError(Channel channel, ConnectionErrorPacket packet) {
        // FIXME do something according to the error
        logger.error(channel.getId() + ": "
                + packet.toString());
        setFinalize(false, null);
        Channels.close(channel);
    }
    private void error(Channel channel, ErrorPacket packet) throws OpenR66ProtocolBusinessNoWriteBackException {
        // FIXME do something according to the error
        logger.error(channel.getId() + ": "
                + packet.toString());
        throw new OpenR66ProtocolBusinessNoWriteBackException(packet.toString());
    }
    private void request(Channel channel, RequestPacket packet) throws OpenR66ProtocolNotAuthenticatedException {
        // FIXME do something
        if (! this.session.isAuthenticated()) {
            throw new OpenR66ProtocolNotAuthenticatedException("Not authenticated");
        }
        
    }
    private void test(Channel channel, TestPacket packet) throws OpenR66ProtocolNotAuthenticatedException, OpenR66ProtocolPacketException {
        /*if (! this.session.isAuthenticated()) {
            throw new OpenR66ProtocolNotAuthenticatedException("Not authenticated");
        }*/
        logger.info(channel.getId() + ": "
                + packet.toString());
        // simply write back after+1
        packet.update();
        if (packet.getType() == LocalPacketFactory.ERRORPACKET) {
            logger.warn(packet.toString());
            ValidPacket validPacket = new ValidPacket(packet.toString(),null,LocalPacketFactory.TESTPACKET);
            // XXX FIXME dont'close, client will do: no, it will not
            setFinalize(true, validPacket);
            writeBack(validPacket, true);
            Channels.close(channel);
            //ChannelUtils.write(channel, validPacket).awaitUninterruptibly();
            //Channels.close(channel);
        } else {
            writeBack(packet, false);
            //ChannelUtils.write(channel, packet);
        }
    }
    private void valid(Channel channel, ValidPacket packet) throws OpenR66ProtocolNotAuthenticatedException {
        // FIXME do something
        if (packet.getTypeValid() == LocalPacketFactory.TESTPACKET) {
            logger.warn("Valid TEST so Will close channel"+localChannelReference.toString());
            setFinalize(true, packet);
            Channels.close(channel);
            return;
        }
        if (! this.session.isAuthenticated()) {
            throw new OpenR66ProtocolNotAuthenticatedException("Not authenticated");
        }
        
    }
    private void shutdown(Channel channel, ShutdownPacket packet) throws OpenR66ProtocolShutdownException, OpenR66ProtocolNotAuthenticatedException, OpenR66ProtocolBusinessException {
        if (! this.session.isAuthenticated()) {
            throw new OpenR66ProtocolNotAuthenticatedException("Not authenticated");
        }
        if (this.session.getAuth().isAdmin() && this.session.getAuth().isKeyValid(packet.getKey())) {
            throw new OpenR66ProtocolShutdownException("Shutdown Type received");                
        }
        logger.error("Invalid Shutdown command");
        throw new OpenR66ProtocolBusinessException("Invalid Shutdown comand");
    }
    
    private void setFinalize(boolean status, Object finalValue) {
        this.status = status;
        if (localChannelReference != null) {
            if (!localChannelReference.getFuture().isDone()) {
                if (status) {
                    localChannelReference.getFuture().setResult(finalValue);
                    localChannelReference.getFuture().setSuccess();                    
                } else {
                    localChannelReference.getFuture().setResult(finalValue);
                    localChannelReference.getFuture().cancel();
                }
            }
        }        
    }
    
    private void writeBack(AbstractLocalPacket packet, boolean await) throws OpenR66ProtocolPacketException {
        NetworkPacket networkPacket;
        try {
            networkPacket = new NetworkPacket(
                    localChannelReference.getLocalId(), localChannelReference
                            .getRemoteId(), packet.getType(), packet.getLocalPacket());
        } catch (OpenR66ProtocolPacketException e) {
            logger.error("Cannot construct message from "+packet.toString(), e);
            throw e;
        }
        if (await) {
            ChannelUtils.write(localChannelReference.getNetworkChannel(), networkPacket).awaitUninterruptibly();
        } else {
            ChannelUtils.write(localChannelReference.getNetworkChannel(), networkPacket);
        }
    }
}
