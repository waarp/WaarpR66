/**
   This file is part of GoldenGate Project (named also GoldenGate or GG).

   Copyright 2009, Frederic Bregier, and individual contributors by the @author
   tags. See the COPYRIGHT.txt in the distribution for a full listing of
   individual contributors.

   All GoldenGate Project is free software: you can redistribute it and/or 
   modify it under the terms of the GNU General Public License as published 
   by the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   GoldenGate is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with GoldenGate .  If not, see <http://www.gnu.org/licenses/>.
 */
package openr66.client;

import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;
import goldengate.common.logging.GgSlf4JLoggerFactory;

import java.net.SocketAddress;

import openr66.configuration.FileBasedConfiguration;
import openr66.context.ErrorCode;
import openr66.context.R66FiniteDualStates;
import openr66.context.authentication.R66Auth;
import openr66.database.DbConstant;
import openr66.database.data.DbHostAuth;
import openr66.protocol.configuration.Configuration;
import openr66.protocol.exception.OpenR66ProtocolNoConnectionException;
import openr66.protocol.exception.OpenR66ProtocolPacketException;
import openr66.protocol.localhandler.LocalChannelReference;
import openr66.protocol.localhandler.packet.BusinessRequestPacket;
import openr66.protocol.networkhandler.NetworkTransaction;
import openr66.protocol.utils.ChannelUtils;
import openr66.protocol.utils.R66Future;

import org.jboss.netty.channel.Channels;
import org.jboss.netty.logging.InternalLoggerFactory;

/**
 * Abstract class for internal Business Request
 * @author Frederic Bregier
 *
 */
public abstract class AbstractBusinessRequest implements Runnable {
    /**
     * Internal Logger
     */
    static protected volatile GgInternalLogger logger;
    
    public static final String BUSINESSREQUEST = "BusinessRequest";

    protected final R66Future future;

    protected final String remoteHost;

    protected final NetworkTransaction networkTransaction;

    private final BusinessRequestPacket businessPacket;

    private LocalChannelReference localChannelReference;

    public AbstractBusinessRequest(Class<?> clasz, 
            R66Future future,
            String remoteHost,
            NetworkTransaction networkTransaction,
            BusinessRequestPacket packet) {
        if (logger == null) {
            logger = GgInternalLoggerFactory.getLogger(clasz);
        }
        this.future = future;
        this.remoteHost = remoteHost;
        this.networkTransaction = networkTransaction;
        this.businessPacket = packet;
    }

    public void run() {
        try {
            initRequest();
            sendRequest();
        } catch (OpenR66ProtocolNoConnectionException e) {
        }
    }
    
    public void initRequest() throws OpenR66ProtocolNoConnectionException {
        DbHostAuth host = R66Auth.getServerAuth(DbConstant.admin.session,
                remoteHost);
        final SocketAddress socketServerAddress = host.getSocketAddress();
        boolean isSSL = host.isSsl();
        localChannelReference = networkTransaction
            .createConnectionWithRetry(socketServerAddress, isSSL, future);
        if (localChannelReference == null) {
            future.setResult(null);
            OpenR66ProtocolNoConnectionException e = 
                new OpenR66ProtocolNoConnectionException(
                    "Cannot connect to server " + host.toString());
            future.setFailure(e);
            throw e;
        }
        localChannelReference.sessionNewState(R66FiniteDualStates.BUSINESSR);
    }
    
    public void sendRequest() {
        try {
            ChannelUtils.writeAbstractLocalPacket(localChannelReference, businessPacket, false);
        } catch (OpenR66ProtocolPacketException e) {
            future.setResult(null);
            future.setFailure(e);
            Channels.close(localChannelReference.getLocalChannel());
            return;
        }
        
    }

    /**
     * Dummy Main method
     * @param args
     */
    public static void main(String[] args) {
        InternalLoggerFactory.setDefaultFactory(new GgSlf4JLoggerFactory(
                null));
        if (logger == null) {
            logger = GgInternalLoggerFactory.getLogger(AbstractBusinessRequest.class);
        }
        if (! getParams(args)) {
            logger.error("Wrong initialization");
            if (DbConstant.admin != null && DbConstant.admin.isConnected) {
                DbConstant.admin.close();
            }
            ChannelUtils.stopLogger();
            System.exit(2);
        }

        Configuration.configuration.pipelineInit();
        NetworkTransaction networkTransaction = new NetworkTransaction();
        R66Future future = new R66Future(true);
        
        logger.info("Start Test of Transaction");
        long time1 = System.currentTimeMillis();
        
        @SuppressWarnings("unused")
        BusinessRequestPacket packet = 
            new BusinessRequestPacket(classname+" "+classarg, 0);
        // XXX FIXME this has to be adapted
        /*
        AbstractBusinessRequest transaction = new AbstractBusinessRequest(
                AbstractBusinessRequest.class,
                future, rhost, networkTransaction, packet);
        
        transaction.run();
        
        future.awaitUninterruptibly();
        */
        long time2 = System.currentTimeMillis();
        logger.debug("Finish Business Request: "+future.isSuccess());
        long delay = time2 - time1;
        if (future.isSuccess()) {
            logger.info("Business Request in status:\nSUCCESS"+
                    "\n    <REMOTE>"+rhost+"</REMOTE>"+
                    "\n    delay: "+delay);
        } else {
            logger.info("Business Request in status:\nFAILURE"+
                    "\n    <REMOTE>"+rhost+"</REMOTE>"+
                    "\n    <ERROR>"+future.getCause()+"</ERROR>"+
                    "\n    delay: "+delay);
                networkTransaction.closeAll();
                System.exit(ErrorCode.Unknown.ordinal());
        }
        networkTransaction.closeAll();
    }


    static protected String rhost = null;
    static protected String classname = null;
    static protected String classarg = null;
    static protected boolean nolog = false;
    
    /**
     * Parse the parameter and set current values
     * @param args
     * @return True if all parameters were found and correct
     */
    protected static boolean getParams(String []args) {
        if (args.length < 5) {
            logger
                    .error("Needs at least 3 or 4 arguments:\n" +
                                "  the XML client configuration file,\n" +
                                "  '-to' the remoteHost Id,\n" +
                                "  '-class' the Business full class name,\n" +
                                "  '-arg' the argument to pass (optional)\n"+
                                "Other options:\n" +
                                "  '-nolog' to not log locally this action\n");
            return false;
        }
        if (! FileBasedConfiguration
                .setClientConfigurationFromXml(Configuration.configuration, args[0])) {
            logger
                    .error("Needs a correct configuration file as first argument");
            return false;
        }
        // Now set default values from configuration
        for (int i = 1; i < args.length; i++) {
            if (args[i].equalsIgnoreCase("-to")) {
                i++;
                rhost = args[i];
            } else if (args[i].equalsIgnoreCase("-class")) {
                i++;
                classname = args[i];
            } else if (args[i].equalsIgnoreCase("-arg")) {
                i++;
                classarg = args[i];
            } else if (args[i].equalsIgnoreCase("-nolog")) {
                nolog = true;
                i++;
            }
        }
        if (rhost != null && classname != null) {
            return true;
        }
        logger.error("All params are not set! Need at least (-to -class)");
        return false;
    }

}
