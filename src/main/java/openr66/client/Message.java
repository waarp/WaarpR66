/**
 * Copyright 2009, Frederic Bregier, and individual contributors by the @author
 * tags. See the COPYRIGHT.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it under the
 * terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 3.0 of the License, or (at your option)
 * any later version.
 *
 * This software is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this software; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA, or see the FSF
 * site: http://www.fsf.org.
 */
package openr66.client;

import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;
import goldengate.common.logging.GgSlf4JLoggerFactory;

import java.net.SocketAddress;

import openr66.configuration.FileBasedConfiguration;
import openr66.context.ErrorCode;
import openr66.context.R66Result;
import openr66.context.authentication.R66Auth;
import openr66.database.DbConstant;
import openr66.database.data.DbHostAuth;
import openr66.protocol.configuration.Configuration;
import openr66.protocol.exception.OpenR66ProtocolPacketException;
import openr66.protocol.localhandler.LocalChannelReference;
import openr66.protocol.localhandler.packet.TestPacket;
import openr66.protocol.localhandler.packet.ValidPacket;
import openr66.protocol.networkhandler.NetworkTransaction;
import openr66.protocol.utils.ChannelUtils;
import openr66.protocol.utils.R66Future;

import org.jboss.netty.channel.Channels;
import org.jboss.netty.logging.InternalLoggerFactory;

import ch.qos.logback.classic.Level;

/**
 * Message testing between two hosts
 * @author Frederic Bregier
 *
 */
public class Message implements Runnable {
    /**
     * Internal Logger
     */
    private static GgInternalLogger logger;

    final private NetworkTransaction networkTransaction;

    final private R66Future future;

    private final String requested;

    final private TestPacket testPacket;

    static String srequested = null;
    static String smessage = "MESSAGE";

    /**
     * Parse the parameter and set current values
     * @param args
     * @return True if all parameters were found and correct
     */
    protected static boolean getParams(String []args) {
        if (args.length < 3) {
            logger
                    .error("Needs 3 arguments:\n" +
                            "  the XML client configuration file,\n" +
                            "  '-to' the remoteHost Id,\n" +
                            "  '-msg' the message\n");
            return false;
        }
        if (! FileBasedConfiguration
                .setClientConfigurationFromXml(args[0])) {
            logger
                    .error("Needs a correct configuration file as first argument");
            return false;
        }
        for (int i = 1; i < args.length; i++) {
            if (args[i].equalsIgnoreCase("-to")) {
                i++;
                srequested = args[i];
            } else if (args[i].equalsIgnoreCase("-msg")) {
                i++;
                smessage = args[i];
            }
        }
        if (srequested == null) {
            logger.error("Requested HostId must be set");
            return false;
        }
        return true;
    }


    public Message(NetworkTransaction networkTransaction,
            R66Future future, String requested, TestPacket packet) {
        if (logger == null) {
            logger = GgInternalLoggerFactory.getLogger(Message.class);
        }
        this.networkTransaction = networkTransaction;
        this.future = future;
        this.requested = requested;
        testPacket = packet;
    }

    public void run() {
        if (logger == null) {
            logger = GgInternalLoggerFactory.getLogger(
                    Message.class);
        }
        // Connection
        DbHostAuth host = R66Auth.getServerAuth(DbConstant.admin.session,
                requested);
        if (host == null) {
            logger.error("Requested host cannot be found: "+requested);
            R66Result result = new R66Result(null, true, ErrorCode.ConnectionImpossible);
            this.future.setResult(result);
            this.future.cancel();
            return;
        }
        SocketAddress socketAddress = host.getSocketAddress();
        boolean isSSL = host.isSsl();
        LocalChannelReference localChannelReference = null;
        localChannelReference = networkTransaction
            .createConnectionWithRetry(socketAddress, isSSL, future);
        socketAddress = null;
        if (localChannelReference == null) {
            logger.error("Cannot connect to server: "+requested);
            R66Result result = new R66Result(null, true, ErrorCode.ConnectionImpossible);
            this.future.setResult(result);
            this.future.cancel();
            return;
        }
        try {
            ChannelUtils.writeAbstractLocalPacket(localChannelReference, testPacket);
        } catch (OpenR66ProtocolPacketException e) {
            future.setResult(null);
            future.setFailure(e);
            Channels.close(localChannelReference.getLocalChannel());
            return;
        }
    }

    public static void main(String[] args) {
        InternalLoggerFactory.setDefaultFactory(new GgSlf4JLoggerFactory(
                Level.WARN));
        if (logger == null) {
            logger = GgInternalLoggerFactory.getLogger(Message.class);
        }
        if (args.length < 1) {
            logger
                    .error("Needs at least the configuration file as first argument");
            return;
        }
        if (! getParams(args)) {
            logger.error("Wrong initialization");
            if (DbConstant.admin != null && DbConstant.admin.isConnected) {
                DbConstant.admin.close();
            }
            System.exit(1);
        }
        NetworkTransaction networkTransaction = null;
        try {
            Configuration.configuration.pipelineInit();
            networkTransaction = new NetworkTransaction();
            R66Future result = new R66Future(true);
            TestPacket packet = new TestPacket("MSG", smessage, 100);
            Message transaction = new Message(
                    networkTransaction, result, srequested,
                    packet);
            transaction.run();
            result.awaitUninterruptibly();
            if (result.isSuccess()) {
                R66Result r66result = result.getResult();
                ValidPacket info = (ValidPacket) r66result.other;
                logger.warn("Test Message Success: "+info.getSheader());
            } else {
                logger.error("Test Message Error: " +
                        result.getResult().toString());
            }
        } finally {
            if (networkTransaction != null) {
                networkTransaction.closeAll();
            }
            if (DbConstant.admin != null) {
                DbConstant.admin.close();
            }
        }
    }

}
