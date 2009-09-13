/**
 * Copyright 2009, Frederic Bregier, and individual contributors
 * by the @author tags. See the COPYRIGHT.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 3.0 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package openr66.client;

import java.net.SocketAddress;

import org.jboss.netty.logging.InternalLoggerFactory;

import ch.qos.logback.classic.Level;
import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;
import goldengate.common.logging.GgSlf4JLoggerFactory;
import openr66.configuration.FileBasedConfiguration;
import openr66.context.ErrorCode;
import openr66.context.R66Result;
import openr66.context.authentication.R66Auth;
import openr66.database.DbConstant;
import openr66.database.data.DbHostAuth;
import openr66.database.exception.OpenR66DatabaseSqlError;
import openr66.protocol.configuration.Configuration;
import openr66.protocol.exception.OpenR66ProtocolPacketException;
import openr66.protocol.localhandler.LocalChannelReference;
import openr66.protocol.localhandler.packet.InformationPacket;
import openr66.protocol.localhandler.packet.ValidPacket;
import openr66.protocol.networkhandler.NetworkTransaction;
import openr66.protocol.utils.ChannelUtils;
import openr66.protocol.utils.R66Future;

/**
 * Class to request information on remote files
 *
 * @author Frederic Bregier
 *
 */
public class RequestInformation implements Runnable {
    /**
     * Internal Logger
     */
    static volatile GgInternalLogger logger;

    protected final NetworkTransaction networkTransaction;
    final R66Future future;
    String requested = null;
    String filename = null;
    String rulename = null;
    byte code;

    static String srequested = null;
    static String sfilename = null;
    static String srulename = null;
    static byte scode = 0;

    /**
     * Parse the parameter and set current values
     * @param args
     * @return True if all parameters were found and correct
     */
    protected static boolean getParams(String []args) {
        if (args.length < 3) {
            logger
                    .error("Needs at least 3 arguments:\n" +
                            "  the XML client configuration file,\n" +
                            "  '-to' the remoteHost Id,\n" +
                            "  '-rule' the rule\n"+
                            "Other options:\n" +
                            "  '-file' the optional file for which to get info,\n" +
                            "  '-exist' to test the existence\n" +
                            "  '-detail' to get the detail on file\n" +
                            "  '-list' to get the list of files\n" +
                            "  '-mlsx' to get the list and details of files");
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
            } else if (args[i].equalsIgnoreCase("-file")) {
                i++;
                sfilename = args[i];
            } else if (args[i].equalsIgnoreCase("-rule")) {
                i++;
                srulename = args[i];
            } else if (args[i].equalsIgnoreCase("-exist")) {
                scode = (byte) InformationPacket.ASKENUM.ASKEXIST.ordinal();
            } else if (args[i].equalsIgnoreCase("-detail")) {
                scode = (byte) InformationPacket.ASKENUM.ASKMLSDETAIL.ordinal();
            } else if (args[i].equalsIgnoreCase("-list")) {
                scode = (byte) InformationPacket.ASKENUM.ASKLIST.ordinal();
            } else if (args[i].equalsIgnoreCase("-mlsx")) {
                scode = (byte) InformationPacket.ASKENUM.ASKMLSLIST.ordinal();
            }
        }
        if (srulename == null || srequested == null) {
            logger.error("Rulename and Requested HostId must be set");
            return false;
        }

        return true;
    }


    /**
     * @param future
     * @param requested
     * @param rulename
     * @param filename
     * @param request
     * @param networkTransaction
     */
    public RequestInformation(R66Future future, String requested, String rulename,
            String filename, byte request,
            NetworkTransaction networkTransaction) {
        this.future = future;
        this.rulename = rulename;
        this.requested = requested;
        this.filename = filename;
        this.code = request;
        this.networkTransaction = networkTransaction;
    }


    public void run() {
        if (logger == null) {
            logger = GgInternalLoggerFactory.getLogger(RequestInformation.class);
        }
        InformationPacket request = new InformationPacket(rulename, code,
                filename);

        // Connection
        DbHostAuth host = R66Auth.getServerAuth(DbConstant.admin.session,
                requested);
        if (host == null) {
            logger.warn("Requested host cannot be found: "+requested);
            R66Result result = new R66Result(null, true, ErrorCode.ConnectionImpossible);
            this.future.setResult(result);
            this.future.cancel();
            return;
        }
        SocketAddress socketAddress = host.getSocketAddress();
        boolean isSSL = host.isSsl();

        LocalChannelReference localChannelReference = networkTransaction
            .createConnectionWithRetry(socketAddress, isSSL, future);
        socketAddress = null;
        if (localChannelReference == null) {
            logger.warn("Cannot connect to server: "+requested);
            R66Result result = new R66Result(null, true, ErrorCode.ConnectionImpossible);
            this.future.setResult(result);
            this.future.cancel();
            return;
        }

        try {
            ChannelUtils.writeAbstractLocalPacket(localChannelReference, request);
        } catch (OpenR66ProtocolPacketException e) {
            logger.warn("Cannot write request");
            R66Result result = new R66Result(null, true, ErrorCode.TransferError);
            this.future.setResult(result);
            this.future.cancel();
            return;
        }
        localChannelReference.getFutureRequest().awaitUninterruptibly();
        if (localChannelReference.getFutureRequest().isSuccess()) {
            R66Result result = localChannelReference.getFutureRequest()
                .getResult();
            ValidPacket info = (ValidPacket) result.other;
            logger.warn("Result: "+info.getSmiddle()+"\n"+info.getSheader());
        } else {
            logger.warn("Error", localChannelReference
                    .getFutureRequest().getCause());
        }
        networkTransaction.closeAll();
    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        InternalLoggerFactory.setDefaultFactory(new GgSlf4JLoggerFactory(
                Level.WARN));
        if (logger == null) {
            logger = GgInternalLoggerFactory.getLogger(RequestInformation.class);
        }
        if (! getParams(args)) {
            logger.error("Wrong initialization");
            if (DbConstant.admin != null && DbConstant.admin.isConnected) {
                try {
                    DbConstant.admin.close();
                } catch (OpenR66DatabaseSqlError e) {
                }
            }
            System.exit(1);
        }
        try {
            Configuration.configuration.pipelineInit();
            NetworkTransaction networkTransaction = new NetworkTransaction();
            R66Future result = new R66Future(true);
            RequestInformation requestInformation =
                new RequestInformation(result, srequested, srulename,
                        sfilename, scode,
                        networkTransaction);
            requestInformation.run();
            result.awaitUninterruptibly();
            // FIXME use result
            if (result.isSuccess()) {
                logger.warn("Success: " +
                        result.getResult().toString());
            } else {
                logger.error("Error: " +
                        result.getResult().toString());
            }

        } finally {
            if (DbConstant.admin != null) {
                try {
                    DbConstant.admin.close();
                } catch (OpenR66DatabaseSqlError e) {
                }
            }
        }
    }

}
