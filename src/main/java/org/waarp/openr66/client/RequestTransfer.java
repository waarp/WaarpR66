/**
 * This file is part of Waarp Project.
 * 
 * Copyright 2009, Frederic Bregier, and individual contributors by the @author tags. See the
 * COPYRIGHT.txt in the distribution for a full listing of individual contributors.
 * 
 * All Waarp Project is free software: you can redistribute it and/or modify it under the terms of
 * the GNU General Public License as published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 * 
 * Waarp is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even
 * the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
 * Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License along with Waarp . If not, see
 * <http://www.gnu.org/licenses/>.
 */
package org.waarp.openr66.client;

import java.net.SocketAddress;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.waarp.common.database.data.AbstractDbData.UpdatedInfo;
import org.waarp.common.database.exception.WaarpDatabaseException;
import org.waarp.common.logging.WaarpLogger;
import org.waarp.common.logging.WaarpLoggerFactory;
import org.waarp.common.logging.WaarpSlf4JLoggerFactory;
import org.waarp.openr66.client.utils.OutputFormat;
import org.waarp.openr66.client.utils.OutputFormat.FIELDS;
import org.waarp.openr66.commander.CommanderNoDb;
import org.waarp.openr66.configuration.FileBasedConfiguration;
import org.waarp.openr66.context.ErrorCode;
import org.waarp.openr66.context.R66FiniteDualStates;
import org.waarp.openr66.context.R66Result;
import org.waarp.openr66.context.authentication.R66Auth;
import org.waarp.openr66.context.task.exception.OpenR66RunnerErrorException;
import org.waarp.openr66.database.DbConstant;
import org.waarp.openr66.database.data.DbHostAuth;
import org.waarp.openr66.database.data.DbTaskRunner;
import org.waarp.openr66.protocol.configuration.Configuration;
import org.waarp.openr66.protocol.configuration.Messages;
import org.waarp.openr66.protocol.configuration.PartnerConfiguration;
import org.waarp.openr66.protocol.exception.OpenR66DatabaseGlobalException;
import org.waarp.openr66.protocol.exception.OpenR66Exception;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolBusinessException;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolPacketException;
import org.waarp.openr66.protocol.localhandler.LocalChannelReference;
import org.waarp.openr66.protocol.localhandler.packet.AbstractLocalPacket;
import org.waarp.openr66.protocol.localhandler.packet.JsonCommandPacket;
import org.waarp.openr66.protocol.localhandler.packet.LocalPacketFactory;
import org.waarp.openr66.protocol.localhandler.packet.ValidPacket;
import org.waarp.openr66.protocol.localhandler.packet.json.StopOrCancelJsonPacket;
import org.waarp.openr66.protocol.localhandler.packet.json.RestartTransferJsonPacket;
import org.waarp.openr66.protocol.networkhandler.NetworkTransaction;
import org.waarp.openr66.protocol.utils.ChannelUtils;
import org.waarp.openr66.protocol.utils.R66Future;

/**
 * Class to request information or request cancellation or restart
 * 
 * @author Frederic Bregier
 * 
 */
public class RequestTransfer implements Runnable {
    /**
     * Internal Logger
     */
    static volatile WaarpLogger logger;

    protected static String _INFO_ARGS =
            Messages.getString("RequestTransfer.0") + Messages.getString("Message.OutputFormat"); //$NON-NLS-1$

    protected final NetworkTransaction networkTransaction;
    final R66Future future;
    final long specialId;
    String requested = null;
    String requester = null;
    boolean cancel = false;
    boolean stop = false;
    boolean restart = false;
    String restarttime = null;
    boolean normalInfoAsWarn = true;

    static long sspecialId;
    static String srequested = null;
    static String srequester = null;
    static String rhost = null;
    static boolean scancel = false;
    static boolean sstop = false;
    static boolean srestart = false;
    static String srestarttime = null;
    static protected boolean snormalInfoAsWarn = true;

    /**
     * Parse the parameter and set current values
     * 
     * @param args
     * @return True if all parameters were found and correct
     */
    protected static boolean getParams(String[] args) {
        _INFO_ARGS = Messages.getString("RequestTransfer.0") + Messages.getString("Message.OutputFormat"); //$NON-NLS-1$
        if (args.length < 5) {
            logger
                    .error(_INFO_ARGS);
            return false;
        }
        if (!FileBasedConfiguration
                .setClientConfigurationFromXml(Configuration.configuration, args[0])) {
            logger
                    .error(Messages.getString("Configuration.NeedCorrectConfig")); //$NON-NLS-1$
            return false;
        }
        for (int i = 1; i < args.length; i++) {
            if (args[i].equalsIgnoreCase("-id")) {
                i++;
                try {
                    sspecialId = Long.parseLong(args[i]);
                } catch (NumberFormatException e) {
                    logger.error(Messages.getString("RequestTransfer.1") + args[i], e); //$NON-NLS-1$
                    return false;
                }
            } else if (args[i].equalsIgnoreCase("-to")) {
                i++;
                srequested = args[i];
                if (Configuration.configuration.getAliases().containsKey(srequested)) {
                    srequested = Configuration.configuration.getAliases().get(srequested);
                }
                rhost = srequested;
                try {
                    srequester = Configuration.configuration.getHostId(DbConstant.admin.getSession(),
                            srequested);
                } catch (WaarpDatabaseException e) {
                    logger.error(Messages.getString("RequestTransfer.5") + srequested, e); //$NON-NLS-1$
                    return false;
                }
            } else if (args[i].equalsIgnoreCase("-from")) {
                i++;
                srequester = args[i];
                if (Configuration.configuration.getAliases().containsKey(srequester)) {
                    srequester = Configuration.configuration.getAliases().get(srequester);
                }
                rhost = srequester;
                try {
                    srequested = Configuration.configuration.getHostId(DbConstant.admin.getSession(),
                            srequester);
                } catch (WaarpDatabaseException e) {
                    logger.error(Messages.getString("RequestTransfer.5") + srequester, e); //$NON-NLS-1$
                    return false;
                }
            } else if (args[i].equalsIgnoreCase("-cancel")) {
                scancel = true;
            } else if (args[i].equalsIgnoreCase("-stop")) {
                sstop = true;
            } else if (args[i].equalsIgnoreCase("-restart")) {
                srestart = true;
            } else if (args[i].equalsIgnoreCase("-start")) {
                i++;
                srestarttime = args[i];
            } else if (args[i].equalsIgnoreCase("-logWarn")) {
                snormalInfoAsWarn = true;
            } else if (args[i].equalsIgnoreCase("-notlogWarn")) {
                snormalInfoAsWarn = false;
            } else if (args[i].equalsIgnoreCase("-delay")) {
                i++;
                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
                if (args[i].charAt(0) == '+') {
                    Date date = new Date(System.currentTimeMillis() +
                            Long.parseLong(args[i].substring(1)));
                    srestarttime = dateFormat.format(date);
                } else {
                    Date date = new Date(Long.parseLong(args[i]));
                    srestarttime = dateFormat.format(date);
                }
            }
            OutputFormat.getParams(args);
        }
        if ((scancel && srestart) || (scancel && sstop) || (srestart && sstop)) {
            logger.error(Messages.getString("RequestTransfer.15") + _INFO_ARGS); //$NON-NLS-1$
            return false;
        }
        if (sspecialId == DbConstant.ILLEGALVALUE || srequested == null) {
            logger.error(Messages.getString("RequestTransfer.16") + _INFO_ARGS); //$NON-NLS-1$
            return false;
        }

        return true;
    }

    /**
     * @param future
     * @param specialId
     * @param requested
     * @param requester
     * @param cancel
     * @param stop
     * @param restart
     * @param networkTransaction
     */
    public RequestTransfer(R66Future future, long specialId, String requested, String requester,
            boolean cancel, boolean stop, boolean restart,
            NetworkTransaction networkTransaction) {
        this(future, specialId, requested, requester, cancel, stop, restart, null, networkTransaction);
    }

    /**
     * @param future
     * @param specialId
     * @param requested
     * @param requester
     * @param cancel
     * @param stop
     * @param restart
     * @param restarttime
     *            in yyyyMMddHHmmss format
     * @param networkTransaction
     */
    public RequestTransfer(R66Future future, long specialId, String requested, String requester,
            boolean cancel, boolean stop, boolean restart, String restarttime,
            NetworkTransaction networkTransaction) {
        this.future = future;
        this.specialId = specialId;
        this.requested = requested;
        this.requester = requester;
        this.cancel = cancel;
        this.stop = stop;
        this.restart = restart;
        this.restarttime = restarttime;
        this.networkTransaction = networkTransaction;
    }

    public void run() {
        DbTaskRunner runner = null;
        try {
            if (logger == null) {
                logger = WaarpLoggerFactory.getLogger(RequestTransfer.class);
            }
            try {
                runner = new DbTaskRunner(null, null,
                                          specialId, requester, requested);
                logger.info("Found previous Runner: " + runner.toString());
            } catch (WaarpDatabaseException e) {
                // Maybe we can ask to the remote
                R66Future futureInfo = new R66Future(true);
                RequestInformation requestInformation =
                    new RequestInformation(futureInfo, requested, null, null,
                                           (byte) -1, specialId, true,
                                           networkTransaction);
                requestInformation.normalInfoAsWarn = normalInfoAsWarn;
                requestInformation.run();
                futureInfo.awaitUninterruptibly();
                if (futureInfo.isSuccess()) {
                    R66Result r66result = futureInfo.getResult();
                    ValidPacket info = (ValidPacket) r66result.getOther();
                    String xml = info.getSheader();
                    try {
                        runner = DbTaskRunner.fromStringXml(xml, true);
                        runner.changeUpdatedInfo(UpdatedInfo.TOSUBMIT);
                        // useful ?
                        CommanderNoDb.todoList.add(runner);

                        logger.info(
                            "Get Runner from remote: " + runner.toString());
                        if (runner.getSpecialId() == DbConstant.ILLEGALVALUE ||
                            !runner.isSender()) {
                            logger.error(Messages.getString(
                                "RequestTransfer.18")); //$NON-NLS-1$
                            future.setResult(new R66Result(
                                new OpenR66DatabaseGlobalException(e), null,
                                true,
                                ErrorCode.Internal, null));
                            future.setFailure(e);
                            return;
                        }
                        if (runner.isAllDone()) {
                            logger.error(Messages.getString(
                                "RequestTransfer.21")); //$NON-NLS-1$
                            future.setResult(new R66Result(
                                new OpenR66DatabaseGlobalException(e), null,
                                true,
                                ErrorCode.Internal, null));
                            future.setFailure(e);
                            return;
                        }
                    } catch (OpenR66ProtocolBusinessException e1) {
                        logger.error(Messages.getString(
                            "RequestTransfer.18")); //$NON-NLS-1$
                        future.setResult(new R66Result(
                            new OpenR66DatabaseGlobalException(e1), null, true,
                            ErrorCode.Internal, null));
                        future.setFailure(e);
                        return;
                    }
                } else {
                    logger.error(
                        Messages.getString("RequestTransfer.18")); //$NON-NLS-1$
                    future.setResult(
                        new R66Result(new OpenR66DatabaseGlobalException(e),
                                      null, true,
                                      ErrorCode.Internal, null));
                    future.setFailure(e);
                    return;
                }
            }
            if (cancel || stop || restart) {
                if (cancel) {
                    // Cancel the task and delete any file if in retrieve
                    if (runner.isAllDone()) {
                        // nothing to do since already finished
                        setDone(runner);
                        logger.info(
                            "Transfer already finished: " + runner.toString());
                        future.setResult(
                            new R66Result(null, true, ErrorCode.TransferOk,
                                          runner));
                        future.getResult().setRunner(runner);
                        future.setSuccess();
                        return;
                    } else {
                        // Send a request of cancel
                        ErrorCode code = sendStopOrCancel(runner,
                                                          LocalPacketFactory.CANCELPACKET);
                        future.setResult(
                            new R66Result(null, true, code,
                                          runner));
                        future.getResult().setRunner(runner);
                        switch (code) {
                        case CompleteOk:
                            logger
                                .info("Transfer cancel requested and done: {}",
                                      runner);
                            future.setSuccess();
                            break;
                        case TransferOk:
                            logger.info(
                                "Transfer cancel requested but already finished: {}",
                                runner);
                            future.setSuccess();
                            break;
                        default:
                            logger.info(
                                "Transfer cancel requested but internal error: {}",
                                runner);
                            future.setFailure(new WaarpDatabaseException("Transfer cancel requested but internal error"));
                            break;
                        }
                    }
                } else if (stop) {
                    // Just stop the task
                    // Send a request
                    ErrorCode code =
                        sendStopOrCancel(runner, LocalPacketFactory.STOPPACKET);
                    future.setResult(
                        new R66Result(null, true, code,
                                      runner));
                    future.getResult().setRunner(runner);
                    switch (code) {
                    case CompleteOk:
                        logger.info("Transfer stop requested and done: {}",
                                    runner);
                        future.setSuccess();
                        break;
                    case TransferOk:
                        logger.info(
                            "Transfer stop requested but already finished: {}",
                            runner);
                        future.setSuccess();
                        break;
                    default:
                        logger.info(
                            "Transfer stop requested but internal error: {}",
                            runner);
                        future.setFailure(new WaarpDatabaseException("Transfer stop requested but internal error"));
                        break;
                    }
                } else if (restart) {
                    // Restart if already stopped and not finished
                    ErrorCode code =
                        sendValid(runner, LocalPacketFactory.VALIDPACKET);
                    future.setResult(
                        new R66Result(null, true, code,
                                      runner));
                    future.getResult().setRunner(runner);
                    switch (code) {
                    case QueryStillRunning:
                        logger.info(
                            "Transfer restart requested but already active and running: {}",
                            runner);
                        future.setSuccess();
                        break;
                    case Running:
                        logger.info(
                            "Transfer restart requested but already running: {}",
                            runner);
                        future.setSuccess();
                        break;
                    case PreProcessingOk:
                        logger.info(
                            "Transfer restart requested and restarted: {}",
                            runner);
                        future.setSuccess();
                        break;
                    case CompleteOk:
                        logger.info(
                            "Transfer restart requested but already finished: {}",
                            runner);
                        future.setSuccess();
                        break;
                    case RemoteError:
                        logger.info(
                            "Transfer restart requested but remote error: {}",
                            runner);
                        future.setSuccess();
                        break;
                    case PassThroughMode:
                        logger.info(
                            "Transfer not restarted since it is in PassThrough mode: {}",
                            runner);
                        future.setSuccess();
                        break;
                    default:
                        logger.info(
                            "Transfer restart requested but internal error: {}",
                            runner);
                        future.setFailure(new WaarpDatabaseException("Transfer restart requested but internal error"));
                        break;
                    }
                }
            } else {
                // Only request
                logger.info(
                    "Transfer information: {}    " + runner.toShortString(),
                    future.isDone());
                future.setResult(
                    new R66Result(null, true, runner.getErrorInfo(), runner));
                future.setSuccess();
            }
        } finally {
            if (!future.isDone()) {
                if (runner != null) {
                    // Default set to success
                    future.setResult(
                        new R66Result(null, true, runner.getErrorInfo(),
                                      runner));
                    future.setSuccess();
                } else {
                    future.setFailure(new WaarpDatabaseException("Transfer requested but internal error"));
                }
            }
        }
    }

    /**
     * Set the runner to DONE
     * 
     * @param runner
     */
    private void setDone(DbTaskRunner runner) {
        if (runner.getUpdatedInfo() != UpdatedInfo.DONE) {
            runner.changeUpdatedInfo(UpdatedInfo.DONE);
            runner.forceSaveStatus();
        }
    }

    private ErrorCode sendValid(DbTaskRunner runner, byte code) {
        DbHostAuth host;
        host = R66Auth.getServerAuth(DbConstant.admin.getSession(),
                this.requester);
        if (host == null) {
            logger.error(Messages.getString("RequestTransfer.39") + this.requester); //$NON-NLS-1$
            OpenR66Exception e =
                    new OpenR66RunnerErrorException("Requester host cannot be found");
            future.setResult(new R66Result(
                    e,
                    null, true,
                    ErrorCode.TransferError, null));
            future.setFailure(e);
            return ErrorCode.Internal;
        }
        // check if requester is "client" so no connect from him but direct action
        logger.debug("Requester Host isClient: " + host.isClient());
        if (host.isClient()) {
            if (code == LocalPacketFactory.VALIDPACKET) {
                logger.info(Messages.getString("RequestTransfer.42") + //$NON-NLS-1$
                        runner.toShortString());
                R66Future transfer = new R66Future(true);
                DirectTransfer transaction = new DirectTransfer(transfer,
                        runner.getRequested(), runner.getOriginalFilename(),
                        runner.getRuleId(), runner.getFileInformation(), false,
                        runner.getBlocksize(), runner.getSpecialId(), networkTransaction);
                transaction.normalInfoAsWarn = normalInfoAsWarn;
                transaction.run();
                transfer.awaitUninterruptibly();
                logger.info("Request done with " + (transfer.isSuccess() ? "success" : "error"));
                if (transfer.isSuccess()) {
                    future.setResult(new R66Result(null, true, ErrorCode.PreProcessingOk, runner));
                    future.getResult().setRunner(runner);
                    future.setSuccess();
                    return ErrorCode.PreProcessingOk;
                } else {
                    R66Result result = transfer.getResult();
                    ErrorCode error = ErrorCode.Internal;
                    if (result != null) {
                        error = result.getCode();
                    }
                    OpenR66Exception e =
                            new OpenR66RunnerErrorException("Transfer in direct mode failed: " + error.getMesg());
                    future.setFailure(e);
                    return error;
                }
            } else {
                // get remote host instead
                host = R66Auth.getServerAuth(DbConstant.admin.getSession(),
                        this.requested);
                if (host == null) {
                    logger.error(Messages.getString("Message.HostNotFound") + this.requested); //$NON-NLS-1$
                    OpenR66Exception e =
                            new OpenR66RunnerErrorException("Requested host cannot be found");
                    future.setResult(new R66Result(
                            e,
                            null, true,
                            ErrorCode.TransferError, null));
                    future.setFailure(e);
                    return ErrorCode.ConnectionImpossible;
                }
            }
        }

        logger.info("Try RequestTransfer to " + host.toString());
        SocketAddress socketAddress;
        try {
            socketAddress = host.getSocketAddress();
        } catch (IllegalArgumentException e) {
            logger.debug("Cannot connect to " + host.toString());
            host = null;
            future.setResult(new R66Result(null, true,
                    ErrorCode.ConnectionImpossible, null));
            future.cancel();
            return ErrorCode.ConnectionImpossible;
        }
        boolean isSSL = host.isSsl();

        LocalChannelReference localChannelReference = networkTransaction
                .createConnectionWithRetry(socketAddress, isSSL, future);
        socketAddress = null;
        if (localChannelReference == null) {
            logger.debug("Cannot connect to " + host.toString());
            host = null;
            future.setResult(new R66Result(null, true,
                    ErrorCode.ConnectionImpossible, null));
            future.cancel();
            return ErrorCode.ConnectionImpossible;
        }
        boolean useJson = PartnerConfiguration.useJson(host.getHostid());
        logger.debug("UseJson: " + useJson);
        AbstractLocalPacket packet = null;
        if (useJson) {
            RestartTransferJsonPacket node = new RestartTransferJsonPacket();
            node.setComment("Request on Transfer");
            node.setRequested(requested);
            node.setRequester(requester);
            node.setSpecialid(specialId);
            if (restarttime != null && code == LocalPacketFactory.VALIDPACKET) {
                // restart time set
                logger.debug("Restart with time: " + restarttime);
                // time to reschedule in yyyyMMddHHmmss format
                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
                try {
                    Date date = dateFormat.parse(restarttime);
                    node.setRestarttime(date);
                } catch (ParseException e) {
                }
                packet = new JsonCommandPacket(node, code);
            } else {
                packet = new JsonCommandPacket(node, code);
            }
        } else {
            if (restarttime != null && code == LocalPacketFactory.VALIDPACKET) {
                // restart time set
                logger.debug("Restart with time: " + restarttime);
                packet = new ValidPacket("Request on Transfer",
                        this.requested + " " + this.requester + " " + this.specialId + " " + restarttime,
                        code);
            } else {
                packet = new ValidPacket("Request on Transfer",
                        this.requested + " " + this.requester + " " + this.specialId,
                        code);
            }
        }
        localChannelReference.sessionNewState(R66FiniteDualStates.VALIDOTHER);
        try {
            ChannelUtils.writeAbstractLocalPacket(localChannelReference, packet, false);
        } catch (OpenR66ProtocolPacketException e) {
            logger.error(Messages.getString("RequestTransfer.63") + host.toString()); //$NON-NLS-1$
            localChannelReference.getLocalChannel().close();
            localChannelReference = null;
            host = null;
            packet = null;
            logger.debug("Bad Protocol", e);
            future.setResult(new R66Result(e, null, true,
                    ErrorCode.TransferError, null));
            future.setFailure(e);
            return ErrorCode.Internal;
        }
        packet = null;
        host = null;
        future.awaitUninterruptibly();

        localChannelReference.getLocalChannel().close();
        localChannelReference = null;

        logger.info("Request done with " + (future.isSuccess() ? "success" : "error"));
        R66Result result = future.getResult();
        if (result != null) {
            return result.getCode();
        }
        return ErrorCode.Internal;
    }

    private ErrorCode sendStopOrCancel(DbTaskRunner runner, byte code) {
        DbHostAuth host;
        host = R66Auth.getServerAuth(DbConstant.admin.getSession(),
                this.requester);
        if (host == null) {
            logger.error(Messages.getString("RequestTransfer.39") + this.requester); //$NON-NLS-1$
            OpenR66Exception e =
                    new OpenR66RunnerErrorException("Requester host cannot be found");
            future.setResult(new R66Result(
                    e,
                    null, true,
                    ErrorCode.TransferError, null));
            future.setFailure(e);
            return ErrorCode.Internal;
        }

        logger.info("Try RequestTransfer to " + host.toString());
        SocketAddress socketAddress;
        try {
            socketAddress = host.getSocketAddress();
        } catch (IllegalArgumentException e) {
            logger.debug("Cannot connect to " + host.toString());
            host = null;
            future.setResult(new R66Result(null, true,
                    ErrorCode.ConnectionImpossible, null));
            future.cancel();
            return ErrorCode.ConnectionImpossible;
        }
        boolean isSSL = host.isSsl();

        LocalChannelReference localChannelReference = networkTransaction
                .createConnectionWithRetry(socketAddress, isSSL, future);
        socketAddress = null;
        if (localChannelReference == null) {
            logger.debug("Cannot connect to " + host.toString());
            host = null;
            future.setResult(new R66Result(null, true,
                    ErrorCode.ConnectionImpossible, null));
            future.cancel();
            return ErrorCode.ConnectionImpossible;
        }
        boolean useJson = PartnerConfiguration.useJson(host.getHostid());
        logger.debug("UseJson: " + useJson);
        AbstractLocalPacket packet = null;
        if (useJson) {
            StopOrCancelJsonPacket node = new StopOrCancelJsonPacket();
            node.setComment("Request on Transfer");
            node.setRequested(requested);
            node.setRequester(requester);
            node.setSpecialid(specialId);
            packet = new JsonCommandPacket(node, code);
        } else {
            packet = new ValidPacket("Request on Transfer",
                    this.requested + " " + this.requester + " " + this.specialId,
                    code);
        }
        localChannelReference.sessionNewState(R66FiniteDualStates.VALIDOTHER);
        try {
            ChannelUtils.writeAbstractLocalPacket(localChannelReference, packet, false);
        } catch (OpenR66ProtocolPacketException e) {
            logger.error(Messages.getString("RequestTransfer.63") + host.toString()); //$NON-NLS-1$
            localChannelReference.getLocalChannel().close();
            localChannelReference = null;
            host = null;
            packet = null;
            logger.debug("Bad Protocol", e);
            future.setResult(new R66Result(e, null, true,
                    ErrorCode.TransferError, null));
            future.setFailure(e);
            return ErrorCode.Internal;
        }
        packet = null;
        host = null;
        future.awaitUninterruptibly();

        localChannelReference.getLocalChannel().close();
        localChannelReference = null;

        logger.info("Request done with " + (future.isSuccess() ? "success" : "error"));
        R66Result result = future.getResult();
        if (result != null) {
            return result.getCode();
        }
        return ErrorCode.Internal;
    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        WaarpLoggerFactory.setDefaultFactory(new WaarpSlf4JLoggerFactory(null));
        if (logger == null) {
            logger = WaarpLoggerFactory.getLogger(RequestTransfer.class);
        }
        if (!getParams(args)) {
            logger.error(Messages.getString("Configuration.WrongInit")); //$NON-NLS-1$
            if (!OutputFormat.isQuiet()) {
                System.out.println(Messages.getString("Configuration.WrongInit")); //$NON-NLS-1$
            }
            if (DbConstant.admin != null && DbConstant.admin.isActive()) {
                DbConstant.admin.close();
            }
            ChannelUtils.stopLogger();
            System.exit(1);
        }
        int value = 99;
        try {
            Configuration.configuration.pipelineInit();
            NetworkTransaction networkTransaction = new NetworkTransaction();
            R66Future result = new R66Future(true);
            RequestTransfer requestTransfer =
                    new RequestTransfer(result, sspecialId, srequested, srequester,
                            scancel, sstop, srestart, srestarttime,
                            networkTransaction);
            requestTransfer.normalInfoAsWarn = snormalInfoAsWarn;
            requestTransfer.run();
            result.awaitUninterruptibly();
            R66Result finalValue = result.getResult();
            OutputFormat outputFormat = new OutputFormat(RequestTransfer.class.getSimpleName(), args);
            if (scancel || sstop || srestart) {
                if (scancel) {
                    if (result.isSuccess()) {
                        value = 0;
                        outputFormat.setValue(FIELDS.status.name(), value);
                        outputFormat.setValue(FIELDS.statusTxt.name(), Messages.getString("RequestTransfer.21")); //$NON-NLS-1$
                        outputFormat.setValue(FIELDS.remote.name(), rhost);
                        outputFormat.setValueString(result.getRunner().getJson());
                        if (requestTransfer.normalInfoAsWarn) {
                            logger.warn(outputFormat.loggerOut());
                        } else {
                            logger.info(outputFormat.loggerOut());
                        }
                        if (!OutputFormat.isQuiet()) {
                            outputFormat.sysout();
                        }
                    } else {
                        switch (finalValue.getCode()) {
                            case CompleteOk:
                                value = 0;
                                outputFormat.setValue(FIELDS.status.name(), value);
                                outputFormat
                                        .setValue(FIELDS.statusTxt.name(), Messages.getString("RequestTransfer.70")); //$NON-NLS-1$
                                outputFormat.setValue(FIELDS.remote.name(), rhost);
                                outputFormat.setValueString(result.getRunner().getJson());
                                logger.warn(outputFormat.loggerOut());
                                if (!OutputFormat.isQuiet()) {
                                    outputFormat.sysout();
                                }
                                break;
                            case TransferOk:
                                value = 3;
                                outputFormat.setValue(FIELDS.status.name(), value);
                                outputFormat
                                        .setValue(FIELDS.statusTxt.name(), Messages.getString("RequestTransfer.71")); //$NON-NLS-1$
                                outputFormat.setValue(FIELDS.remote.name(), rhost);
                                outputFormat.setValueString(result.getRunner().getJson());
                                logger.warn(outputFormat.loggerOut());
                                if (!OutputFormat.isQuiet()) {
                                    outputFormat.sysout();
                                }
                                break;
                            default:
                                value = 4;
                                outputFormat.setValue(FIELDS.status.name(), value);
                                outputFormat
                                        .setValue(FIELDS.statusTxt.name(), Messages.getString("RequestTransfer.72")); //$NON-NLS-1$
                                outputFormat.setValue(FIELDS.remote.name(), rhost);
                                outputFormat.setValueString(result.getRunner().getJson());
                                if (result.getCause() != null) {
                                    outputFormat.setValue(FIELDS.error.name(), result.getCause().getMessage());
                                }
                                logger.error(outputFormat.loggerOut());
                                if (!OutputFormat.isQuiet()) {
                                    outputFormat.sysout();
                                }
                                break;
                        }
                    }
                } else if (sstop) {
                    switch (finalValue.getCode()) {
                        case CompleteOk:
                            value = 0;
                            outputFormat.setValue(FIELDS.status.name(), value);
                            outputFormat.setValue(FIELDS.statusTxt.name(), Messages.getString("RequestTransfer.73")); //$NON-NLS-1$
                            outputFormat.setValue(FIELDS.remote.name(), rhost);
                            outputFormat.setValueString(result.getRunner().getJson());
                            if (requestTransfer.normalInfoAsWarn) {
                                logger.warn(outputFormat.loggerOut());
                            } else {
                                logger.info(outputFormat.loggerOut());
                            }
                            if (!OutputFormat.isQuiet()) {
                                outputFormat.sysout();
                            }
                            break;
                        case TransferOk:
                            value = 0;
                            outputFormat.setValue(FIELDS.status.name(), value);
                            outputFormat.setValue(FIELDS.statusTxt.name(), Messages.getString("RequestTransfer.74")); //$NON-NLS-1$
                            outputFormat.setValue(FIELDS.remote.name(), rhost);
                            outputFormat.setValueString(result.getRunner().getJson());
                            logger.warn(outputFormat.loggerOut());
                            if (!OutputFormat.isQuiet()) {
                                outputFormat.sysout();
                            }
                            break;
                        default:
                            value = 3;
                            outputFormat.setValue(FIELDS.status.name(), value);
                            outputFormat.setValue(FIELDS.statusTxt.name(), Messages.getString("RequestTransfer.75")); //$NON-NLS-1$
                            outputFormat.setValue(FIELDS.remote.name(), rhost);
                            outputFormat.setValueString(result.getRunner().getJson());
                            if (result.getCause() != null) {
                                outputFormat.setValue(FIELDS.error.name(), result.getCause().getMessage());
                            }
                            logger.warn(outputFormat.loggerOut());
                            if (!OutputFormat.isQuiet()) {
                                outputFormat.sysout();
                            }
                            break;
                    }
                } else if (srestart) {
                    switch (finalValue.getCode()) {
                        case QueryStillRunning:
                            value = 0;
                            outputFormat.setValue(FIELDS.status.name(), value);
                            outputFormat.setValue(FIELDS.statusTxt.name(), Messages.getString("RequestTransfer.76")); //$NON-NLS-1$
                            outputFormat.setValue(FIELDS.remote.name(), rhost);
                            outputFormat.setValueString(result.getRunner().getJson());
                            logger.warn(outputFormat.loggerOut());
                            if (!OutputFormat.isQuiet()) {
                                outputFormat.sysout();
                            }
                            break;
                        case Running:
                            value = 0;
                            outputFormat.setValue(FIELDS.status.name(), value);
                            outputFormat.setValue(FIELDS.statusTxt.name(), Messages.getString("RequestTransfer.77")); //$NON-NLS-1$
                            outputFormat.setValue(FIELDS.remote.name(), rhost);
                            outputFormat.setValueString(result.getRunner().getJson());
                            logger.warn(outputFormat.loggerOut());
                            if (!OutputFormat.isQuiet()) {
                                outputFormat.sysout();
                            }
                            break;
                        case PreProcessingOk:
                            value = 0;
                            outputFormat.setValue(FIELDS.status.name(), value);
                            outputFormat.setValue(FIELDS.statusTxt.name(), Messages.getString("RequestTransfer.78")); //$NON-NLS-1$
                            outputFormat.setValue(FIELDS.remote.name(), rhost);
                            outputFormat.setValueString(result.getRunner().getJson());
                            if (requestTransfer.normalInfoAsWarn) {
                                logger.warn(outputFormat.loggerOut());
                            } else {
                                logger.info(outputFormat.loggerOut());
                            }
                            if (!OutputFormat.isQuiet()) {
                                outputFormat.sysout();
                            }
                            break;
                        case CompleteOk:
                            value = 4;
                            outputFormat.setValue(FIELDS.status.name(), value);
                            outputFormat.setValue(FIELDS.statusTxt.name(), Messages.getString("RequestTransfer.79")); //$NON-NLS-1$
                            outputFormat.setValue(FIELDS.remote.name(), rhost);
                            outputFormat.setValueString(result.getRunner().getJson());
                            logger.warn(outputFormat.loggerOut());
                            if (!OutputFormat.isQuiet()) {
                                outputFormat.sysout();
                            }
                            break;
                        case RemoteError:
                            value = 5;
                            outputFormat.setValue(FIELDS.status.name(), value);
                            outputFormat.setValue(FIELDS.statusTxt.name(), Messages.getString("RequestTransfer.80")); //$NON-NLS-1$
                            outputFormat.setValue(FIELDS.remote.name(), rhost);
                            outputFormat.setValueString(result.getRunner().getJson());
                            logger.warn(outputFormat.loggerOut());
                            if (!OutputFormat.isQuiet()) {
                                outputFormat.sysout();
                            }
                            break;
                        case PassThroughMode:
                            value = 6;
                            outputFormat.setValue(FIELDS.status.name(), value);
                            outputFormat.setValue(FIELDS.statusTxt.name(), Messages.getString("RequestTransfer.81")); //$NON-NLS-1$
                            outputFormat.setValue(FIELDS.remote.name(), rhost);
                            outputFormat.setValueString(result.getRunner().getJson());
                            logger.warn(outputFormat.loggerOut());
                            if (!OutputFormat.isQuiet()) {
                                outputFormat.sysout();
                            }
                            break;
                        default:
                            value = 3;
                            outputFormat.setValue(FIELDS.status.name(), value);
                            outputFormat.setValue(FIELDS.statusTxt.name(), Messages.getString("RequestTransfer.82")); //$NON-NLS-1$
                            outputFormat.setValue(FIELDS.remote.name(), rhost);
                            outputFormat.setValueString(result.getRunner().getJson());
                            if (result.getCause() != null) {
                                outputFormat.setValue(FIELDS.error.name(), result.getCause().getMessage());
                            }
                            logger.warn(outputFormat.loggerOut());
                            if (!OutputFormat.isQuiet()) {
                                outputFormat.sysout();
                            }
                            break;
                    }
                }
            } else {
                value = 0;
                // Only request
                outputFormat.setValue(FIELDS.status.name(), value);
                outputFormat.setValue(FIELDS.statusTxt.name(), Messages.getString("RequestTransfer.83")); //$NON-NLS-1$
                outputFormat.setValue(FIELDS.remote.name(), rhost);
                outputFormat.setValueString(result.getRunner().getJson());
                if (requestTransfer.normalInfoAsWarn) {
                    logger.warn(outputFormat.loggerOut());
                } else {
                    logger.info(outputFormat.loggerOut());
                }
                if (!OutputFormat.isQuiet()) {
                    outputFormat.sysout();
                }
            }
        } finally {
            if (DbConstant.admin != null) {
                DbConstant.admin.close();
            }
            System.exit(value);
        }
    }

}
