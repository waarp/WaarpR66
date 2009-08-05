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

import goldengate.common.logging.GgInternalLoggerFactory;
import goldengate.common.logging.GgSlf4JLoggerFactory;


import openr66.commander.ClientRunner;
import openr66.context.ErrorCode;
import openr66.context.R66Result;
import openr66.context.task.exception.OpenR66RunnerErrorException;
import openr66.database.DbConstant;
import openr66.database.data.DbRule;
import openr66.database.data.DbTaskRunner;
import openr66.database.exception.OpenR66DatabaseException;
import openr66.database.exception.OpenR66DatabaseSqlError;
import openr66.protocol.configuration.Configuration;
import openr66.protocol.exception.OpenR66ProtocolNoConnectionException;
import openr66.protocol.exception.OpenR66ProtocolPacketException;
import openr66.protocol.localhandler.packet.RequestPacket;
import openr66.protocol.networkhandler.NetworkTransaction;
import openr66.protocol.utils.R66Future;

import org.jboss.netty.logging.InternalLoggerFactory;

import ch.qos.logback.classic.Level;

/**
 * Direct Transfer from a client with or without database connection
 *
 * @author Frederic Bregier
 *
 */
public class DirectTransfer extends AbstractTransfer {
    protected final NetworkTransaction networkTransaction;
    static protected boolean nolog = false;

    protected static int getSpecialParams(String []args, int rank) {
        if (args[rank].equalsIgnoreCase("-nolog")) {
            nolog = true;
            rank++;
        }
        return rank;
    }

    public DirectTransfer(R66Future future, String remoteHost,
            String filename, String rulename, String fileinfo, boolean isMD5, int blocksize,
            NetworkTransaction networkTransaction) {
        super(SubmitTransfer.class,
                future, filename, rulename, fileinfo, isMD5, remoteHost, blocksize);
        this.networkTransaction = networkTransaction;
    }

    /**
     * Prior to call this method, the pipeline and NetworkTransaction must have been initialized.
     * It is the responsibility of the caller to finish all network resources.
     */
    public void run() {
        DbRule rule;
        try {
            rule = new DbRule(DbConstant.admin.session, rulename);
        } catch (OpenR66DatabaseException e) {
            logger.error("Cannot get Rule: "+rulename, e);
            future.setResult(new R66Result(e, null, true,
                    ErrorCode.Internal));
            future.setFailure(e);
            return;
        }
        int mode = rule.mode;
        if (isMD5) {
            mode = RequestPacket.getModeMD5(mode);
        }
        RequestPacket request = new RequestPacket(rulename,
                mode, filename, blocksize, 0,
                DbConstant.ILLEGALVALUE, fileinfo);
        // Not isRecv since it is the requester, so send => isRetrieve is true
        boolean isRetrieve = ! RequestPacket.isRecvMode(request.getMode());
        DbTaskRunner taskRunner;
        try {
            taskRunner =
                new DbTaskRunner(DbConstant.admin.session,rule,isRetrieve,request,remoteHost);
        } catch (OpenR66DatabaseException e) {
            logger.error("Cannot get task", e);
            future.setResult(new R66Result(e, null, true,
                    ErrorCode.Internal));
            future.setFailure(e);
            return;
        }
        ClientRunner runner = new ClientRunner(networkTransaction, taskRunner);
        R66Future futureRunner = null;
        try {
            futureRunner = runner.runTransfer();
        } catch (OpenR66RunnerErrorException e) {
            logger.error("Cannot Transfer", e);
            future.setResult(new R66Result(e, null, true,
                    ErrorCode.Internal));
            future.setFailure(e);
            return;
        } catch (OpenR66ProtocolNoConnectionException e) {
            logger.error("Cannot Connect", e);
            future.setResult(new R66Result(e, null, true,
                    ErrorCode.ConnectionImpossible));
            future.setFailure(e);
            return;
        } catch (OpenR66ProtocolPacketException e) {
            logger.error("Bad Protocol", e);
            future.setResult(new R66Result(e, null, true,
                    ErrorCode.TransferError));
            future.setFailure(e);
            return;
        }
        futureRunner.awaitUninterruptibly();

        if (futureRunner.isSuccess()) {
            future.setResult(futureRunner
                    .getResult());
            future.setSuccess();
        } else {
            future.setResult(futureRunner
                    .getResult());
            Throwable throwable = futureRunner
                    .getCause();
            if (throwable == null) {
                future.cancel();
            } else {
                future.setFailure(throwable);
            }
        }
    }

    public static void main(String[] args) {
        InternalLoggerFactory.setDefaultFactory(new GgSlf4JLoggerFactory(
                Level.WARN));
        if (logger == null) {
            logger = GgInternalLoggerFactory.getLogger(DirectTransfer.class);
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
        long time1 = System.currentTimeMillis();
        R66Future future = new R66Future(true);

        Configuration.configuration.pipelineInit();
        NetworkTransaction networkTransaction = new NetworkTransaction();
        try {
            DirectTransfer transaction = new DirectTransfer(future,
                    rhost, localFilename, rule, fileInfo, ismd5, block,
                    networkTransaction);
            transaction.run();
            future.awaitUninterruptibly();
            long time2 = System.currentTimeMillis();
            long delay = time2 - time1;
            R66Result result = future.getResult();
            if (future.isSuccess()) {
                if (result.runner.getStatus() == ErrorCode.Warning) {
                    logger.warn("Warning with Id: " +
                            result.runner.getSpecialId()+" on file: " +
                            (result.file != null? result.file.toString() : "no file")
                            +" delay: "+delay);
                } else {
                    logger.warn("Success with Id: " +
                            result.runner.getSpecialId()+" on Final file: " +
                            (result.file != null? result.file.toString() : "no file")
                            +" delay: "+delay);
                }
                if (nolog) {
                    // In case of success, delete the runner
                    try {
                        result.runner.delete();
                    } catch (OpenR66DatabaseException e) {
                        logger.warn("Cannot apply nolog to "+result.runner.toString(), e);
                    }
                }
            } else {
                if (result == null || result.runner == null) {
                    logger.warn("Transfer in Error with no Id", future.getCause());
                    networkTransaction.closeAll();
                    System.exit(1);
                }
                if (result.runner.getStatus() == ErrorCode.Warning) {
                    logger.warn("Transfer in Warning with Id: " +
                            result.runner.getSpecialId(), future.getCause());
                    networkTransaction.closeAll();
                    System.exit(result.code.ordinal());
                } else {
                    logger.error("Transfer in Error with Id: " +
                            result.runner.getSpecialId(), future.getCause());
                    networkTransaction.closeAll();
                    System.exit(result.code.ordinal());
                }
            }
        } finally {
            networkTransaction.closeAll();
        }
    }

}
