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

import openr66.context.ErrorCode;
import openr66.context.R66Result;
import openr66.database.DbConstant;
import openr66.database.data.AbstractDbData;
import openr66.database.data.DbRule;
import openr66.database.data.DbTaskRunner;
import openr66.database.exception.OpenR66DatabaseException;
import openr66.protocol.localhandler.packet.RequestPacket;
import openr66.protocol.utils.ChannelUtils;
import openr66.protocol.utils.R66Future;

import org.jboss.netty.logging.InternalLoggerFactory;

import ch.qos.logback.classic.Level;

/**
 * Client to submit a transfer
 *
 * @author Frederic Bregier
 *
 */
public class SubmitTransfer extends AbstractTransfer {

    public SubmitTransfer(R66Future future, String remoteHost,
            String filename, String rulename, String fileinfo, boolean isMD5, int blocksize) {
        super(SubmitTransfer.class,
                future, filename, rulename, fileinfo, isMD5, remoteHost, blocksize);
    }

    public void run() {
        if (logger == null) {
            logger = GgInternalLoggerFactory.getLogger(SubmitTransfer.class);
        }
        DbRule rule;
        try {
            rule = new DbRule(DbConstant.admin.session, rulename);
        } catch (OpenR66DatabaseException e) {
            logger.error("Cannot get Rule: "+rulename, e);
            future.setResult(new R66Result(e, null, true,
                    ErrorCode.Internal, null));
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
                    ErrorCode.Internal, null));
            future.setFailure(e);
            return;
        }
        taskRunner.changeUpdatedInfo(AbstractDbData.UpdatedInfo.TOSUBMIT);
        try {
            taskRunner.update();
        } catch (OpenR66DatabaseException e) {
            logger.error("Cannot prepare task", e);
            R66Result result = new R66Result(e, null, true,
                    ErrorCode.Internal, taskRunner);
            future.setResult(result);
            future.setFailure(e);
            return;
        }
        R66Result result = new R66Result(null,false,ErrorCode.InitOk, taskRunner);
        future.setResult(result);
        future.setSuccess();
    }
    /**
     *
     * @param args
     *          configuration file, the remoteHost Id, the file to transfer,
     *          the rule, file transfer information as arguments and
     *          optionally isMD5=1 for true or 0 for false(default)
     *          and the blocksize if different than default
     */
    public static void main(String[] args) {
        InternalLoggerFactory.setDefaultFactory(new GgSlf4JLoggerFactory(
                Level.WARN));
        if (logger == null) {
            logger = GgInternalLoggerFactory.getLogger(SubmitTransfer.class);
        }
        if (! getParams(args)) {
            logger.error("Wrong initialization");
            if (DbConstant.admin != null && DbConstant.admin.isConnected) {
                DbConstant.admin.close();
            }
            ChannelUtils.stopLogger();
            System.exit(1);
        }
        R66Future future = new R66Future(true);
        SubmitTransfer transaction = new SubmitTransfer(future,
                rhost, localFilename, rule, fileInfo, ismd5, block);
        transaction.run();
        future.awaitUninterruptibly();
        DbTaskRunner runner = future.getResult().runner;
        if (future.isSuccess()) {
            logger.warn("Prepare transfer in\n    SUCCESS\n    "+runner.toShortString()+
                            "<REMOTE>"+rhost+"</REMOTE>");
        } else {
            if (runner != null) {
                logger.error("Prepare transfer in\n    FAILURE\n     "+runner.toShortString()+
                            "<REMOTE>"+rhost+"</REMOTE>", future.getCause());
            } else {
                logger.error("Prepare transfer in\n    FAILURE\n     ", future.getCause());
            }
            DbConstant.admin.close();
            ChannelUtils.stopLogger();
            System.exit(future.getResult().code.ordinal());
        }
        DbConstant.admin.close();
    }

}
