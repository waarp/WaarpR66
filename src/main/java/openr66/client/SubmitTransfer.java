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

import openr66.configuration.FileBasedConfiguration;
import openr66.context.ErrorCode;
import openr66.context.R66Result;
import openr66.database.DbConstant;
import openr66.database.data.AbstractDbData;
import openr66.database.data.DbRule;
import openr66.database.data.DbTaskRunner;
import openr66.database.exception.OpenR66DatabaseException;
import openr66.protocol.config.Configuration;
import openr66.protocol.localhandler.packet.RequestPacket;
import openr66.protocol.utils.R66Future;

import org.jboss.netty.logging.InternalLoggerFactory;

import ch.qos.logback.classic.Level;

/**
 * Client to submit a transfer
 *
 * @author Frederic Bregier
 *
 */
public class SubmitTransfer implements Runnable {
    /**
     * Internal Logger
     */
    private static GgInternalLogger logger;

    final private R66Future future;

    final private String filename;

    final private String rulename;

    final private boolean isMD5;

    final private String remoteHost;

    final private int blocksize;

    public SubmitTransfer(R66Future future, String remoteHost,
            String filename, String rulename, boolean isMD5, int blocksize) {
        if (logger == null) {
            logger = GgInternalLoggerFactory.getLogger(SubmitTransfer.class);
        }
        this.remoteHost = remoteHost;
        this.future = future;
        this.filename = filename;
        this.rulename = rulename;
        this.isMD5 = isMD5;
        this.blocksize = blocksize;
    }

    public void run() {
        DbRule rule;
        try {
            rule = new DbRule(DbConstant.admin.session, rulename);
        } catch (OpenR66DatabaseException e) {
            logger.error("Cannot get Rule: "+rulename, e);
            future.setFailure(e);
            return;
        }
        int mode = rule.mode;
        if (isMD5) {
            mode = RequestPacket.getModeMD5(mode);
        }
        RequestPacket request = new RequestPacket(rulename,
                mode, filename, blocksize, 0,
                DbConstant.ILLEGALVALUE, "MONTEST test.xml");
        // Not isRecv since it is the requester, so send => isRetrieve is true
        boolean isRetrieve = ! RequestPacket.isRecvMode(request.getMode());
        DbTaskRunner taskRunner;
        try {
            taskRunner =
                new DbTaskRunner(DbConstant.admin.session,rule,isRetrieve,request,remoteHost);
        } catch (OpenR66DatabaseException e) {
            logger.error("Cannot get task", e);
            future.setFailure(e);
            return;
        }
        taskRunner.changeUpdatedInfo(AbstractDbData.UpdatedInfo.UPDATED);
        try {
            taskRunner.update();
        } catch (OpenR66DatabaseException e) {
            logger.error("Cannot prepare task", e);
            future.setFailure(e);
            return;
        }
        R66Result result = new R66Result(null,false,ErrorCode.InitOk);
        result.other = taskRunner;
        future.setResult(result);
        future.setSuccess();
    }
    /**
     *
     * @param args
     *          configuration file, the remoteHost Id, the file to transfer,
     *          the rule as arguments and optionally isMD5=1 for true or 0 for false(default)
     *          and the blocksize if different than default
     */
    public static void main(String[] args) {
        InternalLoggerFactory.setDefaultFactory(new GgSlf4JLoggerFactory(
                Level.WARN));
        if (logger == null) {
            logger = GgInternalLoggerFactory.getLogger(SubmitTransfer.class);
        }
        if (args.length < 4) {
            logger
                    .error("Needs at least the configuration file, the remoteHost Id, the file to transfer, the rule as arguments and optionally isMD5=1 for true or 0 for false(default)");
            return;
        }
        FileBasedConfiguration fileBasedConfiguration = new FileBasedConfiguration();
        if (! fileBasedConfiguration
                .setConfigurationFromXml(args[0])) {
            logger
                    .error("Needs a correct configuration file as first argument");
            return;
        }
        String rhost = args[1];
        String localFilename = args[2];
        String rule = args[3];

        boolean isMD5 = false;
        if (args.length > 4) {
            if (args[4].equals("1")) {
                isMD5 = true;
            }
        }
        int block = Configuration.configuration.BLOCKSIZE;
        if (args.length > 5) {
            block = Integer.parseInt(args[5]);
        }
        R66Future future = new R66Future(true);
        SubmitTransfer transaction = new SubmitTransfer(future,
                rhost, localFilename, rule, isMD5, block);
        transaction.run();
        future.awaitUninterruptibly();
        if (future.isSuccess()) {
            logger.error("Prepare transfer in Success with Id: " +
                    ((DbTaskRunner) future.getResult().other).getSpecialId());
        } else {
            logger.error("Prepare transfer in Error", future.getCause());
        }
    }

}
