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
package openr66.protocol.test;

import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;
import goldengate.common.logging.GgSlf4JLoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import openr66.configuration.FileBasedConfiguration;
import openr66.database.DbConstant;
import openr66.database.data.AbstractDbData;
import openr66.database.data.DbRule;
import openr66.database.data.DbTaskRunner;
import openr66.database.exception.OpenR66DatabaseException;
import openr66.protocol.configuration.Configuration;
import openr66.protocol.localhandler.packet.RequestPacket;
import openr66.protocol.utils.R66Future;

import org.jboss.netty.logging.InternalLoggerFactory;

import ch.qos.logback.classic.Level;

/**
 * @author Frederic Bregier
 *
 */
public class TestSubmitTransfer implements Runnable {
    /**
     * Internal Logger
     */
    private static GgInternalLogger logger;

    final private R66Future future;

    final private String filename;

    final private String rulename;

    final private boolean isMD5;

    final private String remoteHost;

    public TestSubmitTransfer(R66Future future, String remoteHost,
            String filename, String rulename, boolean isMD5) {
        if (logger == null) {
            logger = GgInternalLoggerFactory.getLogger(TestSubmitTransfer.class);
        }
        this.remoteHost = remoteHost;
        this.future = future;
        this.filename = filename;
        this.rulename = rulename;
        this.isMD5 = isMD5;
    }

    public void run() {
        // FIXME data transfer
        // int block = 101;
        int block = Configuration.configuration.BLOCKSIZE;
        DbRule rule;
        try {
            rule = new DbRule(DbConstant.admin.session, rulename);
        } catch (OpenR66DatabaseException e) {
            // TODO Auto-generated catch block
            logger.error("Cannot get Rule: "+rulename, e);
            future.setFailure(e);
            return;
        }
        int mode = rule.mode;
        if (isMD5) {
            mode = RequestPacket.getModeMD5(mode);
        }
        RequestPacket request = new RequestPacket(rulename,
                mode, filename, block, 0,
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
        future.setSuccess();
    }

    public static void main(String[] args) {
        InternalLoggerFactory.setDefaultFactory(new GgSlf4JLoggerFactory(
                Level.WARN));
        if (logger == null) {
            logger = GgInternalLoggerFactory.getLogger(TestSubmitTransfer.class);
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
        ExecutorService executorService = Executors.newCachedThreadPool();
        int nb = 2;
        R66Future[] arrayFuture = new R66Future[nb];

        logger.warn("Start");
        for (int i = 0; i < nb; i ++) {
            arrayFuture[i] = new R66Future(true);
            TestSubmitTransfer transaction = new TestSubmitTransfer(arrayFuture[i],
                    rhost, localFilename, rule, isMD5);
            //executorService.execute(transaction);
            transaction.run();
        }
        int success = 0;
        int error = 0;
        for (int i = 0; i < nb; i ++) {
            arrayFuture[i].awaitUninterruptibly();
            if (arrayFuture[i].isSuccess()) {
                success ++;
            } else {
                error ++;
            }
        }
        executorService.shutdown();
        logger.error("Prepare transfer Success: " + success + " Error: " + error);
    }

}
