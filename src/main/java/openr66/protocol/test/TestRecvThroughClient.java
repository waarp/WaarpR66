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
package openr66.protocol.test;

import goldengate.common.logging.GgInternalLoggerFactory;
import goldengate.common.logging.GgSlf4JLoggerFactory;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.logging.InternalLoggerFactory;

import ch.qos.logback.classic.Level;
import openr66.client.RecvThroughClient;
import openr66.client.RecvThroughHandler;
import openr66.context.ErrorCode;
import openr66.context.R66Result;
import openr66.database.DbConstant;
import openr66.protocol.configuration.Configuration;
import openr66.protocol.exception.OpenR66ProtocolBusinessException;
import openr66.protocol.networkhandler.NetworkTransaction;
import openr66.protocol.utils.R66Future;

/**
 * Test class for Recv Through client
 *
 * @author Frederic Bregier
 *
 */
public class TestRecvThroughClient extends RecvThroughClient {
    public static class TestRecvThroughHandler extends RecvThroughHandler {

        /* (non-Javadoc)
         * @see openr66.client.RecvThroughHandler#writeChannelBuffer(org.jboss.netty.buffer.ChannelBuffer)
         */
        @Override
        public void writeChannelBuffer(ChannelBuffer buffer)
                throws OpenR66ProtocolBusinessException {
            byte [] array = this.getByte(buffer);
            // FIXME one should use the array for its own goal
            logger.info("Write {}", array.length);
        }

    }
    /**
     * @param future
     * @param remoteHost
     * @param filename
     * @param rulename
     * @param fileinfo
     * @param isMD5
     * @param blocksize
     * @param networkTransaction
     */
    public TestRecvThroughClient(R66Future future, TestRecvThroughHandler handler,
            String remoteHost,
            String filename, String rulename, String fileinfo, boolean isMD5,
            int blocksize, NetworkTransaction networkTransaction) {
        super(future, handler, remoteHost, filename, rulename, fileinfo, isMD5, blocksize,
                DbConstant.ILLEGALVALUE, networkTransaction);
    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        InternalLoggerFactory.setDefaultFactory(new GgSlf4JLoggerFactory(
                Level.WARN));
        if (logger == null) {
            logger = GgInternalLoggerFactory.getLogger(TestRecvThroughHandler.class);
        }
        if (! getParams(args)) {
            logger.error("Wrong initialization");
            if (DbConstant.admin != null && DbConstant.admin.isConnected) {
                DbConstant.admin.close();
            }
            System.exit(1);
        }
        Configuration.configuration.pipelineInit();
        NetworkTransaction networkTransaction = new NetworkTransaction();
        try {
            R66Future future = new R66Future(true);
            TestRecvThroughHandler handler = new TestRecvThroughHandler();
            TestRecvThroughClient transaction = new TestRecvThroughClient(future,
                    handler,
                    rhost, localFilename, rule, fileInfo, ismd5, block,
                    networkTransaction);
            long time1 = System.currentTimeMillis();
            transaction.run();
            future.awaitUninterruptibly();

            long time2 = System.currentTimeMillis();
            long delay = time2 - time1;
            R66Result result = future.getResult();
            if (future.isSuccess()) {
                if (result.runner.getErrorInfo() == ErrorCode.Warning) {
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
            } else {
                if (result == null || result.runner == null) {
                    logger.warn("Transfer in Error with no Id", future.getCause());
                    networkTransaction.closeAll();
                    System.exit(1);
                }
                if (result.runner.getErrorInfo() == ErrorCode.Warning) {
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
