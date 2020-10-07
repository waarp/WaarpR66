/*******************************************************************************
 * This file is part of Waarp Project (named also Waarp or GG).
 *
 *  Copyright (c) 2019, Waarp SAS, and individual contributors by the @author
 *  tags. See the COPYRIGHT.txt in the distribution for a full listing of
 *  individual contributors.
 *
 *  All Waarp Project is free software: you can redistribute it and/or
 *  modify it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or (at your
 *  option) any later version.
 *
 *  Waarp is distributed in the hope that it will be useful, but WITHOUT ANY
 *  WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
 *  A PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License along with
 *  Waarp . If not, see <http://www.gnu.org/licenses/>.
 ******************************************************************************/
package org.waarp.openr66.protocol.test;

import io.netty.channel.ChannelFuture;
import org.waarp.common.database.exception.WaarpDatabaseException;
import org.waarp.common.exception.FileEndOfTransferException;
import org.waarp.common.exception.FileTransferException;
import org.waarp.common.file.DataBlock;
import org.waarp.common.logging.WaarpLoggerFactory;
import org.waarp.common.logging.WaarpSlf4JLoggerFactory;
import org.waarp.common.utility.DetectionUtils;
import org.waarp.common.utility.WaarpNettyUtil;
import org.waarp.openr66.client.SendThroughClient;
import org.waarp.openr66.context.ErrorCode;
import org.waarp.openr66.context.R66Result;
import org.waarp.openr66.context.filesystem.R66File;
import org.waarp.openr66.context.task.exception.OpenR66RunnerErrorException;
import org.waarp.openr66.database.DbConstant;
import org.waarp.openr66.protocol.configuration.Configuration;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolPacketException;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolSystemException;
import org.waarp.openr66.protocol.networkhandler.NetworkTransaction;
import org.waarp.openr66.protocol.utils.R66Future;

/**
 * Test class for Send Through client
 *
 * @author Frederic Bregier
 */
public class TestSendThroughClient extends SendThroughClient {

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
  public TestSendThroughClient(R66Future future, String remoteHost,
                               String filename, String rulename,
                               String fileinfo, boolean isMD5,
                               int blocksize,
                               NetworkTransaction networkTransaction) {
    super(future, remoteHost, filename, rulename, fileinfo, isMD5, blocksize,
          DbConstant.ILLEGALVALUE, networkTransaction);
  }

  /**
   * @param args
   */
  public static void main(String[] args) {
    WaarpLoggerFactory.setDefaultFactory(new WaarpSlf4JLoggerFactory(
        null));
    if (logger == null) {
      logger = WaarpLoggerFactory.getLogger(TestSendThroughClient.class);
    }
    if (!getParams(args, false)) {
      logger.error("Wrong initialization");
      if (DbConstant.admin != null && DbConstant.admin.isActive()) {
        DbConstant.admin.close();
      }
      DetectionUtils.SystemExit(1);
      return;
    }
    Configuration.configuration.pipelineInit();
    NetworkTransaction networkTransaction = new NetworkTransaction();
    try {
      R66Future future = new R66Future(true);
      TestSendThroughClient transaction = new TestSendThroughClient(future,
                                                                    rhost,
                                                                    localFilename,
                                                                    rule,
                                                                    fileInfo,
                                                                    ismd5,
                                                                    block,
                                                                    networkTransaction);
      transaction.normalInfoAsWarn = snormalInfoAsWarn;
      long time1 = System.currentTimeMillis();
      if (!transaction.initiateRequest()) {
        logger.error("Transfer in Error", future.getCause());
        return;
      }
      if (transaction.sendFile()) {
        transaction.finalizeRequest();
      } else {
        transaction.transferInError(null);
      }
      future.awaitForDoneOrInterruptible();

      long time2 = System.currentTimeMillis();
      long delay = time2 - time1;
      R66Result result = future.getResult();
      if (future.isSuccess()) {
        if (result.getRunner().getErrorInfo() == ErrorCode.Warning) {
          logger.warn("Warning with Id: " +
                      result.getRunner().getSpecialId() + " on file: " +
                      (result.getFile() != null? result.getFile().toString() :
                          "no file")
                      + " delay: " + delay);
        } else {
          logger.warn("Success with Id: " +
                      result.getRunner().getSpecialId() + " on Final file: " +
                      (result.getFile() != null? result.getFile().toString() :
                          "no file")
                      + " delay: " + delay);
        }
        if (nolog || result.getRunner().shallIgnoreSave()) {
          // In case of success, delete the runner
          try {
            result.getRunner().delete();
          } catch (WaarpDatabaseException e) {
            logger
                .warn("Cannot apply nolog to " + result.getRunner().toString(),
                      e);
          }
        }
      } else {
        if (result == null || result.getRunner() == null) {
          logger.warn("Transfer in Error with no Id", future.getCause());
          networkTransaction.closeAll();
          DetectionUtils.SystemExit(1);
          return;
        }
        if (result.getRunner().getErrorInfo() == ErrorCode.Warning) {
          logger.warn("Transfer in Warning with Id: " +
                      result.getRunner().getSpecialId(), future.getCause());
          networkTransaction.closeAll();
          DetectionUtils.SystemExit(result.getCode().ordinal());
        } else {
          logger.error("Transfer in Error with Id: " +
                       result.getRunner().getSpecialId(), future.getCause());
          networkTransaction.closeAll();
          DetectionUtils.SystemExit(result.getCode().ordinal());
        }
      }
    } finally {
      networkTransaction.closeAll();
    }

  }

  public boolean sendFile() {
    R66File r66file = localChannelReference.getSession().getFile();
    boolean retrieveDone = false;
    try {
      DataBlock block = null;
      try {
        block = r66file.readDataBlock();
      } catch (FileEndOfTransferException e) {
        // Last block (in fact, no data to read)
        retrieveDone = true;
        return retrieveDone;
      }
      if (block == null) {
        // Last block (in fact, no data to read)
        retrieveDone = true;
        return retrieveDone;
      }
      ChannelFuture future1 = null, future2 = null;
      if (block != null) {
        block.getBlock().retain();
        future1 = this.writeWhenPossible(block);
      }
      // While not last block
      while (block != null && !block.isEOF()) {
        WaarpNettyUtil.awaitDoneOrInterrupted(future1);
        if (!future1.isSuccess()) {
          return false;
        }
        try {
          block = r66file.readDataBlock();
        } catch (FileEndOfTransferException e) {
          // Wait for last write
          retrieveDone = true;
          WaarpNettyUtil.awaitDoneOrInterrupted(future1);
          return future1.isSuccess();
        }
        block.getBlock().retain();
        future2 = this.writeWhenPossible(block);
        future1 = future2;
      }
      // Wait for last write
      if (future1 != null) {
        WaarpNettyUtil.awaitDoneOrInterrupted(future1);
        return future1.isSuccess();
      }
      retrieveDone = true;
      return retrieveDone;
    } catch (FileTransferException e) {
      // An error occurs!
      this.transferInError(new OpenR66ProtocolSystemException(e));
      return false;
    } catch (OpenR66ProtocolPacketException e) {
      // An error occurs!
      this.transferInError(e);
      return false;
    } catch (OpenR66RunnerErrorException e) {
      // An error occurs!
      this.transferInError(e);
      return false;
    } catch (OpenR66ProtocolSystemException e) {
      // An error occurs!
      this.transferInError(e);
      return false;
    }
  }

}
