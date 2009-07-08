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
package openr66.filesystem;

import goldengate.common.command.exception.CommandAbstractException;
import goldengate.common.file.filesystembased.FilesystemBasedFileImpl;
import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;

import java.io.File;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author frederic bregier
 *
 */
public class R66File extends FilesystemBasedFileImpl {
    /**
     * Internal Logger
     */
    private static final GgInternalLogger logger = GgInternalLoggerFactory
            .getLogger(R66File.class);

    /**
     * Retrieve lock to ensure only one call at a time for one file
     */
    private final ReentrantLock retrieveLock = new ReentrantLock();

    /**
     * @param session
     * @param dir
     * @param path
     * @param append
     * @throws CommandAbstractException
     */
    public R66File(R66Session session, R66Dir dir, String path, boolean append)
            throws CommandAbstractException {
        super(session, dir, path, append);
    }

    /*
     * (non-Javadoc)
     *
     * @see goldengate.common.file.FileInterface#trueRetrieve()
     */
    @Override
    public void trueRetrieve() {
        retrieveLock.lock();
        try {
            // FIXME TODO
            /*
             * if (!isReady) { return; } // First check if ready to run from
             * Control try { ((FtpSession)
             * session).getDataConn().getFtpTransferControl()
             * .waitForDataNetworkHandlerReady(); } catch (InterruptedException
             * e) { // bad thing logger.warn("DataNetworkHandler was not ready",
             * e); return; }
             *
             * Channel channel = ((FtpSession) session).getDataConn()
             * .getCurrentDataChannel(); DataBlock block = null; try { block =
             * readDataBlock(); } catch (FileEndOfTransferException e) { // Last
             * block (in fact, previous block was the last one, // but it could
             * be aligned with the block size so not // detected) closeFile();
             * ((FtpSession) session).getDataConn().getFtpTransferControl()
             * .setPreEndOfTransfer(); return; } if (block == null) { // Last
             * block (in fact, previous block was the last one, // but it could
             * be aligned with the block size so not // detected) closeFile();
             * ((FtpSession) session).getDataConn().getFtpTransferControl()
             * .setPreEndOfTransfer(); return; } // While not last block
             * ChannelFuture future = null; while (block != null &&
             * !block.isEOF()) { future = ChannelUtils.write(channel, block); //
             * Test if channel is writable in order to prevent OOM if
             * (channel.isWritable()) { try { block = readDataBlock(); } catch
             * (FileEndOfTransferException e) { closeFile(); // Wait for last
             * write future.awaitUninterruptibly(); ((FtpSession)
             * session).getDataConn()
             * .getFtpTransferControl().setPreEndOfTransfer(); return; } } else
             * { return;// Wait for the next InterestChanged } } // Last block
             * closeFile(); if (block != null) { future =
             * ChannelUtils.write(channel, block); } // Wait for last write if
             * (future != null) { future.awaitUninterruptibly(); ((FtpSession)
             * session).getDataConn().getFtpTransferControl()
             * .setPreEndOfTransfer(); } } catch (FileTransferException e) { //
             * An error occurs! ((FtpSession)
             * session).getDataConn().getFtpTransferControl()
             * .setTransferAbortedFromInternal(true); } catch
             * (FtpNoConnectionException e) { logger.error("Should not be", e);
             * ((FtpSession) session).getDataConn().getFtpTransferControl()
             * .setTransferAbortedFromInternal(true); } catch
             * (CommandAbstractException e) { logger.error("Should not be", e);
             * ((FtpSession) session).getDataConn().getFtpTransferControl()
             * .setTransferAbortedFromInternal(true);
             */
        } finally {
            retrieveLock.unlock();
        }
    }

    /**
     * This method is a good to have in a true FileInterface implementation.
     *
     * @return the FileInterface associated with the current FileInterface
     *         operation
     */
    public File getTrueFile() {
        try {
            return getFileFromPath(getFile());
        } catch (CommandAbstractException e) {
            return null;
        }
    }
}
