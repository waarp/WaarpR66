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
package openr66.context.filesystem;

import goldengate.common.command.exception.CommandAbstractException;
import goldengate.common.exception.FileEndOfTransferException;
import goldengate.common.exception.FileTransferException;
import goldengate.common.file.DataBlock;
import goldengate.common.file.filesystembased.FilesystemBasedDirImpl;
import goldengate.common.file.filesystembased.FilesystemBasedFileImpl;
import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicBoolean;

import openr66.context.ErrorCode;
import openr66.context.R66Result;
import openr66.context.R66Session;
import openr66.context.task.exception.OpenR66RunnerErrorException;
import openr66.protocol.exception.OpenR66ProtocolPacketException;
import openr66.protocol.exception.OpenR66ProtocolSystemException;
import openr66.protocol.localhandler.LocalChannelReference;
import openr66.protocol.localhandler.RetrieveRunner;
import openr66.protocol.utils.ChannelUtils;

import org.jboss.netty.channel.ChannelFuture;

/**
 * File representation
 *
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
     * Does the current file is external (i.e. out of R66 base directory)
     */
    private boolean isExternal = false;

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

    /**
     * This constructor is for External file
     *
     * @param session
     * @param dir
     * @param path
     */
    public R66File(R66Session session, R66Dir dir, String path) {
        super(session, dir, path);
        isExternal = true;
    }

    /**
     * Start the retrieve (send to the remote host the local file)
     *
     * @param running When false, should stop the runner
     * @throws OpenR66RunnerErrorException
     * @throws OpenR66ProtocolSystemException
     */
    public void retrieveBlocking(AtomicBoolean running) throws OpenR66RunnerErrorException,
            OpenR66ProtocolSystemException {
        boolean retrieveDone = false;
        LocalChannelReference localChannelReference = getSession()
                .getLocalChannelReference();
        try {
            if (!isReady) {
                return;
            }
            DataBlock block = null;
            try {
                block = readDataBlock();
            } catch (FileEndOfTransferException e) {
                // Last block (in fact, no data to read)
                retrieveDone = true;
                return;
            }
            if (block == null) {
                // Last block (in fact, no data to read)
                retrieveDone = true;
                return;
            }
            // While not last block
            ChannelFuture future = null;
            while (block != null && (!block.isEOF()) && (running.get())) {
                future = RetrieveRunner.writeWhenPossible(
                        block, localChannelReference);
                try {
                    block = readDataBlock();
                } catch (FileEndOfTransferException e) {
                    // Wait for last write
                    try {
                        future.await();
                    } catch (InterruptedException e1) {
                    }
                    if (future.isSuccess()) {
                        retrieveDone = true;
                    }
                    return;
                }
                try {
                    future.await();
                } catch (InterruptedException e) {
                    return;
                }
                if (future.isCancelled()) {
                    return;
                }
            }
            if (!running.get()) {
                // stopped
                return;
            }
            // Last block
            if (block != null) {
                future = ChannelUtils.writeBackDataBlock(localChannelReference, block);
            }
            // Wait for last write
            if (future != null) {
                try {
                    future.await();
                } catch (InterruptedException e) {
                    return;
                }
                if (future.isCancelled()) {
                    return;
                }
            }
            retrieveDone = true;
            return;
        } catch (FileTransferException e) {
            // An error occurs!
            getSession().setFinalizeTransfer(
                    false,
                    new R66Result(new OpenR66ProtocolSystemException(e),
                            getSession(), false, ErrorCode.TransferError, getSession().getRunner()));
        } catch (OpenR66ProtocolPacketException e) {
            // An error occurs!
            getSession()
                    .setFinalizeTransfer(
                            false,
                            new R66Result(e, getSession(), false,
                                    ErrorCode.Internal, getSession().getRunner()));
        } finally {
            if (retrieveDone) {
                try {
                    ChannelUtils.writeEndTransfer(localChannelReference);
                } catch (OpenR66ProtocolPacketException e) {
                    // An error occurs!
                    getSession().setFinalizeTransfer(
                            false,
                            new R66Result(e, getSession(), false,
                                    ErrorCode.Internal, getSession().getRunner()));
                }
            } else {
                // An error occurs!
                getSession().setFinalizeTransfer(
                        false,
                        new R66Result(new OpenR66ProtocolSystemException("Transfer in error"),
                                getSession(), false, ErrorCode.TransferError, getSession().getRunner()));
            }
        }
    }

    /**
     * This method is a good to have in a true FileInterface implementation.
     *
     * @return the File associated with the current FileInterface
     *         operation
     */
    public File getTrueFile() {
        if (isExternal) {
            return new File(currentFile);
        }
        try {
            return getFileFromPath(getFile());
        } catch (CommandAbstractException e) {
            return null;
        }
    }
    /**
     *
     * @return the basename of the current file
     */
    public String getBasename() {
        return getBasename(currentFile);
    }
    /**
     *
     * @param path
     * @return the basename from the given path
     */
    public static String getBasename(String path) {
        File file = new File(path);
        return file.getName();
    }

    @Override
    public R66Session getSession() {
        return (R66Session) session;
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * goldengate.common.file.filesystembased.FilesystemBasedFileImpl#canRead()
     */
    @Override
    public boolean canRead() throws CommandAbstractException {
        if (isExternal) {
            File file = new File(currentFile);
            return file.canRead();
        }
        return super.canRead();
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * goldengate.common.file.filesystembased.FilesystemBasedFileImpl#canWrite()
     */
    @Override
    public boolean canWrite() throws CommandAbstractException {
        if (isExternal) {
            File file = new File(currentFile);
            if (file.exists()) {
                return file.canWrite();
            }
            return file.getParentFile().canWrite();
        }
        return super.canWrite();
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * goldengate.common.file.filesystembased.FilesystemBasedFileImpl#delete()
     */
    @Override
    public boolean delete() throws CommandAbstractException {
        if (isExternal) {
            File file = new File(currentFile);
            checkIdentify();
            if (!isReady) {
                return false;
            }
            if (!file.exists()) {
                return true;
            }
            closeFile();
            return file.delete();
        }
        return super.delete();
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * goldengate.common.file.filesystembased.FilesystemBasedFileImpl#exists()
     */
    @Override
    public boolean exists() throws CommandAbstractException {
        if (isExternal) {
            File file = new File(currentFile);
            return file.exists();
        }
        return super.exists();
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * goldengate.common.file.filesystembased.FilesystemBasedFileImpl#getFileChannel
     * (boolean)
     */
    @Override
    protected FileChannel getFileChannel(boolean isOut) {
        if (!isExternal) {
            return super.getFileChannel(isOut);
        }
        if (!isReady) {
            return null;
        }
        File trueFile = getTrueFile();
        FileChannel fileChannel;
        try {
            if (isOut) {
                if (getPosition() == 0) {
                    FileOutputStream fileOutputStream = new FileOutputStream(
                            trueFile);
                    fileChannel = fileOutputStream.getChannel();
                } else {
                    RandomAccessFile randomAccessFile = new RandomAccessFile(
                            trueFile, "rw");
                    fileChannel = randomAccessFile.getChannel();
                    fileChannel = fileChannel.position(getPosition());
                }
            } else {
                if (!trueFile.exists()) {
                    return null;
                }
                FileInputStream fileInputStream = new FileInputStream(trueFile);
                fileChannel = fileInputStream.getChannel();
                if (getPosition() != 0) {
                    fileChannel = fileChannel.position(getPosition());
                }
            }
        } catch (FileNotFoundException e) {
            logger.error("FileInterface not found in getFileChannel:", e);
            return null;
        } catch (IOException e) {
            logger.error("Change position in getFileChannel:", e);
            return null;
        }
        return fileChannel;
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * goldengate.common.file.filesystembased.FilesystemBasedFileImpl#isDirectory
     * ()
     */
    @Override
    public boolean isDirectory() throws CommandAbstractException {
        if (isExternal) {
            File dir = new File(currentFile);
            return dir.isDirectory();
        }
        return super.isDirectory();
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * goldengate.common.file.filesystembased.FilesystemBasedFileImpl#isFile()
     */
    @Override
    public boolean isFile() throws CommandAbstractException {
        if (isExternal) {
            File file = new File(currentFile);
            return file.isFile();
        }
        return super.isFile();
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * goldengate.common.file.filesystembased.FilesystemBasedFileImpl#length()
     */
    @Override
    public long length() throws CommandAbstractException {
        if (isExternal) {
            File file = new File(currentFile);
            return file.length();
        }
        return super.length();
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * goldengate.common.file.filesystembased.FilesystemBasedFileImpl#renameTo
     * (java.lang.String)
     */
    @Override
    public boolean renameTo(String path) throws CommandAbstractException {
        if (!isExternal) {
            return super.renameTo(path);
        }
        checkIdentify();
        if (!isReady) {
            return false;
        }
        File file = getTrueFile();
        if (file.canRead()) {
            File newFile = getFileFromPath(path);
            if (newFile.getParentFile().canWrite()) {
                if (!file.renameTo(newFile)) {
                    FileOutputStream fileOutputStream;
                    try {
                        fileOutputStream = new FileOutputStream(newFile);
                    } catch (FileNotFoundException e) {
                        logger
                                .warn("Cannot find file: " + newFile.getName(),
                                        e);
                        return false;
                    }
                    FileChannel fileChannelOut = fileOutputStream.getChannel();
                    if (get(fileChannelOut)) {
                        delete();
                    } else {
                        logger.warn("Cannot write file: {}", newFile);
                        return false;
                    }
                }
                currentFile = getRelativePath(newFile);
                isExternal = false;
                isReady = true;
                return true;
            }
        }
        return false;
    }

    /**
     * Move the current file to the path as destination
     *
     * @param path
     * @param external
     *            if True, the path is outside authentication control
     * @return True if the operation is done
     * @throws CommandAbstractException
     */
    public boolean renameTo(String path, boolean external)
            throws CommandAbstractException {
        if (!external) {
            return renameTo(path);
        }
        checkIdentify();
        if (!isReady) {
            return false;
        }
        File file = getTrueFile();
        if (file.canRead()) {
            File newFile = new File(path);
            if (newFile.getParentFile().canWrite()) {
                if (!file.renameTo(newFile)) {
                    FileOutputStream fileOutputStream;
                    try {
                        fileOutputStream = new FileOutputStream(newFile);
                    } catch (FileNotFoundException e) {
                        logger
                                .warn("Cannot find file: " + newFile.getName(),
                                        e);
                        return false;
                    }
                    FileChannel fileChannelOut = fileOutputStream.getChannel();
                    if (get(fileChannelOut)) {
                        delete();
                    } else {
                        logger.warn("Cannot write file: {}", newFile);
                        return false;
                    }
                }
                currentFile = FilesystemBasedDirImpl.normalizePath(newFile
                        .getAbsolutePath());
                isExternal = true;
                isReady = true;
                return true;
            }
        }
        return false;
    }

    /**
     * Replace the current file with the new filename after closing the previous
     * one.
     *
     * @param filename
     * @param isExternal
     * @throws CommandAbstractException
     */
    public void replaceFilename(String filename, boolean isExternal)
            throws CommandAbstractException {
        closeFile();
        currentFile = filename;
        this.isExternal = isExternal;
        isReady = true;
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * goldengate.common.file.filesystembased.FilesystemBasedFileImpl#closeFile
     * ()
     */
    @Override
    public boolean closeFile() throws CommandAbstractException {
        boolean status = super.closeFile();
        // FORCE re-open file
        isReady = true;
        return status;
    }

    /**
     *
     * @return True if this file is outside OpenR66 Base directory
     */
    public boolean isExternal() {
        return isExternal;
    }
    @Override
    public String toString() {
        return "File: " + currentFile + " Ready " + isReady + " " +
                getPosition();
    }
}
