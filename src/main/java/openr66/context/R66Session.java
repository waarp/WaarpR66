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
package openr66.context;

import goldengate.common.command.exception.CommandAbstractException;
import goldengate.common.file.SessionInterface;
import goldengate.common.file.filesystembased.FilesystemBasedFileParameterImpl;
import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;
import openr66.context.authentication.R66Auth;
import openr66.context.filesystem.R66Dir;
import openr66.context.filesystem.R66File;
import openr66.context.filesystem.R66Restart;
import openr66.context.task.exception.OpenR66RunnerErrorException;
import openr66.database.data.DbTaskRunner;
import openr66.database.data.AbstractDbData.UpdatedInfo;
import openr66.database.data.DbTaskRunner.TASKSTEP;
import openr66.database.exception.OpenR66DatabaseException;
import openr66.protocol.configuration.Configuration;
import openr66.protocol.exception.OpenR66ProtocolSystemException;
import openr66.protocol.localhandler.LocalChannelReference;
import openr66.protocol.localhandler.packet.RequestPacket;

/**
 * The global object session in OpenR66, a session by local channel
 *
 * @author frederic bregier
 *
 */
public class R66Session implements SessionInterface {
    /**
     * Internal Logger
     */
    private static final GgInternalLogger logger = GgInternalLoggerFactory
            .getLogger(R66Session.class);

    /**
     * Block size used during file transfer
     */
    private int blockSize = Configuration.configuration.BLOCKSIZE;
    /**
     * The local channel reference
     */
    private LocalChannelReference localChannelReference;
    /**
     * Authentication
     */
    private final R66Auth auth;
    /**
     * Current directory
     */
    private final R66Dir dir;
    /**
     * Current file
     */
    private R66File file;
    /**
     * Does this session is Ready to server a request
     */
    private volatile boolean isReady = false;

    /**
     * Current Restart information
     */
    private final R66Restart restart;

    /**
     * DbTaskRunner
     */
    private DbTaskRunner runner = null;

    private int status = -1;

    /**
     */
    public R66Session() {
        isReady = false;
        auth = new R66Auth(this);
        dir = new R66Dir(this);
        restart = new R66Restart(this);
    }
    /**
     * Debugging purpose
     * @param stat
     */
    public void setStatus(int stat) {
        this.status = stat;
    }
    /*
     * (non-Javadoc)
     *
     * @see goldengate.common.file.SessionInterface#clear()
     */
    @Override
    public void clear() {
        // First check if a transfer was on going
        if (runner != null && !runner.isFinished()) {
            R66Result result = new R66Result(new OpenR66RunnerErrorException(
                    "Close before ending"), this, true,
                    ErrorCode.Disconnection);// True since called from closed
            if (localChannelReference != null) {
                try {
                    setFinalizeTransfer(false, result);
                } catch (OpenR66RunnerErrorException e) {
                } catch (OpenR66ProtocolSystemException e) {
                }
            }
        }
        if (dir != null) {
            dir.clear();
        }
        if (auth != null) {
            auth.clear();
        }
        if (runner != null) {
            runner.clear();
        }
        // No clean of file since it can be used after channel is closed
        // FIXME see if something else has to be done
        isReady = false;
    }

    /*
     * (non-Javadoc)
     *
     * @see goldengate.common.file.SessionInterface#getAuth()
     */
    @Override
    public R66Auth getAuth() {
        return auth;
    }

    /*
     * (non-Javadoc)
     *
     * @see goldengate.common.file.SessionInterface#getBlockSize()
     */
    @Override
    public int getBlockSize() {
        return blockSize;
    }

    /**
     * @param blocksize
     *            the blocksize to set
     */
    public void setBlockSize(int blocksize) {
        blockSize = blocksize;
    }

    /*
     * (non-Javadoc)
     *
     * @see goldengate.common.file.SessionInterface#getDir()
     */
    @Override
    public R66Dir getDir() {
        return dir;
    }

    /*
     * (non-Javadoc)
     *
     * @see goldengate.common.file.SessionInterface#getFileParameter()
     */
    @Override
    public FilesystemBasedFileParameterImpl getFileParameter() {
        return Configuration.getFileParameter();
    }

    /*
     * (non-Javadoc)
     *
     * @see goldengate.common.file.SessionInterface#getRestart()
     */
    @Override
    public R66Restart getRestart() {
        return restart;
    }

    /**
     *
     * @return True if the connection is currently authenticated
     */
    public boolean isAuthenticated() {
        if (auth == null) {
            return false;
        }
        return auth.isIdentified();
    }

    /**
     * @return True if the Channel is ready to accept transfer
     */
    public boolean isReady() {
        return isReady;
    }

    /**
     * @param isReady
     *            the isReady for transfer to set
     */
    public void setReady(boolean isReady) {
        this.isReady = isReady;
    }

    /**
     * @return the runner
     */
    public DbTaskRunner getRunner() {
        return runner;
    }

    /**
     * @param localChannelReference
     *            the localChannelReference to set
     */
    public void setLocalChannelReference(
            LocalChannelReference localChannelReference) {
        this.localChannelReference = localChannelReference;
        this.localChannelReference.setSession(this);
    }

    /**
     * @return the localChannelReference
     */
    public LocalChannelReference getLocalChannelReference() {
        return localChannelReference;
    }

    /**
     * Set the runner, START from the PreTask if necessary, and prepare the file
     *
     * @param runner
     *            the runner to set
     * @throws OpenR66RunnerErrorException
     */
    public void setRunner(DbTaskRunner runner)
            throws OpenR66RunnerErrorException {
        this.runner = runner;
        if (this.runner.isSender()) {
            // Change dir
            try {
                dir.changeDirectory(this.runner.getRule().sendPath);
            } catch (CommandAbstractException e) {
                throw new OpenR66RunnerErrorException(e);
            }
        } else {
            // Change dir
            try {
                dir.changeDirectory(this.runner.getRule().workPath);
            } catch (CommandAbstractException e) {
                throw new OpenR66RunnerErrorException(e);
            }
        }
        if (runner.getRank() > 0) {
            runner.setTransferTask(runner.getRank());
            restart.restartMarker(runner.getBlocksize() * runner.getRank());
        }
        if (runner.getGloballaststep() == TASKSTEP.NOTASK.ordinal() ||
                runner.getGloballaststep() == TASKSTEP.PRETASK.ordinal()) {
            try {
                file = (R66File) dir.setFile(this.runner.getFilename(),
                    false);
            } catch (CommandAbstractException e) {
                // file is not under normal base directory, so is external
                // File should already exist but can be using special code ('*?')
                file = new R66File(this, dir, this.runner.getOriginalFilename());
            }
            this.runner.setPreTask();
            runner.saveStatus();
            this.runner.run();
            runner.saveStatus();
            runner.setTransferTask(runner.getRank());
        }
        // Now create the associated file
        if (this.runner.isSender()) {
            try {
                if (file == null) {
                    try {
                        file = (R66File) dir.setFile(this.runner.getFilename(),
                            false);
                    } catch (CommandAbstractException e) {
                        // file is not under normal base directory, so is external
                        // File should already exist but can be using special code ('*?')
                        file = new R66File(this, dir, this.runner.getFilename());
                    }
                }
                if (RequestPacket.isSendThroughMode(this.runner.getMode())) {
                    // no test on file since it does not really exist
                    logger.info("File is in through mode: {}", file);
                } else if (!file.canRead()) {
                    throw new OpenR66RunnerErrorException("File cannot be read");
                }
            } catch (CommandAbstractException e) {
                throw new OpenR66RunnerErrorException(e);
            }
        } else {
            // File should not exist except if restart
            if (runner.getRank() > 0) {
                // Filename should be get back from runner load from database
                try {
                    file = (R66File) dir.setFile(this.runner
                            .getFilename(), true);
                    if (RequestPacket.isRecvThroughMode(this.runner.getMode())) {
                        // no test on file since it does not really exist
                        logger.info("File is in through mode: {}", file);
                    } else if (!file.canWrite()) {
                        throw new OpenR66RunnerErrorException(
                                "File cannot be write");
                    }
                } catch (CommandAbstractException e) {
                    throw new OpenR66RunnerErrorException(e);
                }
            } else {
                // New FILENAME if necessary and store it
                file = null;
                try {
                    file = dir.setUniqueFile(this.runner.getSpecialId(),
                            this.runner.getOriginalFilename());
                    if (RequestPacket.isRecvThroughMode(this.runner.getMode())) {
                        // no test on file since it does not really exist
                        logger.info("File is in through mode: {}", file);
                        this.runner.deleteTempFile();
                    } else if (!file.canWrite()) {
                        this.runner.deleteTempFile();
                        throw new OpenR66RunnerErrorException(
                                "File cannot be write");
                    }
                } catch (CommandAbstractException e) {
                    this.runner.deleteTempFile();
                    throw new OpenR66RunnerErrorException(e);
                }
            }
        }
        // Store TRUEFILENAME
        try {
            this.runner.setFilename(file.getFile());
        } catch (CommandAbstractException e) {
            this.runner.deleteTempFile();
            throw new OpenR66RunnerErrorException(e);
        }
        if (runner.getGloballaststep() == TASKSTEP.TRANSFERTASK.ordinal()) {
            try {
                file.restartMarker(restart);
            } catch (CommandAbstractException e) {
                this.runner.deleteTempFile();
                throw new OpenR66RunnerErrorException(e);
            }
        }
        this.runner.saveStatus();
        logger.info("Final init: {}", this.runner);
    }

    /**
     * Finalize the transfer step by running the error or post operation according to the status.
     *
     * @param status
     * @param finalValue
     * @throws OpenR66RunnerErrorException
     * @throws OpenR66ProtocolSystemException
     */
    public void setFinalizeTransfer(boolean status, R66Result finalValue)
            throws OpenR66RunnerErrorException, OpenR66ProtocolSystemException {
        if (runner == null) {
            if (status) {
                localChannelReference.validateRequest(finalValue);
            } else {
                localChannelReference.invalidateRequest(finalValue);
            }
            return;
        }
        if (runner.isFinished()) {
            logger.info("Transfer already done but " + status + " on " + file+runner.toShortString(),
                    new OpenR66RunnerErrorException(finalValue.toString()));
            return;
        }
        if (! status) {
            this.runner.deleteTempFile();
            runner.setErrorExecutionStatus(finalValue.code);
        }
        if (status) {
            runner.finishTransferTask(ErrorCode.TransferOk);
        } else {
            runner.finishTransferTask(finalValue.code);
        }
        runner.saveStatus();
        logger.info("Transfer " + status + " on {}", file);
        if (!runner.ready()) {
            // Pre task in error (or even before)
            OpenR66RunnerErrorException runnerErrorException;
            if (!status && finalValue.exception != null) {
                runnerErrorException = new OpenR66RunnerErrorException(
                        "Pre task in error (or even before)",
                        finalValue.exception);
            } else {
                runnerErrorException = new OpenR66RunnerErrorException(
                        "Pre task in error (or even before)");
            }
            finalValue.exception = runnerErrorException;
            logger.warn("Pre task in error (or even before) : "+
                    runnerErrorException.getMessage());
            localChannelReference.invalidateRequest(finalValue);
            throw runnerErrorException;
        }
        try {
            file.closeFile();
        } catch (CommandAbstractException e1) {
            R66Result result = finalValue;
            if (status) {
                result = new R66Result(new OpenR66RunnerErrorException(e1),
                        this, false, ErrorCode.Internal);
            }
            localChannelReference.invalidateRequest(result);
            throw (OpenR66RunnerErrorException) result.exception;
        }
        runner.finalizeTransfer(localChannelReference, file, finalValue, status);
    }

    /**
     * Try to finalize the request if possible
     * @param errorValue
     * @throws OpenR66RunnerErrorException
     * @throws OpenR66ProtocolSystemException
     */
    public void tryFinalizeRequest(R66Result errorValue)
    throws OpenR66RunnerErrorException, OpenR66ProtocolSystemException {
        if (this.getLocalChannelReference().getFutureRequest().isDone()) {
            return;
        }
        if (runner == null) {
            localChannelReference.invalidateRequest(errorValue);
            return;
        }
        // FIXME do the real end
        if (runner.getStatus() == ErrorCode.CompleteOk) {
            //status = true;
            runner.setAllDone();
            try {
                runner.update();
            } catch (OpenR66DatabaseException e) {
            }
            localChannelReference.validateRequest(
                    new R66Result(this, true, ErrorCode.CompleteOk));
        } else if (runner.getStatus() == ErrorCode.TransferOk) {
            // Try to finalize it
            //status = true;
            try {
                this.setFinalizeTransfer(true,
                        new R66Result(this, true, ErrorCode.CompleteOk));
                localChannelReference.validateRequest(
                    localChannelReference.getFutureEndTransfer().getResult());
            } catch (OpenR66ProtocolSystemException e) {
                logger.warn("Cannot validate runner: {}",runner.toShortString());
                runner.changeUpdatedInfo(UpdatedInfo.INERROR);
                runner.setErrorExecutionStatus(errorValue.code);
                try {
                    runner.update();
                } catch (OpenR66DatabaseException e1) {
                }
                this.setFinalizeTransfer(false, errorValue);
            } catch (OpenR66RunnerErrorException e) {
                logger.warn("Cannot validate runner: {}",runner.toShortString());
                runner.changeUpdatedInfo(UpdatedInfo.INERROR);
                runner.setErrorExecutionStatus(errorValue.code);
                try {
                    runner.update();
                } catch (OpenR66DatabaseException e1) {
                }
                this.setFinalizeTransfer(false, errorValue);
            }
        } else {
            // invalidate Request
            this.setFinalizeTransfer(false, errorValue);
        }
    }
    /**
     * @return the file
     */
    public R66File getFile() {
        return file;
    }

    @Override
    public String toString() {
        return "Session: " + status+" "+(auth != null? auth.toString() : "no Auth") + "\n    " +
                (dir != null? dir.toString() : "no Dir") + "\n    " +
                (file != null? file.toString() : "no File") + "\n    " +
                (runner != null? runner.toShortString() : "no Runner");
    }

    /*
     * (non-Javadoc)
     *
     * @see goldengate.common.file.SessionInterface#getUniqueExtension()
     */
    @Override
    public String getUniqueExtension() {
        return Configuration.EXT_R66;
    }
}
