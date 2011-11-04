/**
   This file is part of GoldenGate Project (named also GoldenGate or GG).

   Copyright 2009, Frederic Bregier, and individual contributors by the @author
   tags. See the COPYRIGHT.txt in the distribution for a full listing of
   individual contributors.

   All GoldenGate Project is free software: you can redistribute it and/or 
   modify it under the terms of the GNU General Public License as published 
   by the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   GoldenGate is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with GoldenGate .  If not, see <http://www.gnu.org/licenses/>.
 */
package openr66.context;

import goldengate.common.command.exception.CommandAbstractException;
import goldengate.common.database.data.AbstractDbData.UpdatedInfo;
import goldengate.common.database.exception.GoldenGateDatabaseException;
import goldengate.common.exception.IllegalFiniteStateException;
import goldengate.common.exception.NoRestartException;
import goldengate.common.file.SessionInterface;
import goldengate.common.file.filesystembased.FilesystemBasedFileParameterImpl;
import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;
import goldengate.common.state.MachineState;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

import openr66.context.authentication.R66Auth;
import openr66.context.filesystem.R66Dir;
import openr66.context.filesystem.R66File;
import openr66.context.filesystem.R66Restart;
import openr66.context.task.exception.OpenR66RunnerErrorException;
import openr66.database.data.DbTaskRunner;
import openr66.database.data.DbTaskRunner.TASKSTEP;
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
     * Remote Address
     */
    private SocketAddress raddress;
    /**
     * Local Address
     */
    private SocketAddress laddress;

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

    private String status = "NoStatus";
    
    /**
     * The Finite Machine State
     */
    private final MachineState<R66FiniteDualStates> state;

    /**
     * Create the session
     */
    public R66Session() {
        isReady = false;
        auth = new R66Auth(this);
        dir = new R66Dir(this);
        restart = new R66Restart(this);
        state = R66FiniteDualStates.newSessionMachineState();
    }
    /**
     * Propose a new State
     * @param desiredstate
     * @throws IllegalFiniteStateException if the new status if not ok
     */
    public void newState(R66FiniteDualStates desiredstate) {
        try {
            state.setCurrent(desiredstate);
        } catch (IllegalFiniteStateException e) {
            logger.error("Cannot change of State: {}", this, e);
        }
    }
    /**
     * Debugging purpose
     * @param stat
     */
    public void setStatus(int stat) {
        StackTraceElement elt = Thread.currentThread().getStackTrace()[2];
        this.status = "("+elt.getFileName()+":"+elt.getLineNumber()+"):"+stat;
    }
    /*
     * (non-Javadoc)
     *
     * @see goldengate.common.file.SessionInterface#clear()
     */
    @Override
    public void clear() {
        // First check if a transfer was on going
        if (runner != null && (!runner.isFinished()) && (!runner.continueTransfer())) {
            if (localChannelReference != null) {
                if (!localChannelReference.getFutureRequest().isDone()) {
                    R66Result result = new R66Result(new OpenR66RunnerErrorException(
                            "Close before ending"), this, true,
                            ErrorCode.Disconnection, runner);// True since called from closed
                    result.runner = runner;
                    try {
                        setFinalizeTransfer(false, result);
                    } catch (OpenR66RunnerErrorException e) {
                    } catch (OpenR66ProtocolSystemException e) {
                    }
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
        if (state != null) {
            R66FiniteDualStates.endSessionMachineSate(state);
        }
        // No clean of file since it can be used after channel is closed
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
        if (this.localChannelReference.getNetworkChannel() != null) {
            this.raddress = this.localChannelReference.getNetworkChannel().getRemoteAddress();
            this.laddress = this.localChannelReference.getNetworkChannel().getLocalAddress();
        } else {
            this.raddress = this.laddress = new InetSocketAddress(0);
        }
    }

    /**
     * 
     * @return the remote SocketAddress
     */
    public SocketAddress getRemoteAddress() {
        return this.raddress;
    }
    /**
     * 
     * @return the local SocketAddress
     */
    public SocketAddress getLocalAddress() {
        return this.laddress;
    }
    /**
     * @return the localChannelReference
     */
    public LocalChannelReference getLocalChannelReference() {
        return localChannelReference;
    }

    /**
     * To be called in case of No Session not from a valid LocalChannelHandler
     * @param runner
     * @param localChannelReference
     */
    public void setNoSessionRunner(DbTaskRunner runner, LocalChannelReference localChannelReference) {
        this.runner = runner;
        // Warning: the file is not correctly setup
        try {
            file = (R66File) dir.setFile(this.runner.getFilename(),
                    false);
        } catch (CommandAbstractException e1) {
        }
        this.auth.specialNoSessionAuth(false, Configuration.configuration.HOST_ID);
        this.localChannelReference = localChannelReference;
        if (this.localChannelReference == null) {
            if (this.runner.getLocalChannelReference() != null) {
                this.localChannelReference = this.runner.getLocalChannelReference();
            } else {
                this.localChannelReference = new LocalChannelReference();
            }
            this.localChannelReference.setErrorMessage(this.runner.getErrorInfo().mesg,
                    this.runner.getErrorInfo());
        }
        runner.setLocalChannelReference(this.localChannelReference);
        this.localChannelReference.setSession(this);
    }
    /**
     * Set the File from the runner before PRE operation are done
     * @throws OpenR66RunnerErrorException
     */
    public void setFileBeforePreRunner() throws OpenR66RunnerErrorException {
        // check first if the next step is the PRE task from beginning
        String filename;
        if (this.runner.isPreTaskStarting()) {
            filename = R66Dir.normalizePath(this.runner.getOriginalFilename());
            this.runner.setOriginalFilename(filename);
        } else {
            filename = this.runner.getFilename();
        }
        if (this.runner.isSender()) {
            try {
                if (file == null) {
                    try {
                        file = (R66File) dir.setFile(filename, false);
                    } catch (CommandAbstractException e) {
                        // file is not under normal base directory, so is external
                        // File should already exist but can be using special code ('*?')
                        file = dir.setFileNoCheck(filename);
                    }
                }
                if (RequestPacket.isSendThroughMode(this.runner.getMode())) {
                    // no test on file since it does not really exist
                    logger.debug("File is in through mode: {}", file);
                } else if (!file.canRead()) {
                    // file is not under normal base directory, so is external
                    // File should already exist but cannot use special code ('*?')
                    file = new R66File(this, dir, filename);
                    if (!file.canRead()) {
                        this.runner.setErrorExecutionStatus(ErrorCode.FileNotFound);
                        throw new OpenR66RunnerErrorException("File cannot be read: "+
                            file.getTrueFile().getAbsolutePath());
                    }
                }
            } catch (CommandAbstractException e) {
                throw new OpenR66RunnerErrorException(e);
            }
        } else {
            // not sender so file is just registered as is but no test of existence
            file = new R66File(this, dir, filename);
        }
    }

    /**
     * Set the File from the runner once PRE operation are done
     * @param createFile When True, the file can be newly created if needed.
     *  If False, no new file will be created, thus having an Exception.
     * @throws OpenR66RunnerErrorException
     */
    public void setFileAfterPreRunner(boolean createFile) throws OpenR66RunnerErrorException {
        // Now create the associated file
        if (this.runner.isSender()) {
            try {
                if (file == null) {
                    try {
                        file = (R66File) dir.setFile(this.runner.getFilename(),
                            false);
                    } catch (CommandAbstractException e) {
                        // file is not under normal base directory, so is external
                        // File must already exist but can be using special code ('*?')
                        file = dir.setFileNoCheck(this.runner.getFilename());
                        // file = new R66File(this, dir, this.runner.getFilename());
                    }
                }
                if (RequestPacket.isSendThroughMode(this.runner.getMode())) {
                    // no test on file since it does not really exist
                    logger.debug("File is in through mode: {}", file);
                } else if (!file.canRead()) {
                 // file is not under normal base directory, so is external
                    // File must already exist but cannot used special code ('*?')
                    file = new R66File(this, dir, this.runner.getFilename());
                    if (!file.canRead()) {
                        this.runner.setErrorExecutionStatus(ErrorCode.FileNotFound);
                        throw new OpenR66RunnerErrorException("File cannot be read: "+
                            file.getTrueFile().getAbsolutePath());
                    }
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
                        logger.debug("File is in through mode: {}", file);
                    } else if (!file.canWrite()) {
                        throw new OpenR66RunnerErrorException(
                                "File cannot be write");
                    }
                } catch (CommandAbstractException e) {
                    throw new OpenR66RunnerErrorException(e);
                }
            } else {
                // New FILENAME if necessary and store it
                if (createFile) {
                    file = null;
                    String newfilename = this.runner.getOriginalFilename();
                    if (newfilename.charAt(1) == ':') {
                        // Windows path
                        newfilename = newfilename.substring(2);
                    }
                    this.runner.setFilename(R66File.getBasename(newfilename));
                    try {
                        file = dir.setUniqueFile(this.runner.getSpecialId(),
                                this.runner.getFilename());
                        if (RequestPacket.isRecvThroughMode(this.runner.getMode())) {
                            // no test on file since it does not really exist
                            logger.debug("File is in through mode: {}", file);
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
                } else {
                    throw new OpenR66RunnerErrorException("No file created");
                }
            }
        }
        // Store TRUEFILENAME
        try {
            if (this.runner.isFileMoved()) {
                this.runner.setFileMoved(file.getFile(), true);
            } else {
                this.runner.setFilename(file.getFile());
            }
        } catch (CommandAbstractException e) {
            this.runner.deleteTempFile();
            throw new OpenR66RunnerErrorException(e);
        }
    }
    /**
     * To be used when a request comes with a bad code so it cannot be set normally
     * @param runner
     * @param code
     */
    public void setBadRunner(DbTaskRunner runner, ErrorCode code) {
        this.runner = runner;
        if (code == ErrorCode.QueryAlreadyFinished) {
            if (this.runner.isSender()) {
                // Change dir
                try {
                    dir.changeDirectory(this.runner.getRule().sendPath);
                } catch (CommandAbstractException e) {
                }
            } else {
                // Change dir
                try {
                    dir.changeDirectory(this.runner.getRule().workPath);
                } catch (CommandAbstractException e) {
                }
            }
            this.runner.setPostTask();
            try {
                setFileAfterPreRunner(false);
            } catch (OpenR66RunnerErrorException e) {
            }
        }
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
            logger.debug("restart at "+runner.getRank()+ " {}",runner);
            runner.setTransferTask(runner.getRank());
            restart.restartMarker(runner.getBlocksize() * runner.getRank());
        } else {
            restart.restartMarker(0);
        }
        if (runner.getGloballaststep() == TASKSTEP.NOTASK.ordinal() ||
                runner.getGloballaststep() == TASKSTEP.PRETASK.ordinal()) {
            setFileBeforePreRunner();
            this.runner.setPreTask();
            runner.saveStatus();
            this.runner.run();
            runner.saveStatus();
            runner.setTransferTask(runner.getRank());
        } else {
            runner.reset();
            runner.changeUpdatedInfo(UpdatedInfo.RUNNING);
            runner.saveStatus();
        }
        // Now create the associated file
        setFileAfterPreRunner(true);
        if (runner.getGloballaststep() == TASKSTEP.TRANSFERTASK.ordinal()) {
            if (!this.runner.isSender()) {
                // Check file length according to rank
                if (RequestPacket.isRecvThroughMode(this.runner.getMode())) {
                    // no size can be checked
                } else {
                    try {
                        long length = file.length();
                        long oldPosition = restart.getPosition();
                        restart.setSet(true);
                        if (oldPosition > length) {
                            int newRank = ((int) (length / this.runner.getBlocksize()))
                                - Configuration.RANKRESTART;
                            if (newRank <= 0) {
                                newRank = 1;
                            }
                            logger.warn("Decreased Rank Restart for {} at "+newRank, runner);
                            runner.setTransferTask(newRank);
                            restart.restartMarker(this.runner.getBlocksize() * this.runner.getRank());
                        }
                        try {
                            file.restartMarker(restart);
                        } catch (CommandAbstractException e) {
                            this.runner.deleteTempFile();
                            throw new OpenR66RunnerErrorException(e);
                        }
                    } catch (CommandAbstractException e1) {
                        // length wrong
                        throw new OpenR66RunnerErrorException("File length is wrong", e1);
                    } catch (NoRestartException e) {
                        // length is not to be changed
                    }
                }
            } else {
                try {
                    this.localChannelReference.getFutureRequest().filesize = file.length();
                } catch (CommandAbstractException e1) {
                }
                try {
                    file.restartMarker(restart);
                } catch (CommandAbstractException e) {
                    this.runner.deleteTempFile();
                    throw new OpenR66RunnerErrorException(e);
                }
            }
        }
        this.runner.saveStatus();
        logger.info("Final init: {}", this.runner);
    }
    /**
     * Rename the current receive file from the very beginning since the sender
     * has a post action that changes its name
     * @param newFilename
     * @throws OpenR66RunnerErrorException
     */
    public void renameReceiverFile(String newFilename) throws OpenR66RunnerErrorException {
        // First delete the temporary file if needed
        if (runner.getRank() > 0) {
            logger.error("Renaming file is not correct since transfer does not start from first block");
            // Not correct
            throw new OpenR66RunnerErrorException("Renaming file not correct since transfer already started");
        }
        if (!RequestPacket.isRecvThroughMode(this.runner.getMode())) {
            this.runner.deleteTempFile();
        }
        // Now rename it
        this.runner.setOriginalFilename(newFilename);
        this.setFileAfterPreRunner(true);
        this.runner.saveStatus();
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
        logger.debug(status+":"+finalValue+":"+runner);
        if (runner == null) {
            if (localChannelReference != null) {
                if (status) {
                    localChannelReference.validateRequest(finalValue);
                } else {
                    localChannelReference.invalidateRequest(finalValue);
                }
            }
            return;
        }
        if (runner.isAllDone()) {
            logger.debug("Transfer already done but " + status + " on " + file+runner.toShortString(),
                    new OpenR66RunnerErrorException(finalValue.toString()));
            // FIXME ??
            /*if (! status)
                runner.finalizeTransfer(localChannelReference, file, finalValue, status);*/
            return;
        }
        if (localChannelReference.getFutureRequest().isDone()) {
            logger.debug("Request already done but " + status + " on " + file+runner.toShortString(),
                    new OpenR66RunnerErrorException(finalValue.toString()));
            // Already finished once so do nothing more
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
        logger.debug("Transfer " + status + " on {} and {}", file, runner);
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
            logger.error("Pre task in error (or even before) : "+
                    runnerErrorException.getMessage());
            localChannelReference.invalidateRequest(finalValue);
            throw runnerErrorException;
        }
        try {
            if (file != null) {
                file.closeFile();
            }
        } catch (CommandAbstractException e1) {
            R66Result result = finalValue;
            if (status) {
                result = new R66Result(new OpenR66RunnerErrorException(e1),
                        this, false, ErrorCode.Internal, runner);
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
        if (this.getLocalChannelReference() == null) {
            return;
        }
        if (this.getLocalChannelReference().getFutureRequest().isDone()) {
            return;
        }
        //setRunnerFromLocalChannelReference(localChannelReference);
        if (runner == null) {
            localChannelReference.invalidateRequest(errorValue);
            return;
        }
        // do the real end
        if (runner.getStatus() == ErrorCode.CompleteOk) {
            //status = true;
            runner.setAllDone();
            try {
                runner.update();
            } catch (GoldenGateDatabaseException e) {
            }
            localChannelReference.validateRequest(
                    new R66Result(this, true, ErrorCode.CompleteOk, runner));
        } else if (runner.getStatus() == ErrorCode.TransferOk &&
                ((!runner.isSender()) || errorValue.code == ErrorCode.QueryAlreadyFinished)) {
            // Try to finalize it
            //status = true;
            try {
                this.setFinalizeTransfer(true,
                        new R66Result(this, true, ErrorCode.CompleteOk, runner));
                localChannelReference.validateRequest(
                    localChannelReference.getFutureEndTransfer().getResult());
            } catch (OpenR66ProtocolSystemException e) {
                logger.error("Cannot validate runner:\n    {}",runner.toShortString());
                runner.changeUpdatedInfo(UpdatedInfo.INERROR);
                runner.setErrorExecutionStatus(errorValue.code);
                try {
                    runner.update();
                } catch (GoldenGateDatabaseException e1) {
                }
                this.setFinalizeTransfer(false, errorValue);
            } catch (OpenR66RunnerErrorException e) {
                logger.error("Cannot validate runner:\n    {}",runner.toShortString());
                runner.changeUpdatedInfo(UpdatedInfo.INERROR);
                runner.setErrorExecutionStatus(errorValue.code);
                try {
                    runner.update();
                } catch (GoldenGateDatabaseException e1) {
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
        return "Session: FS[" + state.getCurrent()+"] "+status+"\n "+
                (auth != null? auth.toString() : "no Auth") + "\n    " +
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
