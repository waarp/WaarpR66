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
import goldengate.common.file.SessionInterface;
import goldengate.common.file.filesystembased.FilesystemBasedFileParameterImpl;
import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;
import openr66.authentication.R66Auth;
import openr66.protocol.config.Configuration;
import openr66.protocol.exception.OpenR66ProtocolSystemException;
import openr66.protocol.localhandler.LocalChannelReference;
import openr66.protocol.localhandler.packet.RequestPacket;
import openr66.task.TaskRunner;
import openr66.task.exception.OpenR66RunnerErrorException;

/**
 * @author frederic bregier
 *
 */
public class R66Session implements SessionInterface {
    /**
     * Internal Logger
     */
    private static final GgInternalLogger logger = GgInternalLoggerFactory
            .getLogger(R66Session.class);

    private int blockSize = Configuration.configuration.BLOCKSIZE;

    private LocalChannelReference localChannelReference;

    private final R66Auth auth;

    private final R66Dir dir;

    private R66File file;

    private volatile boolean isReady = false;

    /**
     * Current Restart information
     */
    private final R66Restart restart;

    /**
     * Current request action
     */
    private RequestPacket request = null;

    /**
     * TaskRunner
     */
    private TaskRunner runner = null;

    /**
     */
    public R66Session() {
        isReady = false;
        auth = new R66Auth(this);
        dir = new R66Dir(this);
        restart = new R66Restart(this);
    }

    /*
     * (non-Javadoc)
     *
     * @see goldengate.common.file.SessionInterface#clear()
     */
    @Override
    public void clear() {
        // First check if a transfer was on going
        if (this.runner != null && this.runner.isFinished()) {
            try {
                this.setFinalizeTransfer(false, "Close before ending");
            } catch (OpenR66RunnerErrorException e) {
            } catch (OpenR66ProtocolSystemException e) {
            }
        }
        // TODO Auto-generated method stub
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
     *            the isReady to set
     */
    public void setReady(boolean isReady) {
        this.isReady = isReady;
    }

    /**
     * @return the request
     */
    public RequestPacket getRequest() {
        return request;
    }

    /**
     * @param request
     *            the request to set
     */
    public void setRequest(RequestPacket request) {
        this.request = request;
    }

    /**
     * @return the runner
     */
    public TaskRunner getRunner() {
        return runner;
    }

    /**
     * @param localChannelReference
     *            the localChannelReference to set
     */
    public void setLocalChannelReference(
            LocalChannelReference localChannelReference) {
        this.localChannelReference = localChannelReference;
    }

    /**
     * @return the localChannelReference
     */
    public LocalChannelReference getLocalChannelReference() {
        return localChannelReference;
    }

    /**
     * Set the runner, start from the PreTask if necessary, and prepare the file
     *
     * @param runner
     *            the runner to set
     * @throws OpenR66RunnerErrorException
     */
    public void setRunner(TaskRunner runner) throws OpenR66RunnerErrorException {
        this.runner = runner;
        if (this.runner.isRetrieve()) {
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
        if (request.getRank() > 0) {
            runner.setTransferTask(request.getRank());
            restart.restartMarker(request.getBlocksize() * request.getRank());
        } else {
            this.runner.setPreTask(0);
            runner.saveStatus();
            this.runner.run();
        }
        // Now create the associated file
        if (this.runner.isRetrieve()) {
            // File should already exist
            try {
                file = (R66File) dir.setFile(request.getFilename(),
                        false);
                if (!file.canRead()) {
                    throw new OpenR66RunnerErrorException("File cannot be read");
                }
            } catch (CommandAbstractException e) {
                throw new OpenR66RunnerErrorException(e);
            }
        } else {
            // File should not exist except if restart
            if (request.getRank() > 0) {
                // Filename should be get back from runner load from database
                try {
                    file = (R66File) dir.setFile(request
                            .getFilename(), true);
                    if (!file.canWrite()) {
                        throw new OpenR66RunnerErrorException(
                                "File cannot be write");
                    }
                } catch (CommandAbstractException e) {
                    throw new OpenR66RunnerErrorException(e);
                }
            } else {
                // New filename and store it
                try {
                    file = dir.setUniqueFile(request
                            .getFilename());
                    if (!file.canWrite()) {
                        throw new OpenR66RunnerErrorException(
                                "File cannot be write");
                    }
                } catch (CommandAbstractException e) {
                    throw new OpenR66RunnerErrorException(e);
                }
            }
        }
        try {
            this.runner.setFilename(file.getFile());
        } catch (CommandAbstractException e) {
            throw new OpenR66RunnerErrorException(e);
        }
        try {
            file.restartMarker(restart);
        } catch (CommandAbstractException e) {
            throw new OpenR66RunnerErrorException(e);
        }
        this.runner.saveStatus();
    }

    public void setFinalizeTransfer(boolean status, Object finalValue)
            throws OpenR66RunnerErrorException, OpenR66ProtocolSystemException {
        int rank = runner.finishTransferTask(status);
        runner.saveStatus();
        try {
            file.closeFile();
        } catch (CommandAbstractException e1) {
            localChannelReference.validateAction(false, e1);
            throw new OpenR66RunnerErrorException(e1);
        }
        if (status) {
            runner.setPostTask(0);
            runner.saveStatus();
            try {
                runner.run();
            } catch (OpenR66RunnerErrorException e1) {
                localChannelReference.validateAction(false, e1);
                throw e1;
            }
            runner.saveStatus();
            if (runner.isRetrieve()) {
                // Nothing to do
            } else {
                if (!runner.isFileMoved()) {
                    String finalpath = dir
                            .getFinalUniqueFilename(file);
                    logger.info("Will move file " + finalpath);
                    try {
                        file.renameTo(runner.getRule().setRecvPath(
                                finalpath));
                    } catch (OpenR66ProtocolSystemException e) {
                        localChannelReference.validateAction(false, e);
                        throw e;
                    } catch (CommandAbstractException e) {
                        localChannelReference.validateAction(false, e);
                        throw new OpenR66RunnerErrorException(e);
                    }
                    logger.info("File finally moved: " + file.toString());
                    try {
                        runner.setFilename(file.getFile());
                    } catch (CommandAbstractException e) {
                    }
                }
            }
            runner.setAllDone();
            runner.saveStatus();
            logger.info("Transfer done on " + file + " at rank " + rank);
        } else {
            // error
            runner.setErrorTask(0);
            runner.saveStatus();
            logger.warn("Transfer KO on " + file);
            try {
                runner.run();
            } catch (OpenR66RunnerErrorException e1) {
                localChannelReference.validateAction(false, finalValue);
                throw e1;
            }
            runner.saveStatus();
        }
        localChannelReference.validateAction(status, finalValue);
    }

    /**
     * @return the file
     */
    public R66File getFile() {
        return file;
    }

    @Override
    public String toString() {
        return "Session: " + (auth != null? auth.toString() : "no Auth") + " " +
                (dir != null? dir.toString() : "no Dir") + " " +
                (file != null? file.toString() : "no File") + " " +
                (request != null? request.toString() : "no Request") + " " +
                (runner != null? runner.toString() : "no Runner");
    }
}
