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

import openr66.authentication.R66Auth;
import openr66.protocol.config.Configuration;
import openr66.protocol.localhandler.LocalChannelReference;
import openr66.protocol.localhandler.LocalServerHandler;
import goldengate.common.file.SessionInterface;
import goldengate.common.file.filesystembased.FilesystemBasedFileParameterImpl;

/**
 * @author frederic bregier
 *
 */
public class R66Session implements SessionInterface {
    private LocalChannelReference localChannelReference;
    private final LocalServerHandler localServerHandler;
    private R66Auth auth;
    private R66Dir dir;
    private volatile boolean isReady = false;
    /**
     * Current Restart information
     */
    private R66Restart restart = null;
    
    /**
     * @param localServerHandler
     */
    public R66Session(LocalServerHandler localServerHandler) {
        this.localServerHandler = localServerHandler;
        isReady = false;
    }
    /* (non-Javadoc)
     * @see goldengate.common.file.SessionInterface#clear()
     */
    @Override
    public void clear() {
        // TODO Auto-generated method stub
        if (dir != null) {
            dir.clear();
        }
        if (auth != null) {
            auth.clear();
        }
        isReady = false;
    }

    /* (non-Javadoc)
     * @see goldengate.common.file.SessionInterface#getAuth()
     */
    @Override
    public R66Auth getAuth() {
        return auth;
    }

    /* (non-Javadoc)
     * @see goldengate.common.file.SessionInterface#getBlockSize()
     */
    @Override
    public int getBlockSize() {
        return Configuration.BLOCKSIZE;
    }

    /* (non-Javadoc)
     * @see goldengate.common.file.SessionInterface#getDir()
     */
    @Override
    public R66Dir getDir() {
        return dir;
    }

    /* (non-Javadoc)
     * @see goldengate.common.file.SessionInterface#getFileParameter()
     */
    @Override
    public FilesystemBasedFileParameterImpl getFileParameter() {
        return Configuration.getFileParameter();
    }

    /* (non-Javadoc)
     * @see goldengate.common.file.SessionInterface#getRestart()
     */
    @Override
    public R66Restart getRestart() {
        return restart;
    }
    /**
     * This function is called when the Channel is connected
     */
    public void setControlConnected() {
        //dataConn = new FtpDataAsyncConn(this);
        // AuthInterface must be done before FtpFile
        auth = new R66Auth(this);
        dir = new R66Dir(this);
        restart = new R66Restart(this);
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
     * @return True if the Channel is ready to accept command
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
     * @return the localServerHandler
     */
    public LocalServerHandler getLocalServerHandler() {
        return localServerHandler;
    }
    
}
