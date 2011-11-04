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
package openr66.context.authentication;

import goldengate.common.command.NextCommandReply;
import goldengate.common.command.exception.Reply421Exception;
import goldengate.common.command.exception.Reply530Exception;
import goldengate.common.database.DbSession;
import goldengate.common.database.exception.GoldenGateDatabaseException;
import goldengate.common.file.DirInterface;
import goldengate.common.file.filesystembased.FilesystemBasedAuthImpl;
import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;

import java.io.File;

import openr66.context.R66Session;
import openr66.database.DbConstant;
import openr66.database.data.DbHostAuth;
import openr66.protocol.configuration.Configuration;

/**
 * @author frederic bregier
 *
 */
public class R66Auth extends FilesystemBasedAuthImpl {
    /**
     * Internal Logger
     */
    private static final GgInternalLogger logger = GgInternalLoggerFactory
            .getLogger(R66Auth.class);

    /**
     * Current authentication
     */
    private DbHostAuth currentAuth = null;
    /**
     * is Admin role
     */
    private boolean isAdmin = false;
    /**
     * @param session
     */
    public R66Auth(R66Session session) {
        super(session);
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * goldengate.common.file.filesystembased.FilesystemBasedAuthImpl#businessClean
     * ()
     */
    @Override
    protected void businessClean() {
        currentAuth = null;
        isAdmin = false;
    }

    /*
     * (non-Javadoc)
     *
     * @seegoldengate.common.file.filesystembased.FilesystemBasedAuthImpl#
     * getBaseDirectory()
     */
    @Override
    public String getBaseDirectory() {
        return Configuration.configuration.baseDirectory;
    }

    /*
     * (non-Javadoc)
     *
     * @seegoldengate.common.file.filesystembased.FilesystemBasedAuthImpl#
     * setBusinessPassword(java.lang.String)
     */
    @Override
    protected NextCommandReply setBusinessPassword(String arg0)
            throws Reply421Exception, Reply530Exception {
        throw new Reply421Exception("Command not valid");
    }

    /**
     * @param dbSession
     * @param hostId
     * @param arg0
     * @return True if the connection is OK (authentication is OK)
     * @throws Reply530Exception
     *             if the authentication is wrong
     * @throws Reply421Exception
     *             If the service is not available
     */
    public boolean connection(DbSession dbSession, String hostId, byte[] arg0)
            throws Reply530Exception, Reply421Exception {
        DbHostAuth auth = R66Auth
                .getServerAuth(dbSession, hostId);
        if (auth == null) {
            logger.error("Cannot find authentication for "+hostId);
            setIsIdentified(false);
            currentAuth = null;
            throw new Reply530Exception("HostId not allowed");
        }
        currentAuth = auth;
        if (currentAuth.isKeyValid(arg0)) {
            setIsIdentified(true);
            user = hostId;
            setRootFromAuth();
            getSession().getDir().initAfterIdentification();
            isAdmin = currentAuth.isAdminrole();
            return true;
        }
        throw new Reply530Exception("Key is not valid for this HostId");
    }

    /**
     *
     * @param key
     * @return True if the key is valid for the current user
     */
    public boolean isKeyValid(byte[] key) {
        return currentAuth.isKeyValid(key);
    }

    /**
     * Set the root relative Path from current status of Authentication (should
     * be the highest level for the current authentication). If
     * setBusinessRootFromAuth returns null, by default set /user.
     *
     * @exception Reply421Exception
     *                if the business root is not available
     */
    private void setRootFromAuth() throws Reply421Exception {
        rootFromAuth = setBusinessRootFromAuth();
        if (rootFromAuth == null) {
            rootFromAuth = DirInterface.SEPARATOR;
        }
    }

    /*
     * (non-Javadoc)
     *
     * @seegoldengate.common.file.filesystembased.FilesystemBasedAuthImpl#
     * setBusinessRootFromAuth()
     */
    @Override
    protected String setBusinessRootFromAuth() throws Reply421Exception {
        String path = null;
        String fullpath = getAbsolutePath(path);
        File file = new File(fullpath);
        if (!file.isDirectory()) {
            throw new Reply421Exception("Filesystem not ready");
        }
        return path;
    }

    /*
     * (non-Javadoc)
     *
     * @seegoldengate.common.file.filesystembased.FilesystemBasedAuthImpl#
     * setBusinessUser(java.lang.String)
     */
    @Override
    protected NextCommandReply setBusinessUser(String arg0)
            throws Reply421Exception, Reply530Exception {
        throw new Reply421Exception("Command not valid");
    }

    /*
     * (non-Javadoc)
     *
     * @see goldengate.common.file.AuthInterface#isAdmin()
     */
    @Override
    public boolean isAdmin() {
        return isAdmin;
    }
    /**
     *
     * @return True if the associated host is using SSL
     */
    public boolean isSsl() {
        return currentAuth.isSsl();
    }
    /*
     * (non-Javadoc)
     *
     * @see
     * goldengate.common.file.AuthInterface#isBusinessPathValid(java.lang.String
     * )
     */
    @Override
    public boolean isBusinessPathValid(String newPath) {
        if (newPath == null) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "Auth:" +isIdentified+" "+
                (currentAuth != null? currentAuth.toString()
                        : "no Internal Auth");
    }

    /**
     * @param dbSession
     * @param server
     * @return the SimpleAuth if any for this user
     */
    public static DbHostAuth getServerAuth(DbSession dbSession, String server) {
        DbHostAuth auth = null;
        try {
            auth = new DbHostAuth(dbSession, server);
        } catch (GoldenGateDatabaseException e) {
            logger.warn("Cannot find the authentication", e);
            return null;
        }
        return auth;
    }

    /**
     * Special Authentication for local execution
     * @param isSSL
     * @param hostid
     */
    public void specialNoSessionAuth(boolean isSSL, String hostid) {
        this.isIdentified = true;
        DbHostAuth auth = R66Auth.getServerAuth(DbConstant.admin.session,
                    hostid);
        currentAuth = auth;
        setIsIdentified(true);
        user = auth.getHostid();
        try {
            setRootFromAuth();
        } catch (Reply421Exception e) {
        }
        getSession().getDir().initAfterIdentification();
        isAdmin = isSSL;
        if (isSSL) {
            this.user = Configuration.configuration.ADMINNAME;
        }
    }
}
