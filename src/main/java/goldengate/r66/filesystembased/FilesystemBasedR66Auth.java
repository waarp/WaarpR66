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
package goldengate.r66.filesystembased;

import java.io.File;

import goldengate.common.command.NextCommandReply;
import goldengate.common.command.ReplyCode;
import goldengate.common.command.exception.Reply421Exception;
import goldengate.common.command.exception.Reply502Exception;
import goldengate.common.command.exception.Reply530Exception;
import goldengate.common.file.DirInterface;
import goldengate.common.file.SessionInterface;
import goldengate.common.file.filesystembased.FilesystemBasedAuthImpl;
import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;
import goldengate.r66.core.auth.R66Auth;
import goldengate.r66.core.command.R66CommandCode;

/**
 * @author Frederic Bregier
 *
 */
public class FilesystemBasedR66Auth extends FilesystemBasedAuthImpl {
    /**
     * Internal Logger
     */
    private static final GgInternalLogger logger = GgInternalLoggerFactory
            .getLogger(FilesystemBasedR66Auth.class);

    /**
     * Current authentication
     */
    private R66Auth currentAuth = null;
    
    /**
     * @param session
     */
    public FilesystemBasedR66Auth(SessionInterface session) {
        super(session);
    }

    /* (non-Javadoc)
     * @see goldengate.common.file.filesystembased.FilesystemBasedAuthImpl#businessClean()
     */
    @Override
    protected void businessClean() {
        currentAuth = null;
    }

    /* (non-Javadoc)
     * @see goldengate.common.file.filesystembased.FilesystemBasedAuthImpl#getBaseDirectory()
     */
    @Override
    protected String getBaseDirectory() {
        return null; //XXX TODO FIXME ((FtpSession) getSession()).getConfiguration().getBaseDirectory();
    }

    /* (non-Javadoc)
     * @see goldengate.common.file.filesystembased.FilesystemBasedAuthImpl#setBusinessAccount(java.lang.String)
     */
    @Override
    protected NextCommandReply setBusinessAccount(String account)
            throws Reply421Exception, Reply530Exception, Reply502Exception {
        if (currentAuth == null) {
            throw new Reply530Exception("ACCT needs a USER first");
        }
        if (currentAuth.isAccountValid(account)) {
            logger.debug("Account: {}", account);
            setIsIdentified(true);
            logger.warn("User {} is authentified with account {}", user,
                    account);
            return new NextCommandReply(R66CommandCode.NOOP,
                    ReplyCode.REPLY_230_USER_LOGGED_IN, null);
        }
        throw new Reply530Exception("Account is not valid");
    }

    /* (non-Javadoc)
     * @see goldengate.common.file.filesystembased.FilesystemBasedAuthImpl#setBusinessPassword(java.lang.String)
     */
    @Override
    protected NextCommandReply setBusinessPassword(String password)
            throws Reply421Exception, Reply530Exception {
        if (currentAuth == null) {
            setIsIdentified(false);
            throw new Reply530Exception("PASS needs a USER first");
        }
        if (currentAuth.isPasswordValid(password)) {
            if (user.equals("test")) {
                logger.debug("User test");
                try {
                    return setAccount("test");
                } catch (Reply502Exception e) {
                }
            }
            return new NextCommandReply(R66CommandCode.ACCT,
                    ReplyCode.REPLY_332_NEED_ACCOUNT_FOR_LOGIN, null);
        }
        throw new Reply530Exception("Password is not valid");

    }

    /* (non-Javadoc)
     * @see goldengate.common.file.filesystembased.FilesystemBasedAuthImpl#setBusinessRootFromAuth()
     */
    @Override
    protected String setBusinessRootFromAuth() throws Reply421Exception {
        String path = null;
        if (account == null) {
            path = DirInterface.SEPARATOR + user;
        } else {
            path = DirInterface.SEPARATOR + user + DirInterface.SEPARATOR +
                    account;
        }
        String fullpath = getAbsolutePath(path);
        File file = new File(fullpath);
        if (!file.isDirectory()) {
            throw new Reply421Exception("Filesystem not ready");
        }
        return path;
    }

    /* (non-Javadoc)
     * @see goldengate.common.file.filesystembased.FilesystemBasedAuthImpl#setBusinessUser(java.lang.String)
     */
    @Override
    protected NextCommandReply setBusinessUser(String user)
            throws Reply421Exception, Reply530Exception {
        R66Auth auth = null;//XXX TODO FIXME((FileBasedConfiguration) ((FtpSession) getSession()).getConfiguration()).getSimpleAuth(user);
        if (auth == null) {
            setIsIdentified(false);
            currentAuth = null;
            throw new Reply530Exception("User name not allowed");
        }
        currentAuth = auth;
        logger.debug("User: {}", user);
        return new NextCommandReply(R66CommandCode.PASS,
                ReplyCode.REPLY_331_USER_NAME_OKAY_NEED_PASSWORD, null);
    }

    /* (non-Javadoc)
     * @see goldengate.common.file.AuthInterface#isAdmin()
     */
    @Override
    public boolean isAdmin() {
        return currentAuth.isAdmin;
    }

    /* (non-Javadoc)
     * @see goldengate.common.file.AuthInterface#isBusinessPathValid(java.lang.String)
     */
    @Override
    public boolean isBusinessPathValid(String newPath) {
        if (newPath == null) {
            return false;
        }
        return newPath.startsWith(getBusinessPath());
    }

}
