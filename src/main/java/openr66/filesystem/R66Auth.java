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

import goldengate.common.command.NextCommandReply;
import goldengate.common.command.exception.Reply421Exception;
import goldengate.common.command.exception.Reply502Exception;
import goldengate.common.command.exception.Reply530Exception;
import goldengate.common.file.filesystembased.FilesystemBasedAuthImpl;

/**
 * @author frederic bregier
 *
 */
public class R66Auth extends FilesystemBasedAuthImpl {

    /**
     * @param session
     */
    public R66Auth(R66Session session) {
        super(session);
        // TODO Auto-generated constructor stub
    }

    /* (non-Javadoc)
     * @see goldengate.common.file.filesystembased.FilesystemBasedAuthImpl#businessClean()
     */
    @Override
    protected void businessClean() {
        // TODO Auto-generated method stub
        
    }

    /* (non-Javadoc)
     * @see goldengate.common.file.filesystembased.FilesystemBasedAuthImpl#getBaseDirectory()
     */
    @Override
    protected String getBaseDirectory() {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see goldengate.common.file.filesystembased.FilesystemBasedAuthImpl#setBusinessAccount(java.lang.String)
     */
    @Override
    protected NextCommandReply setBusinessAccount(String arg0)
            throws Reply421Exception, Reply530Exception, Reply502Exception {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see goldengate.common.file.filesystembased.FilesystemBasedAuthImpl#setBusinessPassword(java.lang.String)
     */
    @Override
    protected NextCommandReply setBusinessPassword(String arg0)
            throws Reply421Exception, Reply530Exception {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see goldengate.common.file.filesystembased.FilesystemBasedAuthImpl#setBusinessRootFromAuth()
     */
    @Override
    protected String setBusinessRootFromAuth() throws Reply421Exception {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see goldengate.common.file.filesystembased.FilesystemBasedAuthImpl#setBusinessUser(java.lang.String)
     */
    @Override
    protected NextCommandReply setBusinessUser(String arg0)
            throws Reply421Exception, Reply530Exception {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see goldengate.common.file.AuthInterface#isAdmin()
     */
    @Override
    public boolean isAdmin() {
        // TODO Auto-generated method stub
        return false;
    }

    /* (non-Javadoc)
     * @see goldengate.common.file.AuthInterface#isBusinessPathValid(java.lang.String)
     */
    @Override
    public boolean isBusinessPathValid(String arg0) {
        // TODO Auto-generated method stub
        return false;
    }

}
