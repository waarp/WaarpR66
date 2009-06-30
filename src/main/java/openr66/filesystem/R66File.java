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

/**
 * @author frederic bregier
 *
 */
public class R66File extends FilesystemBasedFileImpl {

    /**
     * @param session
     * @param dir
     * @param path
     * @param append
     * @throws CommandAbstractException
     */
    public R66File(R66Session session, R66Dir dir,
            String path, boolean append) throws CommandAbstractException {
        super(session, dir, path, append);
        // TODO Auto-generated constructor stub
    }

    /* (non-Javadoc)
     * @see goldengate.common.file.FileInterface#trueRetrieve()
     */
    @Override
    public void trueRetrieve() {
        // TODO Auto-generated method stub

    }

}
