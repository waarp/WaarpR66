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

import java.io.File;
import java.io.IOException;

import goldengate.common.command.exception.CommandAbstractException;
import goldengate.common.command.exception.Reply550Exception;
import goldengate.common.file.filesystembased.FilesystemBasedDirImpl;
import goldengate.common.file.filesystembased.FilesystemBasedOptsMLSxImpl;

/**
 * @author frederic bregier
 *
 */
public class R66Dir extends FilesystemBasedDirImpl {

    /**
     * @param session
     */
    public R66Dir(R66Session session) {
        super(session, new FilesystemBasedOptsMLSxImpl());
    }

    /*
     * (non-Javadoc)
     *
     * @see goldengate.common.file.DirInterface#newFile(java.lang.String,
     * boolean)
     */
    @Override
    public R66File newFile(String path, boolean append)
            throws CommandAbstractException {
        return new R66File((R66Session) getSession(), this, path, append);
    }

    /**
     * Same as setUnique() except that File will be prefixed by filename
     * @param filename
     * @return the R66File with a unique filename and a temporary extension
     * @throws CommandAbstractException
     */
    public synchronized R66File setUniqueFile(String filename) throws CommandAbstractException {
        checkIdentify();
        File file = null;
        String prename = System.currentTimeMillis()+"_";
        try {
            file = File.createTempFile(prename,
                    "_"+filename+".stou", getFileFromPath(currentDir));
        } catch (IOException e) {
            throw new Reply550Exception("Cannot create unique file");
        }
        String currentFile = getRelativePath(file);
        return newFile(normalizePath(currentFile), false);
    }
    /**
     *
     * @param file
     * @return the final unique basename without the temporary extension
     */
    public String getFinalUniqueFilename(R66File file) {
        String finalpath = file.getBasename();
        int pos = finalpath.lastIndexOf(".stou");
        if (pos > 0) {
            finalpath = finalpath.substring(0, pos);
        }
        return finalpath;
    }
    public String toString() {
        return "Dir: "+this.currentDir;
    }
}
