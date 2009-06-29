/**
 * Copyright 2009, Frederic Bregier, and individual contributors by the @author
 * tags. See the COPYRIGHT.txt in the distribution for a full listing of
 * individual contributors. This is free software; you can redistribute it
 * and/or modify it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 3.0 of the License,
 * or (at your option) any later version. This software is distributed in the
 * hope that it will be useful, but WITHOUT ANY WARRANTY; without even the
 * implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See
 * the GNU Lesser General Public License for more details. You should have
 * received a copy of the GNU Lesser General Public License along with this
 * software; if not, write to the Free Software Foundation, Inc., 51 Franklin
 * St, Fifth Floor, Boston, MA 02110-1301 USA, or see the FSF site:
 * http://www.fsf.org.
 */
package goldengate.r66.filesystembased;

import goldengate.common.command.exception.CommandAbstractException;
import goldengate.common.file.SessionInterface;
import goldengate.common.file.filesystembased.FilesystemBasedRestartImpl;

/**
 * @author Frederic Bregier
 */
public class FilesystemBasedR66Restart extends FilesystemBasedRestartImpl {

    /**
     * @param session
     */
    public FilesystemBasedR66Restart(SessionInterface session) {
        super(session);
    }

    /*
     * (non-Javadoc)
     * @see goldengate.common.file.Restart#restartMarker(java.lang.String)
     */
    @Override
    public boolean restartMarker(String marker) throws CommandAbstractException {
        // XXX TODO FIXME
        return false;
        /*
         * if (dataConn.getStructure() == TransferStructure.FILE &&
         * dataConn.getMode() == TransferMode.STREAM && dataConn.getType() !=
         * TransferType.LENGTH) { long newposition = 0; try { newposition =
         * Long.parseLong(marker); } catch (NumberFormatException e) { throw new
         * Reply502Exception( "Marker must be length in byte as a position"); }
         * position = newposition; setSet(true); return true; } throw new
         * Reply502Exception(
         * "Marker not implemented for such Mode, Type and Structure");
         */
    }

}
