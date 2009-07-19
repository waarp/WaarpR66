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
package openr66.authentication;

import java.util.Arrays;

/**
 * @author Frederic Bregier
 *
 */
public class R66SimpleAuth {
    /**
     * Host ID
     */
    public String hostId = null;

    /**
     * Key
     */
    public byte[] key = null;

    /**
     * Is the current host Id an administrator (which can shutdown or change
     * bandwidth limitation)
     */
    public boolean isAdmin = false;

    /**
     * @param hostId
     * @param key
     */
    public R66SimpleAuth(String hostId, byte[] key) {
        this.hostId = hostId;
        this.key = key;
    }

    /**
     * Is the given key a valid one
     *
     * @param newkey
     * @return True if the key is valid (or any key is valid)
     */
    public boolean isKeyValid(byte[] newkey) {
        // FIXME is it valid to not have a key ?
        if (key == null) {
            return true;
        }
        if (newkey == null) {
            return false;
        }
        return Arrays.equals(key, newkey);
    }

    /**
     *
     * @param isAdmin
     *            True if the user should be an administrator
     */
    public void setAdmin(boolean isAdmin) {
        this.isAdmin = isAdmin;
    }

    @Override
    public String toString() {
        return "SimpleAuth: " + hostId + " " + isAdmin;
    }
}
