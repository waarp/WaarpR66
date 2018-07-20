/*
 * This file is part of Waarp Project (named also Waarp or GG).
 *
 * Copyright 2009, Waarp SAS, and individual contributors by the @author
 * tags. See the COPYRIGHT.txt in the distribution for a full listing of
 * individual contributors.
 *
 * All Waarp Project is free software: you can redistribute it and/or
 * modify it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your
 * option) any later version.
 *
 * Waarp is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
 * A PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * Waarp . If not, see <http://www.gnu.org/licenses/>.
 */

package org.waarp.openr66.protocol.http.restv2.data;

import org.waarp.openr66.protocol.http.restv2.exception.OpenR66RestBadRequestException;

public abstract class Filter {
    /** The maximum number of entry allowed to be send in the response. 20 by default. */
    public Integer limit = 20;

    /** The starting number from which to start counting the `limit` entries to send back. */
    public Integer offset = 0;

    /**
     * Checks if the 'limit' and 'offset' fields are valid (not negative). Throws an exception if they are not.
     */
    public void check() throws OpenR66RestBadRequestException {
        if (this.limit < 0) {
            throw OpenR66RestBadRequestException.negative("limit");
        } else if (this.offset < 0) {
            throw OpenR66RestBadRequestException.negative("offset");
        }
    }
}
