/*
 *  This file is part of Waarp Project (named also Waarp or GG).
 *
 *  Copyright 2009, Waarp SAS, and individual contributors by the @author
 *  tags. See the COPYRIGHT.txt in the distribution for a full listing of
 *  individual contributors.
 *
 *  All Waarp Project is free software: you can redistribute it and/or
 *  modify it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or (at your
 *  option) any later version.
 *
 *  Waarp is distributed in the hope that it will be useful, but WITHOUT ANY
 *  WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
 *  A PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License along with
 *  Waarp . If not, see <http://www.gnu.org/licenses/>.
 *
 */

package org.waarp.openr66.protocol.http.restv2.data;


import org.waarp.openr66.pojo.Limit;

import javax.ws.rs.DefaultValue;

import static org.waarp.openr66.protocol.http.restv2.RestConstants.HOST_ID;

/** Bandwidth limits POJO for Rest HTTP support for R66. */
@SuppressWarnings({"unused"})
public class RestLimit {
    /**
     * The host's global upload bandwidth limit in B/s. Set to 0 for no limit.
     * Cannot be negative.
     */
    @Bounds(min = 0, max = Long.MAX_VALUE)
    @DefaultValue("0")
    public Long upGlobalLimit;

    /**
     * The host's global download bandwidth limit in B/s. Set to 0 for no limit.
     * Cannot be negative.
     */
    @Bounds(min = 0, max = Long.MAX_VALUE)
    @DefaultValue("0")
    public Long downGlobalLimit;

    /**
     * The upload bandwidth limit per transfer in B/s. Set to 0 for no limit.
     * Cannot be negative.
     */
    @Bounds(min = 0, max = Long.MAX_VALUE)
    @DefaultValue("0")
    public Long upSessionLimit;

    /**
     * The download bandwidth limit per transfer in B/s. Set to 0 for no limit.
     * Cannot be negative.
     */
    @Bounds(min = 0, max = Long.MAX_VALUE)
    @DefaultValue("0")
    public Long downSessionLimit;

    /**
     * The maximum delay (in ms) between 2 checks of the current bandwidth usage.
     * Set to 0 to skip checks. Cannot be negative.
     */
    @Bounds(min = 0, max = Long.MAX_VALUE)
    @DefaultValue("0")
    public Long delayLimit;


    public RestLimit() {}

    public RestLimit(Limit limit) {
        this.upGlobalLimit = limit.getReadGlobalLimit();
        this.downGlobalLimit = limit.getWriteGlobalLimit();
        this.upSessionLimit = limit.getReadSessionLimit();
        this.downSessionLimit = limit.getWriteSessionLimit();
        this.delayLimit = limit.getDelayLimit();
    }

    public Limit toLimit() {
        return new Limit(HOST_ID, this.delayLimit, this.upGlobalLimit,
                this.downGlobalLimit, this.upSessionLimit, this.downSessionLimit);
    }
}
