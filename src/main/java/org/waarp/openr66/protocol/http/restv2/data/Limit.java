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


import com.fasterxml.jackson.annotation.JsonIgnore;

import static org.waarp.openr66.protocol.http.restv2.RestUtils.HOST_ID;

/** Bandwidth limits POJO for Rest HTTP support for R66. */
@SuppressWarnings({"unused", "WeakerAccess"})
public class Limit {

    /** Ths id of the host applying these limits. */
    @JsonIgnore
    public final String hostID = HOST_ID;

    /** The host's global upload bandwidth limit in B/s. Set to 0 for no limit. Cannot be negative. */
    public Integer upGlobalLimit;

    /** The host's global download bandwidth limit in B/s. Set to 0 for no limit. Cannot be negative. */
    public Integer downGlobalLimit;

    /** The upload bandwidth limit per transfer in B/s. Set to 0 for no limit. Cannot be negative. */
    public Integer upSessionLimit;

    /** The download bandwidth limit per transfer in B/s. Set to 0 for no limit. Cannot be negative. */
    public Integer downSessionLimit;

    /**
     * The maximum delay (in ms) between 2 checks of the current bandwidth usage. Set to 0 for no checks. Cannot be
     * negative.
     */
    public Integer delayLimit;

    /** Initialize all missing optional fields with their default values. */
    public void defaultValues() {
        if(this.upGlobalLimit == null)      this.upGlobalLimit = 0;
        if(this.downGlobalLimit == null)    this.downGlobalLimit = 0;
        if(this.upSessionLimit == null)     this.upSessionLimit = 0;
        if(this.downSessionLimit == null)   this.downSessionLimit = 0;
        if(this.delayLimit == null)         this.delayLimit = 0;
    }
}
