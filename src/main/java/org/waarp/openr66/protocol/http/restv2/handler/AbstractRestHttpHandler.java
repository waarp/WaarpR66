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

package org.waarp.openr66.protocol.http.restv2.handler;

import co.cask.http.AbstractHttpHandler;
import io.netty.handler.codec.http.HttpMethod;
import org.waarp.common.role.RoleDefault;
import org.waarp.openr66.context.authentication.R66Auth;

public abstract class AbstractRestHttpHandler extends AbstractHttpHandler {

    private final RoleDefault.ROLE writeRole;

    protected AbstractRestHttpHandler(RoleDefault.ROLE writeRole) {
        this.writeRole = writeRole;
    }

    public boolean isAuthorized(HttpMethod method, R66Auth auth, String uri) {
        if(method == HttpMethod.OPTIONS) {
            return true;
        } else if(method == HttpMethod.GET) {
            return auth.getRole().hasReadOnly();
        } else {
            return auth.isValidRole(writeRole);
        }
    }
}
