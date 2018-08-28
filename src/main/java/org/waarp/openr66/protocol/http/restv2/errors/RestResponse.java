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

package org.waarp.openr66.protocol.http.restv2.errors;

import org.waarp.openr66.protocol.http.restv2.data.RestHost;
import org.waarp.openr66.protocol.http.restv2.data.RestHostConfig;
import org.waarp.openr66.protocol.http.restv2.data.RestLimit;
import org.waarp.openr66.protocol.http.restv2.data.RestRule;
import org.waarp.openr66.protocol.http.restv2.data.RestTransferInitializer;

import java.util.Locale;
import java.util.ResourceBundle;

public abstract class RestResponse {

    public static ResourceBundle restMessages = ResourceBundle.getBundle("RestMessages", Locale.ENGLISH);

    protected static String getClassName(Class c) {
        if(c == RestHost.class)                      return RestResponse.restMessages.getString("Data.Host");
        else if(c == RestHostConfig.class)           return RestResponse.restMessages.getString("Data.HostConfig");
        else if(c == RestLimit.class)                return RestResponse.restMessages.getString("Data.Limit");
        else if(c == RestRule.class)                 return RestResponse.restMessages.getString("Data.Rule");
        else if(c == RestTransferInitializer.class)  return RestResponse.restMessages.getString("Data.Transfer");
        else                                         return c.getSimpleName();

    }

    public abstract String toJson();
}