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

package org.waarp.openr66.protocol.http.restv2.testdatabases;

import org.waarp.openr66.protocol.http.restv2.data.hostconfigs.HostConfig;

import java.util.ArrayList;
import java.util.List;

@Deprecated
public final class HostconfigDatabase {
    @Deprecated
    public static List<HostConfig> configDb = initConfigDb();

    @Deprecated
    private static List<HostConfig> initConfigDb() {
        List<HostConfig> configs = new ArrayList<HostConfig>();
        HostConfig config1 = new HostConfig();
        config1.aliases = new String[]{"host1"};
        config1.roles = new HostConfig.Role[]{
                new HostConfig.Role("server1", new HostConfig.RoleType[]{HostConfig.RoleType.fullAdmin}),
                new HostConfig.Role("server2", new HostConfig.RoleType[]{HostConfig.RoleType.configAdmin})
        };
        config1.business = new String[]{"server2"};
        config1.others = "<root><version>1.0.0</version></root>";

        configs.add(config1);
        return configs;
    }
}
