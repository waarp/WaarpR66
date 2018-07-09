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

package org.waarp.openr66.protocol.http.restv2.test;

import org.waarp.openr66.protocol.http.restv2.data.hostconfigs.HostConfig;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestHostConfig {
    public static List<HostConfig> configDb = initConfigDb();

    private static List<HostConfig> initConfigDb() {
        List<HostConfig> configs = new ArrayList<HostConfig>();
        HostConfig config1 = new HostConfig();
        config1.hostId = "server1";
        config1.aliases = new ArrayList<String>(Collections.singletonList("host1"));
        config1.roles = new ArrayList<Map.Entry<String, List<HostConfig.Role>>>(
                Collections.singleton(new HashMap.SimpleImmutableEntry<String, List<HostConfig.Role>>("server1",
                        new ArrayList<HostConfig.Role>(Collections.singleton(HostConfig.Role.fullAdmin))
                ))
        );
        config1.business = new ArrayList<String>(Collections.singleton("server2"));

        configs.add(config1);
        return configs;
    }
}
