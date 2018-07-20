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

package org.waarp.openr66.protocol.http.restv2.database;

import org.waarp.openr66.protocol.http.restv2.data.hosts.Host;

import java.util.ArrayList;
import java.util.List;

public final class HostsDatabase {
    public static List<Host> hostsDb = initHostsDb();

    private static List<Host> initHostsDb() {
        List<Host> hosts = new ArrayList<Host>();

        Host host1 = new Host();
        host1.hostID = "server1";
        host1.address = "192.168.1.1";
        host1.port = 6666;
        host1.hostKey = "azerty";
        host1.isSSL = false;
        host1.adminRole = true;
        host1.isClient = false;
        host1.isActive = true;
        host1.isProxyfied = false;

        Host host2 = new Host();
        host2.hostID = "server2";
        host2.address = "example.com";
        host2.port = 6667;
        host2.hostKey = "azerty";
        host2.isSSL = true;
        host2.adminRole = false;
        host2.isClient = true;
        host2.isActive = false;
        host2.isProxyfied = true;

        hosts.add(host1);
        hosts.add(host2);
        return hosts;
    }
}
