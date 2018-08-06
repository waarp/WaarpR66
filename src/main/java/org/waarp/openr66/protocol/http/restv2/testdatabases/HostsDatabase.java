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

import org.waarp.openr66.protocol.http.restv2.data.Host;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Deprecated
public final class HostsDatabase {
    
    public static List<Host> hostsDb = initHostsDb();
    
    
    private static List<Host> initHostsDb() {
        List<Host> hosts = new ArrayList<Host>();

        Host host1 = new Host();
        host1.hostID = "server1";
        host1.address = "192.168.1.1";
        host1.port = 6666;
        host1.hostKey = "abcd";
        host1.isSSL = false;
        host1.adminRole = true;
        host1.isClient = false;
        host1.isActive = true;
        host1.isProxyfied = false;

        Host host2 = new Host();
        host2.hostID = "server2";
        host2.address = "example.com";
        host2.port = 6667;
        host2.hostKey = "1234";
        host2.isSSL = true;
        host2.adminRole = false;
        host2.isClient = true;
        host2.isActive = false;
        host2.isProxyfied = true;

        hosts.add(host1);
        hosts.add(host2);
        return hosts;
    }
    
    public static List<Host> selectFilter(int limit, int offset, Host.Order order, String address, Boolean isSSL,
                                           Boolean isActive) {
        List<Host> results = new ArrayList<Host>();
        for(Host host : hostsDb) {
            if((address == null || address.equals(host.address)) &&
                    (isSSL == null || isSSL == host.isSSL) &&
                    (isActive == null || isActive == host.isActive)) {
                results.add(host);
            }
        }

        Collections.sort(results, order.comparator);

        List<Host> answers = new ArrayList<Host>();
        for (int i = offset; (i < offset + limit && i < results.size()); i++) {
            answers.add(results.get(i));
        }

        return answers;
    }

    public static Host select(String id) {
        for(Host host : hostsDb) {
            if(host.hostID.equals(id)) {
                return host;
            }
        }
        return null;
    }

    public static boolean insert(Host host) {
        if(select(host.hostID) != null) {
            return false;
        } else {
            hostsDb.add(host);
            return true;
        }
    }

    public static boolean delete(String id) {
        Host deleted = select(id);
        if(deleted == null) {
            return false;
        } else {
            hostsDb.remove(deleted);
            return true;
        }
    }

    public static boolean modify(String id, Host host) {
        Host old = select(id);
        if(old == null) {
            return false;
        } else {
            hostsDb.remove(old);
            hostsDb.add(host);
            return true;
        }
    }
}
