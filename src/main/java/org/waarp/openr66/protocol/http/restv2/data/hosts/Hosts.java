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

package org.waarp.openr66.protocol.http.restv2.data.hosts;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.waarp.openr66.protocol.http.restv2.RestUtils;
import org.waarp.openr66.protocol.http.restv2.database.HostsDatabase;
import org.waarp.openr66.protocol.http.restv2.exception.ImpossibleException;
import org.waarp.openr66.protocol.http.restv2.exception.OpenR66RestBadRequestException;
import org.waarp.openr66.protocol.http.restv2.exception.OpenR66RestIdNotFoundException;
import org.waarp.openr66.protocol.http.restv2.exception.OpenR66RestInternalServerException;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** This class consists exclusively of static methods that operate on or return hosts. */
public final class Hosts {
    /**
     * Returns the list of all hosts in the database that fit the filters passed as arguments.
     *
     * @param filters The different filters used to generate the desired host list.
     * @return A map entry associating the total number of valid entries and the list of entries that will actually be
     * returned in the response.
     * @throws OpenR66RestBadRequestException Thrown if one of the filters is invalid.
     */
    public static Map.Entry<Integer, List<Host>> filterHosts(HostFilter filters)
            throws OpenR66RestBadRequestException {

        List<Host> results = new ArrayList<Host>();
        for (Host host : HostsDatabase.hostsDb) {
            if ((filters.address == null || host.address.equals(filters.address)) &&
                    (filters.isSSL == null || filters.isSSL.equals(host.isSSL)) &&
                    (filters.isActive == null || filters.isActive.equals(filters.isSSL))) {
                results.add(host);
            }
        }
        Integer total = results.size();
        Collections.sort(results, filters.order.comparator);

        List<Host> answers = new ArrayList<Host>();
        for (int i = filters.offset; (i < filters.offset + filters.limit && i < results.size()); i++) {
            answers.add(results.get(i));
        }

        return new HashMap.SimpleImmutableEntry<Integer, List<Host>>(total, answers);
    }

    /**
     * Adds a host entry to the database if the entry is a valid one.
     *
     * @param host The entry to add to the database.
     * @throws OpenR66RestBadRequestException Thrown if the request is invalid or if a host with the same id already
     *                                        exists in the database
     */
    public static void addHost(Host host) throws OpenR66RestBadRequestException {
        try {
            //check if the host already exists
            loadHost(host.hostID);
            throw OpenR66RestBadRequestException.alreadyExisting("host", host.hostID);
        } catch (OpenR66RestIdNotFoundException valid) {

            for(Field field : Host.class.getFields()) {
                try {
                    Object value = field.get(host);
                    if(RestUtils.isIllegal(value)) {
                        throw OpenR66RestBadRequestException.emptyField(field.getName());
                    }
                } catch (IllegalAccessException e) {
                    throw new ImpossibleException(e);
                }
            }
            HostsDatabase.hostsDb.add(host);
        }
    }

    /**
     * Returns the host entry corresponding to the id passed as argument.
     *
     * @param id The desired host id.
     * @return The corresponding host entry.
     * @throws OpenR66RestIdNotFoundException Thrown if the id does not exist in the database.
     */
    public static Host loadHost(String id) throws OpenR66RestIdNotFoundException {
        //TODO: replace by a real database request
        for (Host host : HostsDatabase.hostsDb) {
            if (host.hostID.equals(id)) {
                return host;
            }
        }

        throw new OpenR66RestIdNotFoundException(
                "{" +
                        "\"userMessage\":\"Not Found\"," +
                        "\"internalMessage\":\"The host of id '" + id + "' does not exist.\"" +
                        "}"
        );
    }

    /**
     * Removes the corresponding host from the database if it exists.
     *
     * @param id The id of the host to delete.
     * @throws OpenR66RestIdNotFoundException Thrown if the host does not exist in the database.
     */
    public static void deleteHost(String id) throws OpenR66RestIdNotFoundException {
        //TODO: replace by a real database request
        Host toDelete = loadHost(id);
        HostsDatabase.hostsDb.remove(toDelete);
    }

    /**
     * Replaces this host entry with the one passed as argument.
     *
     * @param newHost The new entry that replaces this one.
     * @throws OpenR66RestIdNotFoundException Thrown if the host does not exist in the database.
     */
    public static void replace(String id, Host newHost) throws OpenR66RestIdNotFoundException,
            OpenR66RestBadRequestException {
        Host oldHost = loadHost(id);
        for (Field field : Host.class.getFields()) {
            try {
                Object value = field.get(newHost);
                if (RestUtils.isIllegal(value)) {
                    throw OpenR66RestBadRequestException.emptyField(field.getName());
                }
            } catch (IllegalAccessException e) {
                throw new ImpossibleException(e);
            }
        }
        //TODO: replace by a real database request
        HostsDatabase.hostsDb.remove(oldHost);
        HostsDatabase.hostsDb.add(newHost);
    }

    /**
     * Updates a host entry with the one passed as argument.
     *
     * @param newHost The new entry that replaces this one.
     * @throws OpenR66RestIdNotFoundException Thrown if the host does not exist in the database.
     */
    public static void update(String id, Host newHost) throws OpenR66RestIdNotFoundException {
        Host oldHost = loadHost(id);
        for (Field field : Host.class.getFields()) {
            try {
                Object value = field.get(newHost);
                if (RestUtils.isIllegal(value)) {
                    field.set(newHost, field.get(oldHost));
                }
            } catch (IllegalAccessException e) {
                throw new ImpossibleException(e);
            } catch (IllegalArgumentException e) {
                throw new ImpossibleException(e);
            }
        }
        //TODO: replace by a real database request

        HostsDatabase.hostsDb.remove(oldHost);
        HostsDatabase.hostsDb.add(newHost);
    }

    /**
     * Returns the host authentication object as a String usable in a JSON file.
     *
     * @return The host as a String.
     */
    public static String toJsonString(Host host) throws OpenR66RestInternalServerException {
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.writeValueAsString(host);
        } catch (JsonProcessingException e) {
            throw OpenR66RestInternalServerException.jsonProcessing();
        }
    }
}
