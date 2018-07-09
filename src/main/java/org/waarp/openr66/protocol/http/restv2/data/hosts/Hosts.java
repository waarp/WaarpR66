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
import org.waarp.openr66.protocol.http.restv2.exception.OpenR66RestBadRequestException;
import org.waarp.openr66.protocol.http.restv2.exception.OpenR66RestEmptyParamException;
import org.waarp.openr66.protocol.http.restv2.exception.OpenR66RestIdNotFoundException;
import org.waarp.openr66.protocol.http.restv2.exception.OpenR66RestInternalServerException;
import org.waarp.openr66.protocol.http.restv2.test.TestHost;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** This class consists exclusively of static methods that operate on or return hosts. */
public final class Hosts {
    /**
     * Extract the parameters Map outputted by the query decoder and creates a Filter object with it.
     *
     * @param params The Map associating the query parameters names with their respecting values as lists of Strings.
     * @return The Filter object representing the filter applied to the database query.
     * @throws OpenR66RestBadRequestException Thrown if one of the parameters has an invalid value or no value at all.
     */
    public static HostFilter extractHostFilter(Map<String, List<String>> params) throws OpenR66RestBadRequestException {
        HostFilter filters = new HostFilter();
        for (Map.Entry<String, List<String>> param : params.entrySet()) {
            String name = param.getKey();
            try {
                List<String> values = param.getValue();
                for (String value : values) {
                    if (value.isEmpty()) {
                        throw new OpenR66RestEmptyParamException();
                    }
                }
                boolean isSingleton = values.size() == 1;

                if (name.equals("limit") && isSingleton) {
                    filters.limit = Integer.valueOf(values.get(0));
                } else if (name.equals("offset") && isSingleton) {
                    filters.offset = Integer.valueOf(values.get(0));
                } else if (name.equals("order") && isSingleton) {
                    filters.order = HostFilter.Order.valueOf(values.get(0));
                } else if (name.equals("address") && isSingleton) {
                    filters.address = values.get(0);
                } else if (name.equals("isSSL") && isSingleton) {
                    if (values.get(0).equalsIgnoreCase("true")) {
                        filters.isSSL = true;
                    } else if (values.get(0).equalsIgnoreCase("false")) {
                        filters.isSSL = false;
                    } else {
                        throw new IllegalArgumentException();
                    }
                } else if (name.equals("isActive") && isSingleton) {
                    if ("true".equalsIgnoreCase(values.get(0))) {
                        filters.isActive = true;
                    } else if ("false".equalsIgnoreCase(values.get(0))) {
                        filters.isActive = false;
                    } else {
                        throw new IllegalArgumentException();
                    }
                } else {
                    throw new OpenR66RestBadRequestException(
                            "{" +
                                    "\"userMessage\":\"Bad Request\"," +
                                    "\"internalMessage\":\"Unknown parameter '" + name + "'.\"" +
                                    "}"
                    );
                }
            } catch (OpenR66RestEmptyParamException e) {
                throw new OpenR66RestBadRequestException(
                        "{" +
                                "\"userMessage\":\"Bad Request\"," +
                                "\"internalMessage\":\"The parameter '" + name + "' is empty.\"" +
                                "}"
                );
            } catch (NumberFormatException e) {
                throw new OpenR66RestBadRequestException(
                        "{" +
                                "\"userMessage\":\"Bad Request\"," +
                                "\"internalMessage\":\"The parameter '" + name + "' was expecting a number.\"" +
                                "}"
                );
            } catch (IllegalArgumentException e) {
                throw new OpenR66RestBadRequestException(
                        "{" +
                                "\"userMessage\":\"Bad Request\"," +
                                "\"internalMessage\":\"The parameter '" + name + "' has an illegal value.\"" +
                                "}"
                );
            }
        }
        return filters;
    }

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
        for (Host host : TestHost.hostsDb) {
            if ((filters.address == null || host.address.equals(filters.address)) &&
                    (filters.isSSL == null || filters.isSSL.equals(host.isSSL)) &&
                    (filters.isActive == null || filters.isActive.equals(filters.isSSL))) {
                results.add(host);
            }
        }
        Integer total = results.size();
        switch (filters.order) {
            case ascHostID:
                Collections.sort(results, new Comparator<Host>() {
                    @Override
                    public int compare(Host t1, Host t2) {
                        return t1.hostID.compareTo(t2.hostID);
                    }
                });
                break;
            case descHostID:
                Collections.sort(results, new Comparator<Host>() {
                    @Override
                    public int compare(Host t1, Host t2) {
                        return -t1.hostID.compareTo(t2.hostID);
                    }
                });
                break;
            case ascAddress:
                Collections.sort(results, new Comparator<Host>() {
                    @Override
                    public int compare(Host t1, Host t2) {
                        return t1.address.compareTo(t2.address);
                    }
                });
                break;
            case descAddress:
                Collections.sort(results, new Comparator<Host>() {
                    @Override
                    public int compare(Host t1, Host t2) {
                        return -t1.address.compareTo(t2.address);
                    }
                });
                break;
        }

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
            throw new OpenR66RestBadRequestException(
                    "{" +
                            "\"userMessage\":\"Bad Request\"," +
                            "\"internalMessage\":\"The requested id already exists in the database.\"" +
                            "}"
            );
        } catch (OpenR66RestIdNotFoundException e) {
            if (host.hostID.equals("")) {
                throw new OpenR66RestBadRequestException(
                        "{" +
                                "\"userMessage\":\"Bad Request\"," +
                                "\"internalMessage\":\"The host id cannot be empty.\"" +
                                "}"
                );
            }
            if (host.port < 1 || host.port > 65535) {
                throw new OpenR66RestBadRequestException(
                        "{" +
                                "\"userMessage\":\"Bad Request\"," +
                                "\"internalMessage\":\"The entered port is not a valid port number.\"" +
                                "}"
                );
            }
            TestHost.hostsDb.add(host);
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
        for (Host host : TestHost.hostsDb) {
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
        TestHost.hostsDb.remove(toDelete);
    }

    /**
     * Replaces this host entry with the one passed as argument.
     *
     * @param newHost The new entry that replaces this one.
     * @throws OpenR66RestIdNotFoundException Thrown if the host does not exist in the database.
     */
    public static void replace(String id, Host newHost) throws OpenR66RestIdNotFoundException {
        //TODO: replace by a real database request
        Host oldHost = loadHost(id);
        TestHost.hostsDb.remove(oldHost);
        TestHost.hostsDb.add(newHost);
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
            throw new OpenR66RestInternalServerException(
                    "{" +
                            "\"userMessage\":\"JSON Processing Error\"," +
                            "\"internalMessage\":\"Could not transform the response into JSON format.\"," +
                            "\"code\":100" +
                            "}"
            );
            //TODO: replace 100 placeholder with the real Json processing error code
        }
    }
}
