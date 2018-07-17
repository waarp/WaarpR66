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

package org.waarp.openr66.protocol.http.restv2.data.limits;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.waarp.openr66.protocol.http.restv2.exception.OpenR66RestBadRequestException;
import org.waarp.openr66.protocol.http.restv2.exception.OpenR66RestIdNotFoundException;
import org.waarp.openr66.protocol.http.restv2.exception.OpenR66RestInternalServerException;
import org.waarp.openr66.protocol.http.restv2.test.TestLimit;

import java.lang.reflect.Field;

/** This class consists exclusively of static methods that operate on or return limits. */
public final class Limits {

    /**
     * Loads and returns the requested host's bandwidth limits of they exist.
     *
     * @param hostID The queried host from which to get the limits.
     * @return The desired limits object.
     * @throws OpenR66RestIdNotFoundException Thrown if the host does not have any bandwidth limits.
     */
    public static Limit loadLimits(String hostID) throws OpenR66RestIdNotFoundException {
        for (Limit limit : TestLimit.limitDb) {
            if (limit.hostID.equals(hostID)) {
                return limit;
            }
        }
        throw new OpenR66RestIdNotFoundException(
                "{" +
                        "\"userMessage\":\"Limits not found\"," +
                        "\"internalMessage\":\"This host does not have any bandwidth limits.\"" +
                        "}"
        );
    }

    /**
     * Checks a host's new bandwidth limit configuration and add them to the database if the parameters are valid.
     *
     * @param limit The new limit to apply to the host.
     * @throws OpenR66RestBadRequestException Thrown if the host already has bandwidth limit in place or if one of the
     *                                        parameters is invalid.
     */
    public static void initLimits(Limit limit) throws OpenR66RestBadRequestException {
        try {
            loadLimits(limit.hostID);
            throw OpenR66RestBadRequestException.alreadyExisting("bandwidth limits");
        } catch (OpenR66RestIdNotFoundException e) {
            if (limit.upGlobalLimit >= 0 && limit.downGlobalLimit >= 0 && limit.upSessionLimit >= 0 &&
                    limit.downSessionLimit >= 0 && limit.delayLimit >= 0) {
                //TODO: replace by a real database request
                TestLimit.limitDb.add(limit);
            } else {
                throw new OpenR66RestBadRequestException(
                        "{" +
                                "\"userMessage\":\"Bad Request\"," +
                                "\"internalMessage\":\"A bandwidth limit must be a positive number.\"" +
                                "}"
                );
            }
        }
    }

    /**
     * Removes the queried host's bandwidth limits if they exist.
     *
     * @param id The id of the host whose limits should be removed.
     * @throws OpenR66RestIdNotFoundException Thrown if the host does not have any bandwidth limits to delete.
     */
    public static void deleteLimits(String id) throws OpenR66RestIdNotFoundException {
        //TODO: replace by a real database request
        Limit toDelete = loadLimits(id);
        TestLimit.limitDb.remove(toDelete);
    }

    /**
     * Replaces the bandwidth limits entry with the one passed as parameter if it has one.
     *
     * @param id      The id of the host whose limits should be replaced.
     * @param updated The new bandwidth limits object.
     * @throws OpenR66RestIdNotFoundException Thrown if the host does not have bandwidth limits to replace.
     */
    public static void replace(String id, Limit updated) throws OpenR66RestIdNotFoundException {
        for (Field field : Limit.class.getFields()) {
            try {
                Object value = field.get(updated);
                if (value == null || value.toString().equals("")) {
                    throw OpenR66RestBadRequestException.emptyField(field.getName());
                }
            } catch (IllegalAccessException e) {
                assert false;
            }
        }

        //TODO: delete the old limits from the database and insert the new ones
        Limit old = loadLimits(id);
        TestLimit.limitDb.remove(old);
        TestLimit.limitDb.add(updated);
    }

    /**
     * Updates the bandwidth limits entry with the one passed as parameter if it has one.
     *
     * @param id      The id of the host whose limits should be replaced.
     * @param updated The new bandwidth limits object.
     * @throws OpenR66RestIdNotFoundException Thrown if the host does not have bandwidth limits to replace.
     */
    public static void update(String id, Limit updated) throws OpenR66RestIdNotFoundException {
        Limit old = loadLimits(id);
        for (Field field : updated.getClass().getFields()) {
            try {
                Object value = field.get(updated);
                if (value == null || value.toString().equals("")) {
                    field.set(updated, field.get(old));
                }
            } catch (IllegalAccessException e) {
                assert false;
            } catch (IllegalArgumentException e ) {
                assert false;
            }
        }

        //TODO: delete the old limits from the database and insert the new ones
        TestLimit.limitDb.remove(old);
        TestLimit.limitDb.add(updated);
    }

    /**
     * Returns the limits object as a String usable in a JSON file.
     *
     * @param toString The limit object to convert to JSON.
     * @return The limits as a String.
     * @throws OpenR66RestInternalServerException Thrown if the limit object could not be converted to JSON format.
     */
    public static String toJsonString(Limit toString) throws OpenR66RestInternalServerException {
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.writeValueAsString(toString);
        } catch (JsonProcessingException e) {
            throw OpenR66RestInternalServerException.jsonProcessing();
        }
    }
}
