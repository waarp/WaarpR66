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

package org.waarp.openr66.protocol.http.restv2.converters;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.waarp.openr66.pojo.Limit;
import org.waarp.openr66.protocol.http.restv2.errors.UserErrorException;
import org.waarp.openr66.protocol.http.restv2.errors.Error;
import org.waarp.openr66.protocol.http.restv2.errors.Errors;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.waarp.openr66.protocol.http.restv2.RestConstants.SERVER_NAME;
import static org.waarp.openr66.protocol.http.restv2.converters.LimitsConverter.FieldNames.*;
import static org.waarp.openr66.protocol.http.restv2.errors.Errors.ILLEGAL_PARAMETER_VALUE;

/**
 * A collection of utility methods to convert {@link Limit} POJOs
 * to {@link ObjectNode} and vice-versa.
 */
public final class LimitsConverter {

    @SuppressWarnings("unused")
    public static final class FieldNames {
        public static final String WRITE_GLOBAL_LIMIT = "upGlobalLimit";
        public static final String READ_GLOBAL_LIMIT = "downGlobalLimit";
        public static final String WRITE_SESSION_LIMIT = "upSessionLimit";
        public static final String READ_SESSION_LIMIT = "downSessionLimit";
        public static final String DELAY_LIMIT = "delayLimit";
    }


    public static ObjectNode limitToNode(Limit limits) {
        ObjectNode node = new ObjectNode(JsonNodeFactory.instance);
        node.put(READ_GLOBAL_LIMIT, limits.getReadGlobalLimit());
        node.put(WRITE_GLOBAL_LIMIT,limits.getWriteGlobalLimit());
        node.put(READ_SESSION_LIMIT, limits.getReadSessionLimit());
        node.put(WRITE_SESSION_LIMIT, limits.getWriteSessionLimit());
        node.put(DELAY_LIMIT, limits.getDelayLimit());

        return node;
    }

    public static Limit nodeToNewLimit(ObjectNode object) {
        Limit defaultLimits = new Limit(SERVER_NAME, 0, 0, 0, 0, 0);
        return parseNode(object, defaultLimits);
    }

    public static Limit nodeToUpdatedLimit(ObjectNode object, Limit oldLimits) {
        return parseNode(object, oldLimits);
    }

    private static Limit parseNode(ObjectNode object, Limit limits) {
        List<Error> errors = new ArrayList<Error>();

        while (object.fields().hasNext()) {
            Map.Entry<String, JsonNode> field = object.fields().next();
            String name = field.getKey();
            JsonNode value = field.getValue();

            if (name.equalsIgnoreCase(READ_GLOBAL_LIMIT)) {
                if (value.canConvertToLong() && value.asLong() >= 0) {
                    limits.setReadGlobalLimit(value.asLong());
                } else {
                    errors.add(ILLEGAL_PARAMETER_VALUE(name, value.toString()));
                }
            }
            else if (name.equalsIgnoreCase(WRITE_GLOBAL_LIMIT)) {
                if (value.canConvertToLong() && value.asLong() >= 0) {
                    limits.setWriteGlobalLimit(value.asLong());
                } else {
                    errors.add(ILLEGAL_PARAMETER_VALUE(name, value.toString()));
                }
            }
            else if (name.equalsIgnoreCase(READ_SESSION_LIMIT)) {
                if (value.canConvertToLong() && value.asLong() >= 0) {
                    limits.setReadSessionLimit(value.asLong());
                } else {
                    errors.add(ILLEGAL_PARAMETER_VALUE(name, value.toString()));
                }
            }
            else if (name.equalsIgnoreCase(WRITE_SESSION_LIMIT)) {
                if (value.canConvertToLong() && value.asLong() >= 0) {
                    limits.setWriteSessionLimit(value.asLong());
                } else {
                    errors.add(ILLEGAL_PARAMETER_VALUE(name, value.toString()));
                }
            }
            else if (name.equalsIgnoreCase(DELAY_LIMIT)) {
                if (value.canConvertToLong() && value.asLong() >= 0) {
                    limits.setDelayLimit(value.asLong());
                } else {
                    errors.add(ILLEGAL_PARAMETER_VALUE(name, value.toString()));
                }
            }
            else {
                errors.add(Errors.UNKNOWN_FIELD(name));
            }
        }

        if (errors.isEmpty()) {
            return limits;
        } else {
            throw new UserErrorException(errors);
        }
    }


    /** Prevents the default constructor from being called. */
    private LimitsConverter() throws InstantiationException {
        throw new InstantiationException(this.getClass().getName() +
                " cannot be instantiated.");
    }
}
