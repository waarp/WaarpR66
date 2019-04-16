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

package org.waarp.openr66.protocol.http.restv2.utils;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;
import org.waarp.openr66.protocol.http.restv2.errors.UserErrorException;
import org.waarp.openr66.protocol.http.restv2.errors.Errors;

import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.NotSupportedException;
import java.io.IOException;

import static javax.ws.rs.core.HttpHeaders.CONTENT_TYPE;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static org.waarp.openr66.protocol.http.restv2.RestConstants.UTF8_CHARSET;
import static org.waarp.openr66.protocol.http.restv2.errors.Errors.MALFORMED_JSON;
import static org.waarp.openr66.protocol.http.restv2.errors.Errors.MISSING_BODY;

/** A series of utility methods for serializing and deserializing JSON. */
public final class JsonUtils {

    /** Prevents the default constructor from being called. */
    private JsonUtils() throws InstantiationException {
        throw new InstantiationException(this.getClass().getName() +
                " cannot be instantiated.");
    }

    private static final ObjectMapper MAPPER;

    static {
        MAPPER = new ObjectMapper();
        MAPPER.enable(DeserializationFeature.USE_JAVA_ARRAY_FOR_JSON_ARRAY);
        MAPPER.enable(DeserializationFeature.FAIL_ON_NUMBERS_FOR_ENUMS);
        MAPPER.enable(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES);
        MAPPER.enable(DeserializationFeature.ACCEPT_EMPTY_ARRAY_AS_NULL_OBJECT);
        MAPPER.enable(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT);
        MAPPER.enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION);
        MAPPER.enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES);
        MAPPER.enable(DeserializationFeature.FAIL_ON_INVALID_SUBTYPE);
        MAPPER.enable(DeserializationFeature.FAIL_ON_READING_DUP_TREE_KEY);
    }

    public static String nodeToString(ObjectNode object) {
        try {
            return MAPPER.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            throw new InternalServerErrorException(e);
        }
    }

    public static ObjectNode deserializeRequest(HttpRequest request) {
        if (!(request instanceof FullHttpRequest)) {
            throw new UserErrorException(MISSING_BODY());
        }

        try {
            String body = ((FullHttpRequest) request).content()
                    .toString(UTF8_CHARSET);

            JsonNode node = MAPPER.readTree(body);

            if (node.isObject()) {
                return (ObjectNode) node;
            } else {
                throw new UserErrorException(MALFORMED_JSON(0, 0,
                        "The root JSON element is not an object"));
            }
        } catch (JsonParseException e) {
            String contentType = request.headers().get(CONTENT_TYPE);
            if (contentType == null || contentType.isEmpty()) {
                throw new NotSupportedException(APPLICATION_JSON);
            } else {
                throw new UserErrorException(MALFORMED_JSON(e.getLocation().getLineNr(),
                        e.getLocation().getColumnNr(), e.getOriginalMessage()));
            }
        } catch (JsonMappingException e) {
            JsonParser parser = (JsonParser) e.getProcessor();
            try {
                String field = parser.getCurrentName();
                if (field == null) {
                    throw new UserErrorException(Errors.MISSING_BODY());
                } else {
                    throw new UserErrorException(Errors.DUPLICATE_KEY(field));
                }
            } catch (IOException ex) {
                throw new InternalServerErrorException(e);
            }
        } catch (IOException e) {
            throw new InternalServerErrorException(e);
        }
    }
}
