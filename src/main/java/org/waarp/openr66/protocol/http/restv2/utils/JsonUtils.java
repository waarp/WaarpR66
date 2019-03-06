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
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;
import org.glassfish.jersey.message.internal.MediaTypes;
import org.waarp.openr66.protocol.http.restv2.data.Bounds;
import org.waarp.openr66.protocol.http.restv2.data.ConsistencyCheck;
import org.waarp.openr66.protocol.http.restv2.data.Required;
import org.waarp.openr66.protocol.http.restv2.errors.BadRequestException;
import org.waarp.openr66.protocol.http.restv2.errors.Error;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.NotSupportedException;
import java.io.IOException;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static javax.ws.rs.core.HttpHeaders.CONTENT_TYPE;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON_TYPE;
import static org.waarp.openr66.protocol.http.restv2.errors.ErrorFactory.DUPLICATE_KEY;
import static org.waarp.openr66.protocol.http.restv2.errors.ErrorFactory.ILLEGAL_FIELD_VALUE;
import static org.waarp.openr66.protocol.http.restv2.errors.ErrorFactory.MALFORMED_JSON;
import static org.waarp.openr66.protocol.http.restv2.errors.ErrorFactory.MISSING_BODY;
import static org.waarp.openr66.protocol.http.restv2.errors.ErrorFactory.MISSING_FIELD;
import static org.waarp.openr66.protocol.http.restv2.errors.ErrorFactory.UNKNOWN_FIELD;

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
        MAPPER.enable(DeserializationFeature.FAIL_ON_READING_DUP_TREE_KEY);
        MAPPER.enable(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES);
        MAPPER.enable(DeserializationFeature.ACCEPT_EMPTY_ARRAY_AS_NULL_OBJECT);
        MAPPER.enable(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT);
        MAPPER.enable(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL);
        MAPPER.enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION);
        MAPPER.enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES);
        MAPPER.disable(DeserializationFeature.FAIL_ON_INVALID_SUBTYPE);
    }

    /**
     * This method transforms a request body in JSON format into an object of
     * the class passed as argument. If the {@code initializeFields} argument
     * is true the object missing fields will be initialized with the value
     * specified in their attached {@link DefaultValue} annotation.
     *
     * @param request The http request whose body has to be deserialized.
     * @param c       The class of the object that will be created from the JSON
     *                object.
     * @param initializeFields Specifies if missing fields should be initialized.
     * @param <T>     The generic type of the output object.
     * @return The Java object representing the source JSON object.
     *
     */
    public static <T> T requestToObject(HttpRequest request, Class<T> c,
                                        boolean initializeFields) {

        try {
            if (request instanceof FullHttpRequest) {
                String body = ((FullHttpRequest) request).content().toString(
                        Charset.forName("UTF-8"));

                T bodyObject = MAPPER.readValue(body, c);

                if (initializeFields) {
                    checkEntry(bodyObject, true);
                    return fillEmptyFields(bodyObject);
                } else {
                    checkEntry(bodyObject, false);
                    return bodyObject;
                }
            } else {
                throw new BadRequestException(MISSING_BODY());
            }
        } catch (JsonMappingException e) {
            String field = "";
            try {
                JsonParser parser = (JsonParser) e.getProcessor();
                field = parser.getCurrentName();

                if (e.getPath().isEmpty()) {
                    if (field == null) {
                        throw new BadRequestException(MISSING_BODY());
                    } else {
                        throw new BadRequestException(DUPLICATE_KEY(field));
                    }
                }

                c.getField(field);
                String val = parser.getText();
                throw new BadRequestException(ILLEGAL_FIELD_VALUE(field, val));
            } catch (NoSuchFieldException nsf) {
                throw new BadRequestException(UNKNOWN_FIELD(field));
            } catch (IOException ex) {
                throw new InternalServerErrorException(ex);
            }
        } catch (JsonParseException e) {
            String contentType = request.headers().get(CONTENT_TYPE);
            if (contentType == null || contentType.isEmpty()) {
                throw new NotSupportedException(MediaTypes.convertToString(
                        Collections.singleton(APPLICATION_JSON_TYPE)));
            } else {
                throw new BadRequestException(MALFORMED_JSON(e.getOriginalMessage()));
            }
        } catch (IOException e) {
            throw new InternalServerErrorException(e);
        }
    }

    /**
     * Transforms a Java Object into it's equivalent JSON object in a String.
     *
     * @param object The Object to convert to JSON.
     * @return The rule as a JSON String.
     */
    public static String objectToJson(Object object) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            throw new InternalServerErrorException(e);

        }
    }

    /**
     * Initializes all empty (i.e. {@code null}) fields of the given object with
     * their default values. The default values are extracted from the
     * {@link DefaultValue} annotation attached to the field.
     *
     * The field's value will be instantiated with the {@code valueOf} method
     * of its class, or with a {@link String} constructor.
     * If neither exist, or if an exception is thrown while calling them, an
     * {@link InternalServerErrorException} will be thrown, containing the
     * underlying exception.
     *
     * If the field is an array, the value given in the annotation is ignored,
     * and the field is always initialized as an empty array.
     *
     * @param object The object to initialize.
     * @param <T>    The type of the given object.
     * @return The object with all its fields initialized.
     */
    private static <T> T fillEmptyFields(T object) {

        try {
            for (Field field : object.getClass().getFields()) {
                if (field.get(object) == null) {
                    if (field.isAnnotationPresent(DefaultValue.class)) {
                        String defaultValue = field.getAnnotation(DefaultValue.class).value();
                        Class<?> clazz = field.getType();

                        if (clazz.isArray()) {
                            field.set(object, Array.newInstance(clazz.getComponentType(), 0));
                            continue;
                        }

                        try {
                            Method valueOf = clazz.getMethod("valueOf", String.class);
                            Object value = valueOf.invoke(null, defaultValue);

                            field.set(object, value);
                        } catch (NoSuchMethodException e) {
                            try {
                                Constructor stringConst = clazz.getConstructor(String.class);
                                Object value = stringConst.newInstance(defaultValue);

                                field.set(object, value);
                            } catch (NoSuchMethodException ex) {
                                throw new InternalServerErrorException("Cannot " +
                                        "instantiate an object of class '" +
                                        clazz.getName() + "' from a String.");
                            }
                        }
                    }
                }
            }
            return object;
        } catch (IllegalAccessException e) {
            throw new InternalServerErrorException(e);
        } catch (InstantiationException e) {
            throw new InternalServerErrorException(e);
        } catch (InvocationTargetException e) {
            throw new InternalServerErrorException(e);
        }
    }

    /**
     * Checks if the value entered is within the bounds defined in the {@link Bounds}
     * annotation attached to the field.
     *
     * @param value     The value to check.
     * @param bounds    The 'Or' annotation containing the bounds to check.
     * @return  'true' if the value is within the bounds, 'false' if not.
     */
    private static boolean checkBounds(long value, Bounds bounds) {
        return value >= bounds.min() && value <= bounds.max();
    }

    /**
     * Checks if the entry fields have all been initialized and have correct
     * values, then returns the entry. If the {@code checkRequired} parameter is
     * true, all required fields with a {@code null} value will produce an error.
     *
     * @param entry         The entry to check.
     * @param checkRequired Specifies whether or not required fields should be
     *                      checked.
     */
    private static void checkEntry(Object entry, boolean checkRequired) {

        List<Error> errors = new ArrayList<Error>();
        for (Field field : entry.getClass().getFields()) {
            Class clazz = field.getType();
            Object val;
            try {
                val = field.get(entry);
            } catch (IllegalAccessException e) {
                throw new InternalServerErrorException(e);
            }

            if (val == null) {
                if (checkRequired && field.isAnnotationPresent(Required.class)) {
                    errors.add(MISSING_FIELD(field.getName()));
                }
            }
            else {
                if (field.isAnnotationPresent(Bounds.class)) {
                    if (val.getClass() == Integer.class) {
                        long num = (Integer) val;
                        if (!checkBounds(num, field.getAnnotation(Bounds.class))) {
                            errors.add(ILLEGAL_FIELD_VALUE(field.getName(), val.toString()));
                        }
                    }
                    else if (val.getClass() == Long.class) {
                        long num = (Long) val;
                        if (!checkBounds(num, field.getAnnotation(Bounds.class))) {
                            errors.add(ILLEGAL_FIELD_VALUE(field.getName(), val.toString()));
                        }
                    }
                } else if (!clazz.isEnum()) {
                    if (clazz.isArray()) {
                        for (Object obj : (Object[]) val) {
                            checkEntry(obj, checkRequired);
                        }
                    } else if (clazz.isAnnotationPresent(ConsistencyCheck.class)) {
                        checkEntry(val, checkRequired);
                    }
                }
            }
        }
        if(!errors.isEmpty()) {
            throw new BadRequestException(errors);
        }
    }
}
