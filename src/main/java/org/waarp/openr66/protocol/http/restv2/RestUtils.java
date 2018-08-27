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

package org.waarp.openr66.protocol.http.restv2;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;
import org.waarp.common.crypto.HmacSha256;
import org.waarp.openr66.dao.DAOFactory;
import org.waarp.openr66.database.DbConstant;
import org.waarp.openr66.protocol.configuration.Configuration;
import org.waarp.openr66.protocol.http.restv2.data.Bounds;
import org.waarp.openr66.protocol.http.restv2.data.NotEmpty;
import org.waarp.openr66.protocol.http.restv2.data.Or;
import org.waarp.openr66.protocol.http.restv2.exception.OpenR66RestBadRequestException;
import org.waarp.openr66.protocol.http.restv2.exception.OpenR66RestInternalErrorException;
import org.waarp.openr66.protocol.http.restv2.exception.OpenR66RestInvalidEntryException;

import javax.ws.rs.HttpMethod;
import javax.ws.rs.Path;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;


/** A series of utility methods shared by all REST handlers. */
public final class RestUtils {

    /** This is a utility class that should never be instantiated. */
    private RestUtils() {
        throw new UnsupportedOperationException("'RestUtils' cannot be instantiated.");
    }

    /** This server's id. */
    public static final String HOST_ID = Configuration.configuration.getHOST_ID();

    private static Connection getConnection() {
        Connection connection;
        try {
            connection = DbConstant.connectionFactory.getConnection();
            connection.setReadOnly(false);
            return connection;
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static final DAOFactory factory = DAOFactory.getDAOFactory(getConnection());


    /** This server's Hmac key. */
    public static HmacSha256 HMAC;

    /**
     * This method transforms a request body in JSON format into an object of the class passed as argument.
     *
     * @param request The http request whose body has to be deserialized.
     * @param c       The class of the object that will be created from the JSON object.
     * @param <T>     The generic type of the output object.
     * @return The Java object representing the source JSON object.
     * @throws OpenR66RestBadRequestException Thrown if the JSON object is missing one or more fields.
     * @throws OpenR66RestInternalErrorException Thrown if the JSON object could not be processed.
     */
    public static <T> T deserializeRequest(HttpRequest request, Class<T> c)
            throws OpenR66RestBadRequestException, OpenR66RestInternalErrorException {

        try {
            if (request instanceof FullHttpRequest) {
                String body = ((FullHttpRequest) request).content().toString(Charset.forName("UTF-8"));
                ObjectMapper mapper = new ObjectMapper();
                mapper.configure(DeserializationFeature.USE_JAVA_ARRAY_FOR_JSON_ARRAY, true);
                mapper.configure(DeserializationFeature.FAIL_ON_NUMBERS_FOR_ENUMS, true);
                mapper.configure(DeserializationFeature.FAIL_ON_READING_DUP_TREE_KEY, true);
                mapper.configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES, true);

                return mapper.readValue(body, c);
            } else {
                throw new OpenR66RestBadRequestException(RestResponses.emptyBody());
            }
        } catch (JsonMappingException e) {
            if(e.getCause() == null) {
                if(e.getPath().size() < 1) {
                    throw new OpenR66RestBadRequestException(RestResponses.emptyBody());
                } else {
                    try {
                        Field field = c.getField(e.getPath().get(0).getFieldName());
                        throw new OpenR66RestBadRequestException(
                                RestResponses.illegalValue(field.getType(), field.getName()));
                    } catch (NoSuchFieldException nsf) {
                        throw new OpenR66RestBadRequestException(RestResponses.unknownField(
                                e.getPath().get(0).getFieldName()));
                    }
                }
            } else {
                throw new OpenR66RestBadRequestException(e.getCause().getMessage());
            }
        } catch (IllegalArgumentException e) {
            throw new OpenR66RestBadRequestException(RestResponses.emptyBody());
        } catch (IOException e) {
            throw new OpenR66RestInternalErrorException(RestResponses.jsonProcessing());
        }
    }

    /**
     * Converts a Calendar to a String containing the date in ISO-8601 format. If *calendar* is null, returns 'null'.
     *
     * @param calendar The source Calendar object.
     * @return The date of the Calendar object in ISO-8601 format.
     */
    public static String fromCalendar(Calendar calendar) {
        if (calendar == null) {
            return null;
        } else {
            Date date = calendar.getTime();
            String formatted = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ").format(date);
            return formatted.substring(0, 22) + ":" + formatted.substring(22);
        }
    }

    /**
     * Converts a String with a date in ISO-8601 format to a Calendar.
     *
     * @param iso8601string The string containing the date in ISO-8601.
     * @return The Calendar corresponding to the date in the string parameter.
     * @throws IllegalArgumentException Thrown if the string is not a valid ISO-8601 date.
     */
    public static Calendar toCalendar(final String iso8601string) throws IllegalArgumentException {
        try {
            Calendar calendar = GregorianCalendar.getInstance();
            String s = iso8601string.replace("Z", "+00:00");
            s = s.substring(0, 22) + s.substring(23);
            Date date = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ").parse(s);
            calendar.setTime(date);
            return calendar;

        } catch (IndexOutOfBoundsException e) {
            throw new IllegalArgumentException();
        } catch (ParseException e) {
            throw new IllegalArgumentException();
        }

    }

    /**
     * Extracts the available HTTP methods of a handler and returns them as a String suitable as a response to an
     * OPTIONS request on said handler.
     *
     * @param handler The handler from which to extract the available HTTP methods.
     * @return A String listing the available HTTP methods on the handler, separated by comas.
     */
    public static String options(Class handler) {
        ArrayList<String> methods = new ArrayList<String>();
        for (Method method : handler.getMethods()) {
            if (!method.isAnnotationPresent(Path.class)) {
                for (Annotation annotation : method.getDeclaredAnnotations()) {
                    if (annotation.annotationType().isAnnotationPresent(HttpMethod.class)) {
                        HttpMethod http = annotation.annotationType().getAnnotation(HttpMethod.class);
                        if (!methods.contains(http.value())) {
                            methods.add(http.value());
                        }
                    }
                }
            }
        }

        return methods.toString().replaceAll("[\\[\\]]", "");
    }

    /**
     * Transforms a Java Object into it's equivalent JSON object in a String.
     *
     * @param object The Object to convert to JSON.
     * @return The rule as a JSON String.
     * @throws JsonProcessingException Thrown if the rule cannot be transformed in JSON.
     */
    public static String toJsonString(Object object) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.writeValueAsString(object);
    }

    /**
     * Checks if the value entered is within the bounds defined in the 'Or' annotation attached to the field.
     * @param value     The value to check.
     * @param bounds    The 'Or' annotation containing the bounds to check.
     * @return  'true' if the value is within the bounds, 'false' if not.
     */
    private static boolean checkBounds(long value, Or bounds) {
        for(Bounds bound : bounds.value()) {
            if (value >= bound.min() && value <= bound.max()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Checks the fields have all been initialized.
     * @param entry The entry to check.
     * @throws OpenR66RestInternalErrorException    Thrown if one of the entry fields is inaccessible.
     * @throws OpenR66RestInvalidEntryException     Thrown if one of the fields has an invalid value.
     */
    public static void checkEntry(Object entry) throws OpenR66RestInternalErrorException,
            OpenR66RestInvalidEntryException {
        for(Field field : entry.getClass().getFields()) {
            try {
                Object val = field.get(entry);
                if(val == null) {
                    throw new OpenR66RestInvalidEntryException(RestResponses.emptyField(field.getName()));
                } else {
                    Class cla = field.getType();
                    if(field.isAnnotationPresent(NotEmpty.class)) {
                        if(cla == String.class && ((String) val).trim().isEmpty()) {
                            throw new OpenR66RestInvalidEntryException(RestResponses.emptyField(field.getName()));
                        } else if(cla == String[].class) {
                            for(String str : (String[]) val) {
                                if(str.trim().isEmpty()) {
                                    throw new OpenR66RestInvalidEntryException(RestResponses.emptyField(
                                            field.getName()));
                                }
                            }
                        }
                    }
                    else if(field.isAnnotationPresent(Or.class)) {
                        if(val.getClass() == Integer.class){
                            long num = ((Integer) val).longValue();
                            if (!checkBounds(num, field.getAnnotation(Or.class))) {
                                throw new OpenR66RestInvalidEntryException(
                                        RestResponses.invalidNumber(num, field.getName()));
                            }
                        } else if(val.getClass() == Long.class) {
                            long num = (Long) val;
                            if (!checkBounds(num, field.getAnnotation(Or.class))) {
                                throw new OpenR66RestInvalidEntryException(
                                        RestResponses.invalidNumber(num, field.getName()));
                            }
                        }
                    } else if(!cla.isEnum()) {
                        if(cla.isArray()) {
                            for(Object obj : (Object[]) val) {
                                checkEntry(obj);
                            }
                        } else {
                            checkEntry(val);
                        }
                    }
                }
            } catch(IllegalAccessException e) {
                throw new OpenR66RestInternalErrorException(RestResponses.illegalAccess(field));
            }
        }
    }

    /**
     * Converts a string to the corresponding Boolean object. If the String is not either null, "true" or "false" (case
     * insensitive) then an exception is thrown.
     * @param string    The String to convert.
     * @return          The corresponding Boolean object (true, false or null).
     * @throws IOException  Thrown if the String does not represent a Boolean.
     */
    public static Boolean toBoolean(String string) throws IOException {
        if(string == null) {
            return null;
        } else if("true".equalsIgnoreCase(string)) {
            return true;
        } else if("false".equalsIgnoreCase(string)) {
            return false;
        } else {
            throw new IOException();
        }
    }

    public static String toArrayDbList(Object[] array) {
        StringBuilder list = new StringBuilder();
        for(Object object : array) {
            list.append(object.toString()).append(" ");
        }
        return list.toString().trim();
    }
}