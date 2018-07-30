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

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;
import org.waarp.openr66.protocol.http.restv2.exception.ImpossibleException;
import org.waarp.openr66.protocol.http.restv2.exception.OpenR66RestBadRequestException;

import javax.ws.rs.HttpMethod;
import javax.ws.rs.Path;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;


/** A series of utility methods shared by all REST handlers. */
public final class RestUtils {

    /** This server's id. */
    //TODO: replace by loading the id from the config file.
    public static final String HOST_ID = "server1";

    /**
     * This method transforms a request body in JSON format into an object of the class passed as argument.
     *
     * @param request The http request whose body has to be deserialized.
     * @param c       The class of the object that will be created from the JSON object.
     * @param <T>     The generic type of the output object.
     * @return The Java object representing the source JSON object.
     * @throws OpenR66RestBadRequestException Thrown if the JSON object cannot be converted to the specified Java class.
     */
    public static <T> T deserializeRequest(HttpRequest request, Class<T> c)
            throws OpenR66RestBadRequestException {

        try {
            if (request instanceof FullHttpRequest) {
                String body = ((FullHttpRequest) request).content().toString(Charset.forName("UTF-8"));
                ObjectMapper mapper = new ObjectMapper();
                mapper.configure(DeserializationFeature.USE_JAVA_ARRAY_FOR_JSON_ARRAY, true);

                return mapper.readValue(body, c);
            } else {
                throw OpenR66RestBadRequestException.emptyBody();
            }
        } catch (JsonParseException e) {
            throw OpenR66RestBadRequestException.emptyBody();
        } catch (JsonMappingException e) {
            if (e.getPath().size() < 1) {
                throw OpenR66RestBadRequestException.emptyBody();
            } else {
                String field = e.getPath().get(0).getFieldName();
                Class type;
                try {
                    type = c.getField(field).getType();
                } catch (NoSuchFieldException e1) {
                    throw OpenR66RestBadRequestException.unknownField(field, c);
                }
                throw OpenR66RestBadRequestException.illegalValue(type, field);
            }
        } catch (IllegalArgumentException e) {
            throw OpenR66RestBadRequestException.emptyBody();
        } catch (IOException e) {
            throw new ImpossibleException(e);
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
     * Checks if the object passed as argument is an illegal value for an object field.
     *
     * @param value The object which whose value should be checked.
     * @return Returns false if the object is null or empty. Returns true otherwise.
     * @throws OpenR66RestBadRequestException Thrown if the object itself is a valid list but one of the objects
     *                                        contained in it has an illegal field.
     */
    public static boolean isIllegal(Object value) throws OpenR66RestBadRequestException {
        if (value == null || value.toString().equals("")) {
            return true;
        } else if (value.getClass().isArray()) {
            Object[] arr = (Object[]) value;
            for (Object el : arr) {
                if (el == null) {
                    return true;
                } else {
                    for (Field field : el.getClass().getFields()) {
                        try {
                            if (isIllegal(field.get(el))) {
                                throw OpenR66RestBadRequestException.emptyField(field.getName());
                            }
                        } catch (IllegalAccessException e) {
                            throw new ImpossibleException(e);
                        }
                    }
                }
            }
        }
        return false;
    }

    /**
     * Converts a String to a Boolean if the String represents a valid boolean value.
     *
     * @param str The string to convert to a Boolean.
     * @return The Boolean extracted from the String.
     * @throws IllegalArgumentException Thrown if the String parameter does not represent a valid boolean value.
     */
    public static Boolean stringToBoolean(String str) throws IllegalArgumentException {
        if ("true".equalsIgnoreCase(str)) {
            return Boolean.TRUE;
        } else if ("false".equalsIgnoreCase(str)) {
            return Boolean.FALSE;
        } else {
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
}
