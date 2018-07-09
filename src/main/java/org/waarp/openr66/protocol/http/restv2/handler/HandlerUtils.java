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

package org.waarp.openr66.protocol.http.restv2.handler;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;
import org.waarp.openr66.protocol.http.restv2.exception.OpenR66RestBadRequestException;

import java.io.IOException;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;


/** A series of utility methods shared by all REST handlers. */
public class HandlerUtils {

    /**
     * This method transforms a request body in JSON format into an object of the class passed as argument.
     *
     * @param request The http request whose body has to be deserialized.
     * @param c       The class of the object that will be created from the JSON object.
     * @param <T>     The generic type of the output object.
     * @return The Java object representing the source JSON object.
     * @throws OpenR66RestBadRequestException Thrown if the JSON object cannot be converted to the specified Java class.
     */
    static <T> T deserializeRequest(HttpRequest request, Class<T> c)
            throws OpenR66RestBadRequestException {

        try {
            if (request instanceof FullHttpRequest) {
                String body = ((FullHttpRequest) request).content().toString(Charset.forName("UTF-8"));
                ObjectMapper mapper = new ObjectMapper();

                return mapper.readValue(body, c);
            } else {
                throw new IOException();
            }
        } catch (IOException e) {
            throw new OpenR66RestBadRequestException(
                    "{" +
                            "\"userMessage\":\"Bad Request\"," +
                            "\"internalMessage\":\"The request body is missing one or multiple fields.\"" +
                            "}"
            );
        }
    }

    /**
     * Converts a Calendar to a String containing the date in ISO-8601 format.
     *
     * @param calendar The source Calendar object.
     * @return The date of the Calendar object in ISO-8601 format.
     */
    public static String fromCalendar(Calendar calendar) {
        Date date = calendar.getTime();
        String formatted = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ").format(date);
        return formatted.substring(0, 22) + ":" + formatted.substring(22);
    }

    /**
     * Converts a String with a date in ISO-8601 format to a Calendar.
     *
     * @param iso8601string The string containing the date in ISO-8601.
     * @return The Calendar corresponding to the date in the string parameter.
     * @throws OpenR66RestBadRequestException Thrown if the string is not a valid ISO-8601 date.
     */
    public static Calendar toCalendar(final String iso8601string) throws OpenR66RestBadRequestException {
        try {
            Calendar calendar = GregorianCalendar.getInstance();
            String s = iso8601string.replace("Z", "+00:00");
            s = s.substring(0, 22) + s.substring(23);
            Date date = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ").parse(s);
            calendar.setTime(date);
            return calendar;

        } catch (IndexOutOfBoundsException e) {
            throw new OpenR66RestBadRequestException(
                    "{" +
                            "\"userMessage\":\"Bad Request\"," +
                            "\"internalMessage\":\"The inputted date is not a valid ISO-8601 date.\"" +
                            "}"
            );
        } catch (ParseException e) {
            throw new OpenR66RestBadRequestException(
                    "{" +
                            "\"userMessage\":\"Bad Request\"," +
                            "\"internalMessage\":\"The inputted date is not a valid ISO-8601 date.\"" +
                            "}"
            );
        }

    }
}
