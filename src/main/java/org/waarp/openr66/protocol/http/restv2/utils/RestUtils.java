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

import io.netty.handler.codec.http.HttpRequest;
import org.glassfish.jersey.message.internal.AcceptableLanguageTag;
import org.glassfish.jersey.message.internal.HttpHeaderReader;
import org.waarp.gateway.kernel.rest.RestConfiguration.CRUD;
import org.waarp.openr66.protocol.http.restv2.dbhandlers.AbstractRestDbHandler;

import javax.ws.rs.HttpMethod;
import javax.ws.rs.Path;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static javax.ws.rs.HttpMethod.*;
import static javax.ws.rs.core.HttpHeaders.ACCEPT_LANGUAGE;


/** A series of utility methods shared by all handlers of the RESTv2 API. */
public final class RestUtils {

    /** Prevents the default constructor from being called. */
    private RestUtils() throws InstantiationException {
        throw new InstantiationException(this.getClass().getName() +
                " cannot be instantiated.");
    }

    /**
     * Returns the Locale corresponding to the language requested in the request headers.
     * @param request   The HTTP request.
     * @return          The requested Locale.
     */
    public static Locale getRequestLocale(HttpRequest request) {
        String langHead = request.headers().get(ACCEPT_LANGUAGE);

        try {
            List<AcceptableLanguageTag> acceptableLanguages =
                    HttpHeaderReader.readAcceptLanguage(langHead);
            AcceptableLanguageTag bestMatch = acceptableLanguages.get(0);
            for (AcceptableLanguageTag acceptableLanguage : acceptableLanguages) {
                if (acceptableLanguage.getQuality() > bestMatch.getQuality()) {
                    bestMatch = acceptableLanguage;
                }
            }
            return bestMatch.getAsLocale();
        } catch (ParseException e) {
            return Locale.getDefault();
        }
    }

    /**
     * Extracts the available HTTP methods of a handler and returns them as a
     * {@link String} listing all the methods extracted.
     *
     * @param handler The handler from which to extract the available HTTP methods.
     * @return A String listing the available HTTP methods on the handler,
     * separated by comas.
     */
    public static String getMethodList(Class<? extends AbstractRestDbHandler> handler,
                                       byte crud) {
        ArrayList<String> methods = new ArrayList<String>();
        for (Method method : handler.getMethods()) {
            if (!method.isAnnotationPresent(Path.class)) {
                for (Annotation annotation : method.getDeclaredAnnotations()) {
                    if (annotation.annotationType().isAnnotationPresent(
                            HttpMethod.class)) {
                        String methodName = annotation.annotationType()
                                .getAnnotation(HttpMethod.class).value();
                        if (!methods.contains(methodName)) {
                            if ( (methodName.equals(GET) ||
                                    methodName.equals(HEAD) ) &&
                                    CRUD.READ.isValid(crud)) {
                                methods.add(methodName);
                            }
                            else if (methodName.equals(POST) &&
                                    CRUD.CREATE.isValid(crud)) {
                                methods.add(methodName);
                            }
                            else if (methodName.equals(PUT) &&
                                    CRUD.UPDATE.isValid(crud)) {
                                methods.add(methodName);
                            }
                            else if (methodName.equals(DELETE) &&
                                    CRUD.DELETE.isValid(crud)) {
                                methods.add(methodName);
                            }
                        }
                    }
                }
            }
        }

        return methods.toString().replaceAll("[\\[\\]]", "");
    }

    /**
     * Converts a {@link String} to the corresponding Boolean object.
     * If the String is neither "true" or "false" (case insensitive) then an
     * {@link ParseException} is thrown.
     *
     * @param string    The String to convert.
     * @return          The corresponding Boolean object (true, false or null).
     * @throws ParseException Thrown if the String does not represent
     *                                  a valid boolean value.
     */
    public static Boolean stringToBoolean(String string)
            throws ParseException {
        if("true".equalsIgnoreCase(string)) {
            return true;
        } else if("false".equalsIgnoreCase(string)) {
            return false;
        } else {
            throw new ParseException(string, 0);
        }
    }

}