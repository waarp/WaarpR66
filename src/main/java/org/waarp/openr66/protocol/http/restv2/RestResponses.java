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

import java.lang.reflect.Field;

public final class RestResponses {

    private static final int DB_EXCEPTION = 100;

    private static final int JSON_PROCESSING = 101;

    private static final int BASE64_DECODING = 102;

    private static final int ILLEGAL_ACCESS = 103;

    private static final int HASHING_ERROR = 104;


    /** This class is a utility class, and should not be instantiated. */
    private RestResponses () {
        throw new UnsupportedOperationException("'RestResponse' cannot be instantiated.");
    }


    public static String emptyBody() {
        return "{" +
                    "\"userMessage\":\"Empty body\"," +
                    "\"internalMessage\":\"The request body is empty.\"" +
                "}";
    }

    public static String missingField(String fieldName) {
        return String.format(
                "{" +
                    "\"userMessage\":\"Missing field\"," +
                    "\"internalMessage\":\"The field '%s' is missing.\"" +
                "}",
                fieldName);
    }

    public static String emptyField(String fieldName) {
        return String.format(
                "{" +
                    "\"userMessage\":\"Empty field\"," +
                    "\"internalMessage\":\"The field '%s' is empty.\"" +
                "}",
        fieldName);
    }

    public static String alreadyExisting(String entryType, String id) {
        return String.format(
                "{" +
                    "\"userMessage\":\"Already existing\"," +
                    "\"internalMessage\":\"The %s of id '%s' already exists in the database.\"" +
                "}",
        entryType, id);
    }

    public static String invalidEnum(String id, String value) {
        return String.format(
                "{" +
                    "\"userMessage\":\"Invalid enum value\"," +
                    "\"internalMessage\":\"'%s' is not a valid value for '%s'.\"" +
                "}",
        value, id);
    }

    public static String illegalValue(Class type, String id) {
        return String.format(
                "{" +
                    "\"userMessage\":\"Illegal value\"," +
                    "\"internalMessage\":\"The value of field '%s' could not be converted to type %s.\"" +
                "}",
        id, type.getSimpleName());
    }

    public static String notADate(String id, String value) {
        return String.format(
                "{" +
                    "\"userMessage\":\"Not a number\"," +
                    "\"internalMessage\":\"The value '%s' of parameter '%s' is not a valid ISO-8601 date.\"" +
                "}",
        value, id);
    }

    public static String notAPort(String value) {
        return String.format(
                "{" +
                    "\"userMessage\":\"Not a port number\"," +
                    "\"internalMessage\":\"The value '%s' is not a valid port number.\"" +
                    "}",
                value);
    }

    public static String notANumber(String value) {
        return String.format(
                "{" +
                    "\"userMessage\":\"Not a number\"," +
                    "\"internalMessage\":\"The value '%s' is not a number.\"" +
                    "}",
                value);
    }

    public static String invalidNumber(Integer value, String fieldName) {
        return String.format(
                "{" +
                    "\"userMessage\":\"Invalid number\"," +
                    "\"internalMessage\":\"The value '%d' is not valid for field %s.\"" +
                    "}",
                value, fieldName);
    }

    public static String notABoolean(String value) {
        return String.format(
                "{" +
                    "\"userMessage\":\"Not a number\"," +
                    "\"internalMessage\":\"'%s' does not represent a valid boolean value.\"" +
                    "}",
                value);
    }

    public static String unknownField(String fieldName) {
        return String.format(
                "{" +
                    "\"userMessage\":\"Unknown field\"," +
                    "\"internalMessage\":\"Unknown field '%s'.\"" +
                "}",
        fieldName);
    }

    public static String negative(String id) {
        return String.format(
                "{" +
                    "\"userMessage\":\"Illegal value\"," +
                    "\"internalMessage\":\"The parameter '%s' cannot be negative.\"" +
                "}",
        id);
    }

    public static String alreadyInitialized(String entryType) {
        return String.format(
                "{" +
                    "\"userMessage\":\"Already initialized\"," +
                    "\"internalMessage\":\"This host already has a %s in the database.\"" +
                "}",
        entryType);
    }

    public static String invalidCredentials() {
        return "{" +
                    "\"userMessage\":\"Invalid credentials\"," +
                    "\"internalMessage\":\"The provided authentication credentials are invalid.\"" +
                "}";
    }

    public static String dateInThePast() {
        return "{" +
            "\"userMessage\":\"Date in the past\"," +
            "\"internalMessage\":\"A new transfer cannot have its date in the past.\"" +
        "}";
    }

    public static String jsonProcessing() {
        return "{" +
                    "\"userMessage\":\"JSON Processing Error\"," +
                    "\"internalMessage\":\"Could not transform the response into JSON format.\"," +
                    "\"code\":" + JSON_PROCESSING +
                "}";
    }

    public static String base64Decoding() {
        return "{" +
                    "\"userMessage\":\"Base64 Decoding Error\"," +
                    "\"internalMessage\":\"An error occurred during the decoding of the base64 authentication key."+
                    "\",\"code\":" + BASE64_DECODING +
                "}";
    }

    public static String illegalAccess(Field field) {
        return String.format(
                "{" +
                    "\"userMessage\":\"Illegal Access Error\"," +
                    "\"internalMessage\":\"An error occurred when trying to access the field '%s' of class '%s'.\"," +
                    "\"code\":" + ILLEGAL_ACCESS +
                "}",
                field.getName(), field.getDeclaringClass().getSimpleName());
    }

    public static String dbException(Throwable t) {
        return String.format(
                "{" +
                    "\"userMessage\":\"Database Error\"," +
                    "\"internalMessage\":\"%s.\"," +
                    "\"code\":" + DB_EXCEPTION +
                    "}",
                t.getMessage());
    }

    public static String hashError() {
        return "{" +
                    "\"userMessage\":\"Hashing Error\"," +
                    "\"internalMessage\":\"An error occurred during the password hashing.\"," +
                    "\"code\":" + HASHING_ERROR +
                "}";
    }
}