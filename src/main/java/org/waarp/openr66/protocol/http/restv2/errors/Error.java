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

package org.waarp.openr66.protocol.http.restv2.errors;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.waarp.openr66.protocol.http.restv2.utils.JsonUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.ResourceBundle;

/**
 * This class represents an instance of HTTP error encountered  during
 * the processing of a request.
 * To create a new error, use the predefined methods that corresponds to
 * the desired error in the {@link Errors} class.
 * To objectToJson a {@code Error} object as a JSON String to be sent back,
 * use the {@code objectToJson} method with the desired {@code Error}
 * object and {@link Locale}. To objectToJson an entire list of errors, use
 * the {@code serializeErrors} method instead.
 */
public class Error {

    /** The key of the error message to be sent as a response. */
    private final String msgKey;

    /** The message arguments. */
    private final String[] args;

    /** The error code. */
    private final Integer code;

    /**
     * Creates an object representing the response message to a request which
     * produced an error 401 - Bad Request.
     *
     * @param msgKey The name of the property in the {@code restmessages} {@link
     *               ResourceBundle}
     *               corresponding to the error message.
     * @param args The arguments of the error message (typically parameter
     *             or field names) needed to specify what caused the error.
     * @param code The REST error code corresponding to the error.
     */
    Error(String msgKey, String[] args, int code) {
        this.msgKey = msgKey;
        this.args = args;
        this.code = code;
    }

    /**
     * Returns the error as a JSON array serialized in a String.
     * @param lang  The language of the error message.
     * @return The serialized error object contained in a singleton array.
     */
    public String serialize(Locale lang) {
        List<Error> errors = new ArrayList<Error>();
        errors.add(this);
        return serializeErrors(errors, lang);
    }

    /**
     * Returns the error as a JSON object serialized Map.
     *
     * @param lang  The language of the error message.
     * @return      The serialized error object.
     */
    private ObjectNode makeNode(Locale lang) {
        ResourceBundle bundle = ResourceBundle.getBundle("restmessages", lang);
        String message = String.format(lang, bundle.getString(msgKey), (Object[]) args);

        ObjectNode response = new ObjectNode(JsonNodeFactory.instance);
        response.put("message", message);
        response.put("errorCode", code);
        return response;
    }

    /**
     * Returns the list of errors as a JSON array serialized in a String.
     * @param errors The list of Error objects to objectToJson.
     * @param lang   The language of the error messages.
     * @return The serialized list of errors.
     */
    public static String serializeErrors(List<Error> errors, Locale lang) {
        ArrayNode errorsArray = new ArrayNode(JsonNodeFactory.instance);
        for (Error error : errors) {
            errorsArray.add(error.makeNode(lang));
        }
        ObjectNode response = new ObjectNode(JsonNodeFactory.instance);
        response.putArray("errors").addAll(errorsArray);

        return JsonUtils.nodeToString(response);
    }
}
