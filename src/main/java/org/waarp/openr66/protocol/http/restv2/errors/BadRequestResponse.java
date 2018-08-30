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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class BadRequestResponse extends RestResponse {

    private class BadRequest {
        private final String message;
        private final int code;

        private BadRequest(String message, int code) {
            this.message = message;
            this.code = code;
        }

        public String toJson() {
            return String.format(
                    "{" +
                        "\"message\": \"%s\"," +
                        "\"code\": %d" +
                    "}",
                    this.message.replaceAll("\"", "\\\""), this.code
            );
        }

        @Override
        public String toString() {
            return toJson();
        }
    }

    private List<BadRequest> responses = new ArrayList<BadRequest>();

    @Override
    public String toJson() {
        return "{ \"errors\":" + responses.toString() + "}";
    }

    public boolean isEmpty() {
        return responses.isEmpty();
    }

    public BadRequestResponse missingBody() {
        String formattedMessage = RestResponse.formatMessage("BadRequest.MissingBody", null,
                "The request does not have a body.");
        this.responses.add(new BadRequest(formattedMessage, 200));
        return this;
    }

    public BadRequestResponse unknownField(Class object, String field) {
        String[] args = {getClassName(object), field};
        String formattedMessage = RestResponse.formatMessage("BadRequest.UnknownField", args,
                "A %s object does not have a field '%s'.");
        this.responses.add(new BadRequest(formattedMessage, 201));
        return this;
    }

    public BadRequestResponse illegalFieldValue(Class object, String field) {
        String[] args = {field, getClassName(object)};
        String formattedMessage = RestResponse.formatMessage("BadRequest.IllegalFieldValue", args,
                "The value of field '%s' of the %s object is illegal.");
        this.responses.add(new BadRequest(formattedMessage, 202));
        return this;
    }

    public BadRequestResponse missingFieldValue(Class object, String field) {
        String[] args = {field, getClassName(object)};
        String formattedMessage = RestResponse.formatMessage("BadRequest.MissingFieldValue", args,
                "The value of field '%s' of the %s object is missing.");
        this.responses.add(new BadRequest(formattedMessage, 203));
        return this;
    }

    public BadRequestResponse incorrectFieldType(Class object, String field, Class type) {
        String[] args = {field, getClassName(object), getClassName(type)};
        String formattedMessage = RestResponse.formatMessage("BadRequest.IncorrectFieldType", args,
                "The type of the value of field '%s' of the %s object is incorrect. The value should be of type : %s.");
        this.responses.add(new BadRequest(formattedMessage, 204));
        return this;
    }

    public BadRequestResponse illegalParameterValue(String parameter) {
        String[] args = {parameter};
        String formattedMessage = RestResponse.formatMessage("BadRequest.IllegalParameterValue", args,
                "The value of the query parameter '%s' is illegal.");
        this.responses.add(new BadRequest(formattedMessage, 205));
        return this;
    }

    public BadRequestResponse alreadyExisting(Class object, String id) {
        String[] args = {getClassName(object), id};
        String formattedMessage = RestResponse.formatMessage("BadRequest.AlreadyExisting", args,
                "A %s entry of the same id already exists in the database.");
        this.responses.add(new BadRequest(formattedMessage, 206));
        return this;
    }

    public BadRequestResponse alreadyInitialized(Class object) {
        String[] args = {getClassName(object)};
        String formattedMessage = RestResponse.formatMessage("BadRequest.AlreadyInitialized", args,
                "This host's %s has already been initialized.");
        this.responses.add(new BadRequest(formattedMessage, 207));
        return this;
    }
}
