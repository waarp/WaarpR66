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

public class InternalErrorResponse extends RestResponse {

    private final String message;

    private final int code;


    private InternalErrorResponse(String message, int code) {
        this.message = message;
        this.code = code;
    }


    @Override
    public String toJson() {
        return String.format(
                "{" +
                    "\"message\" : \"%s\"," +
                    "\"code\" : %d" +
                "}",
                this.message.replaceAll("\"", "\\\""), this.code
        );
    }

    public static InternalErrorResponse unexpectedError() {
        String formattedMessage = RestResponse.restMessages.getString("InternalError.UnexpectedError");
        return new InternalErrorResponse(formattedMessage,100);
    }

    public static InternalErrorResponse jsonProcessingError() {
        String formattedMessage = RestResponse.restMessages.getString("InternalError.JsonProcessing");
        return new InternalErrorResponse(formattedMessage,101);
    }

    public static InternalErrorResponse illegalAccess() {
        String formattedMessage = RestResponse.restMessages.getString("InternalError.IllegalAccess");
        return new InternalErrorResponse(formattedMessage, 102);
    }

    public static InternalErrorResponse databaseError() {
        String formattedMessage = RestResponse.restMessages.getString("InternalError.DatabaseError");
        return new InternalErrorResponse(formattedMessage, 103);
    }

    public static InternalErrorResponse hashError() {
        String formattedMessage = RestResponse.restMessages.getString("InternalError.HashError");
        return new InternalErrorResponse(formattedMessage, 104);
    }

    public static InternalErrorResponse base64Decoding() {
        String formattedMessage = RestResponse.restMessages.getString("InternalError.Base64Decoding");
        return new InternalErrorResponse(formattedMessage, 105);
    }

    public static InternalErrorResponse unknownHandler() {
        String formattedMessage = RestResponse.restMessages.getString("InternalError.UnknownHandler");
        return new InternalErrorResponse(formattedMessage, 106);
    }
}
