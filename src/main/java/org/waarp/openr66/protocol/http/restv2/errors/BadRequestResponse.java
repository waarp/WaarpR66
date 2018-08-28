package org.waarp.openr66.protocol.http.restv2.errors;

import java.util.ArrayList;
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
        String formattedMessage = RestResponse.restMessages.getString("BadRequest.MissingBody");
        this.responses.add(new BadRequest(formattedMessage, 200));
        return this;
    }

    public BadRequestResponse unknownField(Class object, String field) {

        String formattedMessage = String.format(RestResponse.restMessages.getString("BadRequest.UnknownField"),
                getClassName(object), field);
        this.responses.add(new BadRequest(formattedMessage, 201));
        return this;
    }

    public BadRequestResponse illegalFieldValue(Class object, String field) {
        String formattedMessage = String.format(RestResponse.restMessages.getString("BadRequest.IllegalFieldValue"),
                field, getClassName(object));
        this.responses.add(new BadRequest(formattedMessage, 202));
        return this;
    }

    public BadRequestResponse illegalParameterValue(String parameter) {
        String formattedMessage = String.format(RestResponse.restMessages.getString("BadRequest.IllegalParameterValue"),
                parameter);
        this.responses.add(new BadRequest(formattedMessage, 203));
        return this;
    }

    public BadRequestResponse alreadyExisting(Class object, String id) {
        String formattedMessage = String.format(RestResponse.restMessages.getString("BadRequest.AlreadyExisting"),
                getClassName(object), id);
        this.responses.add(new BadRequest(formattedMessage, 204));
        return this;
    }

    public BadRequestResponse alreadyInitialized(Class object) {
        String formattedMessage = String.format(RestResponse.restMessages.getString("BadRequest.AlreadyInitialized"),
                getClassName(object));
        this.responses.add(new BadRequest(formattedMessage, 205));
        return this;
    }
}
