package org.waarp.openr66.protocol.http.restv2.errors;

public class ForbiddenResponse extends RestResponse {

    private final String message;

    public ForbiddenResponse() {
        this.message = RestResponse.restMessages.getString("Authentication.Forbidden");
    }

    @Override
    public String toJson() {
        return String.format(
                "{" +
                    "\"message\" : \"%s\"" +
                "}",
                this.message.replaceAll("\"", "\\\"")
        );
    }
}