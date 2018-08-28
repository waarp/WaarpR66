package org.waarp.openr66.protocol.http.restv2.errors;

public class UnauthorizedResponse extends RestResponse {

    private final String message;

    public UnauthorizedResponse() {
        this.message = RestResponse.restMessages.getString("Authentication.Unauthorized");
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
