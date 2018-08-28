package org.waarp.openr66.protocol.http.restv2.errors;

public class NotAcceptableResponse extends RestResponse {

    private final String message;

    public NotAcceptableResponse() {
        this.message = RestResponse.restMessages.getString("Media.NotAcceptable");
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