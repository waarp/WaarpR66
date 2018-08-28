package org.waarp.openr66.protocol.http.restv2.errors;

public class UnsupportedMediaResponse extends RestResponse {

    private final String message;

    public UnsupportedMediaResponse(String mediaType) {
        this.message = String.format(RestResponse.restMessages.getString("Media.UnsupportedMedia"), mediaType);
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