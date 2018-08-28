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

    public static InternalErrorResponse jsonProcessingError() {
        String formattedMessage = RestResponse.restMessages.getString("InternalError.JsonProcessing");
        return new InternalErrorResponse(formattedMessage,100);
    }

    public static InternalErrorResponse illegalAccess(Class object, String field) {
        String formattedMessage = String.format(RestResponse.restMessages.getString("InternalError.IllegalAccess"),
                getClassName(object), field);
        return new InternalErrorResponse(formattedMessage, 101);
    }

    public static InternalErrorResponse databaseError() {
        String formattedMessage = RestResponse.restMessages.getString("InternalError.DatabaseError");
        return new InternalErrorResponse(formattedMessage, 102);
    }

    public static InternalErrorResponse hashError() {
        String formattedMessage = RestResponse.restMessages.getString("InternalError.HashError");
        return new InternalErrorResponse(formattedMessage, 103);
    }

    public static InternalErrorResponse base64Decoding() {
        String formattedMessage = RestResponse.restMessages.getString("InternalError.Base64Decoding");
        return new InternalErrorResponse(formattedMessage, 104);
    }

    public static InternalErrorResponse unknownHandler() {
        String formattedMessage = RestResponse.restMessages.getString("InternalError.UnknownHandler");
        return new InternalErrorResponse(formattedMessage, 105);
    }
}
