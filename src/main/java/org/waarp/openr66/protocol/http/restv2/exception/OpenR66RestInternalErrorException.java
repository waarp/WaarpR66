package org.waarp.openr66.protocol.http.restv2.exception;

import org.waarp.openr66.protocol.http.restv2.errors.InternalErrorResponse;

public class OpenR66RestInternalErrorException extends Exception {
    private final InternalErrorResponse response;

    public OpenR66RestInternalErrorException(InternalErrorResponse response) {
        this.response = response;
    }

    public String toJson() {
        return this.response.toJson();
    }
}
