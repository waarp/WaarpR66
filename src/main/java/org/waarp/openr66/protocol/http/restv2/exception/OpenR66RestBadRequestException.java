package org.waarp.openr66.protocol.http.restv2.exception;

import org.waarp.openr66.protocol.http.restv2.errors.BadRequestResponse;

public class OpenR66RestBadRequestException extends Exception {
    private final BadRequestResponse response;

    public OpenR66RestBadRequestException(BadRequestResponse response) {
        this.response = response;
    }

    public String toJson() {
        return response.toJson();
    }
}