/*
 * This file is part of Waarp Project (named also Waarp or GG).
 *
 * Copyright 2009, Waarp SAS, and individual contributors by the @author
 * tags. See the COPYRIGHT.txt in the distribution for a full listing of
 * individual contributors.
 *
 * All Waarp Project is free software: you can redistribute it and/or
 * modify it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your
 * option) any later version.
 *
 * Waarp is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
 * A PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * Waarp . If not, see <http://www.gnu.org/licenses/>.
 */

package org.waarp.openr66.protocol.http.restv2;

import co.cask.http.ExceptionHandler;
import co.cask.http.HttpResponder;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;

/** Handles unknown exceptions that occur during the processing of an HTTP request. */
public class RestExceptionHandler extends ExceptionHandler {

    /**
     * Called when an unknown exception is thrown during the program execution. Creates and sends an http response with
     * a 500 status and the exception message in the body as a JSON object.
     *
     * @param t         The exception thrown during execution.
     * @param request   The http request that failed.
     * @param responder The responder for the request.
     */
    @Override
    public void handle(Throwable t, HttpRequest request, HttpResponder responder) {
        String internal = (t.getMessage() == null) ? "" : t.getMessage().replaceAll("\"", "'");
        String message = String.format("{\"userMessage\":\"Unexpected internal error : " + t.getClass().getSimpleName() +
                "\",\"internalMessage\":\"%s\"}", internal);
        responder.sendJson(HttpResponseStatus.INTERNAL_SERVER_ERROR, message);
    }
}
