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

package org.waarp.openr66.protocol.http.restv2.resthandlers;

import io.cdap.http.ExceptionHandler;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.testcontainers.shaded.org.apache.commons.codec.language.bm.Lang;
import org.waarp.common.logging.WaarpLogger;
import org.waarp.common.logging.WaarpLoggerFactory;
import org.waarp.openr66.protocol.http.restv2.errors.BadRequestException;

import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.NotSupportedException;
import java.util.Locale;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.UNSUPPORTED_MEDIA_TYPE;
import static javax.ws.rs.core.HttpHeaders.ACCEPT;
import static org.waarp.openr66.protocol.http.restv2.errors.Error.serializeErrors;
import static org.waarp.openr66.protocol.http.restv2.utils.RestUtils.getRequestLocale;

/** Handles unknown exceptions that occur during the processing of an HTTP request. */
public class RestExceptionHandler extends ExceptionHandler {

    public static final WaarpLogger logger =
            WaarpLoggerFactory.getLogger(RestExceptionHandler.class);

    /**
     * Called when an  exception is thrown during the program execution.
     *
     * @param t         The exception thrown during execution.
     * @param request   The http request that failed.
     * @param responder The responder for the request.
     */
    @Override
    public void handle(Throwable t, HttpRequest request, HttpResponder responder) {
        if (t instanceof BadRequestException) {
            BadRequestException badRequest =
                    (BadRequestException) t;
            try {
                Locale lang = getRequestLocale(request);
                responder.sendJson(BAD_REQUEST, serializeErrors(badRequest.errors, lang));
            } catch (InternalServerErrorException e) {
                logger.error(e);
                responder.sendStatus(INTERNAL_SERVER_ERROR);
            }
        }
        else if (t instanceof NotSupportedException) {
            DefaultHttpHeaders headers = new DefaultHttpHeaders();
            headers.add(ACCEPT, t.getMessage());
            responder.sendStatus(UNSUPPORTED_MEDIA_TYPE, headers);
        }
        else {
            logger.error(t);
            responder.sendStatus(INTERNAL_SERVER_ERROR);
        }
    }
}
