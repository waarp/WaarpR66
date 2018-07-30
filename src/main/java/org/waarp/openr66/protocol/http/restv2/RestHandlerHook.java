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

import co.cask.http.HandlerHook;
import co.cask.http.HttpResponder;
import co.cask.http.internal.HandlerInfo;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.waarp.common.utility.Base64;
import org.waarp.openr66.protocol.http.restv2.data.hosts.Host;
import org.waarp.openr66.protocol.http.restv2.data.hosts.Hosts;
import org.waarp.openr66.protocol.http.restv2.exception.OpenR66RestIdNotFoundException;
import org.waarp.openr66.protocol.http.restv2.exception.OpenR66RestInternalServerException;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;

/** Hooks called before and after a request handler is called. */
public class RestHandlerHook implements HandlerHook {

    private final boolean authenticated;
    private final boolean signed;

    /** Message sent when the client does not accept the json format. */
    private final static String notAcceptable =
            "{\"userMessage\":\"Not acceptable\",\"internalMessage\":\"Client must accept JSON format.\"}";

    /** Message sent when the request content type is not valid for the specified request. */
    private final static String unsupportedMedia =
            "{\"userMessage\":\"Unsupported media\",\"internalMessage\":\"Request's content type must be %s.\"}";

    /**
     * Sends a response saying the requester must authenticate itself to make the request.
     *
     * @param responder The HttpResponder through which the response is sent.
     */
    private static void unauthorized(HttpResponder responder) {
        HttpHeaders headers = new DefaultHttpHeaders();
        headers.add(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON);
        headers.add(HttpHeaderNames.WWW_AUTHENTICATE, "Basic, HMAC");
        responder.sendString(HttpResponseStatus.UNAUTHORIZED,
                "{\"userMessage\":\"Unauthorized\"," +
                        "\"internalMessage\":\"Must be authenticated to make this request.\"}",
                headers);
    }


    /**
     * Hook called before a request handler is called. Checks the request content type and accepted type. If the
     * accepted type is invalid, the httpResponder sends a code 415 response. If the content type is invalid, sends a
     * code 406 response. In both case the response will be sent immediately and the request handler won't be called.
     *
     * @param httpRequest   The request currently being processed.
     * @param httpResponder The responder to which response must be sent if need be.
     * @param handlerInfo   Information about the handler to which the request will be sent.
     * @return True if the request can be handed to the handler, False if an error occurred and a response must be
     * sent immediately.
     */
    @Override
    public boolean preCall(HttpRequest httpRequest, HttpResponder httpResponder, HandlerInfo handlerInfo) {
        if (this.authenticated) {
            String key = httpRequest.headers().get("X-Auth-Key");
            if (key == null) {
                unauthorized(httpResponder);
                return false;
            } else {
                try {
                    String[] authKey = key.split(" ");

                    if("Basic".equals(authKey[0])) {
                        String[] credentials = new String(Base64.decode(authKey[1]),
                                Charset.forName("UTF-8")).split(":");
                        Host requester = Hosts.loadHost(credentials[0]);
                        if (!requester.hostKey.equals(credentials[1])) {
                            unauthorized(httpResponder);
                            return false;
                        }
                    }
                    else {
                        unauthorized(httpResponder);
                        return false;
                    }

                } catch (IOException e) {
                    httpResponder.sendJson(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                            OpenR66RestInternalServerException.base64Decoding().message);
                    return false;
                } catch (ArrayIndexOutOfBoundsException e) {
                    unauthorized(httpResponder);
                    return false;
                } catch (OpenR66RestIdNotFoundException e) {
                    unauthorized(httpResponder);
                    return false;
                }
            }
        }

        if (httpRequest.method() != HttpMethod.DELETE && httpRequest.method() != HttpMethod.OPTIONS) {
            String contentType = null;
            if (httpRequest.headers().get(HttpHeaderNames.CONTENT_TYPE) != null) {
                contentType = httpRequest.headers().get(HttpHeaderNames.CONTENT_TYPE);
            }
            List<String> accept = null;
            if (httpRequest.headers().get(HttpHeaderNames.ACCEPT) != null) {
                accept = Arrays.asList(httpRequest.headers().get(HttpHeaderNames.ACCEPT).split(", *"));
            }

            boolean acceptJson = (accept == null || accept.contains(HttpHeaderValues.APPLICATION_JSON.toString()));
            boolean contentUrlForm = (contentType == null ||
                    contentType.equals(HttpHeaderValues.APPLICATION_X_WWW_FORM_URLENCODED.toString()));
            boolean contentJson = (contentType != null &&
                    contentType.equals(HttpHeaderValues.APPLICATION_JSON.toString()));

            if (!acceptJson) {
                httpResponder.sendJson(HttpResponseStatus.NOT_ACCEPTABLE, notAcceptable);
                return false;
            }
            if (httpRequest.method() == HttpMethod.GET) {
                if (!contentUrlForm) {
                    httpResponder.sendJson(HttpResponseStatus.UNSUPPORTED_MEDIA_TYPE,
                            String.format(unsupportedMedia, HttpHeaderValues.APPLICATION_X_WWW_FORM_URLENCODED));
                    return false;
                }
            } else if (!contentJson) {
                httpResponder.sendJson(HttpResponseStatus.UNSUPPORTED_MEDIA_TYPE,
                        String.format(unsupportedMedia, HttpHeaderValues.APPLICATION_JSON));
                return false;
            }
        }

        return true;
    }

    /**
     * Hook called before a request handler is called.
     *
     * @param httpRequest        The request currently being processed.
     * @param httpResponseStatus The status of the http response generated by the request handler.
     * @param handlerInfo        Information about the handler to which the request was sent.
     */
    @Override
    public void postCall(HttpRequest httpRequest, HttpResponseStatus httpResponseStatus, HandlerInfo handlerInfo) {

    }


    /**
     * Creates a HandlerHook which will check for authentication and signature on incoming request depending on the
     * parameters.
     *
     * @param authenticated     If true, the HandlerHook will check that incoming requests are authenticated.
     * @param signed            If true, the HandlerHook will check that incoming requests are signed.
     */
    RestHandlerHook(boolean authenticated, boolean signed) {
        this.authenticated = authenticated;
        this.signed = signed;
    }
}
