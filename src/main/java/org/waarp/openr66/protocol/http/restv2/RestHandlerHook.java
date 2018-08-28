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
import org.waarp.openr66.dao.HostDAO;
import org.waarp.openr66.dao.exception.DAOException;
import org.waarp.openr66.pojo.Host;
import org.waarp.openr66.protocol.http.restv2.errors.ForbiddenResponse;
import org.waarp.openr66.protocol.http.restv2.errors.InternalErrorResponse;
import org.waarp.openr66.protocol.http.restv2.errors.NotAcceptableResponse;
import org.waarp.openr66.protocol.http.restv2.errors.RestResponse;
import org.waarp.openr66.protocol.http.restv2.errors.UnauthorizedResponse;
import org.waarp.openr66.protocol.http.restv2.errors.UnsupportedMediaResponse;
import org.waarp.openr66.protocol.http.restv2.handler.AbstractRestHttpHandler;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.regex.Pattern;

/** Hooks called before and after a request handler is called. */
public class RestHandlerHook implements HandlerHook {

    private final boolean authenticated;
    private final boolean signed;

    private static final String keyHeader = "X-Auth-Key";
    private static final String userHeader = "X-Auth-User";
    private static final String tsHeader = "X-Auth-Timestamp";

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
                new UnauthorizedResponse().toJson(),
                headers);
    }

    /**
     * Sends a response saying the requester must authenticate itself to make the request.
     *
     * @param responder The HttpResponder through which the response is sent.
     */
    private static void forbidden(HttpResponder responder) {
        HttpHeaders headers = new DefaultHttpHeaders();
        headers.add(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON);
        responder.sendString(HttpResponseStatus.FORBIDDEN,
                new ForbiddenResponse().toJson(),
                headers);
    }

    private static AbstractRestHttpHandler getHandler(HandlerInfo info)  {
        String name = info.getHandlerName();
        try {
            Class<?> cla = Class.forName(name);
            return (AbstractRestHttpHandler) cla.newInstance();
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException();
        } catch (IllegalAccessException e) {
            throw new IllegalArgumentException();
        } catch (InstantiationException e) {
            throw new IllegalArgumentException();
        } catch (ClassCastException e) {
            throw new IllegalArgumentException();
        }
    }

    private static void setLocale(HttpRequest request) {
        String langHead = request.headers().get(HttpHeaderNames.ACCEPT_LANGUAGE);
        if(langHead != null) {
            String[] langs = langHead.split(",");
            for(String lang : langs) {
                if(lang.matches("fr")) {
                    RestResponse.restMessages = ResourceBundle.getBundle("RestMessages", Locale.FRENCH);
                    return;
                } else if(lang.matches("en")) {
                    RestResponse.restMessages = ResourceBundle.getBundle("RestMessages", Locale.ENGLISH);
                    return;
                }
            }
        }
        RestResponse.restMessages = ResourceBundle.getBundle("RestMessages", Locale.ENGLISH);
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
        setLocale(httpRequest);

        if(this.authenticated) {
            try {
                String key = httpRequest.headers().get(keyHeader);
                String user;
                if (key == null) {
                    unauthorized(httpResponder);
                    return false;
                } else {
                    HostDAO hostDAO = RestUtils.factory.getHostDAO();
                    String[] authKey = key.split("\\s");

                    if ("Basic".equals(authKey[0])) {
                        String[] credentials = new String(Base64.decode(authKey[1])).split(":");
                        if (credentials.length != 2) {
                            unauthorized(httpResponder);
                            return false;
                        }
                        user = credentials[0];
                        Host requester = hostDAO.select(user);
                        String cipher;
                        try {
                            cipher = RestUtils.HMAC.cryptToHex(credentials[1]);
                        } catch (Exception e) {
                            httpResponder.sendJson(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                                    InternalErrorResponse.hashError().toJson());
                            return false;
                        }
                        if (requester == null || !cipher.equals(new String(requester.getHostkey()))) {
                            unauthorized(httpResponder);
                            return false;
                        }
                    } else if ("Hmac".equals(authKey[0])) {
                        String time = httpRequest.headers().get(tsHeader);
                        user = httpRequest.headers().get(userHeader);
                        Host requester = hostDAO.select(user);
                        if (requester == null) {
                            unauthorized(httpResponder);
                            return false;
                        } else {
                            String pswd = new String(requester.getHostkey());
                            String mkkey = RestUtils.HMAC.cryptToHex(time + user + pswd);
                            System.err.println(mkkey);
                            if (!mkkey.equals(authKey[1])) {
                                unauthorized(httpResponder);
                                return false;
                            }
                        }
                    } else {
                        unauthorized(httpResponder);
                        return false;
                    }
                }

                if(!getHandler(handlerInfo).isAuthorized(user, httpRequest.method(), handlerInfo.getMethodName())) {
                    forbidden(httpResponder);
                    return false;
                }


            } catch (IOException e) {
                httpResponder.sendJson(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                        InternalErrorResponse.base64Decoding().toJson());
                return false;
            } catch (ArrayIndexOutOfBoundsException e) {
                unauthorized(httpResponder);
                return false;
            } catch (DAOException e) {
                httpResponder.sendJson(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                        InternalErrorResponse.databaseError().toJson());
                return false;
            } catch (IllegalArgumentException e) {
                httpResponder.sendJson(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                        InternalErrorResponse.unknownHandler().toJson());
                return false;
            } catch (Exception e) {
                httpResponder.sendJson(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                        InternalErrorResponse.hashError().toJson());
                return false;
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
                httpResponder.sendJson(HttpResponseStatus.NOT_ACCEPTABLE, new NotAcceptableResponse().toJson());
                return false;
            }
            if (httpRequest.method() == HttpMethod.GET) {
                if (!contentUrlForm) {
                    httpResponder.sendJson(HttpResponseStatus.UNSUPPORTED_MEDIA_TYPE,
                            new UnsupportedMediaResponse(
                                    HttpHeaderValues.APPLICATION_X_WWW_FORM_URLENCODED.toString()).toJson());
                    return false;
                }
            } else if (!contentJson) {
                httpResponder.sendJson(HttpResponseStatus.UNSUPPORTED_MEDIA_TYPE,
                        new UnsupportedMediaResponse(
                                HttpHeaderValues.APPLICATION_JSON.toString()).toJson());
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
