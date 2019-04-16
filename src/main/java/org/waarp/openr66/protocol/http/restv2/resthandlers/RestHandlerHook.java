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

import io.cdap.http.HandlerHook;
import io.cdap.http.HttpResponder;
import io.cdap.http.internal.HandlerInfo;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.joda.time.DateTime;
import org.waarp.common.crypto.HmacSha256;
import org.waarp.common.exception.CryptoException;
import org.waarp.common.logging.WaarpLogger;
import org.waarp.common.logging.WaarpLoggerFactory;
import org.waarp.common.utility.Base64;
import org.waarp.openr66.dao.HostDAO;
import org.waarp.openr66.dao.exception.DAOException;
import org.waarp.openr66.pojo.Host;
import org.waarp.openr66.protocol.http.restv2.RestServiceInitializer;
import org.waarp.openr66.protocol.http.restv2.converters.HostConfigConverter;
import org.waarp.openr66.protocol.http.restv2.dbhandlers.AbstractRestDbHandler;
import org.waarp.openr66.protocol.http.restv2.dbhandlers.RequiredRole;

import javax.ws.rs.Consumes;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.NotAllowedException;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.lang.reflect.Method;
import java.text.ParseException;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.netty.handler.codec.http.HttpMethod.OPTIONS;
import static io.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.METHOD_NOT_ALLOWED;
import static io.netty.handler.codec.http.HttpResponseStatus.UNAUTHORIZED;
import static io.netty.handler.codec.http.HttpResponseStatus.UNSUPPORTED_MEDIA_TYPE;
import static javax.ws.rs.core.HttpHeaders.ACCEPT;
import static javax.ws.rs.core.HttpHeaders.AUTHORIZATION;
import static javax.ws.rs.core.HttpHeaders.CONTENT_TYPE;
import static javax.ws.rs.core.HttpHeaders.WWW_AUTHENTICATE;
import static javax.ws.rs.core.MediaType.WILDCARD;
import static org.glassfish.jersey.message.internal.HttpHeaderReader.readAcceptMediaType;
import static org.glassfish.jersey.message.internal.MediaTypes.WILDCARD_TYPE_SINGLETON_LIST;
import static org.glassfish.jersey.message.internal.MediaTypes.convertToString;
import static org.glassfish.jersey.message.internal.MediaTypes.createFrom;
import static org.waarp.common.role.RoleDefault.ROLE;
import static org.waarp.common.role.RoleDefault.ROLE.NOACCESS;
import static org.waarp.openr66.protocol.configuration.Configuration.configuration;
import static org.waarp.openr66.protocol.http.restv2.RestConstants.AUTH_TIMESTAMP;
import static org.waarp.openr66.protocol.http.restv2.RestConstants.AUTH_USER;
import static org.waarp.openr66.protocol.http.restv2.RestConstants.DAO_FACTORY;

/**
 * This class defines hooks called before and after the corresponding
 * {@link AbstractRestDbHandler} when a request is made.
 * These hooks check the user authentication and privileges, as well as the
 * request content type.
 */
public class RestHandlerHook implements HandlerHook {

    private final boolean authenticated;
    private final HmacSha256 hmac;
    private final long delay;
    private static final WaarpLogger logger =
            WaarpLoggerFactory.getLogger(RestHandlerHook.class);

    /**
     * Hook called before a request handler is called. Checks if the REST method
     * is active in the CRUD configuration, checks the request's content type,
     * and finally checks the user authentication.
     *
     * @param request     The request currently being processed.
     * @param responder   The responder to which response must be sent if need be.
     * @param handlerInfo Information about the handler to which the request
     *                    will be sent.
     * @return True if the request can be handed to the handler, False if an
     *         error occurred and a response must be sent immediately.
     */
    @Override
    public boolean preCall(HttpRequest request, HttpResponder responder,
                           HandlerInfo handlerInfo) {

        try {
            AbstractRestDbHandler handler = getHandler(handlerInfo);
            if (!handler.checkCRUD(request)) {
                responder.sendStatus(METHOD_NOT_ALLOWED);
                return false;
            }

            Method handleMethod = getMethod(handler, handlerInfo);
            if (this.authenticated && !request.method().equals(OPTIONS)) {
                String user = checkCredentials(request);
                if (!checkAuthorization(user, handleMethod)) {
                    responder.sendStatus(FORBIDDEN);
                    return false;
                }
            }

            List<MediaType> expectedTypes = getExpectedMediaTypes(handleMethod);
            if (!checkContentType(request, expectedTypes)) {
                DefaultHttpHeaders headers = new DefaultHttpHeaders();
                headers.add(ACCEPT, convertToString(expectedTypes));
                responder.sendStatus(UNSUPPORTED_MEDIA_TYPE, headers);
                return false;
            }

            return true;
        } catch (NotAllowedException e) {
            logger.info(e.getMessage());
            DefaultHttpHeaders headers = new DefaultHttpHeaders();
            headers.add(WWW_AUTHENTICATE, "Basic, HMAC");
            responder.sendStatus(UNAUTHORIZED, headers);
        } catch (InternalServerErrorException e) {
            logger.error(e);
            responder.sendStatus(INTERNAL_SERVER_ERROR);
        } catch (Throwable t) {
            logger.error("RESTv2 Unexpected exception caught ->", t);
            responder.sendStatus(INTERNAL_SERVER_ERROR);
        }
        return false;
    }

    /**
     * Returns the {@code AbstractRestDbHandler} corresponding to the info
     * given as parameter.
     *
     * @param handlerInfo   Information about the handler.
     * @return  The corresponding {@link AbstractRestDbHandler}
     */
    private AbstractRestDbHandler getHandler(HandlerInfo handlerInfo) {
        AbstractRestDbHandler handler = null;
        for (AbstractRestDbHandler h : RestServiceInitializer.handlers) {
            if (h.getClass().getName().equals(handlerInfo.getHandlerName())) {
                handler = h;
            }
        }
        return handler;
    }

    /**
     * Returns the {@link Method} object corresponding to the handler method
     * chosen to process the request. This is needed to check for the
     * annotations present on the method.
     *
     * @param handler       The Handler chosen to process the request.
     * @param handlerInfo   Information about the handler.
     * @return      The corresponding {@link Method} object.
     */
    private Method getMethod(AbstractRestDbHandler handler,
                             HandlerInfo handlerInfo) {
        Method method = null;
        for (Method m : handler.getClass().getMethods()) {
            if (m.getName().equals(handlerInfo.getMethodName())
                    && m.getParameterTypes()[0] == HttpRequest.class
                    && m.getParameterTypes()[1] == HttpResponder.class) {
                method = m;
                break;
            }
        }
        if (method == null) {
            throw new NoSuchMethodError("The handler " + handlerInfo.getHandlerName() +
                    " does not have a method " + handlerInfo.getMethodName());
        }
        return method;
    }

    /**
     * Return a {@link List} of all the {@link MediaType} accepted by the given
     * method for the HTTP request based on the types indicated by the
     * {@link Consumes} annotation. If the annotation is absent, the method will
     * be assumed to accept any type.
     *
     * @param method The {@link Method} to inspect.
     * @return  The list of all acceptable {@link MediaType} for the method.
     */
    private List<MediaType> getExpectedMediaTypes(Method method) {
        List<MediaType> consumedTypes = WILDCARD_TYPE_SINGLETON_LIST;

        if (method.isAnnotationPresent(Consumes.class)) {
            consumedTypes = createFrom(method.getAnnotation(Consumes.class));
        } else {
            logger.warn(String.format("[RESTv2] The method %s of handler %s is missing " +
                            "a '%s' annotation for the expected request content type, " +
                            "the default value '%s' was given instead.",
                    method.getName(), method.getDeclaringClass().getSimpleName(),
                    Consumes.class.getSimpleName(), WILDCARD));
        }

        return consumedTypes;
    }

    /**
     * Checks if the content type of the request is compatible with the expected
     * content type of the method called. If no content type header can be
     * found, the request will be assumed to have the correct content type.
     *
     * @param request       The HTTP request sent by the user.
     * @param consumedTypes A list of the acceptable {@link MediaType} for the request.
     * @return  Returns {@code true} if the request content type is acceptable,
     *          {@code false} otherwise.
     */
    private boolean checkContentType(HttpRequest request, List<MediaType> consumedTypes) {

        String contentTypeHeader = request.headers().get(CONTENT_TYPE);
        if (contentTypeHeader == null || contentTypeHeader.isEmpty()) {
            return true;
        }

        MediaType requestType;
        try {
            requestType = readAcceptMediaType(contentTypeHeader).get(0);
        } catch (ParseException e) {
            return false;
        }
        for (MediaType consumedType : consumedTypes) {
            if (requestType.isCompatible(consumedType)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Checks if the user making the request does exist. If the user does exist,
     * this method returns the user's name and password, otherwise throws a
     * {@link NotAllowedException}.
     *
     * @param request  The request currently being processed.
     * @return Returns {@code null} if the user cannot be authenticated.
     *         Otherwise, returns a pair composed of the username and password.
     */
    private String checkCredentials(HttpRequest request)
            throws CryptoException {

        String authorization = request.headers().get(AUTHORIZATION);

        if (authorization == null) {
            throw new NotAllowedException("Missing header for authentication.");
        }

        Pattern basicPattern = Pattern.compile("(Basic) (\\w+=*)");
        Matcher basicMatcher = basicPattern.matcher(authorization);

        if (basicMatcher.find()) {

            String[] credentials;
            try {
                credentials = new String(
                        Base64.decode(basicMatcher.group(2))).split(":", 2);
            } catch (IOException e) {
                throw new InternalServerErrorException(e);
            }
            if (credentials.length != 2) {
                throw new NotAllowedException("Invalid header for Basic authentication.");
            }
            String user = credentials[0];
            String pswd = credentials[1];

            HostDAO hostDAO = null;
            Host host;
            try {
                hostDAO = DAO_FACTORY.getHostDAO();
                if (!hostDAO.exist(user)) {
                    throw new NotAllowedException("User does not exist.");
                }
                host = hostDAO.select(user);
            } catch (DAOException e) {
                throw new InternalServerErrorException(e);
            } finally {
                if (hostDAO != null) {
                    hostDAO.close();
                }
            }

            String key;
            try {
                key = configuration.getCryptoKey().cryptToHex(pswd);
            } catch (Exception e) {
                throw new CryptoException(
                        "An error occurred when encrypting the password", e);
            }
            if (!Arrays.equals(host.getHostkey(), key.getBytes())) {
                throw new NotAllowedException("Invalid password.");
            }

            return user;
        }

        String authUser = request.headers().get(AUTH_USER);
        String authDate = request.headers().get(AUTH_TIMESTAMP);

        Pattern hmacPattern = Pattern.compile("(HMAC) (\\w+)");
        Matcher hmacMatcher = hmacPattern.matcher(authorization);

        if (hmacMatcher.find() && authUser != null && authDate != null) {

            String authKey = hmacMatcher.group(2);
            DateTime requestDate;
            try {
                requestDate = DateTime.parse(authDate);
            } catch (IllegalArgumentException e) {
                throw new NotAllowedException("Invalid authentication timestamp.");
            }
            DateTime limitTime = requestDate.plus(this.delay);
            if (DateTime.now().isAfter(limitTime)) {
                throw new NotAllowedException("Authentication expired.");
            }

            HostDAO hostDAO = null;
            Host host;
            try {
                hostDAO = DAO_FACTORY.getHostDAO();
                if (!hostDAO.exist(authUser)) {
                    throw new NotAllowedException("User does not exist.");
                }
                host = hostDAO.select(authUser);
            } catch (DAOException e) {
                throw new InternalServerErrorException(e);
            } finally {
                if (hostDAO != null) {
                    hostDAO.close();
                }
            }

            String pswd;
            try {
                pswd = configuration.getCryptoKey().decryptInString(host.getHostkey());
            } catch (Exception e) {
                throw new CryptoException(
                        "An error occurred when decrypting the password", e);
            }

            String key;
            try {
                key = this.hmac.cryptToHex(authDate + authUser + pswd);
            } catch (Exception e) {
                throw new CryptoException("An error occurred when hashing the key", e);
            }

            if (Arrays.equals(key.getBytes(), authKey.getBytes())) {
                throw new NotAllowedException("Invalid password.");
            }

            return authUser;
        }

        throw new NotAllowedException("Missing credentials.");
    }

    /**
     * Checks if the user given as argument is authorized to call the given
     * method.
     *
     * @param user    The hostID of the user making the request.
     * @param method  The method called by the request.
     * @return Returns {@code true} if the user is authorized to make the request,
     *         {@code false} otherwise.
     */
    private boolean checkAuthorization(String user, Method method) {

        ROLE requiredRole = NOACCESS;
        if (method.isAnnotationPresent(RequiredRole.class)) {
            requiredRole = method.getAnnotation(RequiredRole.class).value();
        } else {
            logger.warn(String.format("[RESTv2] The method %s of handler %s is " +
                    "missing a '%s' annotation for the minimum required role, " +
                    "the default value '%s' was given instead.",
                    method.getName(), method.getDeclaringClass().getSimpleName(),
                    RequiredRole.class.getSimpleName(), NOACCESS));
        }
        if (requiredRole == NOACCESS) {
            return true;
        }

        List<ROLE> roles = HostConfigConverter.getRoles(user);
        if (roles != null) {
            for (ROLE roleType : roles) {
                if (requiredRole.isContained(roleType.getAsByte())) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Hook called before a request handler is called.
     *
     * @param httpRequest        The request currently being processed.
     * @param httpResponseStatus The status of the http response generated by
     *                           the request handler.
     * @param handlerInfo Information about the handler to which the request
     *                    was sent.
     */
    @Override
    public void postCall(HttpRequest httpRequest,
                         HttpResponseStatus httpResponseStatus,
                         HandlerInfo handlerInfo) {
    }


    /**
     * Creates a HandlerHook which will check for authentication and signature
     * on incoming request depending on the parameters.
     *
     * @param authenticated Specifies if the HandlerHook will check that incoming
     *                      requests are authenticated.
     * @param hmac          The HMAC key used for hmac authentication.
     * @param delay         The delay for which a hmac signed request is valid.
     */
    public RestHandlerHook(boolean authenticated, HmacSha256 hmac, long delay) {
        this.authenticated = authenticated;
        this.hmac = hmac;
        this.delay = delay;
    }
}
