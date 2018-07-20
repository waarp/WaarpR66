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

package org.waarp.openr66.protocol.http.restv2.handler;

import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpResponder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.QueryStringDecoder;
import org.waarp.openr66.protocol.http.restv2.RestUtils;
import org.waarp.openr66.protocol.http.restv2.data.rules.Rule;
import org.waarp.openr66.protocol.http.restv2.data.rules.RuleFilter;
import org.waarp.openr66.protocol.http.restv2.data.rules.Rules;
import org.waarp.openr66.protocol.http.restv2.exception.OpenR66RestBadRequestException;
import org.waarp.openr66.protocol.http.restv2.exception.OpenR66RestInternalServerException;

import javax.ws.rs.GET;
import javax.ws.rs.OPTIONS;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * This is the handler for all request made on the 'rules' database, accessible through the "/v2/rules" URI.
 */
@Path("/v2/rules")
public class RulesHandler extends AbstractHttpHandler {

    /** The list of allowed HTTP methods names on the /v2/rules URI. Should only be used by the OPTIONS methods. */
    private static final String[] allow = {"GET", "POST", "OPTIONS"};

    /**
     * The method called when a GET request is made on /v2/rules. If the request is valid, the Http response will
     * contain an array of rule entries. If not, the response will contain a '400 - Bad request' error message.
     *
     * @param request   The Http request made on the resource.
     * @param responder The Http responder, Http response are given to it in order to be sent back.
     */
    @GET
    public void filterRules(HttpRequest request, HttpResponder responder) {
        try {
            QueryStringDecoder decoder = new QueryStringDecoder(request.uri());
            RuleFilter filters = RestUtils.extractFilters(decoder.parameters(), new RuleFilter());
            filters.check();
            Map.Entry<Integer, List<Rule>> answer = Rules.filterRules(filters);
            List<Rule> resultsList = answer.getValue();
            Integer nbResults = answer.getKey();
            String totalResults = "\"totalResults\":" + nbResults.toString();

            ObjectMapper mapper = new ObjectMapper();
            String results = "\"results\":" + mapper.writeValueAsString(resultsList);

            String responseBody = "{" + totalResults + "," + results + "}";
            responder.sendJson(HttpResponseStatus.OK, responseBody);

        } catch (OpenR66RestBadRequestException e) {
            responder.sendJson(HttpResponseStatus.BAD_REQUEST, e.message);
        } catch (JsonProcessingException e) {
            responder.sendJson(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                    "{" +
                            "\"userMessage\": \"Internal Server Error\"" +
                            "\"internalMessage\": \"Could not convert the response body to JSON format.\"" +
                            "\"code\": 100" +
                            "}");
            //TODO: replace '100' with the actual error code for JSON processing error
        }
    }

    /**
     * The method called when a POST request is made on /v2/rules. If the request is valid and the id does not exists
     * in the database, the new rule entry in the request body will be added to the database and sent back in the Http
     * response. If the id does already exist, or if the request is invalid, a '400 - Bad request' error will be sent
     * ²instead.
     *
     * @param request   The Http request made on the resource.
     * @param responder The Http responder, Http response are given to it in order to be sent back.
     */
    @POST
    public void addRule(HttpRequest request, HttpResponder responder) {
        try {
            Rule rule = RestUtils.deserializeRequest(request, Rule.class);

            Rules.addRule(rule);

            String responseBody = Rules.toJsonString(rule);
            responder.sendJson(HttpResponseStatus.CREATED, responseBody);
        } catch (OpenR66RestBadRequestException e) {
            responder.sendJson(HttpResponseStatus.BAD_REQUEST, e.message);
        } catch (OpenR66RestInternalServerException e) {
            responder.sendJson(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.message);
        }
    }

    /**
     * The method called when an OPTIONS request is made on /v2/rules. If the request is valid, a response will be
     * sent with a list of allowed http methods in the header. If the request isn't valid the response will contain a
     * '400 - Bad request' error message.
     *
     * @param request   The Http request made on the resource.
     * @param responder The Http responder, Http response are given to it in order to be sent back.
     */
    @OPTIONS
    public void options(HttpRequest request, HttpResponder responder) {

        HttpHeaders headers = new DefaultHttpHeaders();
        headers.add("allow", Arrays.toString(allow));
        responder.sendStatus(HttpResponseStatus.OK, headers);
    }
}