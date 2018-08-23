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
import org.waarp.openr66.dao.Filter;
import org.waarp.openr66.dao.RuleDAO;
import org.waarp.openr66.dao.database.DBRuleDAO;
import org.waarp.openr66.dao.exception.DAOException;
import org.waarp.openr66.pojo.Rule;
import org.waarp.openr66.protocol.http.restv2.RestResponses;
import org.waarp.openr66.protocol.http.restv2.RestUtils;
import org.waarp.openr66.protocol.http.restv2.data.RestRule;
import org.waarp.openr66.protocol.http.restv2.exception.OpenR66RestBadRequestException;
import org.waarp.openr66.protocol.http.restv2.exception.OpenR66RestInternalErrorException;
import org.waarp.openr66.protocol.http.restv2.exception.OpenR66RestInvalidEntryException;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.OPTIONS;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * This is the handler for all request made on the 'rules' database, accessible through the "/v2/rules" URI.
 */
@Path("/v2/rules")
public class RulesHandler extends AbstractHttpHandler {

    /**
     * The method called when a GET request is made on /v2/rules. If the request is valid, the Http response will
     * contain an array of rule entries. If not, the response will contain a '400 - Bad request' error message.
     *
     * @param request   The Http request made on the resource.
     * @param responder The Http responder, Http response are given to it in order to be sent back.
     * @param limitStr     Maximum number of entries allowed in the response.
     * @param offsetStr    Index of the first accepted entry in the list of all valid answers.
     * @param orderStr     The criteria used to sort the entries and the direction of the ordering.
     * @param modeTransStr Filter transfer rules that use this kind of transfer mode.
     */
    @GET
    public void filterRules(HttpRequest request, HttpResponder responder,
                            @QueryParam("limit") @DefaultValue("20") String limitStr,
                            @QueryParam("offset") @DefaultValue("0") String offsetStr,
                            @QueryParam("order") @DefaultValue("+id") String orderStr,
                            @QueryParam("modeTrans") List<String> modeTransStr) {
        int limit, offset;
        RestRule.Order order;
        List<RestRule.ModeTrans> modeTrans = new ArrayList<RestRule.ModeTrans>();
        try {
            limit = Integer.parseInt(limitStr);
            order = RestRule.Order.fromString(orderStr);
        } catch(NumberFormatException e) {
            responder.sendJson(HttpResponseStatus.BAD_REQUEST, RestResponses.notANumber(limitStr));
            return;
        } catch (InstantiationException e) {
            responder.sendJson(HttpResponseStatus.BAD_REQUEST, RestResponses.invalidEnum("order", orderStr));
            return;
        }
        try {
            offset = Integer.parseInt(offsetStr);
            for(String str : modeTransStr) {
                modeTrans.add(RestRule.ModeTrans.fromString(str));
            }
        } catch(NumberFormatException e) {
            responder.sendJson(HttpResponseStatus.BAD_REQUEST, RestResponses.notANumber(offsetStr));
            return;
        } catch (InstantiationException e) {
            responder.sendJson(HttpResponseStatus.BAD_REQUEST, RestResponses.invalidEnum("modeTrans", e.getMessage()));
            return;
        }

        if (limit < 0) {
            responder.sendJson(HttpResponseStatus.BAD_REQUEST, RestResponses.negative("limit"));
        } else if (offset < 0) {
            responder.sendJson(HttpResponseStatus.BAD_REQUEST, RestResponses.negative("offset"));
        } else {
            try {
                RuleDAO ruleDAO = RestUtils.factory.getRuleDAO();
                List<Filter> filters = new ArrayList<Filter>();
                for(RestRule.ModeTrans md : modeTrans) {
                    filters.add(new Filter(DBRuleDAO.MODE_TRANS_FIELD, "=", Integer.toString(md.ordinal())));
                }
                List<RestRule> answers;
                if(filters.isEmpty()) {
                    answers = RestRule.toRestList(ruleDAO.getAll());
                } else {
                    answers = RestRule.toRestList(ruleDAO.find(filters));
                }

                Integer nbResults = answers.size();
                Collections.sort(answers, order.comparator);
                String totalResults = "\"totalResults\":" + nbResults.toString();

                List<RestRule> orderedAnswers = new ArrayList<RestRule>();
                for(int i=offset; i<offset+limit && i<answers.size(); i++) {
                    orderedAnswers.add(answers.get(i));
                }

                ObjectMapper mapper = new ObjectMapper();

                String results = "\"results\":" + mapper.writeValueAsString(orderedAnswers);

                String responseBody = "{" + totalResults + "," + results + "}";
                responder.sendJson(HttpResponseStatus.OK, responseBody);

            } catch (DAOException e) {
                responder.sendJson(HttpResponseStatus.INTERNAL_SERVER_ERROR, RestResponses.dbException(e.getCause()));
            } catch (JsonProcessingException e) {
                responder.sendJson(HttpResponseStatus.INTERNAL_SERVER_ERROR, RestResponses.jsonProcessing());
            }
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
            RestRule restRule = RestUtils.deserializeRequest(request, RestRule.class);
            restRule.defaultValues();
            RestUtils.checkEntry(restRule);

            RuleDAO ruleDAO = RestUtils.factory.getRuleDAO();

            if (!ruleDAO.exist(restRule.ruleID)) {
                ruleDAO.insert(restRule.toRule());
                String responseBody = RestUtils.toJsonString(restRule);
                responder.sendJson(HttpResponseStatus.CREATED, responseBody);
            } else {
                responder.sendJson(HttpResponseStatus.BAD_REQUEST, RestResponses.alreadyExisting("rules", restRule.ruleID));
            }
        } catch (OpenR66RestBadRequestException e) {
            responder.sendJson(HttpResponseStatus.BAD_REQUEST, e.message);
        } catch (OpenR66RestInternalErrorException e) {
            responder.sendJson(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.message);
        } catch (JsonProcessingException e) {
            responder.sendJson(HttpResponseStatus.INTERNAL_SERVER_ERROR, RestResponses.jsonProcessing());
        } catch (OpenR66RestInvalidEntryException e) {
            responder.sendJson(HttpResponseStatus.BAD_REQUEST, e.message);
        } catch (DAOException e) {
            responder.sendJson(HttpResponseStatus.INTERNAL_SERVER_ERROR, RestResponses.dbException(e.getCause()));
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
        String allow = RestUtils.options(this.getClass());
        headers.add("allow", allow);
        responder.sendStatus(HttpResponseStatus.OK, headers);
    }
}