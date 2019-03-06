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

package org.waarp.openr66.protocol.http.restv2.dbhandlers;

import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpRequest;
import org.waarp.openr66.dao.Filter;
import org.waarp.openr66.dao.RuleDAO;
import org.waarp.openr66.dao.exception.DAOException;
import org.waarp.openr66.protocol.http.restv2.data.RequiredRole;
import org.waarp.openr66.protocol.http.restv2.data.RestRule;
import org.waarp.openr66.protocol.http.restv2.errors.Error;

import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.OPTIONS;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.CREATED;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static javax.ws.rs.core.HttpHeaders.ALLOW;
import static javax.ws.rs.core.HttpHeaders.CONTENT_TYPE;
import static javax.ws.rs.core.MediaType.APPLICATION_FORM_URLENCODED;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.MediaType.WILDCARD;
import static org.waarp.common.role.RoleDefault.ROLE.NOACCESS;
import static org.waarp.common.role.RoleDefault.ROLE.READONLY;
import static org.waarp.common.role.RoleDefault.ROLE.RULE;
import static org.waarp.openr66.dao.database.DBRuleDAO.MODE_TRANS_FIELD;
import static org.waarp.openr66.protocol.http.restv2.RestConstants.DAO_FACTORY;
import static org.waarp.openr66.protocol.http.restv2.RestConstants.RULES_HANDLER_URI;
import static org.waarp.openr66.protocol.http.restv2.data.RestRule.Order;
import static org.waarp.openr66.protocol.http.restv2.data.RestRule.Order.ascRuleID;
import static org.waarp.openr66.protocol.http.restv2.errors.Error.serializeErrors;
import static org.waarp.openr66.protocol.http.restv2.errors.ErrorFactory.ALREADY_EXISTING;
import static org.waarp.openr66.protocol.http.restv2.errors.ErrorFactory.ILLEGAL_PARAMETER_VALUE;
import static org.waarp.openr66.protocol.http.restv2.utils.JsonUtils.objectToJson;
import static org.waarp.openr66.protocol.http.restv2.utils.JsonUtils.requestToObject;
import static org.waarp.openr66.protocol.http.restv2.utils.RestUtils.getMethodList;
import static org.waarp.openr66.protocol.http.restv2.utils.RestUtils.getRequestLocale;

/**
 * This is the {@link AbstractRestDbHandler} handling all operations on the
 * host's transfer rule database.
 */
@Path(RULES_HANDLER_URI)
public class RulesHandler extends AbstractRestDbHandler {

    public RulesHandler(byte crud) {
        super(crud);
    }

    private static final class Params {
        static final String limit = "limit";
        static final String offset = "offset";
        static final String order = "order";
        static final String modeTrans = "modeTrans";
    }

    /**
     * Method called to obtain a list of transfer rules based on the filters
     * in the query parameters.
     *
     * @param request   The {@link HttpRequest} made on the resource.
     * @param responder The {@link HttpResponder} which sends the reply to the request.
     * @param limit_str     HTTP query parameter, maximum number of entries
     *                      allowed in the response.
     * @param offset_str    HTTP query parameter, index of the first accepted
     *                      entry in the list of all valid answers.
     * @param order_str     HTTP query parameter, the criteria used to sort
     *                      the entries and the way of ordering them.
     * @param modeTrans_str HTTP query parameter, filter transfer rules that use
     *                      this transfer mode.
     */
    @GET
    @Consumes(APPLICATION_FORM_URLENCODED)
    @RequiredRole(READONLY)
    public void filterRules(HttpRequest request, HttpResponder responder,
                            @QueryParam(Params.limit) @DefaultValue("20")
                                        String limit_str,
                            @QueryParam(Params.offset) @DefaultValue("0")
                                        String offset_str,
                            @QueryParam(Params.order) @DefaultValue("+id")
                                        String order_str,
                            @QueryParam(Params.modeTrans) @DefaultValue("")
                                        String modeTrans_str) {

        List<Error> errors = new ArrayList<Error>();

        int limit = 20, offset = 0;
        Order order = ascRuleID;
        RestRule.ModeTrans modeTrans = null;
        try {
            limit = Integer.parseInt(limit_str);
            order = Order.fromString(order_str);
        } catch(NumberFormatException e) {
            errors.add(ILLEGAL_PARAMETER_VALUE("limit", limit_str));
        } catch (InstantiationException e) {
            errors.add(ILLEGAL_PARAMETER_VALUE("order", order_str));
        }
        try {
            offset = Integer.parseInt(offset_str);
            if (!modeTrans_str.isEmpty()) {
                modeTrans = RestRule.ModeTrans.valueOf(modeTrans_str);
            }
        } catch(NumberFormatException e) {
            errors.add(ILLEGAL_PARAMETER_VALUE("offset", offset_str));
        } catch (IllegalArgumentException e) {
            errors.add(ILLEGAL_PARAMETER_VALUE("modeTrans", e.getMessage()));
        }

        if (limit < 0) {
            errors.add(ILLEGAL_PARAMETER_VALUE("limit", limit_str));
        } else if (offset < 0) {
            errors.add(ILLEGAL_PARAMETER_VALUE("offset", offset_str));
        }

        if(errors.isEmpty()) {
            RuleDAO ruleDAO = null;
            try {
                ruleDAO = DAO_FACTORY.getRuleDAO();
                List<Filter> filters = new ArrayList<Filter>();
                if (modeTrans != null) {
                    filters.add(new Filter(MODE_TRANS_FIELD, "=",
                            Integer.toString(modeTrans.ordinal())));
                }
                List<RestRule> answers;
                if (filters.isEmpty()) {
                    answers = RestRule.toRestList(ruleDAO.getAll());
                } else {
                    answers = RestRule.toRestList(ruleDAO.find(filters));
                }

                Integer nbResults = answers.size();
                Collections.sort(answers, order.comparator);

                List<RestRule> orderedAnswers = new ArrayList<RestRule>();
                for (int i = offset; i < offset + limit && i < answers.size(); i++) {
                    orderedAnswers.add(answers.get(i));
                }

                HashMap<String, Object> jsonObject = new HashMap<String, Object>();
                jsonObject.put("results", orderedAnswers);
                jsonObject.put("totalResults", nbResults);
                String responseBody = objectToJson(jsonObject);
                responder.sendJson(OK, responseBody);

            } catch (DAOException e) {
                throw new InternalServerErrorException(e);
            } finally {
                if (ruleDAO != null) {
                    ruleDAO.close();
                }
            }
        }
        else {
            Locale lang = getRequestLocale(request);
            responder.sendJson(BAD_REQUEST, serializeErrors(errors, lang));
        }
    }

    /**
     * Method called to add a new transfer rule in the server database.
     * The reply will contain the created entry in JSON format, unless an
     * unexpected error prevents it or if the request is invalid.
     *
     * @param request   The {@link HttpRequest} made on the resource.
     * @param responder The {@link HttpResponder} which sends the reply to the request.
     */
    @POST
    @Consumes(APPLICATION_JSON)
    @RequiredRole(RULE)
    public void addRule(HttpRequest request, HttpResponder responder) {

        RestRule restRule = requestToObject(request, RestRule.class, true);

        RuleDAO ruleDAO = null;
        try {
            ruleDAO = DAO_FACTORY.getRuleDAO();

            if (!ruleDAO.exist(restRule.ruleID)) {
                ruleDAO.insert(restRule.toRule());
                String responseBody = objectToJson(restRule);
                DefaultHttpHeaders headers = new DefaultHttpHeaders();
                headers.add(CONTENT_TYPE, APPLICATION_JSON);
                headers.add("rule-uri", RULES_HANDLER_URI + restRule.ruleID);
                responder.sendString(CREATED, responseBody, headers);
            } else {
                Error error = ALREADY_EXISTING(restRule.ruleID);
                Locale lang = getRequestLocale(request);
                responder.sendJson(BAD_REQUEST, error.serialize(lang));
            }
        } catch (DAOException e) {
            throw new InternalServerErrorException(e);
        } finally {
            if (ruleDAO != null) {
                ruleDAO.close();
            }
        }

    }

    /**
     * Method called to get a list of all allowed HTTP methods on this entry
     * point. The HTTP methods are sent as an array in the reply's headers.
     *
     * @param request   The {@link HttpRequest} made on the resource.
     * @param responder The {@link HttpResponder} which sends the reply
     *                  to the request.
     */
    @OPTIONS
    @Consumes(WILDCARD)
    @RequiredRole(NOACCESS)
    public void options(HttpRequest request, HttpResponder responder) {
        DefaultHttpHeaders headers = new DefaultHttpHeaders();
        String allow = getMethodList(this.getClass(), this.crud);
        headers.add(ALLOW, allow);
        responder.sendStatus(OK, headers);
    }
}