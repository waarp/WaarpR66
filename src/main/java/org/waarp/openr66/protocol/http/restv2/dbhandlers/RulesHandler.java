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

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpRequest;
import org.waarp.openr66.dao.Filter;
import org.waarp.openr66.dao.RuleDAO;
import org.waarp.openr66.dao.exception.DAOException;
import org.waarp.openr66.pojo.Rule;
import org.waarp.openr66.protocol.http.restv2.converters.RuleConverter.ModeTrans;
import org.waarp.openr66.protocol.http.restv2.converters.RuleConverter.Order;
import org.waarp.openr66.protocol.http.restv2.errors.Error;
import org.waarp.openr66.protocol.http.restv2.errors.UserErrorException;

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
import java.util.List;

import static io.netty.handler.codec.http.HttpResponseStatus.CREATED;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static javax.ws.rs.core.HttpHeaders.ALLOW;
import static javax.ws.rs.core.HttpHeaders.CONTENT_TYPE;
import static javax.ws.rs.core.MediaType.*;
import static org.waarp.common.role.RoleDefault.ROLE.*;
import static org.waarp.openr66.dao.database.DBRuleDAO.MODE_TRANS_FIELD;
import static org.waarp.openr66.protocol.http.restv2.RestConstants.DAO_FACTORY;
import static org.waarp.openr66.protocol.http.restv2.RestConstants.RULES_HANDLER_URI;
import static org.waarp.openr66.protocol.http.restv2.converters.RuleConverter.Order.ascName;
import static org.waarp.openr66.protocol.http.restv2.converters.RuleConverter.nodeToNewRule;
import static org.waarp.openr66.protocol.http.restv2.converters.RuleConverter.ruleToNode;
import static org.waarp.openr66.protocol.http.restv2.errors.Errors.ALREADY_EXISTING;
import static org.waarp.openr66.protocol.http.restv2.errors.Errors.ILLEGAL_PARAMETER_VALUE;
import static org.waarp.openr66.protocol.http.restv2.utils.JsonUtils.deserializeRequest;
import static org.waarp.openr66.protocol.http.restv2.utils.JsonUtils.nodeToString;
import static org.waarp.openr66.protocol.http.restv2.utils.RestUtils.getMethodList;

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
                            @QueryParam(Params.order) @DefaultValue("ascName")
                                        String order_str,
                            @QueryParam(Params.modeTrans) @DefaultValue("")
                                        String modeTrans_str) {

        List<Error> errors = new ArrayList<Error>();

        int limit = 20, offset = 0;
        Order order = ascName;
        ModeTrans modeTrans = null;
        try {
            limit = Integer.parseInt(limit_str);
            order = Order.valueOf(order_str);
        } catch(NumberFormatException e) {
            errors.add(ILLEGAL_PARAMETER_VALUE(Params.limit, limit_str));
        } catch (IllegalArgumentException e) {
            errors.add(ILLEGAL_PARAMETER_VALUE(Params.order, order_str));
        }
        try {
            offset = Integer.parseInt(offset_str);
            if (!modeTrans_str.isEmpty()) {
                modeTrans = ModeTrans.valueOf(modeTrans_str);
            }
        } catch(NumberFormatException e) {
            errors.add(ILLEGAL_PARAMETER_VALUE(Params.offset, offset_str));
        } catch (IllegalArgumentException e) {
            errors.add(ILLEGAL_PARAMETER_VALUE(Params.modeTrans, e.getMessage()));
        }

        if (limit <= 0) {
            errors.add(ILLEGAL_PARAMETER_VALUE(Params.limit, limit_str));
        } else if (offset < 0) {
            errors.add(ILLEGAL_PARAMETER_VALUE(Params.offset, offset_str));
        }

        if (!errors.isEmpty()) {
            throw new UserErrorException(errors);
        }

        List<Filter> filters = new ArrayList<Filter>();
        if (modeTrans != null) {
            filters.add(new Filter(MODE_TRANS_FIELD, "=",
                    Integer.toString(modeTrans.ordinal())));
        }

        List<Rule> rules;
        RuleDAO ruleDAO = null;
        try {
            ruleDAO = DAO_FACTORY.getRuleDAO();
            rules = ruleDAO.find(filters);
        } catch (DAOException e) {
            throw new InternalServerErrorException(e);
        } finally {
            if (ruleDAO != null) {
                ruleDAO.close();
            }
        }

        int totalResults = rules.size();
        Collections.sort(rules, order.comparator);

        ArrayNode results = new ArrayNode(JsonNodeFactory.instance);
        for (int i = offset; i < offset + limit && i < rules.size(); i++) {
            results.add(ruleToNode(rules.get(i)));
        }

        ObjectNode responseObject = new ObjectNode(JsonNodeFactory.instance);
        responseObject.put("totalResults", totalResults);
        responseObject.set("results", results);
        String responseText = nodeToString(responseObject);
        responder.sendJson(OK, responseText);
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

        ObjectNode requestObject = deserializeRequest(request);
        Rule rule = nodeToNewRule(requestObject);

        RuleDAO ruleDAO = null;
        try {
            ruleDAO = DAO_FACTORY.getRuleDAO();

            if (ruleDAO.exist(rule.getName())) {
                throw new UserErrorException(ALREADY_EXISTING(rule.getName()));
            }

            ruleDAO.insert(rule);
        } catch (DAOException e) {
            throw new InternalServerErrorException(e);
        } finally {
            if (ruleDAO != null) {
                ruleDAO.close();
            }
        }

        ObjectNode responseObject = ruleToNode(rule);
        String responseText = nodeToString(responseObject);

        DefaultHttpHeaders headers = new DefaultHttpHeaders();
        headers.add(CONTENT_TYPE, APPLICATION_JSON);
        headers.add("ruleURI", RULES_HANDLER_URI + rule.getName());

        responder.sendString(CREATED, responseText, headers);
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