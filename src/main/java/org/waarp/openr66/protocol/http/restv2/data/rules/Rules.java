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

package org.waarp.openr66.protocol.http.restv2.data.rules;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.waarp.openr66.protocol.http.restv2.RestUtils;
import org.waarp.openr66.protocol.http.restv2.exception.ImpossibleException;
import org.waarp.openr66.protocol.http.restv2.exception.OpenR66RestBadRequestException;
import org.waarp.openr66.protocol.http.restv2.exception.OpenR66RestIdNotFoundException;
import org.waarp.openr66.protocol.http.restv2.exception.OpenR66RestInternalServerException;
import org.waarp.openr66.protocol.http.restv2.testdatabases.RulesDatabase;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** This class consists exclusively of static methods that operate on or return transfer rules. */
public final class Rules {

    /**
     * Returns the rule entry corresponding to the id passed as argument.
     *
     * @param id The desired rule id.
     * @return The corresponding rule entry.
     * @throws OpenR66RestIdNotFoundException Thrown if the id does not exist in the database.
     */
    public static Rule loadRule(String id) throws OpenR66RestIdNotFoundException {
        for (Rule rule : RulesDatabase.rulesDb) {
            if (rule.ruleID.equals(id)) {
                return rule;
            }
        }

        throw new OpenR66RestIdNotFoundException();
    }

    /**
     * Removes the corresponding rule from the database if it exists.
     *
     * @param id The id of the rule to delete.
     * @throws OpenR66RestIdNotFoundException Thrown if the rule does not exist in the database.
     */
    public static void deleteRule(String id) throws OpenR66RestIdNotFoundException {
        Rule toDelete = loadRule(id);
        RulesDatabase.rulesDb.remove(toDelete);
    }

    /**
     * Replaces this rule entry with the one passed as argument.
     *
     * @param newRule The new entry that replaces this one.
     * @throws OpenR66RestIdNotFoundException Thrown if the rule does not exist in the database.
     */
    public static void replace(String id, Rule newRule) throws OpenR66RestIdNotFoundException,
            OpenR66RestBadRequestException {
        for (Field field : Rule.class.getFields()) {
            try {
                Object value = field.get(newRule);
                if (RestUtils.isIllegal(value)) {
                    throw OpenR66RestBadRequestException.emptyField(field.getName());
                }
            } catch (IllegalAccessException e) {
                throw new ImpossibleException(e);
            }
        }
        Rule oldRule = loadRule(id);
        RulesDatabase.rulesDb.remove(oldRule);
        RulesDatabase.rulesDb.add(newRule);
    }

    /**
     * Adds a rule entry to the database if the entry is a valid one.
     *
     * @param rule The entry to add to the database.
     * @throws OpenR66RestBadRequestException Thrown if the request is invalid or if a rule with the same id already
     *                                        exists in the database
     */
    public static void addRule(Rule rule) throws OpenR66RestBadRequestException {
        try {
            //check if the rule already exists
            loadRule(rule.ruleID);
            throw OpenR66RestBadRequestException.alreadyExisting("rule", rule.ruleID);
        } catch (OpenR66RestIdNotFoundException e) {
            for (Field field : Rule.class.getFields()) {
                try {
                    Object value = field.get(rule);
                    if (RestUtils.isIllegal(value)) {
                        throw OpenR66RestBadRequestException.emptyField(field.getName());
                    }
                } catch (IllegalAccessException ignore) {
                    throw new ImpossibleException(e);
                }
            }
            RulesDatabase.rulesDb.add(rule);
        }
    }

    /**
     * Returns the list of all rules in the database that fit the filters passed as arguments.
     *
     * @param limit
     * @param offset
     * @param order
     * @param modeTrans
     * @return A map entry associating the total number of valid entries and the list of entries that will actually be
     * returned in the response.
     * @throws OpenR66RestBadRequestException Thrown if one of the filters is invalid.
     */
    public static Map.Entry<Integer, List<Rule>> filterRules(Integer limit, Integer offset, Rule.Order order,
                                                             List<Rule.ModeTrans> modeTrans)
            throws OpenR66RestBadRequestException {

        List<Rule> results = new ArrayList<Rule>();

        for (Rule rule : RulesDatabase.rulesDb) {
            if (modeTrans.isEmpty() || modeTrans.contains(rule.modeTrans)) {
                results.add(rule);
            }
        }
        Integer total = results.size();
        Collections.sort(results, order.comparator);

        List<Rule> answers = new ArrayList<Rule>();
        for (int i = offset; (i < offset + limit && i < results.size()); i++) {
            answers.add(results.get(i));
        }

        return new HashMap.SimpleImmutableEntry<Integer, List<Rule>>(total, answers);
    }

    /**
     * Transforms the rule object into a String in JSON format.
     *
     * @return The rule as a JSON String.
     * @throws OpenR66RestInternalServerException Thrown if the rule cannot be transformed in JSON.
     */
    public static String toJsonString(Rule rule) throws OpenR66RestInternalServerException {
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.writeValueAsString(rule);
        } catch (JsonProcessingException e) {
            throw OpenR66RestInternalServerException.jsonProcessing();
        }
    }
}
