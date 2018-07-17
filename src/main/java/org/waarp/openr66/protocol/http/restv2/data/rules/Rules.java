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
import org.waarp.openr66.protocol.http.restv2.exception.OpenR66RestBadRequestException;
import org.waarp.openr66.protocol.http.restv2.exception.OpenR66RestEmptyParamException;
import org.waarp.openr66.protocol.http.restv2.exception.OpenR66RestIdNotFoundException;
import org.waarp.openr66.protocol.http.restv2.exception.OpenR66RestInternalServerException;
import org.waarp.openr66.protocol.http.restv2.test.TestRule;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
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
        //TODO: replace by a real database request
        for (Rule rule : TestRule.rulesDb) {
            if (rule.ruleID.equals(id)) {
                return rule;
            }
        }

        throw new OpenR66RestIdNotFoundException(
                "{" +
                        "\"userMessage\":\"Id not found\"," +
                        "\"internalMessage\":\"The rule of id '" + id + "' does not exist.\"" +
                        "}"
        );
    }

    /**
     * Removes the corresponding rule from the database if it exists.
     *
     * @param id The id of the rule to delete.
     * @throws OpenR66RestIdNotFoundException Thrown if the rule does not exist in the database.
     */
    public static void deleteRule(String id) throws OpenR66RestIdNotFoundException {
        //TODO: replace by a real database request
        Rule toDelete = loadRule(id);
        TestRule.rulesDb.remove(toDelete);
    }

    /**
     * Replaces this rule entry with the one passed as argument.
     *
     * @param newRule The new entry that replaces this one.
     * @throws OpenR66RestIdNotFoundException Thrown if the rule does not exist in the database.
     */
    public static void replace(String id, Rule newRule) throws OpenR66RestIdNotFoundException {
        for (Field field : Rule.class.getFields()) {
            try {
                Object value = field.get(newRule);
                if (value == null || value.toString().equals("")) {
                    throw OpenR66RestBadRequestException.emptyField(field.getName());
                }
            } catch (IllegalAccessException e) {
                assert false;
            }
        }
        //TODO: replace by a real database request
        Rule oldRule = loadRule(id);
        TestRule.rulesDb.remove(oldRule);
        TestRule.rulesDb.add(newRule);
    }

    /**
     * Replaces this rule entry with the one passed as argument.
     *
     * @param newRule The new entry that replaces this one.
     * @throws OpenR66RestIdNotFoundException Thrown if the rule does not exist in the database.
     */
    public static void update(String id, Rule newRule) throws OpenR66RestIdNotFoundException {
        Rule oldRule = loadRule(id);
        for (Field field : Rule.class.getFields()) {
            try {
                Object value = field.get(newRule);
                if (value == null || value.toString().equals("")) {
                    field.set(newRule, field.get(oldRule));
                }
            } catch (IllegalAccessException e) {
                assert false;
            }
        }
        //TODO: replace by a real database request
        TestRule.rulesDb.remove(oldRule);
        TestRule.rulesDb.add(newRule);
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
                    if (value == null || value.toString().equals("")) {
                        throw OpenR66RestBadRequestException.emptyField(field.getName());
                    }
                } catch (IllegalAccessException ignore) {
                    assert false;
                }
            }
            TestRule.rulesDb.add(rule);
        }
    }

    /**
     * Extract the parameters Map outputted by the query decoder and creates a Filter object with it.
     *
     * @param params The Map associating the query parameters names with their respecting values as lists of Strings.
     * @return The Filter object representing the filter applied to the database query.
     * @throws OpenR66RestBadRequestException Thrown if one of the parameters has an invalid value or no value at all.
     */
    public static RuleFilter extractRuleFilter(Map<String, List<String>> params) throws OpenR66RestBadRequestException {
        RuleFilter filters = new RuleFilter();
        for (Map.Entry<String, List<String>> param : params.entrySet()) {
            String name = param.getKey();
            try {
                List<String> values = param.getValue();
                for (String value : values) {
                    if (value.isEmpty()) {
                        throw new OpenR66RestEmptyParamException();
                    }
                }
                boolean isSingleton = values.size() == 1;

                if (name.equals("limit") && isSingleton) {
                    filters.limit = Integer.valueOf(values.get(0));
                } else if (name.equals("offset") && isSingleton) {
                    filters.offset = Integer.valueOf(values.get(0));
                } else if (name.equals("order") && isSingleton) {
                    filters.order = RuleFilter.Order.valueOf(values.get(0));
                } else if (name.equals("modeTrans")) {
                    List<Rule.ModeTrans> modeTrans = new ArrayList<Rule.ModeTrans>();
                    for (String str : values) {
                        modeTrans.add(Rule.ModeTrans.valueOf(str));
                    }
                    filters.modeTrans = modeTrans;
                } else {
                    throw new OpenR66RestBadRequestException(
                            "{" +
                                    "\"userMessage\":\"Unknown parameter\"," +
                                    "\"internalMessage\":\"The parameter '" + name + "' is unknown.\"" +
                                    "}"
                    );
                }
            } catch (OpenR66RestEmptyParamException e) {
                throw OpenR66RestBadRequestException.emptyParameter(name);
            } catch (NumberFormatException e) {
                throw new OpenR66RestBadRequestException(
                        "{" +
                                "\"userMessage\":\"Expected number\"," +
                                "\"internalMessage\":\"The parameter '" + name + "' was expecting a number.\"" +
                                "}"
                );
            } catch (IllegalArgumentException e) {
                throw new OpenR66RestBadRequestException(
                        "{" +
                                "\"userMessage\":\"Illegal value\"," +
                                "\"internalMessage\":\"The parameter '" + name + "' has an illegal value.\"" +
                                "}"
                );
            }
        }
        return filters;
    }

    /**
     * Returns the list of all rules in the database that fit the filters passed as arguments.
     *
     * @param filters The different filters used to generate the desired rule list.
     * @return A map entry associating the total number of valid entries and the list of entries that will actually be
     * returned in the response.
     * @throws OpenR66RestBadRequestException Thrown if one of the filters is invalid.
     */
    public static Map.Entry<Integer, List<Rule>> filterRules(RuleFilter filters)
            throws OpenR66RestBadRequestException {

        //TODO: replace by a real database request
        List<Rule> results = new ArrayList<Rule>();
        for (Rule rule : TestRule.rulesDb) {
            if (filters.modeTrans == null || filters.modeTrans.contains(rule.modeTrans)) {
                results.add(rule);
            }
        }
        Integer total = results.size();
        switch (filters.order) {
            case ascRuleID:
                Collections.sort(results, new Comparator<Rule>() {
                    @Override
                    public int compare(Rule t1, Rule t2) {
                        return t1.ruleID.compareTo(t2.ruleID);
                    }
                });
                break;
            case descRuleID:
                Collections.sort(results, new Comparator<Rule>() {
                    @Override
                    public int compare(Rule t1, Rule t2) {
                        return -t1.ruleID.compareTo(t2.ruleID);
                    }
                });
                break;
        }

        List<Rule> answers = new ArrayList<Rule>();
        for (int i = filters.offset; (i < filters.offset + filters.limit && i < results.size()); i++) {
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
