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

package org.waarp.openr66.protocol.http.restv2.converters;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.waarp.openr66.context.task.TaskType;
import org.waarp.openr66.pojo.Rule;
import org.waarp.openr66.pojo.RuleTask;
import org.waarp.openr66.protocol.http.restv2.errors.UserErrorException;
import org.waarp.openr66.protocol.http.restv2.errors.Error;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.waarp.openr66.protocol.http.restv2.converters.RuleConverter.FieldNames.*;
import static org.waarp.openr66.protocol.http.restv2.errors.Errors.FIELD_NOT_ALLOWED;
import static org.waarp.openr66.protocol.http.restv2.errors.Errors.ILLEGAL_FIELD_VALUE;
import static org.waarp.openr66.protocol.http.restv2.errors.Errors.MISSING_FIELD;
import static org.waarp.openr66.protocol.http.restv2.errors.Errors.UNKNOWN_FIELD;

/** RestTransferUtils rule POJO for Rest HTTP support for R66. */
public final class RuleConverter {

    /** Prevents the default constructor from being called. */
    private RuleConverter() throws InstantiationException {
        throw new InstantiationException(this.getClass().getName() +
                " cannot be instantiated.");
    }

    @SuppressWarnings("unused")
    public static final class FieldNames {
        public static final String RULE_NAME = "name";
        public static final String HOST_IDS = "hostIds";
        public static final String MODE_TRANS = "mode";
        public static final String RECV_PATH = "recvPath";
        public static final String SEND_PATH = "sendPath";
        public static final String ARCHIVE_PATH = "archivePath";
        public static final String WORK_PATH = "workPath";
        public static final String R_PRE_TASKS = "rPreTasks";
        public static final String R_POST_TASKS = "rPostTasks";
        public static final String R_ERROR_TASKS = "sPreTasks";
        public static final String S_PRE_TASKS = "rPreTasks";
        public static final String S_POST_TASKS = "rPostTasks";
        public static final String S_ERROR_TASKS = "sErrorTasks";
        public static final String TASK_TYPE = "type";
        public static final String TASK_ARGUMENTS = "arguments";
        public static final String TASK_DELAY = "delay";
    }

    /** All the possible ways to order a list of rule objects. */
    public enum Order {
        /** By ruleID, in ascending order. */
        ascName(new Comparator<Rule>() {
            @Override
            public int compare(Rule t1, Rule t2) {
                return t1.getName().compareTo(t2.getName());
            }
        }),
        /** By ruleID, in descending order. */
        descName(new Comparator<Rule>() {
            @Override
            public int compare(Rule t1, Rule t2) {
                return -t1.getName().compareTo(t2.getName());
            }
        });

        /** The {@link Comparator} used to sort a list of {@code RestRule}. */
        public final Comparator<Rule> comparator;

        Order(Comparator<Rule> comparator) {
            this.comparator = comparator;
        }
    }

    public enum ModeTrans {
        send(1),
        receive(2),
        sendMD5(3),
        receiveMD5(4);

        public final int code;

        ModeTrans(int code) {
            this.code = code;
        }

        public static ModeTrans fromCode(int code) {
            for (ModeTrans mode : values()) {
                if (mode.code == code) {
                    return mode;
                }
            }
            throw new IllegalArgumentException();
        }
    }


    public static ObjectNode ruleToNode(Rule rule) {
        ObjectNode node = new ObjectNode(JsonNodeFactory.instance);
        node.put(RULE_NAME, rule.getName());
        node.putArray(HOST_IDS).addAll(getHostIdsArray(rule.getHostids()));
        node.put(MODE_TRANS, ModeTrans.fromCode(rule.getMode()).name());
        node.put(RECV_PATH, rule.getRecvPath());
        node.put(SEND_PATH, rule.getSendPath());
        node.put(ARCHIVE_PATH, rule.getArchivePath());
        node.put(WORK_PATH, rule.getWorkPath());
        node.set(R_PRE_TASKS, getTaskArray(rule.getRPreTasks()));
        node.set(R_POST_TASKS, getTaskArray(rule.getRPostTasks()));
        node.set(R_ERROR_TASKS, getTaskArray(rule.getRErrorTasks()));
        node.set(S_PRE_TASKS, getTaskArray(rule.getSPreTasks()));
        node.set(S_POST_TASKS, getTaskArray(rule.getSPostTasks()));
        node.set(S_ERROR_TASKS, getTaskArray(rule.getSErrorTasks()));

        return node;
    }

    public static Rule nodeToNewRule(ObjectNode object)
            throws UserErrorException {
        Rule defaultRule = new Rule(null, -1, new ArrayList<String>(), "", "", "", "",
                new ArrayList<RuleTask>(), new ArrayList<RuleTask>(),
                new ArrayList<RuleTask>(), new ArrayList<RuleTask>(),
                new ArrayList<RuleTask>(), new ArrayList<RuleTask>());

        return parseNode(object, defaultRule);
    }

    public static Rule nodeToUpdatedRule(ObjectNode object, Rule oldRule)
            throws UserErrorException {
        return parseNode(object, oldRule);
    }



    private static ArrayNode getHostIdsArray(List<String> hostIds) {
        ArrayNode array = new ArrayNode(JsonNodeFactory.instance);
        for (String host : hostIds) {
            array.add(host);
        }
        return array;
    }

    private static ArrayNode getTaskArray(List<RuleTask> tasks) {
        ArrayNode array = new ArrayNode(JsonNodeFactory.instance);
        for (RuleTask task : tasks) {
            ObjectNode object = new ObjectNode(JsonNodeFactory.instance);
            object.put(TASK_TYPE, task.getType());
            object.put(TASK_ARGUMENTS, task.getPath());
            object.put(TASK_DELAY, task.getDelay());
            array.add(object);
        }
        return array;
    }

    private static List<Error> checkRequiredFields(Rule rule) {
        List<Error> errors = new ArrayList<Error>();
        if (rule.getName() == null || rule.getName().isEmpty()) {
            errors.add(MISSING_FIELD(RULE_NAME));
        }
        if (rule.getMode() == -1) {
            errors.add(MISSING_FIELD(MODE_TRANS));
        }
        return errors;
    }

    private static List<RuleTask> parseTasks(ArrayNode array, String fieldName)
            throws UserErrorException {
        List<RuleTask> result = new ArrayList<RuleTask>();
        List<Error> errors = new ArrayList<Error>();

        Iterator<JsonNode> elements = array.elements();
        while (elements.hasNext()) {
            JsonNode element = elements.next();

            if (element.isObject()) {
                RuleTask task = new RuleTask("", "", 0);
                Iterator<Map.Entry<String, JsonNode>> fields = element.fields();
                while (fields.hasNext()) {
                    Map.Entry<String, JsonNode> field = fields.next();
                    String name = field.getKey();
                    JsonNode value = field.getValue();

                    if (name.equalsIgnoreCase(TASK_TYPE)) {
                        if (value.isTextual()) {
                            try {
                                TaskType type = TaskType.valueOf(value.asText());
                                task.setType(type.name());
                            } catch (IllegalArgumentException e) {
                                errors.add(ILLEGAL_FIELD_VALUE(TASK_TYPE, value.toString()));
                            }
                        } else {
                            errors.add(ILLEGAL_FIELD_VALUE(TASK_TYPE, value.toString()));
                        }
                    }
                    else if (name.equalsIgnoreCase(TASK_ARGUMENTS)) {
                        if (value.isTextual()) {
                            task.setPath(value.asText());
                        } else {
                            errors.add(ILLEGAL_FIELD_VALUE(TASK_ARGUMENTS, value.toString()));
                        }
                    }
                    else if (name.equalsIgnoreCase(TASK_DELAY)) {
                        if (value.canConvertToInt() && value.asInt() >= 0) {
                            task.setDelay(value.asInt());
                        } else {
                            errors.add(ILLEGAL_FIELD_VALUE(TASK_DELAY, value.toString()));
                        }
                    }
                    else {
                        errors.add(UNKNOWN_FIELD(name));
                    }
                }
                if (task.getType().isEmpty()) {
                    errors.add(MISSING_FIELD(TASK_TYPE));
                } else {
                    result.add(task);
                }
            } else {
                errors.add(ILLEGAL_FIELD_VALUE(fieldName, element.toString()));
            }
        }

        if (errors.isEmpty()) {
            return result;
        } else {
            throw new UserErrorException(errors);
        }
    }


    private static Rule parseNode(ObjectNode object, Rule rule)
            throws UserErrorException {
        List<Error> errors = new ArrayList<Error>();

        Iterator<Map.Entry<String, JsonNode>> fields = object.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            String name = field.getKey();
            JsonNode value = field.getValue();

            if (name.equalsIgnoreCase(RULE_NAME)) {
                if (value.isTextual()) {
                    if (rule.getName() == null) {
                        rule.setName(value.asText());
                    } else if (!rule.getName().equals(value.asText())) {
                        errors.add(FIELD_NOT_ALLOWED(RULE_NAME));
                    }
                } else {
                    errors.add(ILLEGAL_FIELD_VALUE(RULE_NAME, value.toString()));
                }
            }
            else if (name.equalsIgnoreCase(HOST_IDS)) {
                if (value.isArray()) {
                    List<String> hosts = new ArrayList<String>();
                    Iterator<JsonNode> elements = value.elements();
                    while (elements.hasNext()) {
                        JsonNode element = elements.next();
                        if (element.isTextual()) {
                            hosts.add(element.asText());
                        } else {
                            errors.add(ILLEGAL_FIELD_VALUE(HOST_IDS, value.toString()));
                        }
                    }
                    rule.setHostids(hosts);
                } else {
                    errors.add(ILLEGAL_FIELD_VALUE(HOST_IDS, value.toString()));
                }
            }
            else if (name.equalsIgnoreCase(MODE_TRANS)) {
                if (value.isTextual()) {
                    try {
                        ModeTrans modeTrans = ModeTrans.valueOf(value.asText());
                        rule.setMode(modeTrans.code);
                    } catch (IllegalArgumentException e) {
                        errors.add(ILLEGAL_FIELD_VALUE(MODE_TRANS, value.toString()));
                    }
                } else {
                    errors.add(ILLEGAL_FIELD_VALUE(MODE_TRANS, value.toString()));
                }
            }
            else if (name.equalsIgnoreCase(RECV_PATH)) {
                if (value.isTextual()) {
                    rule.setRecvPath(value.asText());
                } else {
                    errors.add(ILLEGAL_FIELD_VALUE(RECV_PATH, value.toString()));
                }
            }
            else if (name.equalsIgnoreCase(SEND_PATH)) {
                if (value.isTextual()) {
                    rule.setSendPath(value.asText());
                } else {
                    errors.add(ILLEGAL_FIELD_VALUE(SEND_PATH, value.toString()));
                }
            }
            else if (name.equalsIgnoreCase(ARCHIVE_PATH)) {
                if (value.isTextual()) {
                    rule.setArchivePath(value.asText());
                } else {
                    errors.add(ILLEGAL_FIELD_VALUE(ARCHIVE_PATH, value.toString()));
                }
            }
            else if (name.equalsIgnoreCase(WORK_PATH)) {
                if (value.isTextual()) {
                    rule.setWorkPath(value.asText());
                } else {
                    errors.add(ILLEGAL_FIELD_VALUE(WORK_PATH, value.toString()));
                }
            }
            else if (name.equalsIgnoreCase(R_PRE_TASKS)) {
                if (value.isArray()) {
                    rule.setRPreTasks(parseTasks((ArrayNode) value, R_PRE_TASKS));
                } else {
                    errors.add(ILLEGAL_FIELD_VALUE(R_PRE_TASKS, value.toString()));
                }
            }
            else if (name.equalsIgnoreCase(R_POST_TASKS)) {
                if (value.isArray()) {
                    rule.setRPostTasks(parseTasks((ArrayNode) value, R_POST_TASKS));
                } else {
                    errors.add(ILLEGAL_FIELD_VALUE(R_POST_TASKS, value.toString()));
                }
            }
            else if (name.equalsIgnoreCase(R_ERROR_TASKS)) {
                if (value.isArray()) {
                    rule.setRErrorTasks(parseTasks((ArrayNode) value, R_ERROR_TASKS));
                } else {
                    errors.add(ILLEGAL_FIELD_VALUE(R_ERROR_TASKS, value.toString()));
                }
            }
            else if (name.equalsIgnoreCase(S_PRE_TASKS)) {
                if (value.isArray()) {
                    rule.setSPreTasks(parseTasks((ArrayNode) value, S_PRE_TASKS));
                } else {
                    errors.add(ILLEGAL_FIELD_VALUE(S_PRE_TASKS, value.toString()));
                }
            }
            else if (name.equalsIgnoreCase(S_POST_TASKS)) {
                if (value.isArray()) {
                    rule.setSPostTasks(parseTasks((ArrayNode) value, S_POST_TASKS));
                } else {
                    errors.add(ILLEGAL_FIELD_VALUE(S_POST_TASKS, value.toString()));
                }
            }
            else if (name.equalsIgnoreCase(S_ERROR_TASKS)) {
                if (value.isArray()) {
                    rule.setSErrorTasks(parseTasks((ArrayNode) value, S_ERROR_TASKS));
                } else {
                    errors.add(ILLEGAL_FIELD_VALUE(S_ERROR_TASKS, value.toString()));
                }
            }
            else {
                errors.add(UNKNOWN_FIELD(name));
            }
        }

        errors.addAll(checkRequiredFields(rule));

        if (errors.isEmpty()) {
            return rule;
        } else {
            throw new UserErrorException(errors);
        }
    }
}