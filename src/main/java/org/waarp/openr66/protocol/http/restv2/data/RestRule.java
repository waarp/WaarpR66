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

package org.waarp.openr66.protocol.http.restv2.data;

import org.waarp.openr66.pojo.Rule;
import org.waarp.openr66.pojo.RuleTask;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/** RestTransfer rule POJO for Rest HTTP support for R66. */
@SuppressWarnings({"unused", "WeakerAccess"})
public class RestRule {

    /** All the possible ways to order a list of rule objects. */
    public enum Order {
        /** By ruleID, in ascending order. */
        ascRuleID("+id", new Comparator<RestRule>() {
            @Override
            public int compare(RestRule t1, RestRule t2) {
                return t1.ruleID.compareTo(t2.ruleID);
            }
        }),
        /** By ruleID, in descending order. */
        descRuleID("+id", new Comparator<RestRule>() {
            @Override
            public int compare(RestRule t1, RestRule t2) {
                return -t1.ruleID.compareTo(t2.ruleID);
            }
        });

        public final Comparator<RestRule> comparator;
        public final String value;

        Order(String value, Comparator<RestRule> comparator) {
            this.value = value;
            this.comparator = comparator;
        }

        @Override
        public String toString() {
            return this.value;
        }

        public static Order fromString(String str) throws InstantiationException {
            if(str == null || str.isEmpty()) {
                return ascRuleID;
            }
            else {
                for(Order order : Order.values()) {
                    if(order.value.equals(str)) {
                        return order;
                    }
                }
                throw new InstantiationException();
            }
        }
    }

    /** All the different modes of file transfer. */
    public enum ModeTrans {
        /** The requester will be the one sending the file to the requested. */
        send("send"),
        /** The requester will be the one receiving the file from the requested; */
        receive("receive"),
        /** Same as 'send' but with MD5 file signature. */
        send_md5("send+md5"),
        /** Same as 'receive but with MD5 file signature. */
        receive_md5("receive+md5");

        public final String value;

        ModeTrans(String value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return this.value;
        }

        public static ModeTrans fromString(String str) throws InstantiationException {
            if(str == null || str.isEmpty()) {
                return null;
            }
            else {
                for(ModeTrans mode : ModeTrans.values()) {
                    if(mode.value.equals(str)) {
                        return mode;
                    }
                }
                throw new InstantiationException(str);
            }
        }
    }

    /** All the different types of tasks. */
    public enum TaskType {
        /** Log the transfer info to a file or to the server logs. */
        log,
        /** Move the file. */
        move,
        /** Move the file and rename it. */
        moveRename,
        /** Copy the file. */
        copy,
        /** Copy the file and rename it. */
        copyRename,
        /** Execute a script/program. */
        exec,
        /** Execute a script/program which renames the file. */
        execMove,
        /** Execute a script/program, and its output will be used as the new active file in case of error. */
        execOutput,
        /** Execute a java binary. */
        execJava,
        /** Create a new transfer. */
        transfer,
        /** Verify that the file exists at the argument entered as an argument. */
        validFilePath,
        /** Delete a file. */
        delete,
        /** Create a link to a file and rename it. */
        linkRename,
        /** Reschedule a transfer. */
        reschedule,
        /** Compress the file to a .tar archive. */
        tar,
        /** Compress the file to a .zip archive. */
        zip,
        /** Convert the file to another type of encoding. */
        transcode,
        /** Send the transfer info to the snmp server defined in the server configuration. */
        snmp
    }

    /** An object representing a task processed by a host. */
    public static class Task {
        /**
         * The type of the task.
         *
         * @see TaskType
         */
        public TaskType type;

        /** The argument applied to the task where a substitution can occur (current date, file path...). */
        public String argument;

        /** The maximum delay (in ms) for the execution of the task. Set to 0 for no limit. Cannot be negative. */
        public Integer delay;

        public Task() {}

        public Task(RuleTask task) {
            this.type = TaskType.valueOf(task.getType());
        }

        public RuleTask toRuleTask() {
            return new RuleTask(this.type.toString(), this.argument, this.delay);
        }

        public static List<RuleTask> toRuleTaskList(Task[] tasks) {
            List<RuleTask> taskList = new ArrayList<RuleTask>();
            for(Task task : tasks) {
                taskList.add(task.toRuleTask());
            }
            return taskList;
        }

        public static Task[] fromRuleTaskList(List<RuleTask> taskList) {
            Task[] tasks = new Task[taskList.size()];
            for(int i=0; i<taskList.size(); i++) {
                tasks[i] = new Task(taskList.get(i));
            }
            return tasks;
        }
    }

    /** The rule's unique identifier. */
    @NotEmpty
    public String ruleID;

    /** The IDs of the hosts allowed to use this rule for their transfers. */
    public String[] hostsIDs = new String[0];

    /**
     * The file transfer mode used.
     *
     * @see ModeTrans
     */
    public ModeTrans modeTrans;

    /** The folder in which the file will be stored by the receiver after the transfer. */
    @NotEmpty
    public String recvPath = "/in";

    /** The folder in which the file is stored by the sender before being sent. */
    @NotEmpty
    public String sendPath = "out/";

    /** The folder in which the transfer logs will be exported if an export request is made. */
    @NotEmpty
    public String archivePath = "archive/";

    /** The folder in which received files are temporarily stored while the transfer is running. */
    @NotEmpty
    public String workPath = "work/";

    /**
     * The list of tasks to be executed by the receiver before the file transfer.
     *
     * @see Task
     */
    public Task[] rPreTasks = new Task[0];

    /**
     * The list of tasks to be executed by the receiver after the file transfer.
     *
     * @see Task
     */
    public Task[] rPostTasks = new Task[0];

    /**
     * The list of tasks to be executed by the receiver in case of error.
     *
     * @see Task
     */
    public Task[] rErrorTasks = new Task[0];

    /**
     * The list of tasks to be executed by the sender before the file transfer.
     *
     * @see Task
     */
    public Task[] sPreTasks = new Task[0];

    /**
     * The list of tasks to be executed by the sender after the file transfer.
     *
     * @see Task
     */
    public Task[] sPostTasks = new Task[0];

    /**
     * The list of tasks to be executed by the sender in case of error.
     *
     * @see Task
     */
    public Task[] sErrorTasks = new Task[0];


    public RestRule() {}

    public RestRule(Rule rule) {
        this.ruleID = rule.getName();
        this.modeTrans = ModeTrans.values()[rule.getMode()-1];
        this.hostsIDs = rule.getHostids().toArray(new String[0]);
        this.recvPath = rule.getRecvPath();
        this.sendPath = rule.getSendPath();
        this.archivePath = rule.getArchivePath();
        this.workPath = rule.getWorkPath();
        this.rPreTasks = Task.fromRuleTaskList(rule.getRPreTasks());
        this.rPostTasks = Task.fromRuleTaskList(rule.getRPostTasks());
        this.rErrorTasks = Task.fromRuleTaskList(rule.getRErrorTasks());
        this.sPreTasks = Task.fromRuleTaskList(rule.getSPreTasks());
        this.sPostTasks = Task.fromRuleTaskList(rule.getSPostTasks());
        this.sErrorTasks = Task.fromRuleTaskList(rule.getSErrorTasks());
    }

    public Rule toRule() {
        List<RuleTask> rPreTask = Task.toRuleTaskList(this.rPreTasks);
        List<RuleTask> rPostTask = Task.toRuleTaskList(this.rPostTasks);
        List<RuleTask> rErrorTask = Task.toRuleTaskList(this.rErrorTasks);
        List<RuleTask> sPreTask = Task.toRuleTaskList(this.sPreTasks);
        List<RuleTask> sPostTask = Task.toRuleTaskList(this.sPostTasks);
        List<RuleTask> sErrorTask = Task.toRuleTaskList(this.sErrorTasks);

        return new Rule(this.ruleID, this.modeTrans.ordinal()+1, Arrays.asList(this.hostsIDs), this.recvPath,
                this.sendPath, this.archivePath, this.workPath, rPreTask, rPostTask, rErrorTask, sPreTask,
                sPostTask, sErrorTask);
    }

    public static List<RestRule> toRestList(List<Rule> rules) {
        List<RestRule> restRules = new ArrayList<RestRule>();
        for(Rule rule : rules) {
            restRules.add(new RestRule(rule));
        }
        return restRules;
    }
}