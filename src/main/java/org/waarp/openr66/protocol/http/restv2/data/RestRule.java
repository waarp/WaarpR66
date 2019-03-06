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

package org.waarp.openr66.protocol.http.restv2.data;

import org.waarp.openr66.pojo.Rule;
import org.waarp.openr66.pojo.RuleTask;

import javax.ws.rs.DefaultValue;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlElements;
import javax.xml.bind.annotation.XmlEnum;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/** RestTransfer rule POJO for Rest HTTP support for R66. */
@SuppressWarnings({"unused", "WeakerAccess"})
@XmlType(name = "rule")
public class RestRule {

    @XmlRootElement(name = "rules")
    public static class RestRuleList {
        @XmlElement(name = "rule")
        public List<RestRule> rules;
    }

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

        /** The {@link Comparator} used to sort a list of {@code RestRule}. */
        public final Comparator<RestRule> comparator;

        /** The {@code Order}'s short name. */
        public final String value;

        Order(String value, Comparator<RestRule> comparator) {
            this.value = value;
            this.comparator = comparator;
        }

        /**
         * @return  The {@code Order} instance's short name.
         */
        @Override
        public String toString() {
            return this.value;
        }

        /**
         * Creates a new {@code Order} instance corresponding to the given
         * order short name.
         *
         * @param str   The order's short name.
         * @return      The corresponding {@code Order} instance.
         * @throws InstantiationException Thrown if the given name does not match
         *                                any possible {@code Order} name.
         */
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
        send,
        /** The requester will be the one receiving the file from the requested. */
        receive,
        /** Same as {@code send}' but with MD5 file signature. */
        send_md5,
        /** Same as {@code receive} but with MD5 file signature. */
        receive_md5
    }

    /** All the different types of pre and post tasks for a transfer. */
    @XmlType(name = "type")
    @XmlEnum
    public enum TaskType {
        /** Log the transfer info to a file or to the server logs. */
        LOG,
        /** Move the file. */
        MOVE,
        /** Move the file and rename it. */
        MOVERENAME,
        /** Copy the file. */
        COPY,
        /** Copy the file and rename it. */
        COPYRENAME,
        /** Execute a script/program. */
        EXEC,
        /** Execute a script/program which renames the file. */
        EXECMOVE,
        /**
         * Execute a script/program, and its output will be used as the new
         * active file in case of error.
         */
        EXECOUTPUT,
        /** Execute a java binary. */
        EXECJAVA,
        /** Create a new transfer. */
        TRANSFER,
        /** Verify that the file exists at the argument entered as an argument. */
        VALIDFILEPATH,
        /** Delete a file. */
        DELETE,
        /** Create a link to a file and rename it. */
        LINKRENAME,
        /** Reschedule a transfer. */
        RESCHEDULE,
        /** Compress the file to a .tar archive. */
        TAR,
        /** Compress the file to a .zip archive. */
        ZIP,
        /** Convert the file to another type of encoding. */
        TRANSCODE,
        /**
         * Send the transfer info to the snmp server defined in the server
         * configuration.
         */
        SNMP
    }

    /** An object representing a task processed by a host. */
    @ConsistencyCheck
    @XmlAccessorType(value = XmlAccessType.FIELD)
    public static class Task {
        /**
         * The type of the task.
         *
         * @see TaskType
         */
        @XmlElement
        @Required
        public TaskType type;

        /**
         * The argument applied to the task where a substitution can occur
         * (ex: current date, filename, etc..).
         */
        @XmlElement
        public String arguments = "";

        /**
         * The delay before which the command becomes out of time.
         * Cannot be negative.
         */
        @Bounds(min = 0, max = Long.MAX_VALUE)
        @XmlElement
        public Integer delay = 0;

        public Task() {}

        public Task(RuleTask task) {
            this.type = TaskType.valueOf(task.getType());
            this.arguments = task.getPath();
            this.delay = task.getDelay();
        }

        public RuleTask toRuleTask() {
            return new RuleTask(this.type.toString(),
                    this.arguments, this.delay);
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
    @Required
    @XmlElement
    public String ruleID;

    /** The IDs of the hosts allowed to use this rule for their transfers. */
    @XmlElement
    @DefaultValue("")
    public String[] hostsIDs;

    /**
     * The file transfer mode used.
     *
     * @see ModeTrans
     */
    @Required
    @XmlElement
    public ModeTrans modeTrans;

    /** The folder in which the receiver will store the file after the transfer. */
    @XmlElement
    public String recvPath;

    /** The folder in which the file is stored by the sender before being sent. */
    @XmlElement
    public String sendPath;

    /**
     * The folder in which the transfer logs will be exported if an export
     * request is made.
     */
    @XmlElement
    public String archivePath;

    /**
     * The folder in which received files are temporarily stored while
     * the transfer is running.
     */
    @XmlElement
    public String workPath;

    /**
     * The list of {@link Task} to be executed by the receiver before the file
     * transfer.
     */
    @XmlElementWrapper(name = "rPreTasks")
    @XmlElements(
            @XmlElement(name = "task")
    )
    @DefaultValue("")
    public Task[] rPreTasks;

    /**
     * The list of {@link Task} to be executed by the receiver after the file
     * transfer.
     */
    @XmlElementWrapper(name = "rPostTasks")
    @XmlElements(
            @XmlElement(name = "task")
    )
    @DefaultValue("")
    public Task[] rPostTasks;

    /**
     * The list of {@link Task} to be executed by the receiver in case of
     * error.
     */
    @XmlElementWrapper(name = "rErrorTasks")
    @XmlElements(
            @XmlElement(name = "task")
    )
    @DefaultValue("")
    public Task[] rErrorTasks;

    /**
     * The list of {@link Task} to be executed by the sender before the file
     * transfer.
     */
    @XmlElementWrapper(name = "sPreTasks")
    @XmlElements(
            @XmlElement(name = "task")
    )
    @DefaultValue("")
    public Task[] sPreTasks;

    /**
     * The list of {@link Task} to be executed by the sender after the file
     * transfer.
     */
    @XmlElementWrapper(name = "sPostTasks")
    @XmlElements(
            @XmlElement(name = "task")
    )
    @DefaultValue("")
    public Task[] sPostTasks;

    /**
     * The list of tasks to be executed by the sender in case of error.
     *
     * @see Task
     */
    @XmlElementWrapper(name = "sErrorTasks")
    @XmlElements(
            @XmlElement(name = "task")
    )
    @DefaultValue("")
    public Task[] sErrorTasks;


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

        return new Rule(this.ruleID, this.modeTrans.ordinal()+1,
                Arrays.asList(this.hostsIDs), this.recvPath, this.sendPath,
                this.archivePath, this.workPath, rPreTask, rPostTask,
                rErrorTask, sPreTask, sPostTask, sErrorTask);
    }

    public static List<RestRule> toRestList(List<Rule> rules) {
        List<RestRule> restRules = new ArrayList<RestRule>();
        for(Rule rule : rules) {
            restRules.add(new RestRule(rule));
        }
        return restRules;
    }
}