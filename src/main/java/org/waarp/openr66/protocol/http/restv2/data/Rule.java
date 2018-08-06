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

import java.util.Comparator;

/** Transfer rule POJO for Rest HTTP support for R66. */
@SuppressWarnings({"unused", "WeakerAccess"})
public class Rule {

    /** All the possible ways to order a list of rule objects. */
    public enum Order {
        /** By ruleID, in ascending order. */
        ascRuleID("+id", new Comparator<Rule>() {
            @Override
            public int compare(Rule t1, Rule t2) {
                return t1.ruleID.compareTo(t2.ruleID);
            }
        }),
        /** By ruleID, in descending order. */
        descRuleID("+id", new Comparator<Rule>() {
            @Override
            public int compare(Rule t1, Rule t2) {
                return -t1.ruleID.compareTo(t2.ruleID);
            }
        });

        public final Comparator<Rule> comparator;
        public final String value;

        Order(String value, Comparator<Rule> comparator) {
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
        /** Verify that the file exists at the path entered as an argument. */
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
        public String arguments;

        /** The maximum delay (in ms) for the execution of the task. Set to 0 for no limit. Cannot be negative. */
        public Integer delay;
    }

    /** The rule's unique identifier. */
    public String ruleID;

    /** The IDs of the hosts allowed to use this rule for their transfers. */
    public String[] hostsIDs;

    /**
     * The file transfer mode used.
     *
     * @see ModeTrans
     */
    public ModeTrans modeTrans;

    /** The folder in which the file will be stored by the receiver after the transfer. */
    public String recvPath;

    /** The folder in which the file is stored by the sender before being sent. */
    public String sendPath;

    /** The folder in which the transfer logs will be exported if an export request is made. */
    public String archivePath;

    /** The folder in which received files are temporarily stored while the transfer is running. */
    public String workPath;

    /**
     * The list of tasks to be executed by the receiver before the file transfer.
     *
     * @see Task
     */
    public Task[] rPreTasks;

    /**
     * The list of tasks to be executed by the receiver after the file transfer.
     *
     * @see Task
     */
    public Task[] rPostTasks;

    /**
     * The list of tasks to be executed by the receiver in case of error.
     *
     * @see Task
     */
    public Task[] rErrorTasks;

    /**
     * The list of tasks to be executed by the sender before the file transfer.
     *
     * @see Task
     */
    public Task[] sPreTasks;

    /**
     * The list of tasks to be executed by the sender after the file transfer.
     *
     * @see Task
     */
    public Task[] sPostTasks;

    /**
     * The list of tasks to be executed by the sender in case of error.
     *
     * @see Task
     */
    public Task[] sErrorTasks;


    /** Initialize all missing optional fields with their default values. */
    public void defaultValues() {
        if(this.hostsIDs == null)       this.hostsIDs = new String[0];
        if(this.recvPath == null)       this.recvPath = "in/";
        if(this.sendPath == null)       this.sendPath = "out/";
        if(this.archivePath == null)    this.archivePath = "arch/";
        if(this.workPath == null)       this.workPath = "work/";
        if(this.rPreTasks == null)      this.rPreTasks = new Task[0];
        if(this.rPostTasks == null)     this.rPostTasks = new Task[0];
        if(this.rErrorTasks == null)    this.rErrorTasks = new Task[0];
        if(this.sPreTasks == null)      this.sPreTasks = new Task[0];
        if(this.sPostTasks == null)     this.sPostTasks = new Task[0];
        if(this.sErrorTasks == null)    this.sErrorTasks = new Task[0];
    }
}