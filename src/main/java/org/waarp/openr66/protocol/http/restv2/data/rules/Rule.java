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

import java.util.List;


public class Rule {

    /** All the different modes of file transfer. */
    public enum ModeTrans {
        /** The requester will be the one sending the file to the requested. */
        send,
        /** The requester will be the one receiving the file from the requested; */
        receive,
        /** Same as 'send' but with MD5 file signature. */
        send_md5,
        /** Same as 'receive but with MD5 file signature. */
        receive_md5,
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
        public int delay;
    }

    /** The rule's unique identifier. */
    public String ruleID;

    /** The IDs of the hosts allowed to use this rule for their transfers. */
    public List<String> hostsIDs;

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
    public List<Task> rPreTasks;

    /**
     * The list of tasks to be executed by the receiver after the file transfer.
     *
     * @see Task
     */
    public List<Task> rPostTasks;

    /**
     * The list of tasks to be executed by the receiver in case of error.
     *
     * @see Task
     */
    public List<Task> rErrorTasks;

    /**
     * The list of tasks to be executed by the sender before the file transfer.
     *
     * @see Task
     */
    public List<Task> sPreTasks;

    /**
     * The list of tasks to be executed by the sender after the file transfer.
     *
     * @see Task
     */
    public List<Task> sPostTasks;

    /**
     * The list of tasks to be executed by the sender in case of error.
     *
     * @see Task
     */
    public List<Task> sErrorTasks;
}