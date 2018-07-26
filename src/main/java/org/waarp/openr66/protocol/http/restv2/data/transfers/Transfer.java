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

package org.waarp.openr66.protocol.http.restv2.data.transfers;

import org.waarp.openr66.protocol.http.restv2.RestUtils;
import org.waarp.openr66.protocol.http.restv2.testdatabases.TransfersDatabase;

import java.util.Calendar;

/** Transfer POJO for Rest HTTP support for R66. */
@SuppressWarnings({"unused", "WeakerAccess"})
public class Transfer {

    /** All the possible global steps for a transfer. */
    public enum GlobalStep {
        /** Not currently doing anything */
        noTask,
        /** Currently doing the transfer pre-tasks */
        preTask,
        /** Currently transfering the file. */
        transfer,
        /** Currently doing the transfer post-tasks */
        postTask,
        /** Finished the transfer without error */
        allDone,
        /** Transfer interrupted because of an error */
        error
    }

    /** All the possible sub-steps for a transfer. */
    public enum Step {
        /** */
        running,
        /** */
        initOK,
        /** */
        preProcessingOK,
        /** */
        transferOK,
        /** */
        postProcessingOK,
        /** */
        completeOK
    }

    /** All the possible statutes for a transfer. */
    public enum Status {
        /** Transfer waiting to be processed. */
        toSubmit,
        /** Transfer status was not updated. */
        notUpdated,
        /** Transfer currently running. */
        running,
        /** Transfer currently paused. */
        interrupted,
        /** Transfer finished normally. */
        done,
        /** Transfer interrupted by an error. */
        inError,
        /** Transfer status unknown. */
        unknown
    }

    /** The transfer's unique id, automatically generated at transfer creation. */
    public final Long transferID = ++TransfersDatabase.count;

    /**
     * The transfer's current global step.
     *
     * @see GlobalStep
     */
    public GlobalStep globalStep = GlobalStep.noTask;

    /**
     * The transfer's last finished global step.
     *
     * @see GlobalStep
     */
    public GlobalStep globalLastStep = GlobalStep.noTask;

    /**
     * The transfer's current step.
     *
     * @see Step
     */
    public Step step = Step.running;

    /** The number of packets transferred thus far. */
    public Integer rank = 0;

    /**
     * The transfer's current status.
     *
     * @see Status
     */
    public Status status = Status.unknown;

    /** Additional information about the current status (error messages, etc...). */
    public String stepStatus = "unknown";

    /** The sent file's original name on the sender host before the transfer. */
    public String originalFileName;

    /** The sent file's new name on the receiver host after the transfer. */
    public String fileName;

    /** The id of the rule used for the transfer. */
    public String ruleID;

    /** Size of a block of the sent file (in Bytes) */
    public Integer blockSize;

    /** Additional metadata about the file (size in Bytes). */
    public String fileInfo;

    /** Additional user inputted information about the transfer (comments). */
    public String transferInfo = "";

    /**
     * Scheduled starting date for the transfer.
     *
     * @see Calendar
     */
    public Calendar startTrans;

    /**
     * End date of the transfer, null if the transfer has not finished yet.
     *
     * @see Calendar
     */
    public Calendar stopTrans = null;

    /** Host id of the host which originally made the transfer request. */
    //TODO: replace by loading the host id from the config file
    public String requester = "server1";

    /** Host id of the host to which the transfer was requested. */
    public String requested;


    public String getStartTrans() {
        return RestUtils.fromCalendar(this.startTrans);
    }

    public String getStopTrans() {
        return RestUtils.fromCalendar(this.stopTrans);
    }
}
