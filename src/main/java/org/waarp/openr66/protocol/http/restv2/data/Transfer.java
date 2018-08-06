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

import org.waarp.openr66.protocol.http.restv2.RestUtils;
import org.waarp.openr66.protocol.http.restv2.testdatabases.TransfersDatabase;

import java.util.Calendar;
import java.util.Comparator;
import java.util.GregorianCalendar;

/** Transfer POJO for Rest HTTP support for R66. */
@SuppressWarnings({"unused", "WeakerAccess"})
public class Transfer {

    /** All the possible ways to order a list of transfer objects. */
    public enum Order {
        /** By tansferID, in ascending order. */
        ascTransferID("+id", new Comparator<Transfer>() {
            @Override
            public int compare(Transfer t1, Transfer t2) {
                return t1.transferID.compareTo(t2.transferID);
            }
        }),
        /** By tansferID, in descending order. */
        descTransferID("-id", new Comparator<Transfer>() {
            @Override
            public int compare(Transfer t1, Transfer t2) {
                return -t1.transferID.compareTo(t2.transferID);
            }
        }),
        /** By fileName, in ascending order. */
        ascFileName("+filename", new Comparator<Transfer>() {
            @Override
            public int compare(Transfer t1, Transfer t2) {
                return t1.originalFileName.compareTo(t2.originalFileName);
            }
        }),
        /** By fileName, in descending order. */
        descFileName("-filename", new Comparator<Transfer>() {
            @Override
            public int compare(Transfer t1, Transfer t2) {
                return -t1.originalFileName.compareTo(t2.originalFileName);
            }
        }),
        /** By date of transfer start, in ascending order. */
        ascStartTrans("+startDate", new Comparator<Transfer>() {
            @Override
            public int compare(Transfer t1, Transfer t2) {
                return t1.startTrans.compareTo(t2.startTrans);
            }
        }),
        /** By date of transfer start, in descending order. */
        descStartTrans("-startDate", new Comparator<Transfer>() {
            @Override
            public int compare(Transfer t1, Transfer t2) {
                return -t1.startTrans.compareTo(t2.startTrans);
            }
        }),
        /** By date of transfer end, in ascending order. */
        ascStopTrans("+stopDate", new Comparator<Transfer>() {
            @Override
            public int compare(Transfer t1, Transfer t2) {
                return t1.stopTrans.compareTo(t2.stopTrans);
            }
        }),
        /** By date of transfer end, in descending order. */
        descStopTrans("-stopDate", new Comparator<Transfer>() {
            @Override
            public int compare(Transfer t1, Transfer t2) {
                return -t1.stopTrans.compareTo(t2.stopTrans);
            }
        });

        public final Comparator<Transfer> comparator;
        public final String value;

        Order(String value, Comparator<Transfer> comparator) {
            this.value = value;
            this.comparator = comparator;
        }

        @Override
        public String toString() {
            return this.value;
        }

        public static Order fromString(String str) throws InstantiationException {
            if(str == null || str.isEmpty()) {
                return ascTransferID;
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
    @NonWritable
    public String transferID;

    /**
     * The transfer's current global step.
     *
     * @see GlobalStep
     */
    @NonWritable
    public GlobalStep globalStep;

    /**
     * The transfer's last finished global step.
     *
     * @see GlobalStep
     */
    @NonWritable
    public GlobalStep globalLastStep;

    /**
     * The transfer's current step.
     *
     * @see Step
     */
    @NonWritable
    public Step step;

    /** The number of packets transferred thus far. */
    @NonWritable
    public Integer rank;

    /**
     * The transfer's current status.
     *
     * @see Status
     */
    @NonWritable
    public Status status;

    /** Additional information about the current status (error messages, etc...). */
    @NonWritable
    public String stepStatus;

    /** The sent file's original name on the sender host before the transfer. */
    public String originalFileName;

    /** The sent file's new name on the receiver host after the transfer. */
    @NonWritable
    public String fileName;

    /** The id of the rule used for the transfer. */
    public String ruleID;

    /** Size of a block of the sent file (in Bytes) */
    public Integer blockSize = 4096;

    /** Additional metadata about the file (size in Bytes). */
    @NonWritable
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
    @NonWritable
    public Calendar stopTrans;

    /** Host id of the host which originally made the transfer request. */
    @NonWritable
    public String requester;

    /** Host id of the host to which the transfer was requested. */
    public String requested;


    public void setStartTrans(String date) throws Exception {
        this.startTrans = RestUtils.toCalendar(date);
    }

    public String getStartTrans() {
        return RestUtils.fromCalendar(this.startTrans);
    }

    public String getStopTrans() {
        return RestUtils.fromCalendar(this.stopTrans);
    }

    /** Initialize all Non-Writable fields with their initial values. */
    public void initValues() {
        this.transferID = TransfersDatabase.nextID();
        this.globalStep = GlobalStep.noTask;
        this.globalLastStep = GlobalStep.noTask;
        this.step = Step.running;
        this.rank = 0;
        this.status = Status.unknown;
        this.stepStatus = "unknown";
        this.stopTrans = null;
        this.requester = RestUtils.HOST_ID;
        if(this.startTrans == null) this.startTrans = new GregorianCalendar();
    }
}
