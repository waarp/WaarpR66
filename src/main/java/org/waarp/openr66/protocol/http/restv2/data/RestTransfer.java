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

import org.waarp.openr66.pojo.Transfer;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Comparator;
import java.util.List;

/** RestTransfer POJO for Rest HTTP support for R66. */
@SuppressWarnings({"unused", "WeakerAccess"})
public class RestTransfer {

    /** All the possible ways to order a list of transfer objects. */
    public enum Order {
        /** By tansferID, in ascending order. */
        ascTransferID("+id", new Comparator<RestTransfer>() {
            @Override
            public int compare(RestTransfer t1, RestTransfer t2) {
                return t1.transferID.compareTo(t2.transferID);
            }
        }),
        /** By tansferID, in descending order. */
        descTransferID("-id", new Comparator<RestTransfer>() {
            @Override
            public int compare(RestTransfer t1, RestTransfer t2) {
                return -t1.transferID.compareTo(t2.transferID);
            }
        }),
        /** By fileName, in ascending order. */
        ascFileName("+filename", new Comparator<RestTransfer>() {
            @Override
            public int compare(RestTransfer t1, RestTransfer t2) {
                return t1.originalFileName.compareTo(t2.originalFileName);
            }
        }),
        /** By fileName, in descending order. */
        descFileName("-filename", new Comparator<RestTransfer>() {
            @Override
            public int compare(RestTransfer t1, RestTransfer t2) {
                return -t1.originalFileName.compareTo(t2.originalFileName);
            }
        }),
        /** By date of transfer start, in ascending order. */
        ascStartTrans("+startDate", new Comparator<RestTransfer>() {
            @Override
            public int compare(RestTransfer t1, RestTransfer t2) {
                return t1.startTrans.compareTo(t2.startTrans);
            }
        }),
        /** By date of transfer start, in descending order. */
        descStartTrans("-startDate", new Comparator<RestTransfer>() {
            @Override
            public int compare(RestTransfer t1, RestTransfer t2) {
                return -t1.startTrans.compareTo(t2.startTrans);
            }
        }),
        /** By date of transfer end, in ascending order. */
        ascStopTrans("+stopDate", new Comparator<RestTransfer>() {
            @Override
            public int compare(RestTransfer t1, RestTransfer t2) {
                return t1.stopTrans.compareTo(t2.stopTrans);
            }
        }),
        /** By date of transfer end, in descending order. */
        descStopTrans("-stopDate", new Comparator<RestTransfer>() {
            @Override
            public int compare(RestTransfer t1, RestTransfer t2) {
                return -t1.stopTrans.compareTo(t2.stopTrans);
            }
        });

        public final Comparator<RestTransfer> comparator;
        public final String value;

        Order(String value, Comparator<RestTransfer> comparator) {
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
        /** RestTransfer interrupted because of an error */
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
        /** RestTransfer waiting to be processed. */
        toSubmit,
        /** RestTransfer status was not updated. */
        notUpdated,
        /** RestTransfer currently running. */
        running,
        /** RestTransfer currently paused. */
        interrupted,
        /** RestTransfer finished normally. */
        done,
        /** RestTransfer interrupted by an error. */
        inError,
        /** RestTransfer status unknown. */
        unknown
    }

    /** The transfer's unique id, automatically generated at transfer creation. */
    public Long transferID;

    /**
     * The transfer's current global step.
     *
     * @see GlobalStep
     */
    public GlobalStep globalStep;


    /**
     * The transfer's last finished global step.
     *
     * @see GlobalStep
     */
    public GlobalStep globalLastStep;

    /**
     * The transfer's current step.
     *
     * @see Step
     */
    public Step step;

    /** The number of packets transferred thus far. */
    public Integer rank;

    /**
     * The transfer's current status.
     *
     * @see Status
     */
    public Status status;

    /** Additional information about the current status (error messages, etc...). */
    public String stepStatus;

    /** The sent file's original name on the sender host before the transfer. */
    public String originalFileName;

    /** The sent file's new name on the receiver host after the transfer. */
    public String fileName;

    /** The id of the rule used for the transfer. */
    public String ruleID;

    /** Size of a block of the sent file (in Bytes) */
    public Integer blockSize = 4096;

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
    public Calendar stopTrans;

    /** RestHost id of the host which originally made the transfer request. */
    public String requester;

    /** RestHost id of the host to which the transfer was requested. */
    public String requested;


    public RestTransfer(Transfer trans) {
        this.transferID = trans.getId();
    }

    public static List<RestTransfer> toRestList(List<Transfer> transfers) {
        List<RestTransfer> restTransfers = new ArrayList<RestTransfer>();
        for(Transfer transfer : transfers) {
            restTransfers.add(new RestTransfer(transfer));
        }
        return restTransfers;
    }
}
