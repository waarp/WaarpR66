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

import org.joda.time.DateTime;
import org.waarp.openr66.pojo.Transfer;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.waarp.common.database.data.AbstractDbData.UpdatedInfo;

/** RestTransfer POJO for Rest HTTP support for R66. */
@SuppressWarnings({"unused", "WeakerAccess"})
@XmlType(name = "transfer")
public class RestTransfer {

    @XmlRootElement(name = "transfers")
    public static class RestTransferList {
        @XmlElement(name = "transfer")
        public List<RestTransfer> transfers;
    }

    /** All the possible ways to order a list of transfer objects. */
    public enum Order {
        /** By tansferID, in ascending order. */
        ascTransferID(new Comparator<RestTransfer>() {
            @Override
            public int compare(RestTransfer t1, RestTransfer t2) {
                return t1.transferID.compareTo(t2.transferID);
            }
        }),
        /** By tansferID, in descending order. */
        descTransferID(new Comparator<RestTransfer>() {
            @Override
            public int compare(RestTransfer t1, RestTransfer t2) {
                return -t1.transferID.compareTo(t2.transferID);
            }
        }),
        /** By fileName, in ascending order. */
        ascFileName(new Comparator<RestTransfer>() {
            @Override
            public int compare(RestTransfer t1, RestTransfer t2) {
                return t1.originalFileName.compareTo(t2.originalFileName);
            }
        }),
        /** By fileName, in descending order. */
        descFileName(new Comparator<RestTransfer>() {
            @Override
            public int compare(RestTransfer t1, RestTransfer t2) {
                return -t1.originalFileName.compareTo(t2.originalFileName);
            }
        }),
        /** By date of transfer start, in ascending order. */
        ascStartTrans(new Comparator<RestTransfer>() {
            @Override
            public int compare(RestTransfer t1, RestTransfer t2) {
                return t1.startTrans.compareTo(t2.startTrans);
            }
        }),
        /** By date of transfer start, in descending order. */
        descStartTrans(new Comparator<RestTransfer>() {
            @Override
            public int compare(RestTransfer t1, RestTransfer t2) {
                return -t1.startTrans.compareTo(t2.startTrans);
            }
        }),
        /** By date of transfer end, in ascending order. */
        ascStopTrans(new Comparator<RestTransfer>() {
            @Override
            public int compare(RestTransfer t1, RestTransfer t2) {
                return t1.stopTrans.compareTo(t2.stopTrans);
            }
        }),
        /** By date of transfer end, in descending order. */
        descStopTrans(new Comparator<RestTransfer>() {
            @Override
            public int compare(RestTransfer t1, RestTransfer t2) {
                return -t1.stopTrans.compareTo(t2.stopTrans);
            }
        });

        public final Comparator<RestTransfer> comparator;

        Order(Comparator<RestTransfer> comparator) {
            this.comparator = comparator;
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
        error;

        static GlobalStep valueOf(Transfer.TASKSTEP task) {
            switch (task) {
                case NOTASK:
                    return noTask;
                case PRETASK:
                    return preTask;
                case TRANSFERTASK:
                    return transfer;
                case POSTTASK:
                    return postTask;
                case ALLDONETASK:
                    return allDone;
                case ERRORTASK:
                    return error;
                default:
                    return null;
            }
        }
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
        completeOK,
        /** */
        stoppedTransfer,
        /** */
        canceledTransfer,
        /** */
        queryAlreadyFinished,
        /** */
        other;

        static Step valueOf(int step) {
            switch (step) {
                case 0:
                    return initOK;
                case 1:
                    return preProcessingOK;
                case 2:
                    return transferOK;
                case 3:
                    return postProcessingOK;
                case 4:
                    return completeOK;
                case 5:
                    return running;
                case 6:
                    return stoppedTransfer;
                case 7:
                    return canceledTransfer;
                case 8:
                    return queryAlreadyFinished;
                default:
                    return other;
            }
        }
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
        unknown;

        /**
         * Creates a new {@code Status} instance from the corresponding
         * {@code updatedInfo} integer value.
         *
         * @param updatedInfoNum The status' value.
         * @return  The corresponding {@code Status} enum instance.
         */
        static Status valueOf(int updatedInfoNum) {
            UpdatedInfo updatedInfo =
                    UpdatedInfo.values()[updatedInfoNum];

            switch (updatedInfo) {
                case UNKNOWN:
                    return unknown;
                case NOTUPDATED:
                    return notUpdated;
                case INTERRUPTED:
                    return interrupted;
                case TOSUBMIT:
                    return toSubmit;
                case INERROR:
                    return inError;
                case RUNNING:
                    return running;
                case DONE:
                    return done;
                default:
                    throw new IllegalArgumentException();
            }
        }

        /**
         * @return  The integer value corresponding to this instance of the
         *          {@code Status} enum.
         *
         */
        public int toUpdatedInfo() {
            switch (this) {
                case unknown:
                    return UpdatedInfo.UNKNOWN.ordinal();
                case notUpdated:
                    return UpdatedInfo.NOTUPDATED.ordinal();
                case interrupted:
                    return UpdatedInfo.INTERRUPTED.ordinal();
                case toSubmit:
                    return UpdatedInfo.TOSUBMIT.ordinal();
                case inError:
                    return UpdatedInfo.INERROR.ordinal();
                case running:
                    return UpdatedInfo.RUNNING.ordinal();
                case done:
                    return UpdatedInfo.DONE.ordinal();
                default:
                    throw new IllegalArgumentException();
            }
        }
    }

    /** The transfer's unique id, automatically generated at transfer creation. */
    @XmlElement
    public Long transferID;

    /**
     * The transfer's current global step.
     *
     * @see GlobalStep
     */
    @XmlElement
    public GlobalStep globalStep;


    /**
     * The transfer's last finished global step.
     *
     * @see GlobalStep
     */
    @XmlElement
    public GlobalStep globalLastStep;

    /**
     * The transfer's current step.
     *
     * @see Step
     */
    @XmlElement
    public Step step;

    /** The number of packets transferred thus far. */
    @XmlElement
    public Integer rank;

    /**
     * The transfer's current status.
     *
     * @see Status
     */
    @XmlElement
    public Status status;

    /** Additional information about the current status (ex: error messages...). */
    @XmlElement
    public String stepStatus;

    /** The sent file's original name on the sender host before the transfer. */
    @XmlElement
    public String originalFileName;

    /** The sent file's new name on the receiver host after the transfer. */
    @XmlElement
    public String fileName;

    /** The id of the rule used for the transfer. */
    @XmlElement
    public String ruleID;

    /** Size of a block of the sent file (in Bytes) */
    @XmlElement
    public Integer blockSize;

    /** Additional metadata about the file (size in Bytes). */
    @XmlElement
    public String fileInfo;

    /** Additional user inputted information about the transfer (comments). */
    @XmlElement
    public String transferInfo;

    /** Scheduled starting date for the transfer. */
    private DateTime startTrans;

    public DateTime startTrans() {
        return this.startTrans;
    }

    @XmlElement(name = "startTrans")
    public String getStartTrans() {
        return this.startTrans.toString();
    }

    /** End date of the transfer, null if the transfer has not finished yet. */
    private DateTime stopTrans;

    public DateTime stopTrans() {
        return this.stopTrans;
    }

    @XmlElement(name = "stopTrans")
    public String getStopTrans() {
        return this.startTrans.toString();
    }

    /** RestHost id of the host which will make the transfer request. */
    @XmlElement
    public String requester;

    /** RestHost id of the host to which the transfer was requested. */
    @XmlElement
    public String requested;

    /** RestHost id of the host who originally created the transfer request. */
    @XmlElement
    public String ownerRequest;

    /**
     * Creates a new {@code RestTransfer} from an existing {@link Transfer}
     * instance for serialization purposes.
     *
     * @param trans The model {@link Transfer} POJO.
     */
    public RestTransfer(Transfer trans) {
        this.transferID = trans.getId();
        this.globalStep = GlobalStep.valueOf(trans.getGlobalStep());
        this.globalLastStep = GlobalStep.valueOf(trans.getLastGlobalStep());
        this.step = Step.valueOf(trans.getStep());
        this.rank = trans.getRank();
        this.status = Status.valueOf(trans.getUpdatedInfo());
        this.stepStatus = trans.getStepStatus().trim();
        this.originalFileName = trans.getOriginalName();
        this.fileName = trans.getFilename();
        this.ruleID = trans.getRule();
        this.blockSize = trans.getBlockSize();
        this.fileInfo = trans.getFileInfo();
        this.transferInfo = trans.getTransferInfo();
        this.startTrans = new DateTime(trans.getStart().getTime());
        this.stopTrans = new DateTime(trans.getStop().getTime());
        this.requested = trans.getRequested();
        this.requester = trans.getRequester();
    }

    /**
     * Transforms a list of {@link Transfer} POJOs to an equivalent list of
     * {@code RestTransfer} objects.
     *
     * @param transfers The model {@link Transfer} list.
     * @return  The equivalent {@code RestTransfer} list.
     */
    public static List<RestTransfer> toRestList(List<Transfer> transfers) {
        List<RestTransfer> restTransfers = new ArrayList<RestTransfer>();
        for(Transfer transfer : transfers) {
            restTransfers.add(new RestTransfer(transfer));
        }
        return restTransfers;
    }
}
