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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.waarp.openr66.protocol.http.restv2.database.TransfersDatabase;
import org.waarp.openr66.protocol.http.restv2.exception.OpenR66RestBadRequestException;
import org.waarp.openr66.protocol.http.restv2.exception.OpenR66RestIdNotFoundException;
import org.waarp.openr66.protocol.http.restv2.exception.OpenR66RestInternalServerException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** This class consists exclusively of static methods that operate on or return transfers. */
public final class Transfers {

    protected static long count = 0;


    /**
     * Creates a new transfer instance and add it to the database. The date of the transfer is a `Calendar`
     * object.
     *
     * @param originalFileName The path to the transferred file.
     * @param ruleID           The name of the rule used for the transfer.
     * @param blockSize        The size a a file block.
     * @param fileInfo         The file's metadata (file size, ...).
     * @param startTrans       The date at which to start the transfer in ISO-8601 format.
     * @param requested        The name of the host to which the transfer is requested.
     * @return The new transfer object generated from the parameters.
     */
    public static Transfer createTransfer(String originalFileName, String ruleID, Integer blockSize, String fileInfo,
                                          Calendar startTrans, String requested) throws OpenR66RestBadRequestException {

        if (blockSize <= 0) {
            throw new OpenR66RestBadRequestException(
                    "{" +
                            "\"userMessage\":\"Empty field\"," +
                            "\"internalMessage\":\"The block size cannot be negative or zero.\"" +
                            "}"
            );
        } else if (startTrans != null && startTrans.compareTo(new GregorianCalendar()) < 0) {
            throw new OpenR66RestBadRequestException(
                    "{" +
                            "\"userMessage\":\"Empty field\"," +
                            "\"internalMessage\":\"The starting date cannot be in the past.\"" +
                            "}"
            );
        } else if (ruleID == null || ruleID.equals("")) {
            throw new OpenR66RestBadRequestException(
                    "{" +
                            "\"userMessage\":\"Empty field\"," +
                            "\"internalMessage\":\"The rule id cannot be empty.\"" +
                            "}"
            );
        } else if (requested == null || requested.equals("")) {
            throw new OpenR66RestBadRequestException(
                    "{" +
                            "\"userMessage\":\"Empty field\"," +
                            "\"internalMessage\":\"The requested host id cannot be empty.\"" +
                            "}"
            );
        } else if (originalFileName == null || originalFileName.equals("")) {
            throw new OpenR66RestBadRequestException(
                    "{" +
                            "\"userMessage\":\"Empty field\"," +
                            "\"internalMessage\":\"The file name cannot be empty.\"" +
                            "}"
            );
        } else {
            Transfer trans = new Transfer();
            trans.originalFileName = originalFileName;
            trans.ruleID = ruleID;
            trans.blockSize = blockSize;
            trans.fileInfo = fileInfo;
            trans.startTrans = (startTrans == null) ? new GregorianCalendar() : startTrans;
            trans.requested = requested;

            TransfersDatabase.transfersDb.add(trans);
            //TODO: add the transfer to the database
            return trans;
        }
    }

    /**
     * Returns the transfer entry corresponding to the id passed as argument.
     *
     * @param strId The desired transfer id.
     * @return The corresponding transfer entry.
     * @throws OpenR66RestIdNotFoundException Thrown if the id does not exist in the database.
     */
    public static Transfer loadTransfer(String strId) throws OpenR66RestIdNotFoundException {

        Long id;
        try {
            id = Long.valueOf(strId);
            //TODO: replace by a real database request
            for (Transfer trans : TransfersDatabase.transfersDb) {
                if (trans.transferID.equals(id)) {
                    return trans;
                }
            }
        } catch (NumberFormatException ignored) {
        }
        throw new OpenR66RestIdNotFoundException();
    }

    /**
     * Returns the list of all transfers in the database that fit the filters passed as arguments.
     *
     * @param filters The different filters used to generate the desired transfer list.
     * @return A map entry associating the total number of valid entries and the list of entries that will actually be
     * returned in the response.
     */
    public static Map.Entry<Integer, List<Transfer>> filterTransfers(TransferFilter filters) {

        List<Transfer> results = new ArrayList<Transfer>();
        for (Transfer transfer : TransfersDatabase.transfersDb) {
            if ((filters.ruleID == null || transfer.ruleID.equals(filters.ruleID)) &&
                    (filters.partner == null || transfer.requested.equals(filters.partner) ||
                            transfer.requester.equals(filters.partner)) &&
                    (filters.status == null || Arrays.asList(filters.status).contains(transfer.status)) &&
                    (filters.fileName == null || transfer.originalFileName.equals(filters.fileName)) &&
                    (filters.startTrans == null || transfer.startTrans.compareTo(filters.startTrans) >= 0) &&
                    (filters.stopTrans == null || (transfer.stopTrans != null &&
                            transfer.stopTrans.compareTo(filters.stopTrans) <= 0))
                    ) {
                results.add(transfer);
            }
        }
        Integer total = results.size();
        Collections.sort(results, filters.order.comparator);

        List<Transfer> answers = new ArrayList<Transfer>();
        for (int i = filters.offset; (i < filters.offset + filters.limit && i < results.size()); i++) {
            answers.add(results.get(i));
        }

        return new HashMap.SimpleImmutableEntry<Integer, List<Transfer>>(total, answers);
    }

    /** Restarts the transfer. */
    public static void restart(Transfer trans) {
        trans.status = Transfer.Status.toSubmit;
        trans.globalLastStep = trans.globalStep;
        trans.globalStep = Transfer.GlobalStep.noTask;
        trans.step = Transfer.Step.completeOK;
    }

    /** Stops the transfer. */
    public static void stop(Transfer trans) {
        trans.status = Transfer.Status.interrupted;
    }

    /** Cancels the transfer. */
    public static void cancel(Transfer trans) {
        trans.status = Transfer.Status.inError;
        trans.globalLastStep = trans.globalStep;
        trans.globalStep = Transfer.GlobalStep.error;
        trans.step = Transfer.Step.completeOK;
        trans.stepStatus = "Cancelled";
    }

    /**
     * Transforms the transfer object into a String in JSON format.
     *
     * @return The transfer as a JSON String.
     * @throws OpenR66RestInternalServerException Thrown if the transfer cannot be transformed in JSON.
     */
    public static String toJsonString(Transfer trans) throws OpenR66RestInternalServerException {
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.writeValueAsString(trans);
        } catch (JsonProcessingException e) {
            throw OpenR66RestInternalServerException.jsonProcessing();
        }
    }
}
