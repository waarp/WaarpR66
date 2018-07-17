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
import org.waarp.openr66.protocol.http.restv2.exception.OpenR66RestBadRequestException;
import org.waarp.openr66.protocol.http.restv2.exception.OpenR66RestEmptyParamException;
import org.waarp.openr66.protocol.http.restv2.exception.OpenR66RestIdNotFoundException;
import org.waarp.openr66.protocol.http.restv2.exception.OpenR66RestInternalServerException;
import org.waarp.openr66.protocol.http.restv2.handler.HandlerUtils;
import org.waarp.openr66.protocol.http.restv2.test.TestTransfer;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
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
        } else if (startTrans.compareTo(new GregorianCalendar()) < 0) {
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
            trans.startTrans = startTrans;
            trans.requested = requested;

            TestTransfer.transfersDb.add(trans);
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
            for (Transfer trans : TestTransfer.transfersDb) {
                if (trans.transferID.equals(id)) {
                    return trans;
                }
            }
        } catch (NumberFormatException ignored) {
        }
        throw new OpenR66RestIdNotFoundException(
                "{" +
                        "\"userMessage\":\"Not Found\"," +
                        "\"internalMessage\":\"The transfer of id '" + strId + "' does not exist.\"" +
                        "}"
        );
    }

    /**
     * Extract the parameters Map outputted by the query decoder and creates a Filter object with it.
     *
     * @param params The Map associating the query parameters names with their respecting values as lists of Strings.
     * @return The Filter object representing the filter applied to the database query.
     * @throws OpenR66RestBadRequestException Thrown if one of the parameters has an invalid value or no value at all.
     */
    public static TransferFilter extractTransferFilters(Map<String, List<String>> params)
            throws OpenR66RestBadRequestException {
        TransferFilter filters = new TransferFilter();
        for (Map.Entry<String, List<String>> param : params.entrySet()) {
            String name = param.getKey();
            try {
                List<String> values = param.getValue();
                for (String value : values) {
                    if (value.isEmpty()) {
                        throw new OpenR66RestEmptyParamException();
                    }
                }
                boolean isSingleton = values.size() == 1;

                if (name.equals("limit") && isSingleton) {
                    filters.limit = Integer.valueOf(values.get(0));
                } else if (name.equals("offset") && isSingleton) {
                    filters.offset = Integer.valueOf(values.get(0));
                } else if (name.equals("order") && isSingleton) {
                    filters.order = TransferFilter.Order.valueOf(values.get(0));
                } else if (name.equals("ruleID") && isSingleton) {
                    filters.ruleID = values.get(0);
                } else if (name.equals("partner") && isSingleton) {
                    filters.partner = values.get(0);
                } else if (name.equals("status")) {
                    List<Transfer.Status> statuses = new ArrayList<Transfer.Status>();
                    for (String str : values) {
                        statuses.add(Transfer.Status.valueOf(str));
                    }
                    filters.status = statuses;
                } else if (name.equals("fileName") && isSingleton) {
                    filters.fileName = values.get(0);
                } else if (name.equals("startTrans") && isSingleton) {
                    filters.startTrans = HandlerUtils.toCalendar(values.get(0));
                } else if (name.equals("stopTrans") && isSingleton) {
                    filters.stopTrans = HandlerUtils.toCalendar(values.get(0));
                } else {
                    throw new OpenR66RestBadRequestException(
                            "{" +
                                    "\"userMessage\":\"Invalid parameter\"," +
                                    "\"internalMessage\":\"The parameter '" + name + "' is unknown or has multiple " +
                                    "values.\"" +
                                    "}"
                    );
                }
            } catch (OpenR66RestEmptyParamException e) {
                throw OpenR66RestBadRequestException.emptyParameter(name);
            } catch (NumberFormatException e) {
                throw new OpenR66RestBadRequestException(
                        "{" +
                                "\"userMessage\":\"Expected number\"," +
                                "\"internalMessage\":\"The parameter '" + name + "' is expecting a number.\"" +
                                "}"
                );
            } catch (IllegalArgumentException e) {
                throw new OpenR66RestBadRequestException(
                        "{" +
                                "\"userMessage\":\"Illegal value\"," +
                                "\"internalMessage\":\"The parameter '" + name + "' has an illegal value.\"" +
                                "}"
                );
            }
        }
        return filters;
    }

    /**
     * Returns the list of all transfers in the database that fit the filters passed as arguments.
     *
     * @param filters The different filters used to generate the desired transfer list.
     * @return A map entry associating the total number of valid entries and the list of entries that will actually be
     * returned in the response.
     * @throws OpenR66RestBadRequestException
     */
    public static Map.Entry<Integer, List<Transfer>> filterTransfers(TransferFilter filters)
            throws OpenR66RestBadRequestException {

        List<Transfer> results = new ArrayList<Transfer>();
        for (Transfer transfer : TestTransfer.transfersDb) {
            if ((filters.ruleID == null || transfer.ruleID.equals(filters.ruleID)) &&
                    (filters.partner == null || transfer.requested.equals(filters.partner) ||
                            transfer.requester.equals(filters.partner)) &&
                    (filters.status == null || filters.status.contains(transfer.status)) &&
                    (filters.fileName == null || transfer.originalFileName.equals(filters.fileName)) &&
                    (filters.startTrans == null || transfer.startTrans.compareTo(filters.startTrans) >= 0) &&
                    (filters.stopTrans == null || (transfer.stopTrans != null &&
                            transfer.stopTrans.compareTo(filters.stopTrans) <= 0))
                    ) {
                results.add(transfer);
            }
        }
        Integer total = results.size();
        switch (filters.order) {
            case ascTransferID:
                Collections.sort(results, new Comparator<Transfer>() {
                    @Override
                    public int compare(Transfer t1, Transfer t2) {
                        return t1.transferID.compareTo(t2.transferID);
                    }
                });
                break;
            case descTransferID:
                Collections.sort(results, new Comparator<Transfer>() {
                    @Override
                    public int compare(Transfer t1, Transfer t2) {
                        return -t1.transferID.compareTo(t2.transferID);
                    }
                });
                break;
            case ascFileName:
                Collections.sort(results, new Comparator<Transfer>() {
                    @Override
                    public int compare(Transfer t1, Transfer t2) {
                        return t1.originalFileName.compareTo(t2.originalFileName);
                    }
                });
                break;
            case descFileName:
                Collections.sort(results, new Comparator<Transfer>() {
                    @Override
                    public int compare(Transfer t1, Transfer t2) {
                        return -t1.originalFileName.compareTo(t2.originalFileName);
                    }
                });
                break;
            case ascStartTrans:
                Collections.sort(results, new Comparator<Transfer>() {
                    @Override
                    public int compare(Transfer t1, Transfer t2) {
                        return t1.startTrans.compareTo(t2.startTrans);
                    }
                });
                break;
            case descStartTrans:
                Collections.sort(results, new Comparator<Transfer>() {
                    @Override
                    public int compare(Transfer t1, Transfer t2) {
                        return -t1.startTrans.compareTo(t2.startTrans);
                    }
                });
                break;
            case ascStopTrans:
                Collections.sort(results, new Comparator<Transfer>() {
                    @Override
                    public int compare(Transfer t1, Transfer t2) {
                        return t1.stopTrans.compareTo(t2.stopTrans);
                    }
                });
                break;
            case descStopTrans:
                Collections.sort(results, new Comparator<Transfer>() {
                    @Override
                    public int compare(Transfer t1, Transfer t2) {
                        return -t1.stopTrans.compareTo(t2.stopTrans);
                    }
                });
                break;
        }

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
