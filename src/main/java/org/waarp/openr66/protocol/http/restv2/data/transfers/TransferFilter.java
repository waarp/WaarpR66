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

import java.util.Calendar;
import java.util.List;

/**
 * All the usable filters to request multiple hosts. The response will contain all the entries that satisfy these
 * filters.
 */
public class TransferFilter {
    /** All the possible ways to order a list of transfer objects. */
    enum Order {
        /** By tansferID, in ascending order. */
        ascTransferID,
        /** By tansferID, in descending order. */
        descTransferID,
        /** By fileName, in ascending order. */
        ascFileName,
        /** By fileName, in descending order. */
        descFileName,
        /** By date of transfer start, in ascending order. */
        ascStartTrans,
        /** By date of transfer start, in descending order. */
        descStartTrans,
        /** By date of transfer end, in ascending order. */
        ascStopTrans,
        /** By date of transfer end, in descending order. */
        descStopTrans
    }

    /** The maximum number of entry allowed to be send in the response. 20 by default. */
    Integer limit = 20;

    /** The starting number from which to start counting the `limit` entries to send back. */
    Integer offset = 0;

    /**
     * The parameter and order according to which the response entries should be sorted. By default, entries are
     * sorted by their hostID in ascending order.
     *
     * @see Order
     */
    Order order = Order.ascTransferID;

    /** The response will only contain the transfers which use this transfer rule. Leave empty for all. */
    String ruleID;

    /** The response will only contain the transfers which have this host as partner. Leave empty for all. */
    String partner;

    /**
     * The response will only contain the transfers which have currently one of the following statutes.
     * Leave empty for all.
     */
    List<Transfer.Status> status;

    /** The response will only contain the transfers which concern this file. Leave empty for all. */
    String fileName;

    /** The response will only contain the transfers which started after this date. Leave empty for all. */
    Calendar startTrans;

    /** The response will only contain the transfers which started before this date. Leave empty for all. */
    Calendar stopTrans;
}
