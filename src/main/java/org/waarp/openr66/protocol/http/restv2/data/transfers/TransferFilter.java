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

import org.waarp.openr66.protocol.http.restv2.data.Filter;

import java.util.Calendar;
import java.util.Comparator;

/**
 * All the usable filters to request multiple hosts. The response will contain all the entries that satisfy these
 * filters.
 */
public class TransferFilter extends Filter {

    /** All the possible ways to order a list of transfer objects. */
    public enum Order {
        /** By tansferID, in ascending order. */
        ascTransferID(new Comparator<Transfer>() {
            @Override
            public int compare(Transfer t1, Transfer t2) {
                return t1.transferID.compareTo(t2.transferID);
            }
        }),
        /** By tansferID, in descending order. */
        descTransferID(new Comparator<Transfer>() {
            @Override
            public int compare(Transfer t1, Transfer t2) {
                return -t1.transferID.compareTo(t2.transferID);
            }
        }),
        /** By fileName, in ascending order. */
        ascFileName(new Comparator<Transfer>() {
            @Override
            public int compare(Transfer t1, Transfer t2) {
                return t1.originalFileName.compareTo(t2.originalFileName);
            }
        }),
        /** By fileName, in descending order. */
        descFileName(new Comparator<Transfer>() {
            @Override
            public int compare(Transfer t1, Transfer t2) {
                return -t1.originalFileName.compareTo(t2.originalFileName);
            }
        }),
        /** By date of transfer start, in ascending order. */
        ascStartTrans(new Comparator<Transfer>() {
            @Override
            public int compare(Transfer t1, Transfer t2) {
                return t1.startTrans.compareTo(t2.startTrans);
            }
        }),
        /** By date of transfer start, in descending order. */
        descStartTrans(new Comparator<Transfer>() {
            @Override
            public int compare(Transfer t1, Transfer t2) {
                return -t1.startTrans.compareTo(t2.startTrans);
            }
        }),
        /** By date of transfer end, in ascending order. */
        ascStopTrans(new Comparator<Transfer>() {
            @Override
            public int compare(Transfer t1, Transfer t2) {
                return t1.stopTrans.compareTo(t2.stopTrans);
            }
        }),
        /** By date of transfer end, in descending order. */
        descStopTrans(new Comparator<Transfer>() {
            @Override
            public int compare(Transfer t1, Transfer t2) {
                return -t1.stopTrans.compareTo(t2.stopTrans);
            }
        });

        public final Comparator<Transfer> comparator;

        Order(Comparator<Transfer> comparator) {
            this.comparator = comparator;
        }
    }

    /**
     * The parameter and order according to which the response entries should be sorted. By default, entries are
     * sorted by their hostID in ascending order.
     *
     * @see Order
     */
    public Order order = Order.ascTransferID;

    /** The response will only contain the transfers which use this transfer rule. Leave empty for all. */
    public String ruleID;

    /** The response will only contain the transfers which have this host as partner. Leave empty for all. */
    public String partner;

    /**
     * The response will only contain the transfers which have currently one of the following statutes.
     * Leave empty for all.
     */
    public Transfer.Status[] status;

    /** The response will only contain the transfers which concern this file. Leave empty for all. */
    public String fileName;

    /** The response will only contain the transfers which started after this date. Leave empty for all. */
    public Calendar startTrans;

    /** The response will only contain the transfers which started before this date. Leave empty for all. */
    public Calendar stopTrans;
}
