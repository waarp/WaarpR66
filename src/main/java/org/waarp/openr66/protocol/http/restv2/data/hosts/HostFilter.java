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

package org.waarp.openr66.protocol.http.restv2.data.hosts;

import org.waarp.openr66.protocol.http.restv2.data.Filter;

import java.util.Comparator;

/**
 * All the usable filters to request multiple hosts. The response will contain all the entries that satisfy these
 * filters.
 */
public class HostFilter extends Filter {

    /** All the possible ways to order a list of host objects. */
    enum Order {
        /** By hostID, in ascending order. */
        ascHostID(new Comparator<Host>() {
            @Override
            public int compare(Host t1, Host t2) {
                return t1.hostID.compareTo(t2.hostID);
            }
        }),
        /** By hostID, in descending order. */
        descHostID(new Comparator<Host>() {
            @Override
            public int compare(Host t1, Host t2) {
                return -t1.hostID.compareTo(t2.hostID);
            }
        }),
        /** By address, in ascending order. */
        ascAddress(new Comparator<Host>() {
            @Override
            public int compare(Host t1, Host t2) {
                return t1.address.compareTo(t2.address);
            }
        }),
        /** By address, in descending order. */
        descAddress(new Comparator<Host>() {
            @Override
            public int compare(Host t1, Host t2) {
                return -t1.address.compareTo(t2.address);
            }
        });

        public final Comparator<Host> comparator;

        Order(Comparator<Host> comparator) {
            this.comparator = comparator;
        }
    }

    /**
     * The parameter and order according to which the response entries should be sorted. By default, entries are
     * sorted by their hostID in ascending order.
     *
     * @see Order
     */
    Order order = Order.ascHostID;

    /** The response will only contain the hosts which address is the same as this one. Leave empty for all. */
    String address;

    /**
     * If true, the response will only the hosts that use SSL. If false, returns only those that don't.
     * Leave empty to get both.
     */
    Boolean isSSL;

    /**
     * If true, the response will only the hosts that ara active. If false, returns only those that aren't.
     * Leave empty to get both.
     */
    Boolean isActive;
}
