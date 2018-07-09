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

/**
 * All the usable filters to request multiple transfer rules. The response will contain all the entries that satisfy
 * these filters.
 */
public class RuleFilter {

    /** All the possible ways to order a list of rule objects. */
    enum Order {
        /** By ruleID, in ascending order. */
        ascRuleID,
        /** By ruleID, in descending order. */
        descRuleID,
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
    Order order = Order.ascRuleID;

    /** The response will only contain the rules whose mode of transfer is one of these ones. Leave empty for all. */
    List<Rule.ModeTrans> modeTrans;
}
