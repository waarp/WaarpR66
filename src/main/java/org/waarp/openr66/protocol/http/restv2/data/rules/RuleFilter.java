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

import org.waarp.openr66.protocol.http.restv2.data.Filter;

import java.util.Comparator;

/**
 * All the usable filters to request multiple transfer rules. The response will contain all the entries that satisfy
 * these filters.
 */
public class RuleFilter extends Filter {

    /** All the possible ways to order a list of rule objects. */
    public enum Order {
        /** By ruleID, in ascending order. */
        ascRuleID(new Comparator<Rule>() {
            @Override
            public int compare(Rule t1, Rule t2) {
                return t1.ruleID.compareTo(t2.ruleID);
            }
        }),
        /** By ruleID, in descending order. */
        descRuleID(new Comparator<Rule>() {
            @Override
            public int compare(Rule t1, Rule t2) {
                return -t1.ruleID.compareTo(t2.ruleID);
            }
        });

        public final Comparator<Rule> comparator;

        Order(Comparator<Rule> comparator) {
            this.comparator = comparator;
        }
    }

    /**
     * The parameter and order according to which the response entries should be sorted. By default, entries are
     * sorted by their hostID in ascending order.
     *
     * @see Order
     */
    public Order order = Order.ascRuleID;

    /** The response will only contain the rules whose mode of transfer is one of these ones. Leave empty for all. */
    public Rule.ModeTrans[] modeTrans;
}
