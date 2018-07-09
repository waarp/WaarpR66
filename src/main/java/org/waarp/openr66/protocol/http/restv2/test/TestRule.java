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

package org.waarp.openr66.protocol.http.restv2.test;

import org.waarp.openr66.protocol.http.restv2.data.rules.Rule;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TestRule {
    public static List<Rule> rulesDb = initRulesDb();

    private static Rule.Task copy() {
        Rule.Task copy = new Rule.Task();
        copy.type = Rule.TaskType.copy;
        copy.arguments = "";
        copy.delay = 1000;
        return copy;
    }

    private static Rule.Task move() {
        Rule.Task copy = new Rule.Task();
        copy.type = Rule.TaskType.move;
        copy.arguments = "";
        copy.delay = 1000;
        return copy;
    }

    private static Rule.Task delete() {
        Rule.Task copy = new Rule.Task();
        copy.type = Rule.TaskType.delete;
        copy.arguments = "";
        copy.delay = 1000;
        return copy;
    }

    private static List<Rule> initRulesDb() {
        List<Rule> rules = new ArrayList<Rule>();
        Rule rule1 = new Rule();
        rule1.ruleID = "rule1";
        rule1.hostsIDs = new ArrayList<String>(Collections.singleton("server1"));
        rule1.modeTrans = Rule.ModeTrans.send;
        rule1.recvPath = "/in";
        rule1.sendPath = "/out";
        rule1.archivePath = "/arch";
        rule1.workPath = "/work";
        rule1.rPreTasks = new ArrayList<Rule.Task>(Collections.singleton(copy()));
        rule1.rPostTasks = new ArrayList<Rule.Task>(Collections.singleton(delete()));
        rule1.rErrorTasks = new ArrayList<Rule.Task>(Collections.singleton(move()));
        rule1.sPreTasks = new ArrayList<Rule.Task>(Collections.singleton(delete()));
        rule1.sPostTasks = new ArrayList<Rule.Task>(Collections.singleton(move()));
        rule1.sErrorTasks = new ArrayList<Rule.Task>(Collections.singleton(copy()));

        Rule rule2 = new Rule();
        rule2.ruleID = "rule2";
        rule2.hostsIDs = new ArrayList<String>(Collections.singleton("server2"));
        rule2.modeTrans = Rule.ModeTrans.receive;
        rule2.recvPath = "/in";
        rule2.sendPath = "/out";
        rule2.archivePath = "/arch";
        rule2.workPath = "/work";
        rule2.rPreTasks = new ArrayList<Rule.Task>(Collections.singleton(copy()));
        rule2.rPostTasks = new ArrayList<Rule.Task>(Collections.singleton(delete()));
        rule2.rErrorTasks = new ArrayList<Rule.Task>(Collections.singleton(move()));
        rule2.sPreTasks = new ArrayList<Rule.Task>(Collections.singleton(delete()));
        rule2.sPostTasks = new ArrayList<Rule.Task>(Collections.singleton(move()));
        rule2.sErrorTasks = new ArrayList<Rule.Task>(Collections.singleton(copy()));

        rules.add(rule1);
        rules.add(rule2);
        return rules;
    }
}
