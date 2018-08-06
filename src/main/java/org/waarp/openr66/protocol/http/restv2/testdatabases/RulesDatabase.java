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

package org.waarp.openr66.protocol.http.restv2.testdatabases;

import org.waarp.openr66.protocol.http.restv2.data.Rule;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Deprecated
public final class RulesDatabase {
    public static List<Rule> rulesDb = initRulesDb();
    
    private static Rule.Task copyTask() {
        Rule.Task copy = new Rule.Task();
        copy.type = Rule.TaskType.copy;
        copy.arguments = "path";
        copy.delay = 1000;
        return copy;
    }

    private static Rule.Task moveTask() {
        Rule.Task copy = new Rule.Task();
        copy.type = Rule.TaskType.move;
        copy.arguments = "path";
        copy.delay = 1000;
        return copy;
    }

    private static Rule.Task deleteTask() {
        Rule.Task copy = new Rule.Task();
        copy.type = Rule.TaskType.delete;
        copy.arguments = "path";
        copy.delay = 1000;
        return copy;
    }

    private static List<Rule> initRulesDb() {
        List<Rule> rules = new ArrayList<Rule>();
        Rule rule1 = new Rule();
        rule1.ruleID = "rule1";
        rule1.hostsIDs = new String[]{"server1"};
        rule1.modeTrans = Rule.ModeTrans.send;
        rule1.recvPath = "/in";
        rule1.sendPath = "/out";
        rule1.archivePath = "/arch";
        rule1.workPath = "/work";
        rule1.rPreTasks = new Rule.Task[]{copyTask()};
        rule1.rPostTasks = new Rule.Task[]{deleteTask()};
        rule1.rErrorTasks = new Rule.Task[]{moveTask()};
        rule1.sPreTasks = new Rule.Task[]{deleteTask()};
        rule1.sPostTasks = new Rule.Task[]{moveTask()};
        rule1.sErrorTasks = new Rule.Task[]{copyTask()};

        Rule rule2 = new Rule();
        rule2.ruleID = "rule2";
        rule1.hostsIDs = new String[]{"server2"};
        rule2.modeTrans = Rule.ModeTrans.receive;
        rule2.recvPath = "/in";
        rule2.sendPath = "/out";
        rule2.archivePath = "/arch";
        rule2.workPath = "/work";
        rule2.rPreTasks = new Rule.Task[]{copyTask()};
        rule2.rPostTasks = new Rule.Task[]{deleteTask()};
        rule2.rErrorTasks = new Rule.Task[]{moveTask()};
        rule2.sPreTasks = new Rule.Task[]{deleteTask()};
        rule2.sPostTasks = new Rule.Task[]{moveTask()};
        rule2.sErrorTasks = new Rule.Task[]{copyTask()};

        rules.add(rule1);
        rules.add(rule2);
        return rules;
    }

    public static List<Rule> selectFilter(int limit, int offset, Rule.Order order, List<Rule.ModeTrans> modeTrans) {
        List<Rule> results = new ArrayList<Rule>();
        for(Rule rule : rulesDb) {
            if(modeTrans == null || modeTrans.contains(rule.modeTrans)) {
                results.add(rule);
            }
        }

        Collections.sort(results, order.comparator);

        List<Rule> answers = new ArrayList<Rule>();
        for (int i = offset; (i < offset + limit && i < results.size()); i++) {
            answers.add(results.get(i));
        }

        return answers;
    }

    public static Rule select(String id) {
        for(Rule rule : rulesDb) {
            if(rule.ruleID.equals(id)) {
                return rule;
            }
        }
        return null;
    }

    public static boolean insert(Rule rule) {
        if(select(rule.ruleID) != null) {
            return false;
        } else {
            rulesDb.add(rule);
            return true;
        }
    }

    public static boolean delete(String id) {
        Rule deleted = select(id);
        if(deleted == null) {
            return false;
        } else {
            rulesDb.remove(deleted);
            return true;
        }
    }

    public static boolean modify(String id, Rule rule) {
        Rule old = select(id);
        if(old == null) {
            return false;
        } else {
            rulesDb.remove(old);
            rulesDb.add(rule);
            return true;
        }
    }
}
