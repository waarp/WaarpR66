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

import org.waarp.openr66.protocol.http.restv2.data.Transfer;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.List;

@Deprecated
public final class TransfersDatabase {
    private static long lastID = 0;

    public static String nextID() {
        long id = lastID;
        while(select(String.valueOf(id)) != null) {
            id++;
        }
        return String.valueOf(id);
    }

    static {
        initTransfersDb();
    }

    public static List<Transfer> transfersDb;

    private static void initTransfersDb() {
        transfersDb =  new ArrayList<Transfer>();

        Transfer transfer1 = new Transfer();
        transfer1.initValues();
        transfer1.originalFileName = "/out/test1.txt";
        transfer1.ruleID = "rule1";
        transfer1.blockSize = 4096;
        transfer1.fileInfo = "0";
        transfer1.startTrans = new GregorianCalendar(2019, 1, 1, 0, 0, 1);
        transfer1.requested = "server2";
        insert(transfer1);

        Transfer transfer2 = new Transfer();
        transfer2.initValues();
        transfer2.originalFileName = "/out/test2.txt";
        transfer2.ruleID = "rule2";
        transfer2.blockSize = 2048;
        transfer2.fileInfo = "0";
        transfer2.startTrans = new GregorianCalendar(2018, 10, 1, 0, 0, 1);
        transfer2.requested = "server3";
        insert(transfer2);

        Transfer transfer3 = new Transfer();
        transfer3.initValues();
        transfer3.originalFileName = "/out/test3.txt";
        transfer3.ruleID = "rule1";
        transfer3.blockSize = 4096;
        transfer3.fileInfo = "0";
        transfer3.startTrans = new GregorianCalendar(2018, 12, 1, 0, 0, 1);
        transfer3.requested = "server2";
        transfer3.status = Transfer.Status.done;
        insert(transfer3);

        Transfer transfer4 = new Transfer();
        transfer4.initValues();
        transfer4.originalFileName = "/out/test_no.txt";
        transfer4.ruleID = "rule2";
        transfer4.blockSize = 4096;
        transfer4.fileInfo = "0";
        transfer4.startTrans = new GregorianCalendar(2019, 6, 1, 0, 0, 1);
        transfer4.requested = "server4";
        transfer4.status = Transfer.Status.inError;
        transfer4.stepStatus = "FileNotFound";
        insert(transfer4);

        Transfer transfer5 = new Transfer();
        transfer5.initValues();
        transfer5.originalFileName = "/out/test5.txt";
        transfer5.ruleID = "rule1";
        transfer5.blockSize = 4096;
        transfer5.fileInfo = "0";
        transfer5.startTrans = new GregorianCalendar(2019, 10, 1, 0, 0, 1);
        transfer5.requested = "server4";
        transfer5.status = Transfer.Status.running;
        insert(transfer5);
    }

    public static List<Transfer> selectFilter(int limit, int offset, Transfer.Order order, String ruleID, String partner,
                        List<Transfer.Status> status, String fileName, Calendar startTrans, Calendar stopTrans) {

        List<Transfer> results = new ArrayList<Transfer>();
        for(Transfer transfer : transfersDb) {
            if((ruleID.isEmpty() || ruleID.equals(transfer.ruleID)) &&
                    (partner.isEmpty() || partner.equals(transfer.requested)) &&
                    (fileName.isEmpty() || fileName.equals(transfer.originalFileName)) &&
                    (startTrans == null || startTrans.getTimeInMillis() < transfer.startTrans.getTimeInMillis()) &&
                    (stopTrans == null || stopTrans.getTimeInMillis() < transfer.startTrans.getTimeInMillis()) &&
                    (status.isEmpty() || status.contains(transfer.status))) {
                results.add(transfer);
            }
        }

        Collections.sort(results, order.comparator);

        List<Transfer> answers = new ArrayList<Transfer>();
        for (int i = offset; (i < offset + limit && i < results.size()); i++) {
            answers.add(results.get(i));
        }

        return answers;
    }

    public static Transfer select(String id) {
        for(Transfer transfer : transfersDb) {
            if(transfer.transferID.equals(id)) {
                return transfer;
            }
        }
        return null;
    }

    public static boolean insert(Transfer trans) {
        if(select(trans.transferID) != null) {
            return false;
        } else {
            transfersDb.add(trans);
            lastID = Long.parseLong(trans.transferID);
            return true;
        }
    }

    public static boolean modify(String id, Transfer trans) {
        Transfer old = select(id);
        if(old == null) {
            return false;
        } else {
            transfersDb.remove(old);
            transfersDb.add(trans);
            return true;
        }
    }
}
