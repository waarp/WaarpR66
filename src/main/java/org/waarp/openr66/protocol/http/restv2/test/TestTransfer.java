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

import org.waarp.openr66.protocol.http.restv2.data.transfers.Transfer;

import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.List;

public class TestTransfer {
    private static final int DEFAULT_BLOCK_SIZE = 4096;

    public static List<Transfer> transfersDb = initTransfersDb();

    private static List<Transfer> initTransfersDb() {
        List<Transfer> transfers = new ArrayList<Transfer>();
        Transfer transfer1 = new Transfer();
        transfer1.originalFileName = "/out/test1.txt";
        transfer1.ruleID = "rule1";
        transfer1.blockSize = DEFAULT_BLOCK_SIZE;
        transfer1.fileInfo = "0";
        transfer1.startTrans = new GregorianCalendar(2019, 1, 1, 0, 0, 1);
        transfer1.requested = "server2";

        Transfer transfer2 = new Transfer();
        transfer2.originalFileName = "/out/test2.txt";
        transfer2.ruleID = "rule2";
        transfer2.blockSize = DEFAULT_BLOCK_SIZE / 2;
        transfer2.fileInfo = "0";
        transfer2.startTrans = new GregorianCalendar(2020, 1, 1, 0, 0, 1);
        transfer2.requested = "server3";

        transfers.add(transfer1);
        transfers.add(transfer2);
        return transfers;
    }
}
