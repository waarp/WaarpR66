package org.waarp.openr66.protocol.http.restv2.data;

import org.waarp.openr66.dao.exception.DAOException;
import org.waarp.openr66.pojo.Transfer;
import org.waarp.openr66.protocol.http.restv2.RestUtils;

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.GregorianCalendar;

public class RestTransferInitializer {

    public String ruleID;

    public String fileName;

    public String requested;

    public Integer blockSize = 4096;

    public String fileInfo = "";

    private Calendar start;

    public Calendar getStart() {
        return start;
    }

    public void setStart(String date) throws IllegalArgumentException {
        this.start = RestUtils.toCalendar(date);
    }

    public Transfer toTransfer() throws DAOException {
        int mode = RestUtils.factory.getRuleDAO().select(this.ruleID).getMode();
        boolean retrieve = (mode == 2 || mode == 4);
        Transfer transfer = new Transfer(this.ruleID, mode, retrieve, this.fileName, this.fileInfo, this.blockSize);
        if(this.start != null) {
            transfer.setStart(new Timestamp(this.start.getTimeInMillis()));
        } else {
            transfer.setStart(new Timestamp(new GregorianCalendar().getTimeInMillis()));
        }
        return transfer;
    }
}
