/*
 *  This file is part of Waarp Project (named also Waarp or GG).
 *
 *  Copyright 2009, Waarp SAS, and individual contributors by the @author
 *  tags. See the COPYRIGHT.txt in the distribution for a full listing of
 *  individual contributors.
 *
 *  All Waarp Project is free software: you can redistribute it and/or
 *  modify it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or (at your
 *  option) any later version.
 *
 *  Waarp is distributed in the hope that it will be useful, but WITHOUT ANY
 *  WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
 *  A PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License along with
 *  Waarp . If not, see <http://www.gnu.org/licenses/>.
 *
 */

package org.waarp.openr66.protocol.http.restv2.data;

import org.joda.time.DateTime;
import org.waarp.openr66.dao.RuleDAO;
import org.waarp.openr66.dao.exception.DAOException;
import org.waarp.openr66.pojo.Transfer;
import org.waarp.openr66.protocol.http.restv2.errors.BadRequestException;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.InternalServerErrorException;
import java.sql.Timestamp;

import static org.waarp.openr66.protocol.http.restv2.RestConstants.DAO_FACTORY;
import static org.waarp.openr66.protocol.http.restv2.RestConstants.HOST_ID;
import static org.waarp.openr66.protocol.http.restv2.errors.ErrorFactory.ILLEGAL_FIELD_VALUE;
import static org.waarp.openr66.protocol.http.restv2.errors.ErrorFactory.UNKNOWN_RULE;

/**
 * This class represents the minimum fields necessary to initiate a new transfer.
 * When making a transfer creation request, the body of the request will be
 * deserialized into an instance of this class.
 */
@SuppressWarnings({"CanBeFinal", "unused"})
public class RestTransferInitializer {

    /** The ID of the rule used for the transfer. */
    @Required
    public String ruleID;

    /** The path to the transferred file. */
    @Required
    public String filename;

    /** The name of the host/user who created the new transfer entry. */
    @NotEmpty
    public String ownerReq;

    /** The name of the host to which the transfer request will be made. */
    @Required
    public String requested;

    /** The size of a transfer block (in Bytes). Cannot be negative. */
    @Bounds(min = 1, max = Integer.MAX_VALUE)
    @DefaultValue("65536")
    public Integer blockSize;

    /** Additional information about the transferred file. */
    @DefaultValue("")
    public String transferInfo;

    /**
     * The date at which the transfer should start in ISO-8601 format. If left
     * empty, the date will be set to the current date.
     */
    @DefaultValue("")
    public String start;


    private Timestamp getStart() throws BadRequestException {
        if (this.start == null || this.start.isEmpty()) {
            return new Timestamp(System.currentTimeMillis());
        }

        DateTime start;
        try {
            start = DateTime.parse(this.start);
        } catch (IllegalArgumentException e) {
            throw new BadRequestException(ILLEGAL_FIELD_VALUE("start", this.start));
        }
        if (start.isBeforeNow()) {
            throw new BadRequestException(ILLEGAL_FIELD_VALUE("start", this.start));
        }
        return new Timestamp(start.getMillis());
    }

    /**
     * Transforms this RestTransferInitializer instance into an equivalent
     * {@link Transfer} instance.
     *
     * @return The new {@link Transfer} created from this initializer instance.
     */
    public Transfer toTransfer() {
        RuleDAO ruleDAO = null;
        try {
            ruleDAO = DAO_FACTORY.getRuleDAO();
            if (ruleDAO.exist(this.ruleID)) {
                int mode = ruleDAO.select(this.ruleID).getMode();
                boolean retrieve = (mode == 2 || mode == 4);
                Transfer transfer = new Transfer(this.ruleID, mode, retrieve,
                        this.filename, "", this.blockSize);

            transfer.setStart(this.getStart());
            transfer.setStop(transfer.getStart());
            transfer.setRequested(requested);
            transfer.setRequester(RestConstants.HOST_ID());
            transfer.setOwnerRequest(this.ownerReq);

                return transfer;
            } else {
                throw new BadRequestException(UNKNOWN_RULE(this.ruleID));
            }
        } catch (DAOException e) {
            throw new InternalServerErrorException(e);
        } finally {
            if (ruleDAO != null) {
                ruleDAO.close();
            }
        }
    }
}
