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

package org.waarp.openr66.protocol.http.restv2.converters;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.joda.time.DateTime;
import org.waarp.openr66.dao.HostDAO;
import org.waarp.openr66.dao.RuleDAO;
import org.waarp.openr66.dao.exception.DAOException;
import org.waarp.openr66.pojo.Rule;
import org.waarp.openr66.pojo.Transfer;
import org.waarp.openr66.pojo.UpdatedInfo;
import org.waarp.openr66.protocol.http.restv2.converters.RuleConverter.ModeTrans;
import org.waarp.openr66.protocol.http.restv2.errors.UserErrorException;
import org.waarp.openr66.protocol.http.restv2.errors.Error;
import org.waarp.openr66.protocol.http.restv2.errors.Errors;

import javax.ws.rs.InternalServerErrorException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.waarp.openr66.dao.database.DBTransferDAO.*;
import static org.waarp.openr66.protocol.http.restv2.RestConstants.DAO_FACTORY;
import static org.waarp.openr66.protocol.http.restv2.RestConstants.SERVER_NAME;
import static org.waarp.openr66.protocol.http.restv2.converters.RestTransferUtils.FieldNames.*;
import static org.waarp.openr66.protocol.http.restv2.errors.Errors.*;

/**
 * A collection of utility methods to convert {@link Transfer} POJOs
 * to {@link ObjectNode} and vice-versa.
 */
public final class RestTransferUtils {

    /** All the possible ways to order a list of transfer objects. */
    public enum Order {
        /** By transferId, in ascending order. */
        ascId(ID_FIELD, true),
        /** By transferId, in descending order. */
        descId(ID_FIELD, false),
        /** By fileName, in ascending order. */
        ascFile(ORIGINAL_NAME_FIELD, true),
        /** By fileName, in descending order. */
        descFile(ORIGINAL_NAME_FIELD, false),
        /** By starting date, in ascending order. */
        ascStart(TRANSFER_START_FIELD, true),
        /** By starting date, in descending order. */
        descStart(TRANSFER_START_FIELD, false),
        /** By end date, in ascending order. */
        ascStop(TRANSFER_STOP_FIELD, true),
        /** By end date, in descending order. */
        descStop(TRANSFER_STOP_FIELD, false);

        public final String column;
        public final boolean ascend;


        Order(String column, boolean ascend) {
            this.column = column;
            this.ascend = ascend;
        }
    }

    /** The names of the fields of a JSON transfer object. */
    @SuppressWarnings("unused")
    public static final class FieldNames {
        public static final String TRANSFER_ID = "id";
        public static final String GLOBAL_STEP = "globalStep";
        public static final String GLOBAL_LAST_STEP = "globalLastStep";
        public static final String STEP = "step";
        public static final String RANK = "rank";
        public static final String UPDATED_INFO = "status";
        public static final String STEP_STATUS = "stepStatus";
        public static final String ORIGINAL_FILENAME = "originalFilename";
        public static final String FILENAME = "filename";
        public static final String RULE = "ruleName";
        public static final String BLOCK_SIZE = "blockSize";
        public static final String FILE_INFO = "fileInfo";
        public static final String TRANSFER_INFO = "transferInfo";
        public static final String START = "start";
        public static final String STOP = "stop";
        public static final String REQUESTED = "requested";
        public static final String REQUESTER = "requester";
        public static final String RETRIEVE = "retrieve";
        public static final String ERROR_CODE = "errorCode";
        public static final String ERROR_MESSAGE = "errorMessage";
    }

    /**
     * Tells if the given rule exists in the database.
     *
     * @param rule The name of the rule.
     * @return {@code true} if the rule exists, {@code false} otherwise.
     */
    private static boolean ruleExists(String rule) {
        RuleDAO ruleDAO = null;
        try {
            ruleDAO = DAO_FACTORY.getRuleDAO();
            return ruleDAO.exist(rule);
        } catch (DAOException e) {
            throw new InternalServerErrorException(e);
        } finally {
            if (ruleDAO != null) {
                ruleDAO.close();
            }
        }
    }

    /**
     * Tells if the given host exists in the database.
     *
     * @param host The name of the host.
     * @return {@code true} if the host exists, {@code false} otherwise.
     */
    private static boolean hostExists(String host) {
        HostDAO hostDAO = null;
        try {
            hostDAO = DAO_FACTORY.getHostDAO();
            return hostDAO.exist(host);
        } catch (DAOException e) {
            throw new InternalServerErrorException(e);
        } finally {
            if (hostDAO != null) {
                hostDAO.close();
            }
        }
    }

    /**
     * Tells if the given host is allowed to use given rule.
     *
     * @param host The name of the host.
     * @param rule The name of the rule.
     * @return {@code true} if the host is allowed to use the rule, {@code false}
     *         otherwise
     */
    private static boolean canUseRule(String host, String rule) {
        RuleDAO ruleDAO = null;
        try {
            ruleDAO = DAO_FACTORY.getRuleDAO();
            List<String> hostIds = ruleDAO.select(rule).getHostids();
            return !hostIds.isEmpty() && !hostIds.contains(host);
        } catch (DAOException e) {
            throw new InternalServerErrorException(e);
        } finally {
            if (ruleDAO != null) {
                ruleDAO.close();
            }
        }
    }

    /**
     * Returns a list of {@link Error} corresponding to all the fields required
     * to initialize a transfer that are missing from the given {@link Transfer}
     * object. If no fields are missing, an empty list is returned.
     *
     * @param transfer The {@link Transfer} object to check.
     * @return The list of all missing fields encapsulated in {@link Error} objects.
     */
    private static List<Error> checkRequiredFields(Transfer transfer) {
        List<Error> errors = new ArrayList<Error>();
        if (transfer.getRule() == null || transfer.getRule().isEmpty()) {
            errors.add(MISSING_FIELD(RULE));
        }
        if (transfer.getOriginalName() == null || transfer.getOriginalName().isEmpty()) {
            errors.add(MISSING_FIELD(FILENAME));
        }
        if (transfer.getRequested() == null || transfer.getRequested().isEmpty()) {
            errors.add(MISSING_FIELD(REQUESTED));
        }

        return errors;
    }

    /**
     * Initialize a {@link Transfer} object with the values given in the
     * {@link ObjectNode} parameter. All optional fields missing from the
     * {@code object} parameter will be initialized with their default value.
     *
     * @param object The {@link ObjectNode} from which the transfer is extracted.
     * @return The {@link Transfer} object extracted from the parameter.
     * @throws UserErrorException Thrown if one or multiple fields are either
     *                             missing, or were given an invalid value.
     */
    public static Transfer nodeToNewTransfer(ObjectNode object)
            throws UserErrorException {
        Transfer defaultTransfer = new Transfer(null, null, -1, false, null, null, 65536);
        defaultTransfer.setRequester(SERVER_NAME);
        defaultTransfer.setOwnerRequest(SERVER_NAME);
        defaultTransfer.setBlockSize(65536);
        defaultTransfer.setTransferInfo("");
        defaultTransfer.setStart(new Timestamp(DateTime.now().getMillis()));
        Transfer transfer = parseNode(object, defaultTransfer);

        ModeTrans mode;
        RuleDAO ruleDAO = null;
        try {
            ruleDAO = DAO_FACTORY.getRuleDAO();
            Rule rule = ruleDAO.select(transfer.getRule());
            mode = ModeTrans.fromCode(rule.getMode());
        } catch (DAOException e) {
            throw new InternalServerErrorException(e);
        } finally {
            if (ruleDAO != null) {
                ruleDAO.close();
            }
        }

        transfer.setRetrieveMode(mode == ModeTrans.receive || mode == ModeTrans.receiveMD5);
        transfer.setTransferMode(mode.code);
        transfer.setFileInfo("");
        transfer.setStop(transfer.getStart());
        transfer.setUpdatedInfo(UpdatedInfo.TOSUBMIT);

        return transfer;
    }

    /**
     * Returns an {@link ObjectNode} representing the {@link Transfer} object
     * given as parameter.
     *
     * @param transfer The {@link Transfer} object to serialize.
     * @return The {@link ObjectNode} representing the given transfer.
     */
    public static ObjectNode transferToNode(Transfer transfer) {
        ObjectNode node = new ObjectNode(JsonNodeFactory.instance);
        node.put(TRANSFER_ID, transfer.getId());
        node.put(GLOBAL_STEP, transfer.getGlobalStep().toString());
        node.put(GLOBAL_LAST_STEP, transfer.getLastGlobalStep().toString());
        node.put(STEP, transfer.getStep());
        node.put(RANK, transfer.getRank());
        node.put(UPDATED_INFO, transfer.getUpdatedInfo().toString());
        node.put(STEP_STATUS, transfer.getStepStatus().toString());
        node.put(ERROR_CODE, transfer.getInfoStatus().code);
        node.put(ERROR_MESSAGE, transfer.getInfoStatus().mesg);
        node.put(ORIGINAL_FILENAME, transfer.getOriginalName());
        node.put(FILENAME, transfer.getFilename());
        node.put(RULE, transfer.getRule());
        node.put(BLOCK_SIZE, transfer.getBlockSize());
        node.put(FILE_INFO, transfer.getFileInfo());
        node.put(TRANSFER_INFO, transfer.getTransferInfo());
        node.put(START, new DateTime(transfer.getStart()).toString());
        node.put(STOP, new DateTime(transfer.getStop()).toString());
        node.put(REQUESTED, transfer.getRequested());
        node.put(REQUESTER, transfer.getRequester());
        node.put(RETRIEVE, transfer.getRetrieveMode());

        return node;
    }

    /**
     * Fills the fields of the given {@link Transfer} object with the values
     * extracted from the {@code object} parameter, and returns the result.
     *
     * @param object The {@link ObjectNode} from which the values should be extracted.
     * @param transfer The {@link Transfer} object whose fields will be filled.
     * @return The {@link Transfer} object given as parameter, updated with the
     *         values extracted from {@code object}.
     * @throws UserErrorException Thrown if one or multiple fields are either
     *                             missing, or were given an invalid value.
     */
    private static Transfer parseNode(ObjectNode object, Transfer transfer)
            throws UserErrorException {
        List<Error> errors = new ArrayList<Error>();

        Iterator<Map.Entry<String, JsonNode>> fields = object.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            String name = field.getKey();
            JsonNode value = field.getValue();

            if (name.equalsIgnoreCase(RULE)) {
                if (value.isTextual()) {
                    if (ruleExists(value.asText())) {
                        transfer.setRule(value.asText());
                    } else {
                        errors.add(Errors.UNKNOWN_RULE(value.asText()));
                    }
                } else {
                    errors.add(ILLEGAL_FIELD_VALUE(name, value.toString()));
                }
            }
            else if (name.equalsIgnoreCase(FILENAME)) {
                if (value.isTextual()) {
                    transfer.setOriginalName(value.asText());
                    transfer.setFilename(value.asText());
                } else {
                    errors.add(ILLEGAL_FIELD_VALUE(name, value.toString()));
                }
            }
            else if (name.equalsIgnoreCase(REQUESTED)) {
                if (value.isTextual()) {
                    if (hostExists(value.asText())) {
                        transfer.setRequested(value.asText());
                    } else {
                        errors.add(Errors.UNKNOWN_HOST(value.asText()));
                    }
                } else {
                    errors.add(ILLEGAL_FIELD_VALUE(name, value.toString()));
                }
            }
            else if (name.equalsIgnoreCase(BLOCK_SIZE)) {
                if (value.canConvertToInt() && value.asInt() > 0) {
                    transfer.setBlockSize(value.asInt());
                } else {
                    errors.add(ILLEGAL_FIELD_VALUE(name, value.toString()));
                }
            }
            else if (name.equalsIgnoreCase(FILE_INFO)) {
                if (value.isTextual()) {
                    transfer.setFileInfo(value.asText());
                } else {
                    errors.add(ILLEGAL_FIELD_VALUE(name, value.toString()));
                }
            }
            else if (name.equalsIgnoreCase(START)) {
                if (value.isTextual()) {
                    try {
                        DateTime start = DateTime.parse(value.asText());
                        if (start.isBeforeNow()) {
                            errors.add(ILLEGAL_FIELD_VALUE(name, value.asText()));
                        } else {
                            transfer.setStart(new Timestamp(start.getMillis()));
                        }
                    } catch (IllegalArgumentException e) {
                        errors.add(ILLEGAL_FIELD_VALUE(name, value.asText()));
                    }
                } else {
                    errors.add(ILLEGAL_FIELD_VALUE(name, value.toString()));
                }
            }
        }

        //check that both hosts are allowed to use the transfer rule
        String rule = transfer.getRule();
        String requested = transfer.getRequested();
        String requester = transfer.getRequester();
        if (rule != null && !requested.isEmpty() && canUseRule(requested, rule)) {
            errors.add(RULE_NOT_ALLOWED(requested, rule));
        }
        if (rule != null && !requester.isEmpty() && canUseRule(requester, rule)) {
            errors.add(RULE_NOT_ALLOWED(requester, rule));
        }

        errors.addAll(checkRequiredFields(transfer));

        if (errors.isEmpty()) {
            return transfer;
        } else {
            throw new UserErrorException(errors);
        }
    }


    /** Prevents the default constructor from being called. */
    private RestTransferUtils() throws InstantiationException {
        throw new InstantiationException(this.getClass().getName() +
                " cannot be instantiated.");
    }
}
