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
import org.waarp.openr66.dao.HostDAO;
import org.waarp.openr66.dao.RuleDAO;
import org.waarp.openr66.dao.TransferDAO;
import org.waarp.openr66.dao.exception.DAOException;
import org.waarp.openr66.pojo.Host;

import javax.ws.rs.InternalServerErrorException;
import java.util.List;

import static org.waarp.openr66.protocol.configuration.Configuration.configuration;
import static org.waarp.openr66.protocol.http.restv2.RestConstants.DAO_FACTORY;
import static org.waarp.openr66.protocol.http.restv2.RestConstants.HOST_ID;
import static org.waarp.openr66.protocol.http.restv2.RestServiceInitializer.restStartTime;
import static org.waarp.openr66.protocol.http.restv2.resthandlers.RestExceptionHandler.logger;

/** A POJO representing the general status of the R66 server. */
@SuppressWarnings({"unused", "WeakerAccess"})
public class ServerStatus {

    public class Overall {
        public int allTransfer;

        public int unknown;

        public int notUpdated;

        public int interrupted;

        public int toSubmit;

        public int error;

        public int running;

        public int done;

        public int inRunning;

        public int outRunning;

        public DateTime lastInRunning;

        public DateTime lastOutRunning;

        public int inAll;

        public int outAll;

        public int inError;

        public int outError;

        public Overall() throws DAOException {
            RuleDAO ruleDAO = DAO_FACTORY.getRuleDAO();
            TransferDAO transferDAO = DAO_FACTORY.getTransferDAO();
            List<RestTransfer> transferList = RestTransfer.toRestList(
                    transferDAO.getAll());
            for (RestTransfer transfer : transferList) {
                this.allTransfer++;
                switch (transfer.status) {
                    case notUpdated:
                        this.notUpdated++;
                        break;
                    case interrupted:
                        this.interrupted++;
                        break;
                    case toSubmit:
                        this.toSubmit++;
                        break;
                    case running:
                        this.running++;
                        break;
                    case done:
                        this.done++;
                        break;
                    case inError:
                        this.error++;
                        break;
                    case unknown:
                        this.unknown++;
                        break;
                    default:
                        this.unknown++;
                        break;
                }
                RestRule restRule = new RestRule(
                        ruleDAO.select(transfer.ruleID));
                RestRule.ModeTrans modeTrans = restRule.modeTrans;
                switch (modeTrans) {
                    case send:
                    case send_md5:
                        this.outAll++;
                        if (this.lastOutRunning == null ||
                                this.lastOutRunning.isBefore(transfer.startTrans())) {
                            this.lastOutRunning = transfer.startTrans();
                        }
                        if (transfer.status == RestTransfer.Status.running) {
                            this.outRunning++;
                        } else if (transfer.status == RestTransfer.Status.inError) {
                            this.outError++;
                        }
                        break;
                    case receive:
                    case receive_md5:
                        this.inAll++;
                        if (this.lastInRunning == null ||
                                this.lastInRunning.isBefore(transfer.startTrans())) {
                            this.lastInRunning = transfer.startTrans();
                        }
                        if (transfer.status == RestTransfer.Status.running) {
                            this.inRunning++;
                        } else if (transfer.status == RestTransfer.Status.inError) {
                            this.outError++;
                        }
                }
            }
        }

        public String getLastInRunning() {
            return (this.lastInRunning == null) ? null : this.lastInRunning.toString();
        }

        public String getLastOutRunning() {
            return (this.lastOutRunning == null) ? null : this.lastOutRunning.toString();
        }
    }

    public class Steps {
        public int noTask;

        public int preTask;

        public int transfer;

        public int postTask;

        public int allDone;

        public int error;

        public Steps() throws DAOException {
            TransferDAO transferDAO = DAO_FACTORY.getTransferDAO();
            for (RestTransfer transfer : RestTransfer.toRestList(transferDAO.getAll())) {
                switch (transfer.globalStep) {
                    case noTask:
                        this.noTask++;
                        break;
                    case preTask:
                        this.preTask++;
                        break;
                    case transfer:
                        this.transfer++;
                        break;
                    case postTask:
                        this.postTask++;
                        break;
                    case allDone:
                        this.allDone++;
                        break;
                    case error:
                        this.error++;
                        break;
                }
            }
        }
    }

    public class RunningSteps {
        public int allRunning;

        public int running;

        public int initOk;

        public int preProcessingOk;

        public int transferOk;

        public int postProcessingOk;

        public int completeOk;

        public RunningSteps() throws DAOException {
            TransferDAO transferDAO = DAO_FACTORY.getTransferDAO();
            for (RestTransfer transfer : RestTransfer.toRestList(transferDAO.getAll())) {
                switch (transfer.step) {
                    case running:
                        this.running++;
                        break;
                    case initOK:
                        this.initOk++;
                        break;
                    case preProcessingOK:
                        this.preProcessingOk++;
                        break;
                    case transferOK:
                        this.transferOk++;
                        break;
                    case postProcessingOK:
                        this.postProcessingOk++;
                        break;
                    case completeOK:
                        this.completeOk++;
                        break;
                }
            }
            this.allRunning = this.running + this.initOk + this.preProcessingOk +
                    this.transferOk + this.postProcessingOk;
        }
    }

    public class ErrorTypes {
        public int connectionImpossible;

        public int serverOverloaded;

        public int badAuthent;

        public int externalOp;

        public int transferError;

        public int md5Error;

        public int disconnection;

        public int remoteShutdown;

        public int finalOp;

        public int unimplemented;

        public int shutdown;

        public int remoteError;

        public int internal;

        public int stopped;

        public int canceled;

        public int warning;

        public int unknown;

        public int queryAlreadyFinished;

        public int queryStillRunning;

        public int unknownHost;

        public int loopSelfRequestedHost;

        public int remotelyUnknown;

        public int fileNotFound;

        public int commandNotFound;

        public int passThroughMode;

        public int running;

        public int incorrectCommand;

        public int fileNotAllowed;

        public int sizeNotAllowed;

        public ErrorTypes() throws DAOException {
            TransferDAO transferDAO = DAO_FACTORY.getTransferDAO();
            for (RestTransfer transfer : RestTransfer.toRestList(transferDAO.getAll())) {
                if (!"".equals(transfer.stepStatus)) {
                    if ("C".equals(transfer.stepStatus)) {
                        this.connectionImpossible++;
                    } else if ("l".equals(transfer.stepStatus)) {
                        this.serverOverloaded++;
                    } else if ("A".equals(transfer.stepStatus)) {
                        this.badAuthent++;
                    } else if ("E".equals(transfer.stepStatus)) {
                        this.externalOp++;
                    } else if ("T".equals(transfer.stepStatus)) {
                        this.transferError++;
                    } else if ("M".equals(transfer.stepStatus)) {
                        this.md5Error++;
                    } else if ("D".equals(transfer.stepStatus)) {
                        this.disconnection++;
                    } else if ("r".equals(transfer.stepStatus)) {
                        this.remoteShutdown++;
                    } else if ("F".equals(transfer.stepStatus)) {
                        this.finalOp++;
                    } else if ("U".equals(transfer.stepStatus)) {
                        this.unimplemented++;
                    } else if ("S".equals(transfer.stepStatus)) {
                        this.shutdown++;
                    } else if ("R".equals(transfer.stepStatus)) {
                        this.remoteError++;
                    } else if ("I".equals(transfer.stepStatus)) {
                        this.internal++;
                    } else if ("H".equals(transfer.stepStatus)) {
                        this.stopped++;
                    } else if ("K".equals(transfer.stepStatus)) {
                        this.canceled++;
                    } else if ("W".equals(transfer.stepStatus)) {
                        this.warning++;
                    } else if ("-".equals(transfer.stepStatus)) {
                        this.unknown++;
                    } else if ("Q".equals(transfer.stepStatus)) {
                        this.queryAlreadyFinished++;
                    } else if ("s".equals(transfer.stepStatus)) {
                        this.queryStillRunning++;
                    } else if ("N".equals(transfer.stepStatus)) {
                        this.unknownHost++;
                    } else if ("L".equals(transfer.stepStatus)) {
                        this.loopSelfRequestedHost++;
                    } else if ("u".equals(transfer.stepStatus)) {
                        this.remotelyUnknown++;
                    } else if ("f".equals(transfer.stepStatus)) {
                        this.fileNotFound++;
                    } else if ("c".equals(transfer.stepStatus)) {
                        this.commandNotFound++;
                    } else if ("p".equals(transfer.stepStatus)) {
                        this.passThroughMode++;
                    } else if ("z".equals(transfer.stepStatus)) {
                        this.running++;
                    } else if ("n".equals(transfer.stepStatus)) {
                        this.incorrectCommand++;
                    } else if ("a".equals(transfer.stepStatus)) {
                        this.fileNotAllowed++;
                    } else if ("d".equals(transfer.stepStatus)) {
                        this.sizeNotAllowed++;
                    } else {
                        logger.error("Unknown transfer error code '" +
                                transfer.stepStatus + "' found.");
                    }
                }
            }
        }
    }

    public final String hostID = HOST_ID;

    public final boolean running;

    public final boolean useSSL = configuration.isUseSSL();

    public final boolean useNoSSL = configuration.isUseNOSSL();

    public final DateTime date = DateTime.now();

    public final DateTime fromDate = restStartTime;

    public final int secondsRunning;

    public final int networkConnections;

    public final int nbThreads;

    public final long downBandwidth;

    public final long upBandwidth;

    public final Overall overall;

    public final Steps steps;

    public final RunningSteps runningSteps;

    public final ErrorTypes errorTypes;

    public ServerStatus() {
        this.secondsRunning = (int) ((DateTime.now().getMillis() -
                restStartTime.getMillis()) / 1000);

        HostDAO hostDAO = null;
        try {
            hostDAO = DAO_FACTORY.getHostDAO();
            Host host = hostDAO.select(HOST_ID);
            
            this.running = (host != null) ? host.isActive() :
                    !configuration.isShutdown();
        } catch (DAOException e) {
            throw new InternalServerErrorException(e);
        } finally {
            if (hostDAO != null) {
                hostDAO.close();
            }
        }

        this.downBandwidth = 0;
        this.upBandwidth = 0;
        this.networkConnections = 0;
        this.nbThreads = 0;

        try {
            this.overall = new Overall();
            this.runningSteps = new RunningSteps();
            this.steps = new Steps();
            this.errorTypes = new ErrorTypes();
        } catch (DAOException e) {
            throw new InternalServerErrorException(e);
        }
    }

    public String getDate() {
        return this.date.toString();
    }

    public String getFromDate() {
        return this.fromDate.toString();
    }
}
